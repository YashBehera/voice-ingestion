package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"voice-ingestion/internal/bus"
	"voice-ingestion/internal/ingestion"
	"voice-ingestion/internal/pipeline"
	"voice-ingestion/internal/router"
)

var (
	httpAddr = flag.String("http", ":8080", "HTTP server address")
	udpAddr  = flag.String("udp", ":5004", "RTP UDP server address")
	bitrate  = flag.Int("bitrate", 24000, "Opus encoding bitrate (bps)")
	redDepth = flag.Int("red-depth", 2, "RFC2198 redundancy depth (0 to disable)")
)

// Global state holding references for the HTTP handlers
var (
	globalPipeline *pipeline.Pipeline
	natsBus        *bus.NATSBus
	sessionRouter  *router.SessionRouter
	rtpAdapter     *ingestion.RTPAdapter

	mu            sync.Mutex
	replayAdapter *ingestion.ReplayAdapter
)

func main() {
	flag.Parse()

	log.Println("Initializing Enterprise Voice Ingestion Worker Cluster...")

	// 1. Synthesize local test wav file if missing
	ensureTestWav()

	// 2. Setup NATS EventBus & Session Router
	natsBus = bus.NewNATSBus()
	defer natsBus.Close()

	sessionRouter = router.NewSessionRouter(*bitrate, *redDepth, natsBus)
	defer sessionRouter.Close()

	// 3. Setup Media Pipeline (RED + Opus)
	var err error
	globalPipeline, err = pipeline.NewPipeline(*bitrate, *redDepth, nil)
	if err != nil {
		log.Fatalf("Failed to initialize pipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start Media Pipeline
	globalPipeline.Start()
	defer globalPipeline.Stop()

	// 5. Setup Ingestion Adapters
	wsAdapter := ingestion.NewWebSocketAdapter(sessionRouter)
	if err := wsAdapter.Start(ctx); err != nil {
		log.Fatalf("Failed to start WS adapter: %v", err)
	}
	defer wsAdapter.Stop()

	rtpAdapter, err = ingestion.NewRTPAdapter(*udpAddr, globalPipeline)
	if err != nil {
		log.Fatalf("Failed to create RTP adapter: %v", err)
	}
	if err := rtpAdapter.Start(ctx); err != nil {
		log.Fatalf("Failed to start RTP adapter: %v", err)
	}
	defer rtpAdapter.Stop()

	// 6. Register HTTP routes
	// Serve static files (dashboard)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "./static/index.html")
	})

	// WebSocket ingestion endpoint
	http.HandleFunc("/ws/ingest", wsAdapter.Handler())

	// Status and control endpoints
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/replay/start", handleReplayStart)
	http.HandleFunc("/api/replay/stop", handleReplayStop)

	server := &http.Server{Addr: *httpAddr}

	// 7. Run server and listen for OS signals
	go func() {
		log.Printf("HTTP control dashboard running at http://localhost%s", *httpAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down Voice Ingestion Worker Cluster gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)

	mu.Lock()
	if replayAdapter != nil {
		replayAdapter.Stop()
	}
	mu.Unlock()

	log.Println("Voice Ingestion Worker Cluster stopped cleanly.")
}

// ensureTestWav creates a sample vocal-modulated WAV file if it does not exist.
func ensureTestWav() {
	_ = os.MkdirAll("testdata", 0755)
	path := "testdata/input.wav"
	if _, err := os.Stat(path); err == nil {
		return // already exists
	}

	log.Println("Generating synthesized audio file testdata/input.wav for replay demonstrations...")

	f, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create test wav: %v", err)
		return
	}
	defer f.Close()

	sampleRate := 16000
	durationSec := 10
	totalSamples := sampleRate * durationSec

	// 44-byte WAV Header
	header := make([]byte, 44)
	copy(header[0:4], []byte("RIFF"))
	binary.LittleEndian.PutUint32(header[4:8], uint32(36+totalSamples*2))
	copy(header[8:12], []byte("WAVE"))
	copy(header[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(header[16:20], 16) // Subchunk1Size
	binary.LittleEndian.PutUint16(header[20:22], 1)  // PCM format
	binary.LittleEndian.PutUint16(header[22:24], 1)  // Mono
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(header[28:32], uint32(sampleRate*2))
	binary.LittleEndian.PutUint16(header[32:34], 2)
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], []byte("data"))
	binary.LittleEndian.PutUint32(header[40:44], uint32(totalSamples*2))

	_, _ = f.Write(header)

	for i := 0; i < totalSamples; i++ {
		t := float64(i) / float64(sampleRate)
		val := math.Sin(2*math.Pi*440*t) * 0.5
		if int(t)%2 == 0 {
			val += math.Sin(2*math.Pi*880*t) * 0.25
		}

		sample := int16(val * 16384)
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(sample))
		_, _ = f.Write(buf)
	}
	log.Println("Synthesized testdata/input.wav successfully.")
}

// HTTP API Handlers

type StatusResponse struct {
	Pipeline struct {
		SamplesIngested int64  `json:"samples_ingested"`
		FramesEncoded   int64  `json:"frames_encoded"`
		ActiveSource    string `json:"active_source"`
	} `json:"pipeline"`
	Analytics struct {
		PacketsReceived int64   `json:"packets_received"`
		OutofOrderCount int64   `json:"outof_order_count"`
		MissingCount    int64   `json:"missing_count"`
		RecoveredCount  int64   `json:"recovered_count"`
		JitterMs        float64 `json:"jitter_ms"`
		P50LatencyMs    float64 `json:"p50_latency_ms"`
		P95LatencyMs    float64 `json:"p95_latency_ms"`
		P99LatencyMs    float64 `json:"p99_latency_ms"`
	} `json:"analytics"`
	NATSBus struct {
		MessagesPublished int64 `json:"messages_published"`
		BytesPublished    int64 `json:"bytes_published"`
	} `json:"nats_bus"`
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var res StatusResponse

	// Pipeline stats
	samples, frames, lastIngest := globalPipeline.GetStats()
	res.Pipeline.SamplesIngested = samples
	res.Pipeline.FramesEncoded = frames

	if time.Since(lastIngest) < 1*time.Second {
		mu.Lock()
		if replayAdapter != nil && time.Since(lastIngest) < 1*time.Second {
			res.Pipeline.ActiveSource = "File Replay"
		} else {
			res.Pipeline.ActiveSource = "WebSocket (Browser Mic) / RTP"
		}
		mu.Unlock()
	} else {
		res.Pipeline.ActiveSource = ""
	}

	if rtpAdapter != nil {
		received, lost, recovered := rtpAdapter.GetReceiverMetrics()
		if received > 0 {
			res.Analytics.PacketsReceived = received
			res.Analytics.MissingCount = lost
			res.Analytics.RecoveredCount = recovered
		}
	}

	if natsBus != nil {
		msgs, bytes := natsBus.GetMetrics()
		res.NATSBus.MessagesPublished = msgs
		res.NATSBus.BytesPublished = bytes
	}

	_ = json.NewEncoder(w).Encode(res)
}

func handleReplayStart(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mu.Lock()
	defer mu.Unlock()

	if replayAdapter != nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": "Replay is already active",
		})
		return
	}

	path := "testdata/input.wav"
	adapter := ingestion.NewReplayAdapter(path, globalPipeline, true)

	ctx := context.Background()
	if err := adapter.Start(ctx); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	replayAdapter = adapter
	log.Println("Started audio file replay simulation via HTTP API")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "File replay simulation started",
	})
}

func handleReplayStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	mu.Lock()
	defer mu.Unlock()

	if replayAdapter == nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": "No active replay to stop",
		})
		return
	}

	_ = replayAdapter.Stop()
	replayAdapter = nil
	log.Println("Stopped audio file replay simulation via HTTP API")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "File replay simulation stopped",
	})
}
