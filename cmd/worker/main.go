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

	"voice-ingestion/internal/broker"
	"voice-ingestion/internal/consumers"
	"voice-ingestion/internal/ingestion"
	"voice-ingestion/internal/pipeline"
)

var (
	httpAddr = flag.String("http", ":8080", "HTTP server address")
	udpAddr  = flag.String("udp", ":5004", "RTP UDP server address")
	bitrate  = flag.Int("bitrate", 24000, "Opus encoding bitrate (bps)")
	redDepth = flag.Int("red-depth", 2, "RFC2198 redundancy depth (0 to disable)")
)

// Global state holding references for the HTTP handlers
var (
	globalBroker     *broker.Broker
	globalPipeline   *pipeline.Pipeline
	sttConsumer      *consumers.SpeechToTextConsumer
	recConsumer      *consumers.RecorderConsumer
	analyticsConsumer *consumers.AnalyticsConsumer
	rtpAdapter       *ingestion.RTPAdapter
	
	// Dynamic test consumers
	mu           sync.Mutex
	slowConsumer *consumers.SlowConsumer
	replayAdapter *ingestion.ReplayAdapter
)

func main() {
	flag.Parse()

	log.Println("Initializing Voice Ingestion Worker...")

	// 1. Synthesize local test wav file if missing
	ensureTestWav()

	// 2. Setup Broker
	globalBroker = broker.NewBroker()
	defer globalBroker.Close()

	// 3. Setup Media Pipeline (RED + Opus)
	var err error
	globalPipeline, err = pipeline.NewPipeline(*bitrate, *redDepth, globalBroker)
	if err != nil {
		log.Fatalf("Failed to initialize pipeline: %v", err)
	}

	// 4. Initialize Core Consumers
	sttConsumer, err = consumers.NewSpeechToTextConsumer(0.015) // RMS threshold 0.015
	if err != nil {
		log.Fatalf("Failed to create STT consumer: %v", err)
	}

	recConsumer, err = consumers.NewRecorderConsumer("./recordings")
	if err != nil {
		log.Fatalf("Failed to create Recorder consumer: %v", err)
	}

	analyticsConsumer = consumers.NewAnalyticsConsumer()

	// 5. Register Core Consumers with Broker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sttQueue := globalBroker.Register(sttConsumer.ID(), 100)
	if err := sttConsumer.Start(ctx, sttQueue); err != nil {
		log.Fatalf("Failed to start STT consumer: %v", err)
	}

	recQueue := globalBroker.Register(recConsumer.ID(), 100)
	if err := recConsumer.Start(ctx, recQueue); err != nil {
		log.Fatalf("Failed to start Recorder consumer: %v", err)
	}

	analyticsQueue := globalBroker.Register(analyticsConsumer.ID(), 100)
	if err := analyticsConsumer.Start(ctx, analyticsQueue); err != nil {
		log.Fatalf("Failed to start Analytics consumer: %v", err)
	}

	// 6. Start Media Pipeline
	globalPipeline.Start()
	defer globalPipeline.Stop()

	// 7. Setup Ingestion Adapters
	wsAdapter := ingestion.NewWebSocketAdapter(globalPipeline)
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

	// 8. Register HTTP routes
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
	http.HandleFunc("/api/consumer/slow/start", handleSlowStart)
	http.HandleFunc("/api/consumer/slow/stop", handleSlowStop)
	http.HandleFunc("/api/consumer/crash", handleCrashTrigger)

	server := &http.Server{Addr: *httpAddr}

	// 9. Run server and listen for OS signals
	go func() {
		log.Printf("HTTP control dashboard running at http://localhost%s", *httpAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down Voice Ingestion Worker gracefully...")
	
	// Stop HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)

	// Stop consumers
	sttConsumer.Stop()
	recConsumer.Stop()
	analyticsConsumer.Stop()
	
	mu.Lock()
	if slowConsumer != nil {
		slowConsumer.Stop()
	}
	if replayAdapter != nil {
		replayAdapter.Stop()
	}
	mu.Unlock()

	log.Println("Voice Ingestion Worker stopped cleanly.")
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
	duration := 15 // 15 seconds
	numSamples := sampleRate * duration
	dataSize := uint32(numSamples * 2)

	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 36+dataSize)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1) // PCM
	binary.LittleEndian.PutUint16(header[22:24], 1) // Mono
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(header[28:32], uint32(sampleRate*2))
	binary.LittleEndian.PutUint16(header[32:34], 2)
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataSize)

	_, _ = f.Write(header)

	for i := 0; i < numSamples; i++ {
		t := float64(i) / float64(sampleRate)
		
		// Synthesize complex speech harmonics: base frequency 220Hz + 440Hz + 880Hz
		val := 0.4*math.Sin(2*math.Pi*220*t) + 0.3*math.Sin(2*math.Pi*440*t) + 0.15*math.Sin(2*math.Pi*880*t)
		
		// Modulate amplitude slowly to simulate speech sentences with pauses (speech cadence)
		// Active for 2 seconds, silent for 1 second, repeating
		cycle := math.Mod(t, 3.0)
		var modulation float64
		if cycle < 2.0 {
			// Speech active, modulate with envelope
			modulation = 0.5 + 0.5*math.Sin(2*math.Pi*2*t)
		} else {
			// Silent pause
			modulation = 0.001
		}
		val = val * modulation

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
	Speech struct {
		SpeechDetected bool     `json:"speech_detected"`
		CurrentRms     float64  `json:"current_rms"`
		Transcripts    []string `json:"transcripts"`
	} `json:"speech"`
	Consumers []broker.ConsumerInfo `json:"consumers"`
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	var res StatusResponse
	
	// Pipeline stats
	samples, frames, lastIngest := globalPipeline.GetStats()
	res.Pipeline.SamplesIngested = samples
	res.Pipeline.FramesEncoded = frames
	
	// Determine active source based on last ingest timestamp (< 1 second ago)
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

	// Analytics
	res.Analytics = StatusResponse{}.Analytics // defaults
	if analyticsConsumer != nil {
		stats := analyticsConsumer.GetStats()
		res.Analytics.PacketsReceived = stats.PacketsReceived
		res.Analytics.OutofOrderCount = stats.OutofOrderCount
		res.Analytics.MissingCount = stats.MissingCount
		res.Analytics.JitterMs = stats.JitterMs
		res.Analytics.P50LatencyMs = stats.P50LatencyMs
		res.Analytics.P95LatencyMs = stats.P95LatencyMs
		res.Analytics.P99LatencyMs = stats.P99LatencyMs
	}

	// Overwrite network metrics from RTP adapter if it has received packets
	if rtpAdapter != nil {
		received, lost, recovered := rtpAdapter.GetReceiverMetrics()
		if received > 0 {
			res.Analytics.PacketsReceived = received
			res.Analytics.MissingCount = lost
			res.Analytics.RecoveredCount = recovered
		}
	}

	// STT VAD
	if sttConsumer != nil {
		detected, rms, transcripts := sttConsumer.GetState()
		res.Speech.SpeechDetected = detected
		res.Speech.CurrentRms = rms
		res.Speech.Transcripts = transcripts
	}

	// Consumers lists
	res.Consumers = globalBroker.GetConsumers()

	_ = json.NewEncoder(w).Encode(res)
}

func handleReplayStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if replayAdapter != nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	replayAdapter = ingestion.NewReplayAdapter("testdata/input.wav", globalPipeline, true)
	if err := replayAdapter.Start(context.Background()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		replayAdapter = nil
		return
	}

	w.WriteHeader(http.StatusOK)
}

func handleReplayStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if replayAdapter != nil {
		_ = replayAdapter.Stop()
		replayAdapter = nil
	}

	w.WriteHeader(http.StatusOK)
}

func handleSlowStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if slowConsumer != nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Lag = 100ms (5 times slower than frame rate of 20ms!)
	slowConsumer = consumers.NewSlowConsumer("slow-consumer", 100*time.Millisecond, 0)
	q := globalBroker.Register(slowConsumer.ID(), 10) // Small queue size of 10 to trigger drops quickly
	_ = slowConsumer.Start(context.Background(), q)

	w.WriteHeader(http.StatusOK)
}

func handleSlowStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if slowConsumer != nil {
		_ = slowConsumer.Stop()
		globalBroker.Deregister(slowConsumer.ID())
		slowConsumer = nil
	}

	w.WriteHeader(http.StatusOK)
}

func handleCrashTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	// Register a consumer that panics/crashes after 5 packets
	crashy := consumers.NewSlowConsumer("crashy-consumer", 0, 5)
	q := globalBroker.Register(crashy.ID(), 20)
	_ = crashy.Start(context.Background(), q)

	w.WriteHeader(http.StatusOK)
}
