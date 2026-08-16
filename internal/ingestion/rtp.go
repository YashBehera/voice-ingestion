package ingestion

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
	"voice-ingestion/internal/pipeline"
	"voice-ingestion/internal/rtcp"
)

// RTPAdapter binds to a UDP port and ingests real-time packetized audio (RTP).
// It supports RTP packets containing either raw PCM (L16) or encoded Opus, with RFC2198 recovery.
type RTPAdapter struct {
	mu         sync.Mutex
	address    string
	pipeline   *pipeline.Pipeline
	decoder    *pipeline.OpusDecoder
	receiver   *pipeline.RedReceiver
	rtcpEngine *rtcp.RTCPFeedbackEngine
	conn       *net.UDPConn
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	running    bool

	// Added for dynamic WAV recording
	wavFile     *os.File
	lastPktTime time.Time
	wavMutex    sync.Mutex
	wavPath     string

	// TCP/UDP Address of client for RTCP feedback loop
	clientAddr  net.Addr
	clientMutex sync.Mutex
}

// NewRTPAdapter creates a new RTP/UDP Ingestion Adapter.
func NewRTPAdapter(address string, p *pipeline.Pipeline) (*RTPAdapter, error) {
	dec, err := pipeline.NewOpusDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create opus decoder for RTP adapter: %w", err)
	}

	// RED Receiver with buffer capacity of 8 packets to support loss recovery
	receiver := pipeline.NewRedReceiver(8)
	rtcpEngine := rtcp.NewRTCPFeedbackEngine(0x12345678)

	return &RTPAdapter{
		address:    address,
		pipeline:   p,
		decoder:    dec,
		receiver:   receiver,
		rtcpEngine: rtcpEngine,
	}, nil
}

// ID returns the identifier of this ingestion source.
func (a *RTPAdapter) ID() string {
	return "rtp"
}

// Server starts/begins listening on the UDP port.
func (a *RTPAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return nil
	}

	addr, err := net.ResolveUDPAddr("udp", a.address)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address %s: %w", a.address, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP %s: %w", a.address, err)
	}
	a.conn = conn

	// Increase socket kernel read buffer to prevent OS packet drops under high traffic bursts
	if err := conn.SetReadBuffer(4 * 1024 * 1024); err != nil { // 4MB buffer
		log.Printf("Warning: failed to set UDP read buffer: %v", err)
	}

	a.ctx, a.cancel = context.WithCancel(ctx)
	a.running = true
	a.receiver.Reset()

	// Spawn multiple concurrent reader workers to scale socket ingestion throughput
	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		a.wg.Add(1)
		go a.readLoop()
	}

	log.Printf("RTP/UDP Ingestion Adapter listening on udp://%s (4 workers)", a.address)
	return nil
}

// Stop closes the UDP connection and stops the read loop.
func (a *RTPAdapter) Stop() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	if a.cancel != nil {
		a.cancel()
	}
	if a.conn != nil {
		a.conn.Close() // this will unblock the read loop immediately
	}
	a.mu.Unlock()

	a.wg.Wait()
	a.closeWavFile()
	log.Println("RTP/UDP Ingestion Adapter stopped")
	return nil
}

func (a *RTPAdapter) readLoop() {
	defer a.wg.Done()
	buf := make([]byte, 2048)

	for {
		n, srcAddr, err := a.conn.ReadFrom(buf)
		if err != nil {
			return
		}

		a.clientMutex.Lock()
		a.clientAddr = srcAddr
		a.clientMutex.Unlock()

		select {
		case <-a.ctx.Done():
			return
		default:
			a.processPacket(buf[:n])
		}
	}
}

// processPacket parses the RTP packet according to RFC 3550.
func (a *RTPAdapter) processPacket(data []byte) {
	if len(data) < 12 {
		return
	}

	// Validate version
	version := (data[0] >> 6) & 0x03
	if version != 2 {
		return
	}

	cc := data[0] & 0x0F
	x := (data[0] >> 4) & 0x01
	payloadType := data[1] & 0x7F
	seq := binary.BigEndian.Uint16(data[2:4])
	timestamp := binary.BigEndian.Uint32(data[4:8])

	headerLen := 12 + int(cc)*4
	if len(data) < headerLen {
		return
	}

	if x == 1 {
		if len(data) < headerLen+4 {
			return
		}
		extLen := int(data[headerLen+2])<<8 | int(data[headerLen+3])
		headerLen += 4 + extLen*4
	}

	if len(data) < headerLen {
		return
	}

	payload := data[headerLen:]
	if len(payload) == 0 {
		return
	}

	// Handle RTP/Opus (111) or RTP/RED (112) using our RedReceiver buffer
	if payloadType == 111 || payloadType == 112 {
		// Record arrival in RTCP Engine
		a.rtcpEngine.RecordArrival(seq, timestamp)

		// Trigger RTCP feedback report every 50 packets (~1 second of audio)
		if seq%50 == 0 {
			a.sendRTCPReport()
		}

		a.receiver.Push(pipeline.MediaPacket{
			SequenceNumber: seq,
			Timestamp:      timestamp,
			PayloadType:    payloadType,
			Payload:        payload,
		})

		// Pop all sequenced/recovered packets
		for {
			pkt, ok, skipped := a.receiver.PopNext(false)
			if !ok {
				break
			}

			var pcm []int16
			var err error

			if skipped {
				// Packet lost: trigger Packet Loss Concealment (PLC)
				pcm, err = a.decoder.DecodeLost()
			} else {
				pcm, err = a.decoder.Decode(pkt.Payload)
			}

			if err != nil {
				continue
			}

			// Push decoded PCM frames to pipeline (48kHz)
			a.pipeline.PushPCM(pcm, pipeline.SampleRate, a.ID())
			a.writePCMToWav(pcm, payloadType == 112)
		}
		return
	}

	// Handle raw PCM formats (96 @ 16kHz, 97 @ 48kHz big-endian)
	switch payloadType {
	case 96:
		if len(payload)%2 != 0 {
			return
		}
		samples := make([]int16, len(payload)/2)
		for i := 0; i < len(samples); i++ {
			samples[i] = int16(binary.BigEndian.Uint16(payload[2*i : 2*i+2]))
		}
		a.pipeline.PushPCM(samples, 16000, a.ID())

	case 97:
		if len(payload)%2 != 0 {
			return
		}
		samples := make([]int16, len(payload)/2)
		for i := 0; i < len(samples); i++ {
			samples[i] = int16(binary.BigEndian.Uint16(payload[2*i : 2*i+2]))
		}
		a.pipeline.PushPCM(samples, 48000, a.ID())
	}
}

// GetReceiverMetrics returns the receiver packet statistics (received, lost, recovered)
func (a *RTPAdapter) GetReceiverMetrics() (received, lost, recovered int64) {
	return a.receiver.GetMetrics()
}

func (a *RTPAdapter) writePCMToWav(pcm []int16, hasRED bool) {
	a.wavMutex.Lock()
	defer a.wavMutex.Unlock()

	now := time.Now()
	// If 2 seconds of silence, close old file to start a fresh recording session
	if a.wavFile != nil && now.Sub(a.lastPktTime) > 2*time.Second {
		log.Printf("[RTP Recorder Debug] Idle timeout reached (%v). Closing current file: %s", now.Sub(a.lastPktTime), a.wavPath)
		a.closeWavFileLocked()
	}

	a.lastPktTime = now

	if a.wavFile == nil {
		path := "recordings/scenario_b_with_red.wav"
		if !hasRED {
			path = "recordings/scenario_a_no_red.wav"
		}
		a.wavPath = path

		_ = os.MkdirAll("recordings", 0755)
		f, err := os.Create(path)
		if err != nil {
			log.Printf("Failed to create WAV recording %s: %v", path, err)
			return
		}
		a.wavFile = f

		// Write 44-byte WAV header template
		header := make([]byte, 44)
		copy(header[0:4], []byte("RIFF"))
		copy(header[8:12], []byte("WAVE"))
		copy(header[12:16], []byte("fmt "))
		binary.LittleEndian.PutUint32(header[16:20], 16)
		binary.LittleEndian.PutUint16(header[20:22], 1)
		binary.LittleEndian.PutUint16(header[22:24], 1)
		binary.LittleEndian.PutUint32(header[24:28], 48000)
		binary.LittleEndian.PutUint32(header[28:32], 96000)
		binary.LittleEndian.PutUint16(header[32:34], 2)
		binary.LittleEndian.PutUint16(header[34:36], 16)
		copy(header[36:40], []byte("data"))
		_, _ = f.Write(header)
		log.Printf("[RTP Recorder] Started recording to %s (hasRED=%t)", path, hasRED)
	}

	byteBuf := make([]byte, len(pcm)*2)
	for i, val := range pcm {
		binary.LittleEndian.PutUint16(byteBuf[i*2:], uint16(val))
	}
	_, err := a.wavFile.Write(byteBuf)
	if err != nil {
		log.Printf("[RTP Recorder Error] Failed to write PCM chunk: %v", err)
	}
}

func (a *RTPAdapter) closeWavFile() {
	a.wavMutex.Lock()
	defer a.wavMutex.Unlock()
	a.closeWavFileLocked()
}

func (a *RTPAdapter) closeWavFileLocked() {
	if a.wavFile == nil {
		return
	}
	fInfo, err := a.wavFile.Stat()
	sz := int64(0)
	if err == nil {
		sz = fInfo.Size()
		dataSz := sz - 44
		riffSz := sz - 8

		_, _ = a.wavFile.Seek(4, io.SeekStart)
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(riffSz))
		_, _ = a.wavFile.Write(buf)

		_, _ = a.wavFile.Seek(40, io.SeekStart)
		binary.LittleEndian.PutUint32(buf, uint32(dataSz))
		_, _ = a.wavFile.Write(buf)
	}
	_ = a.wavFile.Close()
	a.wavFile = nil
	log.Printf("[RTP Recorder] Finished recording and saved %s (final size: %d bytes)", a.wavPath, sz)
}

func (a *RTPAdapter) sendRTCPReport() {
	a.clientMutex.Lock()
	addr := a.clientAddr
	a.clientMutex.Unlock()

	if addr == nil {
		return
	}

	report := a.rtcpEngine.GenerateReport()

	// Serialize Receiver Report to 24-byte binary block
	buf := make([]byte, 24)
	binary.BigEndian.PutUint32(buf[0:4], report.SSRC)
	buf[4] = report.FractionLost
	binary.BigEndian.PutUint32(buf[5:9], report.TotalLost)
	binary.BigEndian.PutUint32(buf[9:13], report.HighestSeq)
	binary.BigEndian.PutUint32(buf[13:17], report.Jitter)
	binary.BigEndian.PutUint32(buf[17:21], uint32(report.RecommendedRED))

	_, _ = a.conn.WriteTo(buf, addr)
}
