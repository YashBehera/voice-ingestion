package ingestion

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"voice-ingestion/internal/pipeline"
)

// RTPAdapter binds to a UDP port and ingests real-time packetized audio (RTP).
// It supports RTP packets containing either raw PCM (L16) or encoded Opus, with RFC2198 recovery.
type RTPAdapter struct {
	mu         sync.Mutex
	address    string
	pipeline   *pipeline.Pipeline
	decoder    *pipeline.OpusDecoder
	receiver   *pipeline.RedReceiver
	conn       *net.UDPConn
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	running    bool
}

// NewRTPAdapter creates a new RTP/UDP Ingestion Adapter.
func NewRTPAdapter(address string, p *pipeline.Pipeline) (*RTPAdapter, error) {
	dec, err := pipeline.NewOpusDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create opus decoder for RTP adapter: %w", err)
	}

	// RED Receiver with buffer capacity of 8 packets to support loss recovery
	receiver := pipeline.NewRedReceiver(8)

	return &RTPAdapter{
		address:  address,
		pipeline: p,
		decoder:  dec,
		receiver: receiver,
	}, nil
}

// ID returns the identifier of this ingestion source.
func (a *RTPAdapter) ID() string {
	return "rtp"
}

// Start begins listening on the UDP port.
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

	a.ctx, a.cancel = context.WithCancel(ctx)
	a.running = true
	a.receiver.Reset()

	a.wg.Add(1)
	go a.readLoop()

	log.Printf("RTP/UDP Ingestion Adapter listening on udp://%s", a.address)
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
	log.Println("RTP/UDP Ingestion Adapter stopped")
	return nil
}

func (a *RTPAdapter) readLoop() {
	defer a.wg.Done()
	buf := make([]byte, 2048)

	for {
		n, _, err := a.conn.ReadFrom(buf)
		if err != nil {
			return
		}

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
