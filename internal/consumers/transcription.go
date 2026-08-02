package consumers

import (
	"context"
	"log"
	"math"
	"sync"
	"time"
	"voice-ingestion/internal/broker"
	"voice-ingestion/internal/pipeline"
)

// SpeechToTextConsumer implements a VAD-based local transcriber.
// It decodes Opus audio, runs a Root Mean Square (RMS) Voice Activity Detector,
// and outputs live voice activity states and simulated transcription.
type SpeechToTextConsumer struct {
	mu             sync.Mutex
	decoder        *pipeline.OpusDecoder
	receiver       *pipeline.RedReceiver
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	running        bool
	speechActive   bool
	speechStart    time.Time
	lastSpeechTime time.Time
	rmsThreshold   float64 // VAD threshold (normalized RMS 0.0 to 1.0)
	
	// Outputs for the web dashboard / API
	transcripts    []string
	speechDetected bool
	currentRms     float64
}

// NewSpeechToTextConsumer creates a new STT/VAD consumer.
func NewSpeechToTextConsumer(rmsThreshold float64) (*SpeechToTextConsumer, error) {
	dec, err := pipeline.NewOpusDecoder()
	if err != nil {
		return nil, err
	}

	// RED Receiver with a buffer capacity of 8 packets
	receiver := pipeline.NewRedReceiver(8)

	return &SpeechToTextConsumer{
		decoder:      dec,
		receiver:     receiver,
		rmsThreshold: rmsThreshold,
		transcripts:  make([]string, 0),
	}, nil
}

// ID returns the unique identifier.
func (c *SpeechToTextConsumer) ID() string {
	return "speech-to-text"
}

// Start spawns the consumption loop.
func (c *SpeechToTextConsumer) Start(ctx context.Context, queue *broker.BoundedQueue) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running = true
	c.receiver.Reset()

	c.wg.Add(1)
	go c.run(queue)

	log.Println("Speech-To-Text Consumer started")
	return nil
}

// Stop stops the consumer cleanly.
func (c *SpeechToTextConsumer) Stop() error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()

	c.wg.Wait()
	log.Println("Speech-To-Text Consumer stopped")
	return nil
}

func (c *SpeechToTextConsumer) run(queue *broker.BoundedQueue) {
	defer c.wg.Done()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	// List of simulated transcript sentences to output when speech is active
	mockSentences := []string{
		"Hello, checking voice ingestion system.",
		"Real-time audio normalization is functioning.",
		"Opus codec compression verified at 48kHz.",
		"RFC2198 Redundancy recovering network packet loss.",
		"Downstream consumers are fully isolated.",
		"Low-latency audio streaming demo is active.",
	}
	sentenceIdx := 0

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Pop from queue (blocking)
			pkt, ok := queue.Pop()
			if !ok {
				return // Queue closed
			}

			// Push to RED receiver for reordering/recovery
			c.receiver.Push(pkt)

			// Drain RED receiver of all sequenced, recovered packets
			for {
				recoveredPkt, ok, skipped := c.receiver.PopNext(false)
				if !ok {
					break // no complete packets in sequence yet
				}

				var pcm []int16
				var err error

				if skipped {
					// Packet was lost and not recoverable, invoke PLC (Packet Loss Concealment)
					pcm, err = c.decoder.DecodeLost()
				} else {
					// Normal decode
					pcm, err = c.decoder.Decode(recoveredPkt.Payload)
				}

				if err != nil {
					continue
				}

				// Run VAD on decoded PCM
				c.processVAD(pcm, &sentenceIdx, mockSentences)
			}
		}
	}
}

func (c *SpeechToTextConsumer) processVAD(pcm []int16, sentenceIdx *int, mockSentences []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(pcm) == 0 {
		return
	}

	// Calculate RMS (Root Mean Square) energy normalized to 0.0 - 1.0
	var sumSq float64
	for _, s := range pcm {
		val := float64(s) / 32768.0
		sumSq += val * val
	}
	rms := math.Sqrt(sumSq / float64(len(pcm)))
	c.currentRms = rms

	isSpeech := rms > c.rmsThreshold

	if isSpeech {
		c.lastSpeechTime = time.Now()
		if !c.speechActive {
			c.speechActive = true
			c.speechDetected = true
			c.speechStart = time.Now()
			log.Printf("[VAD] Voice detected (RMS=%f)", rms)
		} else {
			// If speech has been active for more than 1.5 seconds, emit a transcribed sentence
			if time.Since(c.speechStart) > 1500*time.Millisecond {
				sentence := mockSentences[*sentenceIdx%len(mockSentences)]
				*sentenceIdx++
				c.transcripts = append(c.transcripts, sentence)
				if len(c.transcripts) > 10 {
					c.transcripts = c.transcripts[1:] // limit history
				}
				log.Printf("[VAD Transcript] %s", sentence)
				c.speechStart = time.Now() // reset timer for next sentence
			}
		}
	} else {
		// Hangover time: wait 500ms of silence before declaring speech stopped
		if c.speechActive && time.Since(c.lastSpeechTime) > 500*time.Millisecond {
			c.speechActive = false
			c.speechDetected = false
			log.Println("[VAD] Silence detected")
		}
	}
}

// GetState returns current VAD status and transcripts for API/UI.
func (c *SpeechToTextConsumer) GetState() (bool, float64, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Create a copy of transcripts to avoid race conditions
	tCopy := make([]string, len(c.transcripts))
	copy(tCopy, c.transcripts)
	
	return c.speechDetected, c.currentRms, tCopy
}
