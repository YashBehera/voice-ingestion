package pipeline

import (
	"context"
	"sync"
	"time"
)

// AudioChunk represents a chunk of PCM audio from an ingestion source.
type AudioChunk struct {
	PCM        []int16
	SampleRate int
	SourceID   string
}

// PacketPublisher defines the interface for publishing packetized media.
// This allows the pipeline to push RED/Opus packets without importing the broker (SOLID/Dependency Inversion).
type PacketPublisher interface {
	Publish(packet MediaPacket)
}

// Pipeline coordinates audio ingestion, resampling, encoding, and packetization.
type Pipeline struct {
	mu           sync.Mutex
	encoder      *OpusEncoder
	packer       *RedPacker
	publisher    PacketPublisher
	pcmBuf       []int16
	inputChan    chan AudioChunk
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      bool

	// Observability
	SamplesIngested  int64
	FramesEncoded    int64
	LastIngestTime   time.Time
}

// NewPipeline creates and configures a new unified media pipeline.
func NewPipeline(bitrate int, redDepth int, publisher PacketPublisher) (*Pipeline, error) {
	enc, err := NewOpusEncoder(bitrate)
	if err != nil {
		return nil, err
	}

	packer := NewRedPacker(redDepth)

	return &Pipeline{
		encoder:   enc,
		packer:    packer,
		publisher: publisher,
		pcmBuf:    make([]int16, 0, SampleRate), // preallocate 1 second of buffer
		inputChan: make(chan AudioChunk, 100),   // bounded channel for ingestion safety
	}, nil
}

// PushPCM pushes an audio chunk to the pipeline's processing queue.
func (p *Pipeline) PushPCM(pcm []int16, sampleRate int, sourceID string) {
	if len(pcm) == 0 {
		return
	}
	// Non-blocking send with drop policy if queue is full (backpressure safety)
	select {
	case p.inputChan <- AudioChunk{PCM: pcm, SampleRate: sampleRate, SourceID: sourceID}:
	default:
		// Logging/metrics for queue overflow would go here
		// We drop incoming frame if pipeline queue is overwhelmed to protect memory
	}
}

// Start starts the pipeline processing loop.
func (p *Pipeline) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.running = true
	p.packer.Reset()

	p.wg.Add(1)
	go p.run()
}

// Stop stops the pipeline processing loop cleanly.
func (p *Pipeline) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.cancel()
	p.mu.Unlock()

	p.wg.Wait()
}

func (p *Pipeline) run() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case chunk, ok := <-p.inputChan:
			if !ok {
				return
			}
			p.processChunk(chunk)
		}
	}
}

func (p *Pipeline) processChunk(chunk AudioChunk) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.LastIngestTime = time.Now()
	p.SamplesIngested += int64(len(chunk.PCM))

	// 1. Resample to 48kHz mono PCM if necessary
	resampled := ResampleMonoPCM(chunk.PCM, chunk.SampleRate, SampleRate)

	// 2. Append to our PCM accumulator buffer
	p.pcmBuf = append(p.pcmBuf, resampled...)

	// 3. Process all complete 20ms frames in the buffer
	for len(p.pcmBuf) >= FrameSamples {
		frame := p.pcmBuf[:FrameSamples]

		// Encode to Opus
		opusData, err := p.encoder.Encode(frame)
		if err != nil {
			// Log error and continue
			p.pcmBuf = p.pcmBuf[FrameSamples:]
			continue
		}

		// Packetize using RED
		packet := p.packer.Pack(opusData)

		// Publish to the consumers via the publisher interface
		if p.publisher != nil {
			p.publisher.Publish(packet)
		}

		p.FramesEncoded++

		// Advance the buffer
		p.pcmBuf = p.pcmBuf[FrameSamples:]
	}
}

// GetStats returns pipeline health metrics.
func (p *Pipeline) GetStats() (samplesIngested, framesEncoded int64, lastIngest time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.SamplesIngested, p.FramesEncoded, p.LastIngestTime
}
