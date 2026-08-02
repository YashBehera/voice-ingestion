package pipeline

import (
	"fmt"
	"sync"

	opus "gopkg.in/hraban/opus.v2"
)

// Target settings for the unified pipeline
const (
	SampleRate  = 48000
	Channels    = 1
	FrameMs     = 20
	FrameSamples = (SampleRate * FrameMs) / 1000 // 960 samples for 20ms at 48kHz
)

// OpusEncoder wraps the Cgo hraban/opus encoder to ensure thread safety.
type OpusEncoder struct {
	mu      sync.Mutex
	encoder *opus.Encoder
}

// NewOpusEncoder creates and configures a new Opus Encoder.
func NewOpusEncoder(bitrate int) (*OpusEncoder, error) {
	// For voice ingestion, we use AppVoip for optimal speech coding
	enc, err := opus.NewEncoder(SampleRate, Channels, opus.AppVoIP)
	if err != nil {
		return nil, fmt.Errorf("failed to create opus encoder: %w", err)
	}

	if bitrate > 0 {
		if err := enc.SetBitrate(bitrate); err != nil {
			return nil, fmt.Errorf("failed to set bitrate: %w", err)
		}
	}

	return &OpusEncoder{
		encoder: enc,
	}, nil
}

// Encode encodes raw PCM audio (must be 960 samples for 20ms) into an Opus frame.
func (e *OpusEncoder) Encode(pcm []int16) ([]byte, error) {
	if len(pcm) != FrameSamples {
		return nil, fmt.Errorf("invalid PCM frame size: got %d samples, expected %d", len(pcm), FrameSamples)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Max recommended output buffer size for a single Opus frame is 4000 bytes
	buf := make([]byte, 4000)
	n, err := e.encoder.Encode(pcm, buf)
	if err != nil {
		return nil, fmt.Errorf("opus encode error: %w", err)
	}

	return buf[:n], nil
}

// OpusDecoder wraps the Cgo hraban/opus decoder to ensure thread safety.
type OpusDecoder struct {
	mu      sync.Mutex
	decoder *opus.Decoder
}

// NewOpusDecoder creates a new Opus Decoder.
func NewOpusDecoder() (*OpusDecoder, error) {
	dec, err := opus.NewDecoder(SampleRate, Channels)
	if err != nil {
		return nil, fmt.Errorf("failed to create opus decoder: %w", err)
	}
	return &OpusDecoder{
		decoder: dec,
	}, nil
}

// Decode decodes an Opus payload into PCM samples.
func (d *OpusDecoder) Decode(data []byte) ([]int16, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pcm := make([]int16, FrameSamples)
	n, err := d.decoder.Decode(data, pcm)
	if err != nil {
		return nil, fmt.Errorf("opus decode error: %w", err)
	}

	return pcm[:n], nil
}

// DecodeLost signals to the decoder that a packet was lost, allowing it to perform PLC (Packet Loss Concealment).
func (d *OpusDecoder) DecodeLost() ([]int16, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	pcm := make([]int16, FrameSamples)
	// Passing a nil or empty slice signals a lost packet to perform PLC
	n, err := d.decoder.Decode(nil, pcm)
	if err != nil {
		return nil, fmt.Errorf("opus PLC decode error: %w", err)
	}

	return pcm[:n], nil
}
