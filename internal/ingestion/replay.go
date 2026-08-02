package ingestion

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"voice-ingestion/internal/pipeline"
)

// ReplayAdapter reads a local WAV file and streams it into the pipeline,
// simulating real-time playback.
type ReplayAdapter struct {
	mu         sync.Mutex
	filePath   string
	pipeline   *pipeline.Pipeline
	loop       bool
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	running    bool
}

// NewReplayAdapter creates a new File Replay Ingestion Adapter.
func NewReplayAdapter(filePath string, p *pipeline.Pipeline, loop bool) *ReplayAdapter {
	return &ReplayAdapter{
		filePath: filePath,
		pipeline: p,
		loop:     loop,
	}
}

// ID returns the identifier of this ingestion source.
func (a *ReplayAdapter) ID() string {
	return "replay"
}

// Start begins replaying the file in a background goroutine.
func (a *ReplayAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return nil
	}

	// Verify file exists
	if _, err := os.Stat(a.filePath); err != nil {
		return fmt.Errorf("wav file not found: %s: %w", a.filePath, err)
	}

	a.ctx, a.cancel = context.WithCancel(ctx)
	a.running = true

	a.wg.Add(1)
	go a.replayLoop()

	log.Printf("File Replay Ingestion Adapter started for %s (loop=%t)", a.filePath, a.loop)
	return nil
}

// Stop halts the replay loop.
func (a *ReplayAdapter) Stop() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()

	a.wg.Wait()
	log.Println("File Replay Ingestion Adapter stopped")
	return nil
}

func (a *ReplayAdapter) replayLoop() {
	defer a.wg.Done()

	for {
		err := a.playWavFile()
		if err != nil {
			log.Printf("Replay error playing %s: %v", a.filePath, err)
			time.Sleep(1 * time.Second) // pause before retry or exit
		}

		a.mu.Lock()
		if !a.running || !a.loop {
			a.running = false
			a.mu.Unlock()
			return
		}
		a.mu.Unlock()

		select {
		case <-a.ctx.Done():
			return
		default:
		}
	}
}

// playWavFile opens, parses, and plays the WAV file chunk by chunk.
func (a *ReplayAdapter) playWavFile() error {
	file, err := os.Open(a.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	sampleRate, channels, bitsPerSample, dataOffset, dataSize, err := parseWavHeader(file)
	if err != nil {
		return fmt.Errorf("invalid wav header: %w", err)
	}

	if bitsPerSample != 16 {
		return fmt.Errorf("unsupported bits per sample: %d (only 16-bit WAV is supported)", bitsPerSample)
	}

	// Seek to data section
	if _, err := file.Seek(dataOffset, io.SeekStart); err != nil {
		return err
	}

	// Stream settings: we send 20ms chunks
	chunkDuration := 20 * time.Millisecond
	samplesPerChunk := (sampleRate * 20) / 1000
	bytesPerSample := 2
	bytesPerChunk := samplesPerChunk * channels * bytesPerSample

	buf := make([]byte, bytesPerChunk)
	ticker := time.NewTicker(chunkDuration)
	defer ticker.Stop()

	bytesReadTotal := int64(0)

	for {
		select {
		case <-a.ctx.Done():
			return nil
		case <-ticker.C:
			a.mu.Lock()
			running := a.running
			a.mu.Unlock()
			if !running {
				return nil
			}

			// Read one chunk
			n, err := io.ReadFull(file, buf)
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil // finished playing
				}
				return err
			}

			bytesReadTotal += int64(n)
			if dataSize > 0 && bytesReadTotal > dataSize {
				return nil // reached end of data chunk
			}

			// Convert read bytes to samples
			samplesReadCount := n / (channels * bytesPerSample)
			if samplesReadCount == 0 {
				continue
			}

			// Process and mix down to mono if needed
			monoPCM := make([]int16, samplesReadCount)
			for i := 0; i < samplesReadCount; i++ {
				offset := i * channels * bytesPerSample
				if channels == 1 {
					// Mono
					monoPCM[i] = int16(binary.LittleEndian.Uint16(buf[offset : offset+2]))
				} else {
					// Stereo: average the two channels
					left := int16(binary.LittleEndian.Uint16(buf[offset : offset+2]))
					right := int16(binary.LittleEndian.Uint16(buf[offset+2 : offset+4]))
					monoPCM[i] = int16((int32(left) + int32(right)) / 2)
				}
			}

			// Push raw PCM to pipeline
			a.pipeline.PushPCM(monoPCM, sampleRate, a.ID())
		}
	}
}

// parseWavHeader parses the standard WAV RIFF header, returning sampleRate, channels, bitsPerSample, dataOffset, and dataSize.
func parseWavHeader(r io.Reader) (sampleRate, channels, bitsPerSample int, dataOffset, dataSize int64, err error) {
	header := make([]byte, 44)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return 0, 0, 0, 0, 0, errors.New("not a valid RIFF WAVE file")
	}

	// We parse chunks properly to find 'fmt ' and 'data' blocks, as some WAV files have metadata chunks (like LIST) before data.
	// But as a standard, we can search for the "fmt " chunk and "data" chunk sequentially.
	// To be robust, let's write a small scanner.
	// Seek to byte 12 (start of chunks)
	channels = int(binary.LittleEndian.Uint16(header[22:24]))
	sampleRate = int(binary.LittleEndian.Uint32(header[24:28]))
	bitsPerSample = int(binary.LittleEndian.Uint16(header[34:36]))

	// Find the 'data' chunk header
	// In standard WAV, data chunk starts at 36 ("data" marker, 40: data size)
	// But let's verify if there is any offset.
	dataOffset = 44
	dataSize = int64(binary.LittleEndian.Uint32(header[40:44]))

	if string(header[36:40]) != "data" {
		// If it's not "data", it means there might be other chunks (like JUNK, INFO).
		// We fallback to scan the file.
		// (For standard simple WAVs, 44-byte header is standard).
		// Let's do a quick scan if needed. Let's assume standard format first.
		if seeker, ok := r.(io.Seeker); ok {
			// Scan chunk headers
			if _, err := seeker.Seek(12, io.SeekStart); err != nil {
				return sampleRate, channels, bitsPerSample, dataOffset, dataSize, nil
			}

			buf := make([]byte, 8)
			offset := int64(12)
			for {
				n, err := r.Read(buf)
				if err != nil {
					break
				}
				if n < 8 {
					break
				}
				chunkID := string(buf[0:4])
				chunkSize := binary.LittleEndian.Uint32(buf[4:8])

				if chunkID == "fmt " {
					// We read fmt chunk details
					fmtBuf := make([]byte, 16)
					if _, err := io.ReadFull(r, fmtBuf); err == nil {
						channels = int(binary.LittleEndian.Uint16(fmtBuf[2:4]))
						sampleRate = int(binary.LittleEndian.Uint32(fmtBuf[4:8]))
						bitsPerSample = int(binary.LittleEndian.Uint16(fmtBuf[14:16]))
					}
					// seek back
					offset += 8 + int64(chunkSize)
					if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
						break
					}
				} else if chunkID == "data" {
					dataOffset = offset + 8
					dataSize = int64(chunkSize)
					break
				} else {
					// skip chunk
					offset += 8 + int64(chunkSize)
					if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
						break
					}
				}
			}
		}
	}

	return sampleRate, channels, bitsPerSample, dataOffset, dataSize, nil
}
