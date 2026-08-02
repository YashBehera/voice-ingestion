package consumers

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"voice-ingestion/internal/broker"
	"voice-ingestion/internal/pipeline"
)

// RecorderConsumer decodes media packets and records them to a WAV file.
type RecorderConsumer struct {
	mu           sync.Mutex
	outputDir    string
	fileName     string
	file         *os.File
	decoder      *pipeline.OpusDecoder
	receiver     *pipeline.RedReceiver
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      bool
	bytesWritten uint32
}

// NewRecorderConsumer creates a new WAV recording consumer.
func NewRecorderConsumer(outputDir string) (*RecorderConsumer, error) {
	dec, err := pipeline.NewOpusDecoder()
	if err != nil {
		return nil, err
	}

	receiver := pipeline.NewRedReceiver(8)

	return &RecorderConsumer{
		outputDir: outputDir,
		decoder:   dec,
		receiver:  receiver,
	}, nil
}

// ID returns the identifier of this consumer.
func (c *RecorderConsumer) ID() string {
	return "recorder"
}

// Start creates the output directory and file, writes the dummy WAV header,
// and starts the packet consume-and-write loop.
func (c *RecorderConsumer) Start(ctx context.Context, queue *broker.BoundedQueue) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	// Create output dir if not exists
	if err := os.MkdirAll(c.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create recordings directory: %w", err)
	}

	c.fileName = fmt.Sprintf("ingest_recording_%d.wav", time.Now().Unix())
	fullPath := filepath.Join(c.outputDir, c.fileName)

	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create wav file %s: %w", fullPath, err)
	}
	c.file = f
	c.bytesWritten = 0

	// Write dummy WAV header first
	if err := c.writeWavHeader(0); err != nil {
		f.Close()
		return fmt.Errorf("failed to write wav header: %w", err)
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running = true
	c.receiver.Reset()

	c.wg.Add(1)
	go c.run(queue)

	log.Printf("Recorder Consumer started, writing to %s", fullPath)
	return nil
}

// Stop finalizes the WAV header, closes the file, and stops the consumer.
func (c *RecorderConsumer) Stop() error {
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

	// Update WAV header with actual data size
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file != nil {
		if err := c.writeWavHeader(c.bytesWritten); err != nil {
			log.Printf("Failed to finalize wav header: %v", err)
		}
		c.file.Close()
		log.Printf("Recorder Consumer stopped, finalized %s (%d bytes)", c.fileName, c.bytesWritten)
		c.file = nil
	}

	return nil
}

func (c *RecorderConsumer) run(queue *broker.BoundedQueue) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			pkt, ok := queue.Pop()
			if !ok {
				return // Queue closed
			}

			c.receiver.Push(pkt)

			for {
				recoveredPkt, ok, skipped := c.receiver.PopNext(false)
				if !ok {
					break
				}

				var pcm []int16
				var err error

				if skipped {
					pcm, err = c.decoder.DecodeLost()
				} else {
					pcm, err = c.decoder.Decode(recoveredPkt.Payload)
				}

				if err != nil {
					continue
				}

				// Write PCM samples to file
				if err := c.writePCM(pcm); err != nil {
					log.Printf("Recorder failed to write PCM: %v", err)
					return
				}
			}
		}
	}
}

func (c *RecorderConsumer) writePCM(pcm []int16) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.file == nil {
		return errors.New("file not open")
	}

	// Write samples in little endian format
	buf := make([]byte, len(pcm)*2)
	for i, s := range pcm {
		binary.LittleEndian.PutUint16(buf[2*i:2*i+2], uint16(s))
	}

	n, err := c.file.Write(buf)
	if err != nil {
		return err
	}

	c.bytesWritten += uint32(n)
	return nil
}

// writeWavHeader writes the 44-byte standard PCM WAV header.
func (c *RecorderConsumer) writeWavHeader(dataSize uint32) error {
	if c.file == nil {
		return errors.New("file not open")
	}

	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 36+dataSize)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)                      // Format: PCM (1)
	binary.LittleEndian.PutUint16(header[22:24], uint16(pipeline.Channels)) // Mono (1)
	binary.LittleEndian.PutUint32(header[24:28], uint32(pipeline.SampleRate))
	binary.LittleEndian.PutUint32(header[28:32], uint32(pipeline.SampleRate*pipeline.Channels*2)) // ByteRate
	binary.LittleEndian.PutUint16(header[32:34], uint16(pipeline.Channels*2))                      // BlockAlign
	binary.LittleEndian.PutUint16(header[34:36], 16)                                             // Bits per sample
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataSize)

	// Seek to the beginning of the file to write/overwrite header
	_, err := c.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = c.file.Write(header)
	return err
}

// GetFileName returns the name of the currently recording file
func (c *RecorderConsumer) GetFileName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fileName
}
