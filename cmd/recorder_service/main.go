package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"voice-ingestion/internal/bus"
	"voice-ingestion/internal/pipeline"
)

func main() {
	topic := flag.String("topic", "media.session.*", "NATS JetStream media topic to subscribe")
	group := flag.String("group", "recorder-workers", "NATS Consumer Group name")
	flag.Parse()

	log.Printf("[Recorder Microservice] Starting decoupled WAV archival recorder (Group: %s, Topic: %s)", *group, *topic)

	// Create recordings directory if not exists
	_ = os.MkdirAll("recordings", 0755)

	// Create the live WAV output file
	f, err := os.Create("recordings/live_recorder_output.wav")
	if err != nil {
		log.Fatalf("Failed to create recordings file: %v", err)
	}
	defer f.Close()

	// Write 44-byte WAV header template
	header := make([]byte, 44)
	copy(header[0:4], []byte("RIFF"))
	copy(header[8:12], []byte("WAVE"))
	copy(header[12:16], []byte("fmt "))
	binary.LittleEndian.PutUint32(header[16:20], 16)    // Subchunk1Size
	binary.LittleEndian.PutUint16(header[20:22], 1)     // PCM format
	binary.LittleEndian.PutUint16(header[22:24], 1)     // Mono
	binary.LittleEndian.PutUint32(header[24:28], 48000) // Normalized 48kHz
	binary.LittleEndian.PutUint32(header[28:32], 96000) // Byte rate (48000 * 2)
	binary.LittleEndian.PutUint16(header[32:34], 2)     // Block align
	binary.LittleEndian.PutUint16(header[34:36], 16)    // 16-bit
	copy(header[36:40], []byte("data"))
	_, _ = f.Write(header)

	// Initialize Opus Decoder
	decoder, err := pipeline.NewOpusDecoder()
	if err != nil {
		log.Fatalf("Failed to create Opus decoder: %v", err)
	}

	var processingCount int64
	var droppedCount int64
	maxConcurrent := int64(8)

	eventBus, err := bus.NewNATSBus()
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer eventBus.Close()

	// Broadcast active queue stats back to worker telemetry hub
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		for range ticker.C {
			stats := bus.ConsumerStats{
				ID:      "recorder",
				Len:     int(atomic.LoadInt64(&processingCount)),
				Cap:     int(maxConcurrent),
				Dropped: atomic.LoadInt64(&droppedCount),
			}
			payload, _ := json.Marshal(stats)
			_ = eventBus.Publish("media.metrics.recorder", payload)
		}
	}()

	err = eventBus.Subscribe(*topic, *group, func(msg bus.Message) error {
		// Backpressure load-shedding check
		if atomic.LoadInt64(&processingCount) >= maxConcurrent {
			atomic.AddInt64(&droppedCount, 1)
			return nil
		}

		atomic.AddInt64(&processingCount, 1)
		defer atomic.AddInt64(&processingCount, -1)

		if len(msg.Payload) < 2 {
			return nil
		}

		payloadType := msg.Payload[0]
		payload := msg.Payload[1:]

		var opusFrame []byte
		if payloadType == 111 {
			opusFrame = payload
		} else if payloadType == 112 {
			// Unpack RED payload to get the primary frame
			blocks, err := pipeline.UnpackRED(payload, 0)
			if err != nil || len(blocks) == 0 {
				return nil
			}
			// The last block is the primary audio block
			opusFrame = blocks[len(blocks)-1].Payload
		} else {
			return nil
		}

		// Decode Opus to PCM
		pcm, err := decoder.Decode(opusFrame)
		if err != nil {
			return nil
		}

		// Write PCM samples as Little-Endian bytes to the WAV file
		byteBuf := make([]byte, len(pcm)*2)
		for i, val := range pcm {
			binary.LittleEndian.PutUint16(byteBuf[i*2:], uint16(val))
		}
		_, _ = f.Write(byteBuf)

		// Simulate disk write duration / buffering delay
		time.Sleep(30 * time.Millisecond)

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe Recorder microservice: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	// Update WAV header sizes with actual written length
	fInfo, err := f.Stat()
	if err == nil {
		sz := fInfo.Size()
		dataSz := sz - 44
		riffSz := sz - 8

		_, _ = f.Seek(4, io.SeekStart)
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(riffSz))
		_, _ = f.Write(buf)

		_, _ = f.Seek(40, io.SeekStart)
		binary.LittleEndian.PutUint32(buf, uint32(dataSz))
		_, _ = f.Write(buf)
	}

	log.Println("[Recorder Microservice] Saved recorded file to recordings/live_recorder_output.wav")
	log.Println("[Recorder Microservice] Shutting down...")
}
