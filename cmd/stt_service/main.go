package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"voice-ingestion/internal/bus"
)

func main() {
	topic := flag.String("topic", "media.session.*", "NATS JetStream media topic to subscribe")
	group := flag.String("group", "stt-workers", "NATS Consumer Group name")
	flag.Parse()

	log.Printf("[STT Microservice] Starting decoupled speech-to-text consumer (Group: %s, Topic: %s)", *group, *topic)

	var processingCount int64
	var droppedCount int64
	maxConcurrent := int64(15)

	eventBus := bus.NewNATSBus()

	// Broadcast active queue stats back to worker telemetry hub
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		for range ticker.C {
			stats := bus.ConsumerStats{
				ID:      "speech-to-text",
				Len:     int(atomic.LoadInt64(&processingCount)),
				Cap:     int(maxConcurrent),
				Dropped: atomic.LoadInt64(&droppedCount),
			}
			payload, _ := json.Marshal(stats)
			_ = eventBus.Publish("media.metrics.speech-to-text", payload)
		}
	}()

	err := eventBus.Subscribe(*topic, *group, func(msg bus.Message) error {
		// Backpressure load-shedding check
		if atomic.LoadInt64(&processingCount) >= maxConcurrent {
			atomic.AddInt64(&droppedCount, 1)
			return nil
		}

		atomic.AddInt64(&processingCount, 1)
		defer atomic.AddInt64(&processingCount, -1)

		// Simulate Speech-to-Text inference duration
		time.Sleep(80 * time.Millisecond)

		log.Printf("[STT Microservice] Received media packet %s (%d bytes) - Processing VAD & Speech Transcript...", msg.ID, len(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe STT microservice: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[STT Microservice] Shutting down...")
}
