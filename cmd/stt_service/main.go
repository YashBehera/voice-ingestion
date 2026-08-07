package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"voice-ingestion/internal/bus"
)

func main() {
	topic := flag.String("topic", "media.session.*", "NATS JetStream media topic to subscribe")
	group := flag.String("group", "stt-workers", "NATS Consumer Group name")
	flag.Parse()

	log.Printf("[STT Microservice] Starting decoupled speech-to-text consumer (Group: %s, Topic: %s)", *group, *topic)

	eventBus := bus.NewNATSBus()
	err := eventBus.Subscribe(*topic, *group, func(msg bus.Message) error {
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
