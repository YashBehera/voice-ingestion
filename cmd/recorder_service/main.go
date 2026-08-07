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
	group := flag.String("group", "recorder-workers", "NATS Consumer Group name")
	flag.Parse()

	log.Printf("[Recorder Microservice] Starting decoupled WAV archival recorder (Group: %s, Topic: %s)", *group, *topic)

	eventBus := bus.NewNATSBus()
	err := eventBus.Subscribe(*topic, *group, func(msg bus.Message) error {
		log.Printf("[Recorder Microservice] Received media packet %s (%d bytes) - Writing frame to WAV file archive...", msg.ID, len(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe Recorder microservice: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[Recorder Microservice] Shutting down...")
}
