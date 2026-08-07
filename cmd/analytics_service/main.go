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
	group := flag.String("group", "analytics-workers", "NATS Consumer Group name")
	flag.Parse()

	log.Printf("[Analytics Microservice] Starting decoupled quality monitor (Group: %s, Topic: %s)", *group, *topic)

	eventBus := bus.NewNATSBus()
	err := eventBus.Subscribe(*topic, *group, func(msg bus.Message) error {
		log.Printf("[Analytics Microservice] Monitoring stream Quality of Service for %s (%d bytes)", msg.ID, len(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe Analytics microservice: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[Analytics Microservice] Shutting down...")
}
