package consumers

import (
	"context"
	"voice-ingestion/internal/broker"
)

// Consumer represents a downstream component that consumes RED/Opus packets.
// Examples: Speech-to-Text transcriber, Recorder, Analytics collector.
type Consumer interface {
	// ID returns a unique identifier for this consumer.
	ID() string

	// Start begins consuming packets from the provided queue.
	// It runs asynchronously and should return immediately, spawning its own goroutines.
	Start(ctx context.Context, queue *broker.BoundedQueue) error

	// Stop stops the consumer and releases any resources.
	Stop() error
}
