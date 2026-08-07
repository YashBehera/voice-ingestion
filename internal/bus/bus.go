package bus

import (
	"time"
)

// Message represents an event payload published to the NATS JetStream message bus.
type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
}

// HandlerFunc is the callback executed when a message is consumed.
type HandlerFunc func(msg Message) error

// EventBus defines the contract for distributed message publishing and subscription.
type EventBus interface {
	Publish(topic string, payload []byte) error
	Subscribe(topic string, group string, handler HandlerFunc) error
	Close() error
}
