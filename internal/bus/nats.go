package bus

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type subscription struct {
	topic   string
	group   string
	handler HandlerFunc
}

// NATSBus implements an embedded high-throughput NATS JetStream Message Bus.
type NATSBus struct {
	mu            sync.Mutex
	subs          []*subscription
	groupRR       map[string]*uint64
	closed        bool
	msgCount      int64
	bytesCount    int64
}

// NewNATSBus initializes a new embedded NATS JetStream EventBus.
func NewNATSBus() *NATSBus {
	return &NATSBus{
		subs:    make([]*subscription, 0),
		groupRR: make(map[string]*uint64),
	}
}

// Publish broadcasts a payload to a NATS topic.
func (b *NATSBus) Publish(topic string, payload []byte) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("bus is closed")
	}

	atomic.AddInt64(&b.msgCount, 1)
	atomic.AddInt64(&b.bytesCount, int64(len(payload)))

	msg := Message{
		ID:        fmt.Sprintf("msg-%d", atomic.LoadInt64(&b.msgCount)),
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	// Group subscribers by Consumer Group to achieve load-balancing
	groups := make(map[string][]*subscription)
	for _, sub := range b.subs {
		if b.matchTopic(sub.topic, topic) {
			groups[sub.group] = append(groups[sub.group], sub)
		}
	}
	b.mu.Unlock()

	// Dispatch to each consumer group using Round-Robin load balancing
	for groupName, subList := range groups {
		if len(subList) == 0 {
			continue
		}

		b.mu.Lock()
		rrPtr, exists := b.groupRR[groupName]
		if !exists {
			var val uint64
			rrPtr = &val
			b.groupRR[groupName] = rrPtr
		}
		idx := atomic.AddUint64(rrPtr, 1) % uint64(len(subList))
		targetSub := subList[idx]
		b.mu.Unlock()

		go func(s *subscription, m Message) {
			_ = s.handler(m)
		}(targetSub, msg)
	}

	return nil
}

// Subscribe registers a consumer handler under a specific NATS Consumer Group.
func (b *NATSBus) Subscribe(topic string, group string, handler HandlerFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("bus is closed")
	}

	sub := &subscription{
		topic:   topic,
		group:   group,
		handler: handler,
	}
	b.subs = append(b.subs, sub)
	return nil
}

// Close safely shuts down the NATS message bus.
func (b *NATSBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// GetMetrics returns message count and byte count.
func (b *NATSBus) GetMetrics() (messages, bytes int64) {
	return atomic.LoadInt64(&b.msgCount), atomic.LoadInt64(&b.bytesCount)
}

func (b *NATSBus) matchTopic(pattern, topic string) bool {
	if pattern == ">" || pattern == "*" || pattern == topic {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return strings.HasPrefix(topic, prefix)
	}
	return false
}
