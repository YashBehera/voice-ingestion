package bus

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const defaultNATSURL = nats.DefaultURL

// ConsumerStats holds the metrics reported by downstream services.
type ConsumerStats struct {
	ID        string    `json:"id"`
	Len       int       `json:"len"`
	Cap       int       `json:"cap"`
	Dropped   int64     `json:"dropped"`
	Timestamp time.Time `json:"-"`
}

// NATSBus is an adapter around the official NATS Go client. It intentionally
// contains no broker or transport implementation: a NATS server must be
// supplied separately via NATS_URL (default nats://127.0.0.1:4222).
type NATSBus struct {
	nc *nats.Conn

	mu     sync.Mutex
	closed bool
	subs   []*nats.Subscription

	publishedCount int64
	publishedBytes int64
	deliveredCount int64

	statsMu  sync.Mutex
	statsMap map[string]ConsumerStats
}

// NewNATSBus connects to an official NATS server. It fails rather than
// silently falling back to an isolated in-memory or custom TCP broker.
func NewNATSBus() (*NATSBus, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = defaultNATSURL
	}

	nc, err := nats.Connect(
		url,
		nats.Name("voice-ingestion"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS at %s: %w", url, err)
	}

	b := &NATSBus{
		nc:       nc,
		statsMap: make(map[string]ConsumerStats),
	}

	if err := b.Subscribe("media.metrics.*", "stats-collector", func(msg Message) error {
		var stats ConsumerStats
		if err := json.Unmarshal(msg.Payload, &stats); err != nil {
			return fmt.Errorf("decode consumer telemetry: %w", err)
		}
		stats.Timestamp = time.Now()

		b.statsMu.Lock()
		b.statsMap[stats.ID] = stats
		b.statsMu.Unlock()
		return nil
	}); err != nil {
		nc.Close()
		return nil, err
	}

	return b, nil
}

// Publish sends a message through the configured NATS server.
func (b *NATSBus) Publish(topic string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("NATS bus is closed")
	}

	if err := b.nc.Publish(topic, payload); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	atomic.AddInt64(&b.publishedCount, 1)
	atomic.AddInt64(&b.publishedBytes, int64(len(payload)))
	return nil
}

// Subscribe registers a NATS queue subscription when group is set, giving the
// official server responsibility for load-balancing that consumer group.
func (b *NATSBus) Subscribe(topic string, group string, handler HandlerFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("NATS bus is closed")
	}

	callback := func(msg *nats.Msg) {
		atomic.AddInt64(&b.deliveredCount, 1)
		_ = handler(Message{
			ID:        fmt.Sprintf("%s-%d", msg.Subject, atomic.LoadInt64(&b.deliveredCount)),
			Topic:     msg.Subject,
			Payload:   msg.Data,
			Timestamp: time.Now(),
		})
	}

	var (
		sub *nats.Subscription
		err error
	)
	if group == "" {
		sub, err = b.nc.Subscribe(topic, callback)
	} else {
		sub, err = b.nc.QueueSubscribe(topic, group, callback)
	}
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", topic, err)
	}
	if err := b.nc.FlushTimeout(5 * time.Second); err != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("register subscription for %s: %w", topic, err)
	}
	b.subs = append(b.subs, sub)
	return nil
}

// GetConsumerStats returns telemetry received during the last two seconds.
func (b *NATSBus) GetConsumerStats() map[string]ConsumerStats {
	b.statsMu.Lock()
	defer b.statsMu.Unlock()

	active := make(map[string]ConsumerStats)
	for id, stats := range b.statsMap {
		if time.Since(stats.Timestamp) < 2*time.Second {
			active[id] = stats
		}
	}
	return active
}

// GetMetrics returns messages and bytes successfully handed to the NATS client.
func (b *NATSBus) GetMetrics() (messages, bytes int64) {
	return atomic.LoadInt64(&b.publishedCount), atomic.LoadInt64(&b.publishedBytes)
}

// Close drains active subscriptions and closes the official NATS connection.
func (b *NATSBus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	nc := b.nc
	b.mu.Unlock()

	if err := nc.Drain(); err != nil {
		nc.Close()
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	return nil
}
