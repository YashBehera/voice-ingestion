package consumers

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
	"voice-ingestion/internal/broker"
)

// SlowConsumer is a test consumer used to demonstrate downstream isolation.
// It can be configured to process packets very slowly (causing buffer overflows)
// or to panic (simulate crashes) after receiving a specific number of packets.
type SlowConsumer struct {
	mu            sync.Mutex
	id            string
	processingLag time.Duration
	panicAfter    int
	packetsSeen   int
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       bool

	// Stats for evaluation
	ProcessedCount int64
	PanicTriggered bool
}

// NewSlowConsumer creates a new test isolation consumer.
func NewSlowConsumer(id string, processingLag time.Duration, panicAfter int) *SlowConsumer {
	return &SlowConsumer{
		id:            id,
		processingLag: processingLag,
		panicAfter:    panicAfter,
	}
}

// ID returns the unique identifier.
func (c *SlowConsumer) ID() string {
	return c.id
}

// Start launches the consumer loop.
func (c *SlowConsumer) Start(ctx context.Context, queue *broker.BoundedQueue) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.running = true
	c.packetsSeen = 0
	c.ProcessedCount = 0
	c.PanicTriggered = false

	c.wg.Add(1)
	go c.run(queue)

	log.Printf("Slow/Crashy Consumer [%s] started (lag=%v, panicAfter=%d)", c.id, c.processingLag, c.panicAfter)
	return nil
}

// Stop halts execution.
func (c *SlowConsumer) Stop() error {
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
	log.Printf("Slow/Crashy Consumer [%s] stopped", c.id)
	return nil
}

func (c *SlowConsumer) run(queue *broker.BoundedQueue) {
	defer c.wg.Done()

	// CRITICAL: We defer a recovery function to catch any panic triggered inside this consumer.
	// This ensures a consumer crash NEVER affects the broker or other concurrent consumers!
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[CRITICAL ERROR] Consumer [%s] crashed: %v. Recovering and stopping cleanly.", c.id, r)
			c.mu.Lock()
			c.PanicTriggered = true
			c.running = false
			c.mu.Unlock()
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Pop packet (blocking)
			_, ok := queue.Pop()
			if !ok {
				return // Queue closed
			}

			c.mu.Lock()
			c.packetsSeen++
			currentSeen := c.packetsSeen
			c.mu.Unlock()

			// 1. Check if we should trigger a simulated panic
			if c.panicAfter > 0 && currentSeen >= c.panicAfter {
				log.Printf("Consumer [%s] reached trigger point (%d packets). Simulating panic/crash...", c.id, c.panicAfter)
				panic(errors.New("simulated database transaction timeout crash"))
			}

			// 2. Simulate processing lag (blocking sleep)
			if c.processingLag > 0 {
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(c.processingLag):
				}
			}

			c.mu.Lock()
			c.ProcessedCount++
			c.mu.Unlock()
		}
	}
}

// GetStats returns current metrics for testing
func (c *SlowConsumer) GetStats() (processed int64, panicOccurred bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ProcessedCount, c.PanicTriggered
}
