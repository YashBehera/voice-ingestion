package broker

import (
	"sync"
	"voice-ingestion/internal/pipeline"
)

// ActiveConsumer represents a registered downstream consumer in the broker.
type ActiveConsumer struct {
	ID    string
	Queue *BoundedQueue
}

// Broker manages consumer registration and distributes media packets.
// It implements pipeline.PacketPublisher interface.
type Broker struct {
	mu        sync.RWMutex
	consumers map[string]*ActiveConsumer
}

// NewBroker initializes a new consumer broker.
func NewBroker() *Broker {
	return &Broker{
		consumers: make(map[string]*ActiveConsumer),
	}
}

// Register registers a new consumer and returns its dedicated bounded queue.
func (b *Broker) Register(id string, queueCapacity int) *BoundedQueue {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If consumer already exists, deregister the old one first
	if old, ok := b.consumers[id]; ok {
		old.Queue.Close()
	}

	queue := NewBoundedQueue(queueCapacity)
	b.consumers[id] = &ActiveConsumer{
		ID:    id,
		Queue: queue,
	}

	return queue
}

// Deregister removes a consumer and closes its queue.
func (b *Broker) Deregister(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if consumer, ok := b.consumers[id]; ok {
		consumer.Queue.Close()
		delete(b.consumers, id)
	}
}

// Publish distributes a packet to all active consumers in a non-blocking manner.
// If a consumer's queue is full, the oldest packet in that queue is dropped.
func (b *Broker) Publish(packet pipeline.MediaPacket) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, consumer := range b.consumers {
		// Non-blocking push ensures that a slow consumer queue does not delay publishing
		consumer.Queue.Push(packet)
	}
}

// GetConsumers returns a list of active consumer IDs and their queue states.
type ConsumerInfo struct {
	ID      string
	Len     int
	Cap     int
	Dropped int64
}

func (b *Broker) GetConsumers() []ConsumerInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	infos := make([]ConsumerInfo, 0, len(b.consumers))
	for _, c := range b.consumers {
		infos = append(infos, ConsumerInfo{
			ID:      c.ID,
			Len:     c.Queue.Len(),
			Cap:     c.Queue.Cap(),
			Dropped: c.Queue.GetDroppedCount(),
		})
	}
	return infos
}

// Close closes the broker, deregistering all consumers and closing their queues.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, consumer := range b.consumers {
		consumer.Queue.Close()
		delete(b.consumers, id)
	}
}
