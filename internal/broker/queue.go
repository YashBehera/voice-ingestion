package broker

import (
	"sync"
	"voice-ingestion/internal/pipeline"
)

// BoundedQueue is a thread-safe, non-blocking-write, blocking-read queue
// implementing a "drop-oldest" policy on overflow.
type BoundedQueue struct {
	mu         sync.RWMutex
	ch         chan pipeline.MediaPacket
	capacity   int
	droppedCnt int64
	closed     bool
}

// NewBoundedQueue creates a new drop-oldest queue with the specified capacity.
func NewBoundedQueue(capacity int) *BoundedQueue {
	return &BoundedQueue{
		ch:       make(chan pipeline.MediaPacket, capacity),
		capacity: capacity,
	}
}

// Push adds a packet to the queue. If the queue is full, the oldest packet
// is popped and discarded to make room, maintaining low-latency and isolation.
func (q *BoundedQueue) Push(pkt pipeline.MediaPacket) bool {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return false
	}
	q.mu.RUnlock()

	select {
	case q.ch <- pkt:
		return true
	default:
		// Queue is full, attempt to drop the oldest item to make room
		select {
		case <-q.ch:
			// Oldest item discarded successfully
			q.mu.Lock()
			q.droppedCnt++
			q.mu.Unlock()
		default:
			// Queue was read concurrently, did not block
		}

		// Try writing again
		select {
		case q.ch <- pkt:
			return true
		default:
			// If it still fails, drop the new packet to prevent blocking
			return false
		}
	}
}

// Pop blocks until a packet is available, returning the packet and ok status.
func (q *BoundedQueue) Pop() (pipeline.MediaPacket, bool) {
	pkt, ok := <-q.ch
	return pkt, ok
}

// Close closes the queue and prevents further writes.
func (q *BoundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.ch)
}

// GetDroppedCount returns the number of packets dropped due to buffer overflow.
func (q *BoundedQueue) GetDroppedCount() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.droppedCnt
}

// Len returns the current number of items in the queue.
func (q *BoundedQueue) Len() int {
	return len(q.ch)
}

// Cap returns the capacity of the queue.
func (q *BoundedQueue) Cap() int {
	return q.capacity
}
