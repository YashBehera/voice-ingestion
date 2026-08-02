package broker

import (
	"testing"
	"voice-ingestion/internal/pipeline"
)


func TestQueueOperations(t *testing.T) {
	q := NewBoundedQueue(3)

	if q.Cap() != 3 {
		t.Errorf("Expected capacity 3, got %d", q.Cap())
	}

	// Push 3 packets
	q.Push(pipeline.MediaPacket{SequenceNumber: 1})
	q.Push(pipeline.MediaPacket{SequenceNumber: 2})
	q.Push(pipeline.MediaPacket{SequenceNumber: 3})

	if q.Len() != 3 {
		t.Errorf("Expected len 3, got %d", q.Len())
	}

	// Push 4th packet, which should trigger drop-oldest (packet with Seq=1 drops)
	q.Push(pipeline.MediaPacket{SequenceNumber: 4})

	if q.Len() != 3 {
		t.Errorf("Expected len to remain 3 after overflow, got %d", q.Len())
	}

	if q.GetDroppedCount() != 1 {
		t.Errorf("Expected dropped count to be 1, got %d", q.GetDroppedCount())
	}

	// Pop and check sequence numbers: should be 2, 3, 4 (Seq 1 dropped!)
	pkt, ok := q.Pop()
	if !ok || pkt.SequenceNumber != 2 {
		t.Errorf("Expected to pop seq 2, got %v (ok=%t)", pkt.SequenceNumber, ok)
	}

	pkt, ok = q.Pop()
	if !ok || pkt.SequenceNumber != 3 {
		t.Errorf("Expected to pop seq 3, got %v", pkt.SequenceNumber)
	}

	pkt, ok = q.Pop()
	if !ok || pkt.SequenceNumber != 4 {
		t.Errorf("Expected to pop seq 4, got %v", pkt.SequenceNumber)
	}
}

func TestBrokerConsumerIsolation(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Register a fast consumer (size 5) and a slow consumer (size 2)
	fastQueue := b.Register("fast", 5)
	slowQueue := b.Register("slow", 2)

	// Publish 4 packets
	for i := 0; i < 4; i++ {
		b.Publish(pipeline.MediaPacket{SequenceNumber: uint16(i)})
	}

	// Fast consumer should have all 4 packets
	if fastQueue.Len() != 4 {
		t.Errorf("Fast consumer should have 4 packets, got %d", fastQueue.Len())
	}

	// Slow consumer queue size is 2, so it should have capped at 2 packets and dropped 2
	if slowQueue.Len() != 2 {
		t.Errorf("Slow consumer should have exactly 2 packets in queue, got %d", slowQueue.Len())
	}

	if slowQueue.GetDroppedCount() != 2 {
		t.Errorf("Slow consumer should have dropped 2 packets, got %d", slowQueue.GetDroppedCount())
	}

	// Pop fast consumer packets, verify order
	for i := 0; i < 4; i++ {
		pkt, ok := fastQueue.Pop()
		if !ok || pkt.SequenceNumber != uint16(i) {
			t.Errorf("Fast consumer popped incorrect sequence: got %d, expected %d", pkt.SequenceNumber, i)
		}
	}

	// Pop slow consumer packets: since queue is size 2 and drop-oldest was used,
	// the queue should contain the LATEST two packets (2 and 3). Packets 0 and 1 dropped!
	pkt, ok := slowQueue.Pop()
	if !ok || pkt.SequenceNumber != 2 {
		t.Errorf("Slow consumer expected seq 2, got %d", pkt.SequenceNumber)
	}

	pkt, ok = slowQueue.Pop()
	if !ok || pkt.SequenceNumber != 3 {
		t.Errorf("Slow consumer expected seq 3, got %d", pkt.SequenceNumber)
	}
}
