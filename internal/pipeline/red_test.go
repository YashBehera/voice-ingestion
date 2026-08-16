package pipeline

import (
	"bytes"
	"testing"
)

func TestREDPackingAndUnpacking(t *testing.T) {
	packer := NewRedPacker(2)
	packer.Reset()

	// Pack three frames: frame 0, 1, 2
	payload0 := []byte{0xAA, 0x11}
	payload1 := []byte{0xBB, 0x22, 0x33}
	payload2 := []byte{0xCC, 0x44, 0x55, 0x66}

	// 1. Pack frame 0 (history is empty, so RED packet has only primary frame 0)
	pkt0 := packer.Pack(payload0)
	if pkt0.SequenceNumber != 0 {
		t.Errorf("Expected seq 0, got %d", pkt0.SequenceNumber)
	}

	// Unpack RED payload of packet 0
	blocks0, err := UnpackRED(pkt0.Payload, pkt0.Timestamp)
	if err != nil {
		t.Fatalf("UnpackRED failed for packet 0: %v", err)
	}
	if len(blocks0) != 1 {
		t.Fatalf("Expected 1 block, got %d", len(blocks0))
	}
	if !bytes.Equal(blocks0[0].Payload, payload0) {
		t.Errorf("Block 0 payload mismatch: got %v, expected %v", blocks0[0].Payload, payload0)
	}

	// 2. Pack frame 1 (history contains frame 0)
	pkt1 := packer.Pack(payload1)
	blocks1, err := UnpackRED(pkt1.Payload, pkt1.Timestamp)
	if err != nil {
		t.Fatalf("UnpackRED failed for packet 1: %v", err)
	}
	// Depth is 2, history length was 1, so we expect 2 blocks: frame 0 (redundant) and frame 1 (primary)
	if len(blocks1) != 2 {
		t.Fatalf("Expected 2 blocks, got %d", len(blocks1))
	}
	if !bytes.Equal(blocks1[0].Payload, payload0) { // oldest first
		t.Errorf("Expected block 0 to be frame 0 payload")
	}
	if !bytes.Equal(blocks1[1].Payload, payload1) { // primary last
		t.Errorf("Expected block 1 to be frame 1 payload")
	}

	// 3. Pack frame 2 (history contains frame 0 and 1)
	pkt2 := packer.Pack(payload2)
	blocks2, err := UnpackRED(pkt2.Payload, pkt2.Timestamp)
	if err != nil {
		t.Fatalf("UnpackRED failed for packet 2: %v", err)
	}
	// History length was 2, so we expect 3 blocks: frame 0, frame 1, and frame 2
	if len(blocks2) != 3 {
		t.Fatalf("Expected 3 blocks, got %d", len(blocks2))
	}
	if !bytes.Equal(blocks2[0].Payload, payload0) ||
		!bytes.Equal(blocks2[1].Payload, payload1) ||
		!bytes.Equal(blocks2[2].Payload, payload2) {
		t.Errorf("Payload order mismatch in RED blocks")
	}

	// Check timestamps
	if blocks2[2].Timestamp != pkt2.Timestamp {
		t.Errorf("Primary block timestamp mismatch")
	}
	if blocks2[1].Timestamp != pkt2.Timestamp-FrameSamples {
		t.Errorf("Redundant block 1 timestamp mismatch")
	}
	if blocks2[0].Timestamp != pkt2.Timestamp-2*FrameSamples {
		t.Errorf("Redundant block 0 timestamp mismatch")
	}
}

func TestRedReceiverRecovery(t *testing.T) {
	receiver := NewRedReceiver(5)
	packer := NewRedPacker(2)
	packer.Reset()

	payload0 := []byte("packet 0")
	payload1 := []byte("packet 1")
	payload2 := []byte("packet 2")

	// Pack packets
	pkt0 := packer.Pack(payload0)
	_ = packer.Pack(payload1)
	pkt2 := packer.Pack(payload2)

	// Simulate receiving pkt 0
	receiver.Push(pkt0)
	pop0, ok, skipped := receiver.PopNext(false)
	if !ok || skipped || !bytes.Equal(pop0.Payload, payload0) {
		t.Fatalf("Failed to retrieve packet 0 cleanly")
	}

	// Simulate loss of pkt 1 (we DO NOT push pkt1 to the receiver)
	// Now we push pkt 2 (which contains primary payload2, and redundant copies of payload1 and payload0)
	receiver.Push(pkt2)

	// Pop next should recover pkt 1 from the RED payload of pkt 2!
	pop1, ok, skipped := receiver.PopNext(false)
	if !ok {
		t.Fatalf("Expected to pop packet, got none (failed to recover seq 1)")
	}
	if skipped {
		t.Errorf("Packet 1 was flagged as skipped, should have been recovered")
	}
	if pop1.SequenceNumber != 1 {
		t.Errorf("Expected sequence 1, got %d", pop1.SequenceNumber)
	}
	if !bytes.Equal(pop1.Payload, payload1) {
		t.Errorf("Payload recovery mismatch: got %s, expected %s", string(pop1.Payload), string(payload1))
	}

	// Pop again should return pkt 2 (the primary)
	pop2, ok, skipped := receiver.PopNext(false)
	if !ok || skipped || pop2.SequenceNumber != 2 || !bytes.Equal(pop2.Payload, payload2) {
		t.Fatalf("Failed to retrieve packet 2 cleanly after recovery")
	}
}

func TestRedReceiverForcedSkip(t *testing.T) {
	receiver := NewRedReceiver(3) // buffer capacity 3
	
	// Push packet 0
	receiver.Push(MediaPacket{SequenceNumber: 0, PayloadType: 111, Payload: []byte("pkt0")})
	// Pop packet 0
	pkt, ok, skipped := receiver.PopNext(false)
	if !ok || skipped || pkt.SequenceNumber != 0 {
		t.Fatalf("Failed to pop packet 0")
	}

	// We lose packets 1, 2, 3 (not pushed).
	// Then we push packet 4, 5, 6.
	// Since capacity is 3, pushing 4, 5, 6 fills the buffer.
	receiver.Push(MediaPacket{SequenceNumber: 4, PayloadType: 111, Payload: []byte("pkt4")})
	receiver.Push(MediaPacket{SequenceNumber: 5, PayloadType: 111, Payload: []byte("pkt5")})
	receiver.Push(MediaPacket{SequenceNumber: 6, PayloadType: 111, Payload: []byte("pkt6")})

	// Now we call PopNext(false). Since buffer size is 3 >= maxBufferSize, it must skip!
	// Under the new correct logic:
	// Call 1: skip 1
	p1, ok, skipped := receiver.PopNext(false)
	if !ok || !skipped || p1.SequenceNumber != 1 {
		t.Fatalf("Expected skipped packet 1, got ok=%v, skipped=%v, seq=%d", ok, skipped, p1.SequenceNumber)
	}
	// Call 2: skip 2
	p2, ok, skipped := receiver.PopNext(false)
	if !ok || !skipped || p2.SequenceNumber != 2 {
		t.Fatalf("Expected skipped packet 2, got seq=%d", p2.SequenceNumber)
	}
	// Call 3: skip 3
	p3, ok, skipped := receiver.PopNext(false)
	if !ok || !skipped || p3.SequenceNumber != 3 {
		t.Fatalf("Expected skipped packet 3, got seq=%d", p3.SequenceNumber)
	}
	// Call 4: pop packet 4 (valid!)
	p4, ok, skipped := receiver.PopNext(false)
	if !ok || skipped || p4.SequenceNumber != 4 || string(p4.Payload) != "pkt4" {
		t.Fatalf("Expected valid packet 4, got ok=%v, skipped=%v, seq=%d, payload=%s", ok, skipped, p4.SequenceNumber, string(p4.Payload))
	}
}
