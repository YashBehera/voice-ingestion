package pipeline

import (
	"errors"
	"fmt"
	"sync"
)

// MediaPacket represents a packet in our internal audio stream.
type MediaPacket struct {
	SequenceNumber uint16
	Timestamp      uint32
	PayloadType    uint8 // 111 for raw Opus, 112 for RED
	Payload        []byte
}

// RedBlock represents an unpacked block from a RED packet.
type RedBlock struct {
	PayloadType uint8
	Timestamp   uint32
	Payload     []byte
}

// RedPacker keeps a history of recent packets and encodes them into RFC2198 RED packets.
type RedPacker struct {
	mu        sync.Mutex
	depth     int
	history   []MediaPacket
	nextSeq   uint16
	nextTime  uint32
}

// NewRedPacker creates a new RFC2198 RED packetizer with the specified redundancy depth.
func NewRedPacker(depth int) *RedPacker {
	return &RedPacker{
		depth:   depth,
		history: make([]MediaPacket, 0, depth),
	}
}

// Pack encodes a raw Opus payload into an RFC2198 RED packet, updating history.
func (p *RedPacker) Pack(opusPayload []byte) MediaPacket {
	p.mu.Lock()
	defer p.mu.Unlock()

	seq := p.nextSeq
	timestamp := p.nextTime

	p.nextSeq++
	p.nextTime += FrameSamples

	// Create the current primary packet
	primary := MediaPacket{
		SequenceNumber: seq,
		Timestamp:      timestamp,
		PayloadType:    111, // Opus
		Payload:        opusPayload,
	}

	// If depth is 0, we don't pack RED, we just return the raw Opus packet
	if p.depth <= 0 {
		return primary
	}

	// We look back at history to find redundant blocks
	// We want up to p.depth redundant blocks
	redundantBlocks := make([]MediaPacket, 0, p.depth)
	for i := len(p.history) - 1; i >= 0 && len(redundantBlocks) < p.depth; i-- {
		redundantBlocks = append([]MediaPacket{p.history[i]}, redundantBlocks...)
	}

	// Update history with the new primary packet
	p.history = append(p.history, primary)
	if len(p.history) > p.depth {
		p.history = p.history[1:]
	}

	// Construct the RED packet
	// 1. Calculate headers size
	// Each redundant header is 4 bytes
	// The primary header is 1 byte
	headerSize := len(redundantBlocks)*4 + 1
	payloadSize := 0
	for _, block := range redundantBlocks {
		payloadSize += len(block.Payload)
	}
	payloadSize += len(primary.Payload)

	redPayload := make([]byte, headerSize+payloadSize)
	writeIdx := 0

	// 2. Write redundant headers
	for _, block := range redundantBlocks {
		offset := timestamp - block.Timestamp
		length := len(block.Payload)

		// Byte 0: F=1 (1 bit) | PT (7 bits)
		redPayload[writeIdx] = 0x80 | (block.PayloadType & 0x7F)
		// Byte 1-3: timestamp offset (14 bits) and block length (10 bits)
		redPayload[writeIdx+1] = byte((offset >> 6) & 0xFF)
		redPayload[writeIdx+2] = byte(((offset & 0x3F) << 2) | uint32((length>>8)&0x03))
		redPayload[writeIdx+3] = byte(length & 0xFF)
		writeIdx += 4
	}

	// 3. Write primary header
	// F=0 (1 bit) | PT (7 bits)
	redPayload[writeIdx] = primary.PayloadType & 0x7F
	writeIdx++

	// 4. Write payloads (oldest first, then primary)
	for _, block := range redundantBlocks {
		copy(redPayload[writeIdx:], block.Payload)
		writeIdx += len(block.Payload)
	}
	copy(redPayload[writeIdx:], primary.Payload)

	return MediaPacket{
		SequenceNumber: seq,
		Timestamp:      timestamp,
		PayloadType:    112, // RED
		Payload:        redPayload,
	}
}

// Reset resets the packetizer sequence and timestamp
func (p *RedPacker) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextSeq = 0
	p.nextTime = 0
	p.history = p.history[:0]
}

// UnpackRED parses an RFC2198 RED packet payload into individual blocks.
func UnpackRED(redPayload []byte, primaryTimestamp uint32) ([]RedBlock, error) {
	if len(redPayload) < 1 {
		return nil, errors.New("empty RED payload")
	}

	var headers []struct {
		pt     uint8
		offset uint32
		length uint16
		isLast bool
	}

	readIdx := 0
	for {
		if readIdx >= len(redPayload) {
			return nil, errors.New("malformed RED headers: out of bounds")
		}

		b := redPayload[readIdx]
		isLast := (b & 0x80) == 0
		pt := b & 0x7F

		if isLast {
			headers = append(headers, struct {
				pt     uint8
				offset uint32
				length uint16
				isLast bool
			}{pt: pt, offset: 0, length: 0, isLast: true})
			readIdx++
			break
		} else {
			if readIdx+3 >= len(redPayload) {
				return nil, errors.New("malformed RED redundant header: out of bounds")
			}
			offset := (uint32(redPayload[readIdx+1]) << 6) | (uint32(redPayload[readIdx+2]) >> 2)
			length := (uint16(redPayload[readIdx+2]&0x03) << 8) | uint16(redPayload[readIdx+3])
			headers = append(headers, struct {
				pt     uint8
				offset uint32
				length uint16
				isLast bool
			}{pt: pt, offset: offset, length: length, isLast: false})
			readIdx += 4
		}
	}

	// Calculate primary block length
	var redundantSum uint16
	for _, h := range headers {
		if !h.isLast {
			redundantSum += h.length
		}
	}

	totalHeadersSize := readIdx
	primaryLength := len(redPayload) - totalHeadersSize - int(redundantSum)
	if primaryLength < 0 {
		return nil, fmt.Errorf("malformed RED payload: lengths sum (%d) exceeds packet payload (%d)", redundantSum, len(redPayload)-totalHeadersSize)
	}

	// Update the primary header length
	headers[len(headers)-1].length = uint16(primaryLength)

	// Read data blocks
	blocks := make([]RedBlock, 0, len(headers))
	dataStart := totalHeadersSize

	for _, h := range headers {
		if dataStart+int(h.length) > len(redPayload) {
			return nil, errors.New("malformed RED data block: out of bounds")
		}

		blockData := make([]byte, h.length)
		copy(blockData, redPayload[dataStart:dataStart+int(h.length)])
		dataStart += int(h.length)

		blocks = append(blocks, RedBlock{
			PayloadType: h.pt,
			Timestamp:   primaryTimestamp - h.offset,
			Payload:     blockData,
		})
	}

	return blocks, nil
}

// RedReceiver is a jitter/recovery buffer that takes RED/Opus packets,
// reorders them, recovers lost packets, and outputs them in sequence.
type RedReceiver struct {
	mu           sync.Mutex
	buffer       map[uint16]MediaPacket
	nextSeq      uint16
	initialized  bool
	maxBufferSize int

	// Metrics
	PacketsReceived  int64
	PacketsLost      int64
	PacketsRecovered int64
}

// NewRedReceiver creates a RedReceiver with a specific buffer size (e.g. 10 packets)
func NewRedReceiver(maxBufferSize int) *RedReceiver {
	return &RedReceiver{
		buffer:        make(map[uint16]MediaPacket),
		maxBufferSize: maxBufferSize,
	}
}

// Push inputs an incoming packet (which could be Opus or RED).
// It unpacks it, stores all recovered frames in the buffer, and returns recovered packets.
func (r *RedReceiver) Push(packet MediaPacket) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.PacketsReceived++

	if !r.initialized {
		r.nextSeq = packet.SequenceNumber
		r.initialized = true
	}

	// 1. If it's a raw Opus packet
	if packet.PayloadType == 111 {
		r.insert(packet.SequenceNumber, packet)
		return
	}

	// 2. If it's a RED packet, unpack it
	if packet.PayloadType == 112 {
		blocks, err := UnpackRED(packet.Payload, packet.Timestamp)
		if err != nil {
			// If malformed, insert primary as best effort
			r.insert(packet.SequenceNumber, MediaPacket{
				SequenceNumber: packet.SequenceNumber,
				Timestamp:      packet.Timestamp,
				PayloadType:    111,
				Payload:        packet.Payload,
			})
			return
		}

		// Insert each block into the buffer
		for _, block := range blocks {
			// Find the sequence number of this block
			// Since samples per frame is fixed (FrameSamples = 960), we can map timestamp offsets
			offsetSamples := packet.Timestamp - block.Timestamp
			seqOffset := uint16(offsetSamples / FrameSamples)
			blockSeq := packet.SequenceNumber - seqOffset

			// If this is a redundant block and is not already in the buffer, and hasn't been played yet,
			// it means we successfully recovered a lost packet via RED!
			if blockSeq < packet.SequenceNumber {
				if !r.initialized || !seqBefore(blockSeq, r.nextSeq) {
					if _, ok := r.buffer[blockSeq]; !ok {
						r.PacketsRecovered++
					}
				}
			}

			// We don't overwrite if it's already in the buffer (primary is preferred, though they are identical)
			r.insert(blockSeq, MediaPacket{
				SequenceNumber: blockSeq,
				Timestamp:      block.Timestamp,
				PayloadType:    111, // Treat as raw Opus once unpacked
				Payload:        block.Payload,
			})
		}
	}
}

func (r *RedReceiver) insert(seq uint16, packet MediaPacket) {
	// If it's older than what we have already played, discard it
	if r.initialized && seqBefore(seq, r.nextSeq) {
		return
	}
	r.buffer[seq] = packet
}

// PopNext retrieves the next expected packet in sequence.
// If the next packet is missing, it checks if it is lost.
// If force is true or the buffer exceeds maxBufferSize, it will skip missing packets and return the next available one.
func (r *RedReceiver) PopNext(force bool) (MediaPacket, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.initialized {
		return MediaPacket{}, false, false
	}

	// Check if the next expected packet is in the buffer
	if pkt, ok := r.buffer[r.nextSeq]; ok {
		delete(r.buffer, r.nextSeq)
		r.nextSeq++
		return pkt, true, false
	}

	// If not, are we forced to skip, or did our buffer grow too large?
	// If the buffer size exceeds maxBufferSize, we must skip the missing packets.
	if force || len(r.buffer) >= r.maxBufferSize {
		// Find the oldest sequence number currently in the buffer that is after r.nextSeq
		var oldestSeq uint16
		found := false

		for seq := range r.buffer {
			if !found || seqBefore(seq, oldestSeq) {
				oldestSeq = seq
				found = true
			}
		}

		if found {
			// We skipped some packets!
			skippedCount := oldestSeq - r.nextSeq
			r.PacketsLost += int64(skippedCount)
			r.nextSeq = oldestSeq

			// Now pop the oldest available packet
			pkt := r.buffer[oldestSeq]
			delete(r.buffer, oldestSeq)
			r.nextSeq++
			return pkt, true, true // true for skipped/lost packet
		}
	}

	return MediaPacket{}, false, false
}

// Reset clears the receiver state
func (r *RedReceiver) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buffer = make(map[uint16]MediaPacket)
	r.initialized = false
	r.nextSeq = 0
	r.PacketsReceived = 0
	r.PacketsLost = 0
	r.PacketsRecovered = 0
}

// GetMetrics returns receiver statistics
func (r *RedReceiver) GetMetrics() (received, lost, recovered int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.PacketsReceived, r.PacketsLost, r.PacketsRecovered
}

// Helper to handle sequence number wrap-around
func seqBefore(seq1, seq2 uint16) bool {
	return (seq1 < seq2 && seq2-seq1 < 32768) || (seq1 > seq2 && seq1-seq2 > 32768)
}
