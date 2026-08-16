package rtcp

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// ReceiverReport represents an RFC 3550 RTCP Receiver Report.
type ReceiverReport struct {
	SSRC           uint32
	FractionLost   uint8
	TotalLost      uint32
	HighestSeq     uint32
	Jitter         uint32
	RecommendedRED int // 1, 2, or 3
}

// RTCPFeedbackEngine tracks packet arrivals and calculates real-time network loss & jitter.
type RTCPFeedbackEngine struct {
	mu           sync.Mutex
	ssrc         uint32
	lastSeq      uint16
	packetsRecv  int64
	packetsLost  int64
	jitter       float64
	lastTransit  int64
	recommended  int
	lastReport   time.Time
}

// NewRTCPFeedbackEngine initializes an RTCP feedback engine.
func NewRTCPFeedbackEngine(ssrc uint32) *RTCPFeedbackEngine {
	return &RTCPFeedbackEngine{
		ssrc:        ssrc,
		recommended: 2, // Default depth 2
		lastReport:  time.Now(),
	}
}

// RecordArrival registers a packet arrival timestamp and sequence number to compute RFC 3550 jitter.
func (e *RTCPFeedbackEngine) RecordArrival(seq uint16, rtpTimestamp uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.packetsRecv++
	nowMs := time.Now().UnixMilli()
	transit := nowMs - int64(rtpTimestamp/48) // 48 samples per ms at 48kHz

	if e.lastTransit != 0 {
		d := math.Abs(float64(transit - e.lastTransit))
		// RFC 3550 smoothing formula: J = J + (|D| - J) / 16
		e.jitter += (d - e.jitter) / 16.0
	}
	e.lastTransit = transit

	// Detect sequence gaps in raw UDP packets to calculate network-level loss
	if e.lastSeq != 0 {
		diff := seq - e.lastSeq
		if diff > 1 && diff < 30000 {
			// All sequence numbers in the gap were dropped by the network
			e.packetsLost += int64(diff - 1)
		}
	}

	e.lastSeq = seq
}

// RecordLoss registers a dropped sequence number.
func (e *RTCPFeedbackEngine) RecordLoss() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.packetsLost++
}

// GenerateReport produces an RFC 3550 Receiver Report and calculates dynamic RED depth recommendation.
func (e *RTCPFeedbackEngine) GenerateReport() ReceiverReport {
	e.mu.Lock()
	defer e.mu.Unlock()

	total := e.packetsRecv + e.packetsLost
	var fracLost float64
	if total > 0 {
		fracLost = float64(e.packetsLost) / float64(total)
	}

	// Dynamic RED Adaptation Logic:
	// If loss > 15%, increase RED depth to 3
	// If loss < 5%, decrease RED depth to 1
	// Otherwise, keep RED depth 2
	if fracLost > 0.15 {
		e.recommended = 3
	} else if fracLost < 0.05 {
		e.recommended = 1
	} else {
		e.recommended = 2
	}

	e.lastReport = time.Now()

	return ReceiverReport{
		SSRC:           e.ssrc,
		FractionLost:   uint8(fracLost * 256.0),
		TotalLost:      uint32(e.packetsLost),
		HighestSeq:     uint32(e.lastSeq),
		Jitter:         uint32(e.jitter),
		RecommendedRED: e.recommended,
	}
}

// MarshalPack serializes the ReceiverReport into a binary RTCP packet (PT 201).
func (r ReceiverReport) MarshalPack() []byte {
	buf := make([]byte, 32)
	buf[0] = 0x80 | 1 // Version 2, 1 Reception Report Count
	buf[1] = 201      // Payload Type 201: Receiver Report
	binary.BigEndian.PutUint16(buf[2:4], 7) // Length in 32-bit words minus 1
	binary.BigEndian.PutUint32(buf[4:8], r.SSRC)
	buf[8] = r.FractionLost
	binary.BigEndian.PutUint32(buf[9:13], r.TotalLost)
	binary.BigEndian.PutUint32(buf[13:17], r.HighestSeq)
	binary.BigEndian.PutUint32(buf[17:21], r.Jitter)
	buf[21] = byte(r.RecommendedRED)
	return buf
}
