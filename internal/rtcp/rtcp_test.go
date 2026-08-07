package rtcp

import (
	"testing"
	"time"
)

func TestRTCPFeedbackEngine(t *testing.T) {
	engine := NewRTCPFeedbackEngine(0x12345678)

	// Simulate arrivals
	engine.RecordArrival(100, uint32(time.Now().UnixMilli()*48))
	engine.RecordArrival(101, uint32((time.Now().UnixMilli()+20)*48))
	engine.RecordLoss()

	report := engine.GenerateReport()
	if report.SSRC != 0x12345678 {
		t.Fatalf("Expected SSRC 0x12345678, got %d", report.SSRC)
	}

	packed := report.MarshalPack()
	if len(packed) < 22 {
		t.Fatalf("Expected packed RTCP report length >= 22, got %d", len(packed))
	}
}
