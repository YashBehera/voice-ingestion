package pipeline

import (
	"testing"
)

func TestResampleMonoPCM(t *testing.T) {
	// 1. Same sample rate: should return exact copy
	input := []int16{0, 100, 200, 300, 400}
	resampled := ResampleMonoPCM(input, 16000, 16000)
	if len(resampled) != len(input) {
		t.Errorf("Expected length %d, got %d", len(input), len(resampled))
	}
	for i := range input {
		if resampled[i] != input[i] {
			t.Errorf("Index %d: expected %d, got %d", i, input[i], resampled[i])
		}
	}

	// 2. Up-sampling from 16kHz to 48kHz (ratio = 3.0)
	// Output length should be exactly 3x the input length.
	inputUp := []int16{100, 200}
	// Expected linear interp:
	// For output idx 0 (srcIdx 0.0) -> inputUp[0] = 100
	// For output idx 1 (srcIdx 0.33) -> 100*(1-0.33) + 200*0.33 = 133
	// For output idx 2 (srcIdx 0.67) -> 100*(1-0.67) + 200*0.67 = 166
	// For output idx 3 (srcIdx 1.0) -> inputUp[1] = 200
	// ... (output idx 4, 5 high index clip to last value)
	resampledUp := ResampleMonoPCM(inputUp, 16000, 48000)
	expectedLen := len(inputUp) * 3
	if len(resampledUp) != expectedLen {
		t.Errorf("Expected length %d, got %d", expectedLen, len(resampledUp))
	}
	
	if resampledUp[0] != 100 {
		t.Errorf("Expected resampledUp[0] = 100, got %d", resampledUp[0])
	}
	if resampledUp[3] != 200 {
		t.Errorf("Expected resampledUp[3] = 200, got %d", resampledUp[3])
	}

	// 3. Empty input edge case
	resampledEmpty := ResampleMonoPCM([]int16{}, 16000, 48000)
	if resampledEmpty != nil {
		t.Errorf("Expected nil for empty input, got %v", resampledEmpty)
	}
}
