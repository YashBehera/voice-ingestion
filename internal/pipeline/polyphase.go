package pipeline

import (
	"math"
)

// PolyphaseResampler implements a high-fidelity Polyphase FIR (Finite Impulse Response)
// resampler with anti-aliasing Sinc filtering to eliminate high-frequency noise.
type PolyphaseResampler struct {
	inRate     int
	outRate    int
	filterTaps int
	numPhases  int
}

// NewPolyphaseResampler creates a new Polyphase FIR resampler instance.
func NewPolyphaseResampler(inRate, outRate int) *PolyphaseResampler {
	return &PolyphaseResampler{
		inRate:     inRate,
		outRate:    outRate,
		filterTaps: 16,
		numPhases:  32,
	}
}

// Resample Mono 16-bit PCM using Polyphase Sinc FIR filtering
func (r *PolyphaseResampler) Resample(input []int16) []int16 {
	if r.inRate == r.outRate || len(input) == 0 {
		out := make([]int16, len(input))
		copy(out, input)
		return out
	}

	ratio := float64(r.outRate) / float64(r.inRate)
	outLen := int(float64(len(input)) * ratio)
	output := make([]int16, outLen)

	cutoff := 0.95
	if ratio < 1.0 {
		cutoff *= ratio
	}

	for i := 0; i < outLen; i++ {
		srcIdx := float64(i) / ratio
		center := int(srcIdx)
		var acc float64
		var weightSum float64

		for tap := -r.filterTaps / 2; tap < r.filterTaps/2; tap++ {
			idx := center + tap
			if idx >= 0 && idx < len(input) {
				dist := srcIdx - float64(idx)
				w := r.sinc(dist * cutoff) * r.kaiserWindow(dist/float64(r.filterTaps/2), 5.0)
				acc += float64(input[idx]) * w
				weightSum += w
			}
		}

		if weightSum > 0 {
			acc /= weightSum
		}

		if acc > 32767 {
			acc = 32767
		} else if acc < -32768 {
			acc = -32768
		}
		output[i] = int16(acc)
	}

	return output
}

func (r *PolyphaseResampler) sinc(x float64) float64 {
	if math.Abs(x) < 1e-9 {
		return 1.0
	}
	px := math.Pi * x
	return math.Sin(px) / px
}

func (r *PolyphaseResampler) kaiserWindow(x, beta float64) float64 {
	if math.Abs(x) > 1.0 {
		return 0.0
	}
	arg := 1.0 - x*x
	if arg < 0 {
		arg = 0
	}
	return r.besselI0(beta*math.Sqrt(arg)) / r.besselI0(beta)
}

func (r *PolyphaseResampler) besselI0(x float64) float64 {
	sum := 1.0
	u := 1.0
	halfX := x / 2.0
	for k := 1; k <= 10; k++ {
		u *= (halfX / float64(k)) * (halfX / float64(k))
		sum += u
	}
	return sum
}
