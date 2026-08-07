package pipeline

// ResampleMonoPCM resamples 16-bit mono PCM audio from inRate to outRate using linear interpolation.
// It is designed to be low-latency and computationally efficient.
func ResampleMonoPCM(input []int16, inRate, outRate int) []int16 {
	if inRate == outRate {
		out := make([]int16, len(input))
		copy(out, input)
		return out
	}

	if len(input) == 0 {
		return nil
	}

	ratio := float64(outRate) / float64(inRate)
	outLen := int(float64(len(input)) * ratio)
	output := make([]int16, outLen)

	for i := 0; i < outLen; i++ {
		srcIdx := float64(i) / ratio
		low := int(srcIdx)
		high := low + 1
		weight := srcIdx - float64(low)

		if high >= len(input) {
			output[i] = input[low]
		} else {
			val := float64(input[low])*(1.0-weight) + float64(input[high])*weight
			output[i] = int16(val)
		}
	}

	return output
}
