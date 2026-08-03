class PCMProcessor extends AudioWorkletProcessor {
    process(inputs, outputs, parameters) {
        const input = inputs[0];
        // If there's an active input channel, post the Float32 samples to the main thread
        if (input && input[0] && input[0].length > 0) {
            const inputChannel = input[0];
            // Send a copy of the Float32 array
            this.port.postMessage(inputChannel);
        }
        return true;
    }
}

registerProcessor('pcm-processor', PCMProcessor);
