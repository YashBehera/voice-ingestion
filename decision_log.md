# Architecture Decisions - Voice Ingestion Worker

Core design choices made for the media worker architecture.

| Decision | Alternatives Rejected | Rationale | Reversal Trigger |
| :--- | :--- | :--- | :--- |
| **Go (Golang) Runtime** | Rust, Python Asyncio | Built-in concurrency primitives (goroutines/channels) make broker fan-out simple. Low GC pause times and native networking (UDP/WebSockets) are ideal for real-time streaming. | Cgo integration with C audio libraries becomes a bottleneck compared to writing native Rust bindings. |
| **48kHz Mono 16-bit PCM Internal format** | Stereo, variable rate, raw Opus | Standard high-fidelity voice processing pipeline rate. Simplifies linear resampling, VAD energy math, and aligns with the native sample rate expected by the Opus encoder. | Input feeds are primarily multi-channel spatial streams that must be preserved. |
| **Drop-oldest Bounded Queue** | Blocking writes, drop-newest | Real-time audio prioritizes new data. Dropping the oldest frame bounds queue latency. Non-blocking writes isolate slow consumers from halting the broker. | Downstream requires lossless archiving where backpressure should halt ingestion instead of dropping data. |
| **RED Depth = 2** | Depth = 1, Depth = 3 | Restores up to 2 consecutive lost packets while keeping the packet payload size well below the standard 1500-byte Ethernet MTU. | Network profiles show packet loss bursts of > 3 packets, or network bandwidth is too constrained for overhead. |
| **8-Frame Sequence Buffer** | Dynamic jitter buffer | Lightweight, low-overhead array map. Sorts out-of-order packets and recovers missing frames in 0-40ms, avoiding heavy jitter calculations. | Inbound network jitter is high enough to require dynamically adjusting buffer latency to prevent PLC triggers. |
| **Offline VAD Simulator** | Real STT API (Gemini/Whisper) | Local RMS energy calculations run offline with zero API cost, zero network latency, and zero configuration overhead. | The system requires actual semantic text matching rather than validating audio processing throughput. |
| **Docker-Based Compiles** | Native Host Makefiles | Compiling Cgo bindings (`libopus`/`libopusfile`) inside Docker ensures builds are 100% portable across developer environments. | Image build speed becomes a bottleneck for iteration loops compared to cached native compilation. |
