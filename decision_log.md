# Architectural Decision Log - Voice Ingestion Worker

This log documents the core technical decisions made during the design and implementation of the Voice Ingestion service, explained in plain English.

---

### Q1: Why do we use RED redundancy on UDP/RTP inputs but not on TCP/WebSocket inputs?
*   **Decision**: Enable RFC 2198 RED (Redundant Audio Data) exclusively for UDP streams.
*   **Why**: UDP is an unreliable protocol—packets get lost when users walk down hallways or switch Wi-Fi routers, causing voice stuttering. RED packages duplicate history frames to heal these losses. WebSockets run on TCP, which has built-in 100% reliability at the OS kernel layer. Adding RED over TCP would waste network bandwidth with zero benefit.

### Q2: Why did we build a Session Router (`router.go`) instead of a single global pipeline?
*   **Decision**: Route incoming chunks by their Call/Session ID into isolated per-call pipeline workers.
*   **Why**: A single global pipeline would mix audio samples from different users together, causing everyone to hear each other's voices scrambled. The `SessionRouter` ensures that Call A has its own resampler, encoder, and packer completely isolated from Call B, enabling secure multi-tenant hosting.

### Q3: Why did we implement a Drop-Oldest Bounded Queue instead of standard Go channels?
*   **Decision**: Use a thread-safe custom queue that discards the oldest audio frame when full.
*   **Why**: Real-time voice demands low latency. If one downstream consumer (like a slow disk-writer) slows down, using standard channels would block the main dispatcher, freezing the server and causing audio dropouts for healthy consumers. Drop-oldest ensures slow consumers discard late audio to catch up, maintaining real-time playback for everyone else.

### Q4: Why did we choose Go (Golang) over Rust or Python Asyncio for the core worker?
*   **Decision**: Build the service in Go.
*   **Why**: Go provides native concurrency primitives (goroutines and channels) that make streaming simple, with low garbage collection (GC) latency ideal for real-time media. Python is too slow due to the GIL, and Rust adds development overhead for memory management where Go's performance is already more than sufficient.

### Q5: Why do we normalize all audio to 48kHz Mono 16-bit PCM internally?
*   **Decision**: Standardize all input formats to 48kHz mono PCM before processing.
*   **Why**: Inputs come in at various formats (16kHz from browser mic, 8kHz from VoIP, stereo from files). Normalizing to 48kHz mono PCM simplifies digital signal processing (DSP), aligns with the native sample rate of the Opus encoder, and ensures downstream microservices only have to support one clean audio format.

### Q6: Why did we transition from a local in-memory broker to NATS JetStream?
*   **Decision**: Broadcast encoded audio packets to NATS JetStream topics.
*   **Why**: An in-memory broker is a single point of failure; if the server restarts, all live recording data is lost. NATS JetStream stores streams on disk, load-balances traffic across multiple worker pods, and lets us add new features (like live translation) with zero downtime and zero changes to the core ingestion server.

### Q7: Why does the Web dashboard use a background AudioWorklet instead of running on the main browser thread?
*   **Decision**: Offload audio Float32-to-Int16 conversions to a background audio thread (`pcm-processor.js`).
*   **Why**: The browser's main thread handles rendering the UI and user clicks. Running mathematical conversions on high-frequency audio data on the main thread causes UI stuttering and audio packet gaps. Offloading it to a background thread guarantees smooth, glitch-free recording.

### Q8: Why does the RTP/UDP adapter spawn exactly 4 parallel worker goroutines to read off the socket?
*   **Decision**: Parallelize UDP socket ingestion using a worker pool.
*   **Why**: Reading UDP packets from the OS is a blocking operation. Under high call traffic, a single reader goroutine gets bottlenecked, causing the OS kernel to drop packets. Spawning 4 workers lets multiple CPU cores process incoming packets in parallel.

### Q9: Why did we build a custom Polyphase Resampler instead of using simple Linear Interpolation for production?
*   **Decision**: Use Polyphase FIR Sinc filtering to handle all internal audio resampling.
*   **Why**: Linear interpolation is fast but introduces high-frequency aliasing noise, which sounds like metallic static and degrades Speech-to-Text accuracy. Polyphase filtering mathematically filters out this aliasing, delivering clean, high-fidelity voice.

### Q10: Why does the client-side UDP sender resample, compress to Opus, and package with RED before sending?
*   **Decision**: Perform resampling, Opus compression, and RED packing on the client device before transmission.
*   **Why**: Streaming raw PCM over the internet would require 1.5 Mbps of upload bandwidth per call, which would quickly saturate home/office network upload limits (often capped at 10-20 Mbps) and crash the server's network card. Compressing to Opus shrinks bandwidth by 32x (down to 24kbps), and wrapping in RED protects the audio packets from getting lost over unstable UDP network connections.

### Q11: Why did we choose 48kHz specifically as our target sample rate?
*   **Decision**: Standardize on 48kHz for the internal pipeline rate.
*   **Why**: 
    1.  **Opus Native Matching**: The Opus audio codec (the industry standard for VoIP) internally runs its mathematical algorithms at 48kHz for high-quality audio.
    2.  **Full Human Audio Spectrum**: According to the Nyquist theorem, a 48kHz sample rate captures sounds up to 24kHz. This covers 100% of the human hearing range (up to 20kHz), ensuring no critical speech consonants (like "sh" or "s") are clipped.

### Q12: Why do we increase the UDP socket kernel read buffer to 4MB?
*   **Decision**: Call `conn.SetReadBuffer(4 * 1024 * 1024)` on the UDP connection immediately after binding.
*   **Why**: By default, operating systems allocate very small socket buffers (often 64KB to 256KB) for UDP ports. If there is a massive burst of incoming calls and the Go application experiences a tiny scheduling delay or Garbage Collection pause, this small buffer overflows in microseconds, causing the OS kernel to silently drop packets. Setting a 4MB buffer acts as a giant waiting room in RAM that can hold over 28,000 packets, ensuring no voice data is dropped at the OS boundary.

### Q13: Why do we resample and encode on the UDP client (sender/main.go) but stream raw PCM on the WebSocket client (app.js)?
*   **Decision**: Client-side resample/encode for UDP streams, server-side resample/encode for TCP/WebSocket streams.
*   **Why**: 
    1.  **Network Protocol Guarantees**: UDP is unreliable and subject to packet loss, so the client must compress the audio to Opus to fit MTU limits and wrap it in RED redundancy to heal drops. WebSockets run on TCP, which guarantees 100% reliable, in-order packet delivery at the OS kernel layer, making client-side RED or custom packetization completely unnecessary.
    2.  **Browser Sandbox Limitations**: Browsers do not have native JS APIs to pack RED redundancy or compile manual RTP packets. Implementing it in `app.js` would require running a heavy WebAssembly (WASM) Opus build, which increases webpage load sizes and drains client CPU/battery. Offloading normalization to the server keeps the web client lightweight and performant.