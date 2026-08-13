# Project Retrospective & Lessons Learned

This retrospective captures the real technical hurdles we faced during development, the assumptions we started with, and how we course-corrected.

---

### Story 1: The Cgo Portability Trap
*   **The Assumption**: We wanted to use a pure Go Opus package (like `pion/opus`) so developers could compile the project instantly on any computer without needing local audio libraries or C compilers.
*   **The Problem**: We quickly realized that pure Go Opus encoders are incomplete. They lack critical VoIP features like voice optimization profiles, Discontinuous Transmission (DTX), and target bitrate scaling. To get production-grade compression, we had to use the official C-based Xiph Opus library via Go bindings (`hraban/opus`).
*   **How we solved it**: We built a **multi-stage Dockerfile**. The "builder" stage installs `gcc` and `libopus-dev` to compile the Go binary with dynamic links, while the "runner" stage uses a clean, lightweight image with only the shared library dependencies.
*   **Lesson Learned**: Don't waste time trying to rewrite complex, decades-old codecs in Go. Instead, solve compile-time portability using standardized Docker builder environments.

### Story 2: The Lagging Consumer Freeze
*   **The Assumption**: We initially used standard Go channels to broadcast audio frames to all subscribers (WAV recorder, STT, analytics), assuming they would always read data fast enough.
*   **The Problem**: During testing, we simulated a slow consumer (like a slow hard drive write). Because Go channels block when full, the slow consumer blocked the entire dispatcher loop. This starved the healthy consumers (like the Speech-to-Text engine), causing them to drop audio frames too.
*   **How we solved it**: We replaced standard channels with a thread-safe, custom **`BoundedQueue`** using a non-blocking `select` drop-oldest write path. Now, if a consumer falls behind, it simply drops its oldest audio packets to catch up, keeping the rest of the system running in real-time.
    *   *Note the clear distinction between drop types:*
        1.  **Network Drops (UDP)**: Restored automatically over the internet using RFC 2198 RED backup frames.
        2.  **Queue Drops (Backpressure)**: Intentionally discarded permanently. If we tried to inject RED backups to heal slow consumer drops, we would send even more data to an already overloaded consumer, worsening the congestion.
*   **Lesson Learned**: In real-time media systems, slow consumers must drop packets. You cannot let one slow process freeze the entire streaming pipeline.

### Story 3: The "Works on My Machine" Makefile
*   **The Assumption**: We expected developers to run test suites and compile binaries directly on their host machines using local compiler tools.
*   **The Problem**: Reviewers and developer environments frequently lacked Xcode Command Line Tools or Linux build-essentials, causing local `make build` and `make test` commands to fail immediately.
*   **How we solved it**: We moved the entire compilation and testing loop inside Docker. We rewrote the `Makefile` so that running `make test` compiles the builder environment and runs tests inside a container, requiring only Docker to be installed on the host.
*   **Lesson Learned**: Never assume all developers have identical local compiler toolchains. Keep all building and testing encapsulated in Docker for a seamless setup experience.

### Story 4: The RED Recovery Window Limit
*   **The Assumption**: We assumed that because we are using RFC 2198 RED redundancy, we could recover any lost network packet, regardless of how long ago it occurred or how many packets in a row were lost.
*   **The Problem**: If a packet is lost too far in the past (e.g., Packet #101 is lost and we are already receiving Packet #120), trying to recover it recursively would waste bandwidth and cause massive lag (400ms delay), breaking the real-time constraint of the conversation. RED packets are limited to a depth of 2 (only carrying backup for the previous 2 frames), and our sequence buffer (`RedReceiver`) is capped at 8 packets (160ms window).
*   **How we solved it**: We enforced a strict time-bound window. If a packet is lost and cannot be recovered within the 8-packet window, the receiver gives up on the packet and invokes Opus Packet Loss Concealment (`DecodeLost`) to generate synthetic background noise, immediately skipping forward to play the live packet in real time.
*   **Lesson Learned**: In real-time communications, playing a fresh packet on time is always better than waiting for or recursively recovering an ancient lost packet. Let PLC conceal old losses rather than introducing lag.
