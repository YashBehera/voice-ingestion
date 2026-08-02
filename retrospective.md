# Post-Mortem & Retrospective

Technical hurdles and design course-corrections encountered during development.

---

## 1. Cgo Portability vs Pure Go Opus

*   **Assumption**: Use a pure Go Opus package (like `pion/opus`) to achieve simple Go cross-compilation without host dependencies or Cgo dynamic links.
*   **Reality**: Pure Go encoders are still missing key features like VoIP profiles, DTX, and target bitrate adjustments. Standard production deployments still run Xiph's C library. The Go binding `gopkg.in/hraban/opus.v2` requires `libopus` headers.
*   **Correction**: Switched to a multi-stage Dockerfile compilation. The builder installs developer packages (`gcc`, `libopus-dev`) to compile the binary, while the runner runtime uses a slim image containing only the dynamically linked libraries.
*   **Lesson**: Don't try to rewrite complex codecs in Go. Solve compile-time portability inside standard, reproducible Docker builder layers.

---

## 2. Downstream Backpressure Blocking the Broker

*   **Assumption**: Use standard Go channel writes to broadcast audio frames to consumers, expecting they would read fast enough.
*   **Reality**: If one consumer lags (e.g. blocking file writes or slow network logs), a blocking write to that consumer's channel blocks the main broker routine. This immediately starves all other healthy consumers (transcriber, analytics), violating isolation requirements.
*   **Correction**: Replaced standard channels with a thread-safe `BoundedQueue` using a non-blocking select drop-oldest write path. When a consumer's queue is full, the oldest frame is discarded to make room for the new frame.
*   **Lesson**: In media distribution, slow consumers must drop packets. Bounding queues and using non-blocking writes is required to guarantee isolation.

---

## 3. Host Makefile Dependencies

*   **Assumption**: Developers would run test suites and compiles natively on the host machine using local compiler setups.
*   **Reality**: The host developer machine was missing Xcode Command Line Tools, causing `/usr/bin/make` targets to fail instantly.
*   **Correction**: Delegated all test and run actions inside Docker containers by configuring the Makefile targets to map directly to Docker.
*   **Lesson**: Never assume a uniform host developer setup. Keeping compile environments containerized ensures zero-dependency setups work for every reviewer.
