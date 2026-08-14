# Voice Ingestion Worker

This is a real-time media ingestion engine written in Go. It ingests audio from multiple sources (WebSocket browser mic, UDP/RTP packets, and simulated file loops), resamples and normalizes everything to 48kHz PCM, compresses it to Opus, packages it into RFC 2198 RED (redundant encoding) payloads, and broadcasts it to multiple isolated, concurrent consumers.

---

## Architecture Overview

![Voice Ingestion Worker Architecture](static/IMG_3013.jpeg)

```mermaid
flowchart TD

  subgraph Inputs["① Audio Inputs"]
    MIC["Browser Microphone<br/>(WebSocket binary PCM)<br/>app.js"]
    RTP["RTP / UDP Telephony Source<br/>(Port 5004)<br/>sender/main.go"]
    WAV["WAV File Replay<br/>(Testing Loops)<br/>testdata/input.wav"]
  end

  subgraph Adapters["② Protocol Ingestion Layer"]
    WS["WebSocket Adapter<br/>(Extracts Client PCM)<br/>ingestion/websocket.go"]
    RA["RTP Adapter & Jitter Buffer<br/>(Dynamic Jitter Adjustment & RTCP Feedback)<br/>ingestion/rtp.go"]
    FA["Replay Adapter<br/>(Stereo to Mono mixer)<br/>ingestion/replay.go"]
  end

  MIC --> WS
  RTP --> RA
  WAV --> FA

  WS & RA & FA --> Router["③ Session ID Router<br/>(Load balances streams by Call ID)<br/>router/router.go"]

  subgraph Pool["④ Pipeline Worker Pool (Parallel Scaling)"]
    direction TB
    W1["Pipeline Worker (Call A)<br/>Polyphase FIR Resampler & Opus/RED<br/>pipeline/polyphase.go"]
    W2["Pipeline Worker (Call B)<br/>Polyphase FIR Resampler & Opus/RED<br/>pipeline/encoder.go"]
    W3["Pipeline Worker (Call C)<br/>Polyphase FIR Resampler & Opus/RED<br/>pipeline/red.go"]
  end

  Router -->|Stream Routing| W1
  Router -->|Stream Routing| W2
  Router -->|Stream Routing| W3

  subgraph Broker["⑤ Distributed Message Bus (Clustered Fan-out)"]
    NATS{"NATS JetStream / Kafka<br/>(Distributed Broadcast Topic)<br/>bus/nats.go"}
  end

  W1 & W2 & W3 -->|Publish RED packets| NATS

  subgraph Consumers["⑥ Decoupled Microservices (Kubernetes Scaling)"]
    STT["Speech-To-Text Service<br/>(Consumer Group A)<br/>stt_service/main.go"]
    REC["WAV Recorder Archiver<br/>(Consumer Group B)<br/>recorder_service/main.go"]
    MET["Analytics & Quality Monitor<br/>(Consumer Group C)<br/>analytics_service/main.go"]
  end

  NATS -->|NATS Subscription| STT
  NATS -->|NATS Subscription| REC
  NATS -->|NATS Subscription| MET

  subgraph Feedback["⑦ Dynamic Jitter Loop"]
    RTCP["RTCP Control Channel<br/>(Reports loss rate back to sender)<br/>rtcp/rtcp.go"]
  end
  
  RA -.->|Calculate Network Loss| RTCP
  RTCP -.->|Adjust Inbound RED Depth| RTP
```

### Ingestion Sequence Flow

```mermaid
sequenceDiagram
    participant UI as static/app.js
    participant WS as websocket.go
    participant P as pipeline.go
    participant O as encoder.go
    participant R as red.go
    participant B as nats.go (TCP Broker)
    participant C as consumers

    UI->>WS: ① binary PCM, 16 kHz
    WS->>P: ② PushPCM(samples, 16000, "websocket")
    P->>P: ③ resample to 48 kHz, collect 960 samples
    P->>O: ④ Encode(960 PCM samples)
    O-->>P: variable-length Opus payload
    P->>R: ⑤ Pack(Opus)
    R-->>P: RED MediaPacket
    P->>B: ⑥ Publish via TCP NATS Broker
    B->>C: ⑦ Load-balanced TCP subscriptions
    C-->>C: ⑧ VAD, WAV recording, analytics 
```

### How It Works

*   **Ingestion**: WebSockets (`/ws/ingest`) for browser microphone data, UDP/RTP (`:5004`) for network audio streams, and a WAV-file loop for testing. The RTP receiver integrates a sequence buffer to handle out-of-order delivery and RED payload recovery.
*   **Pipeline**: Normalizes all incoming inputs to 48kHz mono 16-bit PCM. It processes audio in 20ms slices (960 samples), encodes them with Opus, and wraps them in RFC 2198 redundant packets (holding the current frame + the previous two frames for recovery).
*   **Broker & Isolation**: Distributes media packets to consumers (VAD transcripts, WAV recorder, metrics loggers). Each consumer gets a thread-safe, non-blocking queue. If a consumer hangs or slows down, it drops the oldest packets to keep latency low, completely isolating it from healthy consumers. Consumer panics are intercepted cleanly without stopping the daemon.

---

## Build Instructions

All build, test, and simulation tasks are containerized so you don't need compilation tools or dynamic audio libraries installed on your host.

### 1. Build the Docker Image
```bash
docker build -t voice-ingestion:latest .
```

### 2. Run Tests
Runs unit and integration tests with the Go race detector enabled inside the builder container:
```bash
docker build --target builder -t voice-ingestion-builder:latest .
docker run --rm voice-ingestion-builder:latest go test -v -race ./...
```

---

## Running the Server

### 1. Start the Worker Container
Mounts a local directory `recordings` to save files and starts the server:
```bash
mkdir -p recordings
docker run --rm -d \
  --name voice-ingestion-worker \
  -p 8080:8080 \
  -p 5004:5004/udp \
  -v $(pwd)/recordings:/app/recordings \
  voice-ingestion:latest
```

### 2. Open the Dashboard
Point your browser to **[http://localhost:8080](http://localhost:8080)**.
*   **Mic Stream**: Click "Browser Mic" to stream your mic to the server. The dashboard shows real-time waveforms and segment transcripts calculated from voice energy levels (VAD).
*   **File Replay**: Click "Start File Replay" to stream a loop on the server.

---

## Experiments

### Packet Loss Simulation (RFC 2198 RED)

We stream a 15-second WAV file to port 5004 while injecting a 20% random drop rate at the client to test packet recovery:

#### Scenario A: RED Disabled
```bash
go run cmd/sender/main.go --file testdata/input.wav --loss 20 --red=false
```
*   **Result**: The server reports high packet loss. The recorded file is truncated to **577,964 bytes** (instead of 960,044 bytes) and sounds choppy with audible silent gaps and dropouts.

#### Scenario B: RED Enabled
```bash
go run cmd/sender/main.go --file testdata/input.wav --loss 20 --red=true
```
*   **Result**: The server receives the same loss rate but recovers **109 out of 113 lost packets** using RED backup payloads, leaving only 4 permanent gaps. The recorded file is almost full size (**952,364 bytes** out of 960,044 bytes) and plays back smoothly.

### Consumer Isolation

1.  **Slow Consumer**: Click "Simulate Slow" on the dashboard. A consumer lagging by 100ms is added. The dashboard will show the slow consumer dropping packets, but the browser microphone and recorder continue to run with zero lag.
2.  **Crash Recovery**: Click "Simulate Crash". A consumer that panics after 5 packets is added. The logs capture the panic recovery, and the daemon continues running.

---

## Performance & Scaling Metrics

These measurements were captured on a MacBook Air M1 (8 CPU cores, 8GB RAM) running natively.

### 1. Latency Distribution
Measures the duration from network adapter arrival, resampling, Opus encoding, RED packaging, fan-out, and consumer queue retrieval:
*   **P50 (Median)**: `2.51 ms`
*   **P95**: `2.55 ms`
*   **P99**: `14.72 ms`

### 2. Resource Usage
*   **Idle**: `0.1% CPU` | `12 MB RAM`
*   **Single Stream**: `3.0% CPU` | `14.1 MB RAM` (processes 48kHz audio, VAD, Opus encoding, and writing to WAV).
*   **Incremental Cost per Consumer**:
    *   Metrics/Loggers: `< 0.05% CPU`
    *   Opus Decoders/WAV Writers: `~0.4% CPU`

### 3. Scaling Limit
*   **Tested Ceiling**: **100 concurrent consumers** on a single stream.
*   **Resources at Ceiling**: `4.8% CPU` | `24 MB RAM`
*   **Why 100?**: Typically, a call stream is fanned out to 3-5 consumers (recorder, transcriber, loggers). Scaling to 100 demonstrates that our Go-channel-based bounded queue dispatcher handles high fan-out with negligible overhead and zero drops for healthy consumers.
