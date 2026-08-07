# Stage 1: Build the Go application
FROM golang:1.25-bookworm AS builder

# Install development headers for Opus and C compiler tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus-dev \
    libopusfile-dev \
    gcc \
    libc6-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependencies first for caching
COPY go.mod ./
# Note: we will generate go.sum inside the container or locally. Let's download packages
RUN go mod download

# Copy the entire workspace
COPY . .

# Build statically linked or dynamic (using libopus0) binaries
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/worker ./cmd/worker
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/sender ./cmd/sender
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/stt_service ./cmd/stt_service
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/recorder_service ./cmd/recorder_service
RUN CGO_ENABLED=1 GOOS=linux go build -o /app/analytics_service ./cmd/analytics_service

# Stage 2: Runtime image
FROM debian:bookworm-slim

# Install the runtime Opus shared library
RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 \
    libopusfile0 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries from the builder
COPY --from=builder /app/worker /app/worker
COPY --from=builder /app/sender /app/sender
COPY --from=builder /app/stt_service /app/stt_service
COPY --from=builder /app/recorder_service /app/recorder_service
COPY --from=builder /app/analytics_service /app/analytics_service
COPY --from=builder /app/static /app/static

# Create a recordings directory
RUN mkdir -p /app/recordings

# Expose HTTP control/WebSocket port and RTP/UDP port
EXPOSE 8080 5004/udp

# Set the default entry point to the worker
ENTRYPOINT ["/app/worker"]
