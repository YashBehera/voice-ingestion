package ingestion

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"sync"
	"voice-ingestion/internal/pipeline"
	"voice-ingestion/internal/router"

	"github.com/gorilla/websocket"
)

// WebSocketAdapter accepts PCM audio streaming over WebSocket connections.
type WebSocketAdapter struct {
	mu       sync.Mutex
	router   *router.SessionRouter
	upgrader websocket.Upgrader
	ctx      context.Context
	cancel   context.CancelFunc
	running  bool
}

// NewWebSocketAdapter creates a new WebSocket Ingestion Adapter.
func NewWebSocketAdapter(r *router.SessionRouter) *WebSocketAdapter {
	return &WebSocketAdapter{
		router: r,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				// Allow connection from any origin for the dashboard demo UI
				return true
			},
		},
	}
}

// ID returns the identifier of this ingestion source.
func (a *WebSocketAdapter) ID() string {
	return "websocket"
}

// Start initializes the adapter context.
func (a *WebSocketAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.ctx, a.cancel = context.WithCancel(ctx)
	a.running = true
	log.Println("WebSocket Ingestion Adapter started")
	return nil
}

// Stop cancels the context to stop any running connection handlers.
func (a *WebSocketAdapter) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}

	a.running = false
	if a.cancel != nil {
		a.cancel()
	}
	log.Println("WebSocket Ingestion Adapter stopped")
	return nil
}

// Handler returns the HTTP handler function for upgrading WebSocket requests.
func (a *WebSocketAdapter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		if !a.running {
			http.Error(w, "WebSocket Ingestion Adapter is not running", http.StatusServiceUnavailable)
			a.mu.Unlock()
			return
		}
		a.mu.Unlock()

		conn, err := a.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WebSocket upgrade failed: %v", err)
			return
		}
		defer conn.Close()

		// Get sample rate from query param (default: 48000)
		sampleRate := 48000
		if rateStr := r.URL.Query().Get("rate"); rateStr != "" {
			if parsedRate, err := strconv.Atoi(rateStr); err == nil && parsedRate > 0 {
				sampleRate = parsedRate
			}
		}

		// Read loop
		for {
			select {
			case <-a.ctx.Done():
				return
			default:
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					// Client disconnected or error occurred
					return
				}

				if msgType != websocket.BinaryMessage {
					continue
				}

				if len(data)%2 != 0 {
					continue
				}

				// Convert little-endian bytes to int16 PCM samples
				samplesCount := len(data) / 2
				samples := make([]int16, samplesCount)
				for i := 0; i < samplesCount; i++ {
					low := int16(data[2*i])
					high := int16(data[2*i+1])
					samples[i] = low | (high << 8)
				}

				// Extract session ID or default to "websocket"
				sessionID := r.URL.Query().Get("session_id")
				if sessionID == "" {
					sessionID = a.ID()
				}

				chunk := pipeline.AudioChunk{
					PCM:        samples,
					SampleRate: sampleRate,
					SourceID:   sessionID,
				}

				// Push chunk to session router for parallel processing
				_ = a.router.RouteChunk(chunk)
			}
		}
	}
}
