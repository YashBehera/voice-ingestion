package router

import (
	"fmt"
	"sync"
	"voice-ingestion/internal/bus"
	"voice-ingestion/internal/pipeline"
)

// SessionWorker wraps an individual Pipeline instance dedicated to a single Call/Session ID.
type SessionWorker struct {
	SessionID string
	Pipeline  *pipeline.Pipeline
}

// SessionRouter routes audio inputs to dedicated pipeline workers and broadcasts encoded outputs to NATS.
type SessionRouter struct {
	mu         sync.Mutex
	bitrate    int
	redDepth   int
	bus        bus.EventBus
	workers    map[string]*SessionWorker
	publisher  pipeline.PacketPublisher
}

type busPublisher struct {
	bus bus.EventBus
}

func (p *busPublisher) Publish(packet pipeline.MediaPacket) {
	topic := fmt.Sprintf("media.session.%d", packet.SequenceNumber%10)
	payload := append([]byte{byte(packet.PayloadType)}, packet.Payload...)
	_ = p.bus.Publish(topic, payload)
}

// NewSessionRouter creates a new multi-tenant session router.
func NewSessionRouter(bitrate int, redDepth int, eventBus bus.EventBus) *SessionRouter {
	return &SessionRouter{
		bitrate:   bitrate,
		redDepth:  redDepth,
		bus:       eventBus,
		workers:   make(map[string]*SessionWorker),
		publisher: &busPublisher{bus: eventBus},
	}
}

// RouteChunk routes an incoming AudioChunk to its assigned SessionWorker pipeline.
func (r *SessionRouter) RouteChunk(chunk pipeline.AudioChunk) error {
	r.mu.Lock()
	worker, exists := r.workers[chunk.SourceID]
	if !exists {
		p, err := pipeline.NewPipeline(r.bitrate, r.redDepth, r.publisher)
		if err != nil {
			r.mu.Unlock()
			return fmt.Errorf("failed to create pipeline for session %s: %w", chunk.SourceID, err)
		}
		p.Start()
		worker = &SessionWorker{
			SessionID: chunk.SourceID,
			Pipeline:  p,
		}
		r.workers[chunk.SourceID] = worker
	}
	r.mu.Unlock()

	worker.Pipeline.PushPCM(chunk.PCM, chunk.SampleRate, chunk.SourceID)
	return nil
}

// Close gracefully stops all active session workers.
func (r *SessionRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, w := range r.workers {
		w.Pipeline.Stop()
	}
	r.workers = make(map[string]*SessionWorker)
}
