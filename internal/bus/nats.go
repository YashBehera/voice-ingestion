package bus

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
type subscription struct {
	topic   string
	group   string
	handler HandlerFunc
}

// ConsumerStats holds the metrics reported by microservices
type ConsumerStats struct {
	ID        string    `json:"id"`
	Len       int       `json:"len"`
	Cap       int       `json:"cap"`
	Dropped   int64     `json:"dropped"`
	Timestamp time.Time `json:"-"`
}

// Global thread-safe mutex registry to serialize TCP socket writes and prevent frame interleaving corruption
var writeMutexes sync.Map

// NATSBus implements a high-throughput, TCP-socket-networked event bus.
type NATSBus struct {
	mu         sync.Mutex
	subs       []*subscription
	groupRR    map[string]*uint64
	closed     bool
	msgCount   int64
	bytesCount int64

	// TCP network support for out-of-process multi-service communication
	listener net.Listener
	clients  []net.Conn
	conn     net.Conn // client connection

	// Distributed consumer telemetry tracking
	statsMutex sync.Mutex
	statsMap   map[string]ConsumerStats
}

// NewNATSBus initializes a networked TCP broker on port 4222, falling back to client mode.
func NewNATSBus() *NATSBus {
	b := &NATSBus{
		subs:     make([]*subscription, 0),
		groupRR:  make(map[string]*uint64),
		clients:  make([]net.Conn, 0),
		statsMap: make(map[string]ConsumerStats),
	}

	// Try to start TCP Server on NATS default port (4222)
	l, err := net.Listen("tcp", "127.0.0.1:4222")
	if err == nil {
		b.listener = l
		go b.runServer()
		log.Println("[NATS TCP Broker] Running embedded central TCP server on 127.0.0.1:4222")

		// Register stats collector subscriber internally on the server broker
		_ = b.Subscribe("media.metrics.*", "stats-collector", func(msg Message) error {
			log.Printf("[NATS Debug] Received metrics packet: Topic=%s Payload=%s", msg.Topic, string(msg.Payload))
			var stats ConsumerStats
			if err := json.Unmarshal(msg.Payload, &stats); err == nil {
				log.Printf("[NATS Debug] Successfully parsed stats: %+v", stats)
				stats.Timestamp = time.Now()
				b.statsMutex.Lock()
				b.statsMap[stats.ID] = stats
				b.statsMutex.Unlock()
			} else {
				log.Printf("[NATS Debug] Unmarshal error: %v", err)
			}
			return nil
		})
	} else {
		// Port already in use, try to connect to the running server as a client
		conn, err := net.DialTimeout("tcp", "127.0.0.1:4222", 1*time.Second)
		if err == nil {
			b.conn = conn
			go b.runClient()
			log.Println("[NATS TCP Broker] Connected to central TCP server on 127.0.0.1:4222")
		} else {
			log.Println("[NATS TCP Broker] Port 4222 bound but server unresponsive. Running in isolated in-memory mode.")
		}
	}

	return b
}

func (b *NATSBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true

	if b.listener != nil {
		_ = b.listener.Close()
		for _, c := range b.clients {
			_ = c.Close()
			writeMutexes.Delete(c)
		}
	}
	if b.conn != nil {
		_ = b.conn.Close()
		writeMutexes.Delete(b.conn)
	}
	return nil
}

func (b *NATSBus) Publish(topic string, payload []byte) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("bus is closed")
	}

	// If we are a client process, write the published frame up to the central broker
	if b.conn != nil {
		b.mu.Unlock()
		return b.sendTCPFrame(b.conn, topic, payload)
	}

	atomic.AddInt64(&b.msgCount, 1)
	atomic.AddInt64(&b.bytesCount, int64(len(payload)))

	// Dispatch to NATS subscribers (this includes remote clients registered in b.subs!)
	b.dispatchLocal(topic, payload)
	b.mu.Unlock()

	return nil
}

func (b *NATSBus) Subscribe(topic string, group string, handler HandlerFunc) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("bus is closed")
	}

	sub := &subscription{
		topic:   topic,
		group:   group,
		handler: handler,
	}
	b.subs = append(b.subs, sub)

	// If we are a client process, register this subscription filter with the server
	if b.conn != nil {
		regTopic := fmt.Sprintf("SUB:%s:%s", topic, group)
		_ = b.sendTCPFrame(b.conn, regTopic, []byte{})
	}

	return nil
}

func (b *NATSBus) GetConsumerStats() map[string]ConsumerStats {
	b.statsMutex.Lock()
	defer b.statsMutex.Unlock()
	log.Printf("[NATS Debug] GetConsumerStats called. statsMap size: %d", len(b.statsMap))
	m := make(map[string]ConsumerStats)
	for k, v := range b.statsMap {
		log.Printf("[NATS Debug] Map entry: key=%s value=%+v age=%v", k, v, time.Since(v.Timestamp))
		if time.Since(v.Timestamp) < 2*time.Second {
			m[k] = v
		}
	}
	return m
}

func (b *NATSBus) sendTCPFrame(conn net.Conn, topic string, payload []byte) error {
	// Acquire connection-specific mutex to prevent interleaved writes
	val, _ := writeMutexes.LoadOrStore(conn, &sync.Mutex{})
	mu := val.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	topicBytes := []byte(topic)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(topicBytes)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	_, err := conn.Write(header)
	if err != nil {
		return err
	}
	_, err = conn.Write(topicBytes)
	if err != nil {
		return err
	}
	_, err = conn.Write(payload)
	return err
}

func (b *NATSBus) dispatchLocal(topic string, payload []byte) {
	msg := Message{
		ID:        fmt.Sprintf("msg-%d", atomic.LoadInt64(&b.msgCount)),
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	groups := make(map[string][]*subscription)
	for _, sub := range b.subs {
		if b.matchTopic(sub.topic, topic) {
			groups[sub.group] = append(groups[sub.group], sub)
		}
	}

	for groupName, subList := range groups {
		if len(subList) == 0 {
			continue
		}

		rrPtr, exists := b.groupRR[groupName]
		if !exists {
			var val uint64
			rrPtr = &val
			b.groupRR[groupName] = rrPtr
		}
		idx := atomic.AddUint64(rrPtr, 1) % uint64(len(subList))
		targetSub := subList[idx]

		go func(s *subscription, m Message) {
			_ = s.handler(m)
		}(targetSub, msg)
	}
}

func (b *NATSBus) runServer() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.clients = append(b.clients, conn)
		b.mu.Unlock()
		go b.handleServerClient(conn)
	}
}

func (b *NATSBus) handleServerClient(conn net.Conn) {
	defer func() {
		conn.Close()
		writeMutexes.Delete(conn)
	}()

	var connSubs []struct {
		topic string
		group string
	}

	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(conn, header)
		if err != nil {
			break
		}

		topicLen := binary.BigEndian.Uint32(header[0:4])
		payloadLen := binary.BigEndian.Uint32(header[4:8])

		topicBytes := make([]byte, topicLen)
		_, err = io.ReadFull(conn, topicBytes)
		if err != nil {
			break
		}
		topic := string(topicBytes)

		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(conn, payload)
		if err != nil {
			break
		}

		// Handle client subscription commands: SUB:<topic>:<group>
		if len(topic) > 4 && topic[:4] == "SUB:" {
			parts := strings.Split(topic, ":")
			if len(parts) == 3 {
				subTopic := parts[1]
				subGroup := parts[2]

				b.mu.Lock()
				sub := &subscription{
					topic: subTopic,
					group: subGroup,
					handler: func(msg Message) error {
						return b.sendTCPFrame(conn, msg.Topic, msg.Payload)
					},
				}
				b.subs = append(b.subs, sub)
				b.mu.Unlock()

				connSubs = append(connSubs, struct {
					topic string
					group string
				}{topic: subTopic, group: subGroup})
			}
			continue
		}

		// Re-broadcast incoming published message to all subscribers
		b.mu.Lock()
		atomic.AddInt64(&b.msgCount, 1)
		atomic.AddInt64(&b.bytesCount, int64(len(payload)))
		b.dispatchLocal(topic, payload)
		b.mu.Unlock()
	}

	// Clean up remote subscriptions on connection drop
	b.mu.Lock()
	defer b.mu.Unlock()
	var activeSubs []*subscription
	for _, sub := range b.subs {
		keep := true
		for _, cs := range connSubs {
			if sub.topic == cs.topic && sub.group == cs.group {
				keep = false
				break
			}
		}
		if keep {
			activeSubs = append(activeSubs, sub)
		}
	}
	b.subs = activeSubs
}

func (b *NATSBus) runClient() {
	defer writeMutexes.Delete(b.conn)
	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(b.conn, header)
		if err != nil {
			break
		}

		topicLen := binary.BigEndian.Uint32(header[0:4])
		payloadLen := binary.BigEndian.Uint32(header[4:8])

		topicBytes := make([]byte, topicLen)
		_, err = io.ReadFull(b.conn, topicBytes)
		if err != nil {
			break
		}
		topic := string(topicBytes)

		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(b.conn, payload)
		if err != nil {
			break
		}

		b.mu.Lock()
		atomic.AddInt64(&b.msgCount, 1)
		atomic.AddInt64(&b.bytesCount, int64(len(payload)))
		b.dispatchLocal(topic, payload)
		b.mu.Unlock()
	}
}

func (b *NATSBus) matchTopic(pattern, topic string) bool {
	if pattern == "*" || pattern == ">" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == topic
	}
	patParts := strings.Split(pattern, ".")
	topParts := strings.Split(topic, ".")
	if len(patParts) != len(topParts) {
		return false
	}
	for i := range patParts {
		if patParts[i] != "*" && patParts[i] != topParts[i] {
			return false
		}
	}
	return true
}

func (b *NATSBus) GetMetrics() (int64, int64) {
	return atomic.LoadInt64(&b.msgCount), atomic.LoadInt64(&b.bytesCount)
}
