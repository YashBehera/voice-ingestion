package bus

import (
	"sync"
	"testing"
	"time"
)

func TestNATSBusPublishSubscribe(t *testing.T) {
	bus := NewNATSBus()
	defer bus.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	var receivedPayload string

	err := bus.Subscribe("media.session.*", "test-group", func(msg Message) error {
		receivedPayload = string(msg.Payload)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	err = bus.Publish("media.session.1", []byte("hello-opus"))
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if receivedPayload != "hello-opus" {
			t.Fatalf("Expected payload 'hello-opus', got '%s'", receivedPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message delivery")
	}
}
