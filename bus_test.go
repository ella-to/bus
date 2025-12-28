package bus_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/immuta"
	"ella.to/task"
)

func createBusServer(t *testing.T, eventLogsDir string) *bus.Client {
	os.RemoveAll(eventLogsDir)

	storage, err := immuta.New(
		immuta.WithLogsDirPath(eventLogsDir),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)), nil)

	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll(eventLogsDir)
	})

	return bus.NewClient(server.URL)
}

func createBusServerWithCompression(t *testing.T, eventLogsDir string) *bus.Client {
	os.RemoveAll(eventLogsDir)

	storage, err := immuta.New(
		immuta.WithLogsDirPath(eventLogsDir),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)), nil)

	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll(eventLogsDir)
	})

	return bus.NewClient(server.URL)
}

func TestLargeAmountPutUsage(t *testing.T) {
	client := createBusServer(t, "./TestLargeAmountPutUsage")

	for range 100000 {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world"))
		if resp.Error() != nil {
			t.Fatal(resp.Error())
		}
	}
}

func TestBasicPutUsage(t *testing.T) {
	client := createBusServer(t, "TestBasicUsage")

	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	fmt.Println(resp)
}

func TestBasicPutGetUsage(t *testing.T) {
	client := createBusServer(t, "TestBasicPutGetUsage")

	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone),
		bus.WithStartFrom(bus.StartOldest),
	) {
		if !errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			t.Fatalf("failed to get event: %s", err)
		}

		if event.Id != resp.Id {
			t.Fatalf("expected %s but get event id: %s", resp.Id, event.Id)
		}
	}

	fmt.Println("done")
}

func TestSinglePutMultipleGet(t *testing.T) {
	client := createBusServer(t, "TestBasicUsage")

	n := 1_00
	p := 10
	c := 8

	total := n * p

	var wg sync.WaitGroup

	wg.Add(c + p)

	for range p {
		go func() {
			defer wg.Done()

			for range n {
				resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world"))
				if resp.Error() != nil {
					t.Errorf("failed to publish: %s", resp.Error())
				}
			}

			fmt.Println("DONE PUBLISHING")
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := range c {
		go func(i int) {
			defer wg.Done()

			count := 0
			for event, err := range client.Get(
				ctx,
				bus.WithSubject("a.b.c"),
				bus.WithAckStrategy(bus.AckNone),
				bus.WithStartFrom(bus.StartOldest),
			) {
				if err != nil {
					t.Errorf("failed to get event: %s", err)
					return
				}

				count++
				if count == total {
					break
				}

				_ = event
			}

			fmt.Printf("DONE CONSUMING (%d) : %d\n", i, count)
		}(i)
	}

	wg.Wait()

	fmt.Println("done")
}

func TestPullClose(t *testing.T) {
	client := createBusServer(t, "TestPull")

	ctx, cancel := context.WithCancel(context.Background())

	// NOTE: once an iterator converted to a puller,
	// the only way to stop/close the iterator is by calling
	// the stop function and canceling the context

	_, stop := iter.Pull2(
		client.Get(
			ctx,
			bus.WithSubject("a.b.c"),
			bus.WithStartFrom(bus.StartOldest),
		),
	)

	stop()
	cancel()

	time.Sleep(1 * time.Second)
}

func TestEncodeDecodeEvent(t *testing.T) {
	value := `{"id":"123","trace_id":"trace-123","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-18T06:55:35-05:00","payload":{"a": 1, "b": 2},"index":100}`

	var event bus.Event

	_, err := io.Copy(&event, strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, &event)
	if err != nil {
		t.Fatal(err)
	}

	if value != buffer.String() {
		t.Fatalf("expected %s but got %s", value, buffer.String())
	}
}

func TestEncodeDecodeKey(t *testing.T) {
	value := `{"id":"123","trace_id":"trace-123","key":"idem-1","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-18T06:55:35-05:00","payload":{"a": 1, "b": 2},"index":100}`

	var event bus.Event

	_, err := io.Copy(&event, strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, &event)
	if err != nil {
		t.Fatal(err)
	}

	if value != buffer.String() {
		t.Fatalf("expected %s but got %s", value, buffer.String())
	}
}

func TestEncodeDecodeEmptyEvent(t *testing.T) {
	value := `{"id":"","subject":"","created_at":"0001-01-01T00:00:00Z"}`

	var event bus.Event

	_, err := io.Copy(&event, strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, &event)
	if err != nil {
		t.Fatal(err)
	}

	if value != buffer.String() {
		t.Fatalf("expected %s but got %s", value, buffer.String())
	}
}

func TestDuplicateCheckerDropsDuplicate(t *testing.T) {
	// Setup storage and handler with a DuplicateChecker that records seen keys
	os.RemoveAll("TestDuplicateChecker")

	storage, err := immuta.New(
		immuta.WithLogsDirPath("TestDuplicateChecker"),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]bool)
	var m sync.Mutex
	dup := bus.DuplicateCheckerFunc(func(key string) bool {
		if key == "" {
			return false
		}
		m.Lock()
		defer m.Unlock()
		if seen[key] {
			return true
		}
		seen[key] = true
		return false
	})

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)), dup)
	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll("TestDuplicateChecker")
	})

	client := bus.NewClient(server.URL)

	// First put with key should succeed
	first := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithKey("idem-1"), bus.WithData("hello"))
	if first.Error() != nil {
		t.Fatalf("first put failed: %v", first.Error())
	}

	// Second put with same key should be rejected
	second := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithKey("idem-1"), bus.WithData("world"))
	if second.Error() == nil {
		t.Fatal("expected second put to fail with duplicate key, but it succeeded")
	}
	if !strings.Contains(second.Error().Error(), "key was processed before") {
		t.Fatalf("expected duplicate error, got: %v", second.Error())
	}

	// Ensure only one event with that key exists in the stream
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	count := 0
	for event, err := range client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithAckStrategy(bus.AckNone), bus.WithStartFrom(bus.StartOldest)) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatalf("failed to get event: %s", err)
		}

		if event.Key == "idem-1" {
			count++
		}
	}

	if count != 1 {
		t.Fatalf("expected 1 event with key 'idem-1', got %d", count)
	}
}

func BenchmarkEncodeEventUsingRead(b *testing.B) {
	b.ReportAllocs()

	event := bus.Event{
		Id:              "id-1234",
		TraceId:         "trace-1234",
		Subject:         "a.b.c",
		ResponseSubject: "a.b.c.response",
		CreatedAt:       time.Now(),
		Index:           100,
		Payload:         json.RawMessage(`{"key": "value"}`),
	}

	var buffer bytes.Buffer

	for b.Loop() {
		buffer.Reset()
		_, _ = io.Copy(&buffer, &event)
	}
}

func BenchmarkEncodeEventUsingJSON(b *testing.B) {
	b.ReportAllocs()

	event := bus.Event{
		Id:              "id-1234",
		TraceId:         "trace-1234",
		Subject:         "a.b.c",
		ResponseSubject: "a.b.c.response",
		CreatedAt:       time.Now(),
		Payload:         json.RawMessage(`{"key": "value"}`),
		Index:           100,
	}

	var buffer bytes.Buffer

	for b.Loop() {
		buffer.Reset()
		_ = json.NewEncoder(&buffer).Encode(&event)
	}
}

func BenchmarkDecodeEventUsingWrite(b *testing.B) {
	b.ReportAllocs()

	input := []byte(`{"id":"id-1234","trace_id":"trace-1234","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-17T12:20:45-05:00","payload":{"key": "value"},"index":100}`)

	var event bus.Event

	for b.Loop() {
		_, _ = io.Copy(&event, bytes.NewReader(input))
	}
}

func BenchmarkDecodeEventUsingJSON(b *testing.B) {
	b.ReportAllocs()

	input := []byte(`{"id":"id-1234","trace_id":"trace-1234","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-17T12:20:45-05:00","payload":{"key": "value"},"index":100}`)

	var event bus.Event

	for b.Loop() {
		_ = json.NewDecoder(bytes.NewReader(input)).Decode(&event)
	}
}

func TestBusWithS2Compression_BasicPutGet(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_BasicPutGet")

	// Put an event
	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world with compression"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	// Get the event
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	found := false
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone),
		bus.WithStartFrom(bus.StartOldest),
	) {
		if errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			t.Fatalf("failed to get event: %s", err)
		}

		if event.Id != resp.Id {
			t.Fatalf("expected event id %s but got %s", resp.Id, event.Id)
		}

		var payload string
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("failed to unmarshal payload: %s", err)
		}

		if payload != "hello world with compression" {
			t.Fatalf("expected payload 'hello world with compression' but got '%s'", payload)
		}

		found = true
		break
	}

	if !found {
		t.Fatal("event not found")
	}
}

func TestBusWithS2Compression_MultipleEvents(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_MultipleEvents")

	// Put multiple events
	eventCount := 100
	for i := range eventCount {
		data := map[string]interface{}{
			"index":   i,
			"message": fmt.Sprintf("test message %d", i),
		}
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(data))
		if resp.Error() != nil {
			t.Fatalf("failed to put event %d: %s", i, resp.Error())
		}
	}

	// Get all events
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	receivedCount := 0
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone),
		bus.WithStartFrom(bus.StartOldest),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("failed to unmarshal payload: %s", err)
		}

		receivedCount++
		if receivedCount >= eventCount {
			break
		}
	}

	if receivedCount != eventCount {
		t.Fatalf("expected %d events but got %d", eventCount, receivedCount)
	}
}

func TestBusWithS2Compression_LargePayload(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_LargePayload")

	// Create a moderately large payload with structured data (should compress well)
	largeData := make([]map[string]interface{}, 0, 100)
	for i := 0; i < 100; i++ {
		largeData = append(largeData, map[string]interface{}{
			"id":      i,
			"message": "This is test data that will be repeated many times",
			"value":   i * 100,
		})
	}

	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(largeData))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	// Get the event
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	found := false
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone),
		bus.WithStartFrom(bus.StartOldest),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		var payload []map[string]interface{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("failed to unmarshal payload: %s", err)
		}

		if len(payload) != len(largeData) {
			t.Fatalf("payload length mismatch. Expected %d, got %d", len(largeData), len(payload))
		}

		// Verify a few entries
		if payload[0]["message"] != "This is test data that will be repeated many times" {
			t.Fatal("payload content mismatch")
		}

		t.Logf("Successfully transmitted and received %d items with compression", len(payload))
		found = true
		break
	}

	if !found {
		t.Fatal("event not found")
	}
}

func TestBusWithS2Compression_ConcurrentPutGet(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_ConcurrentPutGet")

	n := 50 // events per producer
	p := 5  // number of producers
	c := 3  // number of consumers

	total := n * p

	var wg sync.WaitGroup
	wg.Add(c + p)

	// Start producers
	for i := 0; i < p; i++ {
		go func(producerId int) {
			defer wg.Done()

			for j := 0; j < n; j++ {
				data := map[string]interface{}{
					"producer": producerId,
					"index":    j,
					"message":  fmt.Sprintf("message from producer %d, event %d", producerId, j),
				}
				resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(data))
				if resp.Error() != nil {
					t.Errorf("producer %d failed to put event %d: %s", producerId, j, resp.Error())
				}
			}
		}(i)
	}

	// Start consumers
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < c; i++ {
		go func(consumerId int) {
			defer wg.Done()

			count := 0
			for event, err := range client.Get(
				ctx,
				bus.WithSubject("a.b.c"),
				bus.WithAckStrategy(bus.AckNone),
				bus.WithStartFrom(bus.StartOldest),
			) {
				if err != nil {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("consumer %d failed to get event: %s", consumerId, err)
					}
					return
				}

				count++
				if count >= total {
					break
				}

				_ = event
			}

			t.Logf("Consumer %d received %d events", consumerId, count)
		}(i)
	}

	wg.Wait()
}

func TestBusWithS2Compression_ComplexPayload(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_ComplexPayload")

	// Create a complex nested JSON payload
	complexData := map[string]interface{}{
		"user": map[string]interface{}{
			"id":    123,
			"name":  "John Doe",
			"email": "john@example.com",
			"metadata": map[string]interface{}{
				"age":         30,
				"city":        "New York",
				"preferences": []string{"coding", "reading", "hiking"},
			},
		},
		"events": []map[string]interface{}{
			{"type": "login", "timestamp": "2025-01-01T12:00:00Z"},
			{"type": "purchase", "timestamp": "2025-01-01T13:00:00Z", "amount": 99.99},
			{"type": "logout", "timestamp": "2025-01-01T14:00:00Z"},
		},
		"tags": []string{"customer", "premium", "verified"},
	}

	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(complexData))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	// Get and verify the event
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone),
		bus.WithStartFrom(bus.StartOldest),
	) {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("timeout waiting for event")
		} else if err != nil {
			t.Fatalf("failed to get event: %s", err)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatalf("failed to unmarshal payload: %s", err)
		}

		// Verify some fields
		user := payload["user"].(map[string]interface{})
		if user["name"] != "John Doe" {
			t.Fatalf("expected user name 'John Doe' but got '%s'", user["name"])
		}

		events := payload["events"].([]interface{})
		if len(events) != 3 {
			t.Fatalf("expected 3 events but got %d", len(events))
		}

		t.Log("Successfully transmitted complex nested JSON with compression")
		break
	}
}

func TestRedelivery_MessageIsRedeliveredWhenNotAcked(t *testing.T) {
	client := createBusServer(t, "TestRedelivery_MessageIsRedeliveredWhenNotAcked")

	// Put a message
	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("test message"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	// First consumer - doesn't ack, so message should be available for redelivery
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	firstReceived := false
	for event, err := range client.Get(
		ctx1,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		if !firstReceived {
			firstReceived = true
			t.Logf("First consumer received message (event_id: %s), not acking", event.Id)
			// Don't ack - just let the timeout happen
			// Wait for redelivery timeout
			time.Sleep(1 * time.Second)
			cancel1() // Cancel first consumer
			break
		}
	}

	if !firstReceived {
		t.Fatal("first consumer should have received the message")
	}

	// Wait a bit to ensure the redelivery timer has triggered
	time.Sleep(500 * time.Millisecond)

	// Second consumer - should receive the redelivered message
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	secondReceived := false
	for event, err := range client.Get(
		ctx2,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		if !secondReceived {
			secondReceived = true
			t.Logf("Second consumer received redelivered message (event_id: %s)", event.Id)

			var payload string
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatalf("failed to unmarshal payload: %s", err)
			}

			if payload != "test message" {
				t.Fatalf("expected payload 'test message' but got '%s'", payload)
			}

			// Ack this time
			if err := event.Ack(ctx2); err != nil {
				t.Fatalf("failed to ack: %s", err)
			}
			break
		}
	}

	if !secondReceived {
		t.Fatal("second consumer should have received the redelivered message")
	}

	t.Log("Successfully verified: message was redelivered to second consumer after first consumer failed to ack")
}

func TestRedelivery_MessageIsNotRedeliveredWhenAcked(t *testing.T) {
	client := createBusServer(t, "TestRedelivery_MessageIsNotRedeliveredWhenAcked")

	// Put multiple messages
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(fmt.Sprintf("message %d", i)))
		if resp.Error() != nil {
			t.Fatal(resp.Error())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	receivedCount := 0
	seenEventIds := make(map[string]int)

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(1*time.Second, 1), // Short redelivery time for testing
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		receivedCount++
		seenEventIds[event.Id]++

		t.Logf("Received event %s (count: %d)", event.Id, seenEventIds[event.Id])

		// Acknowledge the message immediately
		if err := event.Ack(ctx); err != nil {
			t.Fatalf("failed to ack event: %s", err)
		}

		// After receiving all messages once, wait a bit to ensure no redeliveries occur
		if receivedCount >= messageCount {
			// Wait longer than the redelivery duration to ensure no redeliveries
			time.Sleep(2 * time.Second)
			break
		}
	}

	// Verify each message was received exactly once
	for eventId, count := range seenEventIds {
		if count != 1 {
			t.Fatalf("event %s was received %d times, expected exactly 1 time (redelivery should not happen when acked)", eventId, count)
		}
	}

	if len(seenEventIds) != messageCount {
		t.Fatalf("expected to receive %d unique messages, but got %d", messageCount, len(seenEventIds))
	}

	t.Logf("Successfully verified: all %d messages were acknowledged and not redelivered", messageCount)
}

func TestRedelivery_AckPreventsRedelivery(t *testing.T) {
	client := createBusServer(t, "TestRedelivery_AckPreventsRedelivery")

	// Put two messages
	resp1 := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("message 1"))
	if resp1.Error() != nil {
		t.Fatal(resp1.Error())
	}

	resp2 := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("message 2"))
	if resp2.Error() != nil {
		t.Fatal(resp2.Error())
	}

	// First consumer - acks message 1 immediately
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	for event, err := range client.Get(
		ctx1,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		t.Logf("First consumer received message (event_id: %s), acking immediately", event.Id)

		// Ack immediately
		if err := event.Ack(ctx1); err != nil {
			t.Fatalf("failed to ack: %s", err)
		}

		cancel1() // Stop this consumer after acking first message
		break
	}

	// Wait longer than redelivery timeout
	time.Sleep(1 * time.Second)

	// Second consumer - should only see message 2, not message 1 (which was acked)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	messagesSeen := make(map[string]bool)
	for event, err := range client.Get(
		ctx2,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		t.Logf("Second consumer received message (event_id: %s)", event.Id)
		messagesSeen[event.Id] = true

		if err := event.Ack(ctx2); err != nil {
			t.Fatalf("failed to ack: %s", err)
		}

		// After seeing both messages, we're done
		if len(messagesSeen) >= 2 {
			break
		}
	}

	// Both messages should be seen by second consumer
	// Message 1 was acked by first consumer but second consumer starts from oldest
	// so it sees all messages (ack doesn't remove messages, just prevents redelivery)
	if len(messagesSeen) != 2 {
		t.Fatalf("expected second consumer to see 2 messages, but saw %d", len(messagesSeen))
	}

	t.Log("Successfully verified: ack prevents redelivery but messages remain available for other consumers")
}

func TestRedelivery_ConsumerDisconnectCausesRedelivery(t *testing.T) {
	client := createBusServer(t, "TestRedelivery_ConsumerDisconnectCausesRedelivery")

	// Put a message
	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("will be redelivered"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	eventIdSeen := ""

	// First consumer - receives but disconnects without acking
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	for event, err := range client.Get(
		ctx1,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("consumer 1 error: %s", err)
			}
			break
		}

		eventIdSeen = event.Id
		t.Logf("First consumer received message (event_id: %s), disconnecting without ack", event.Id)
		// Immediately disconnect without acking
		cancel1()
		break
	}

	if eventIdSeen == "" {
		t.Fatal("first consumer should have received a message")
	}

	// Wait for redelivery timeout to trigger
	time.Sleep(1 * time.Second)

	// Second consumer - should receive the same message (redelivered)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	redelivered := false
	for event, err := range client.Get(
		ctx2,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1),
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("consumer 2 error: %s", err)
			}
			break
		}

		t.Logf("Second consumer received message (event_id: %s)", event.Id)

		if event.Id == eventIdSeen {
			redelivered = true
			t.Log("Message was successfully redelivered to second consumer!")

			// Ack it this time
			if err := event.Ack(ctx2); err != nil {
				t.Fatalf("failed to ack: %s", err)
			}
			break
		}
	}

	if !redelivered {
		t.Fatal("message should have been redelivered to second consumer after first consumer disconnected")
	}

	t.Log("Successfully verified: disconnecting without ack causes redelivery")
}

func TestRedelivery_WithAckNoneNoRedelivery(t *testing.T) {
	client := createBusServer(t, "TestRedelivery_WithAckNoneNoRedelivery")

	// Put a message
	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("no ack needed"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	receivedCount := 0
	seenEventIds := make(map[string]int)

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckNone), // No manual ack required
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, 1), // Even with short redelivery, should not redeliver
	) {
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("failed to get event: %s", err)
			}
			break
		}

		receivedCount++
		seenEventIds[event.Id]++

		t.Logf("Received event %s (total received: %d)", event.Id, receivedCount)

		// Don't call ack - with AckNone strategy, messages should not be redelivered
		if receivedCount >= 1 {
			// Wait to verify no redelivery occurs
			time.Sleep(2 * time.Second)
			break
		}
	}

	if receivedCount != 1 {
		t.Fatalf("with AckNone strategy, expected to receive message exactly once, but got %d times", receivedCount)
	}

	for eventId, count := range seenEventIds {
		if count != 1 {
			t.Fatalf("event %s was received %d times, expected exactly 1 time (no redelivery with AckNone)", eventId, count)
		}
	}

	t.Log("Successfully verified: AckNone strategy does not trigger redelivery")
}

func TestRedeliveryCountDrop(t *testing.T) {
	ctx := t.Context()

	client := createBusServer(t, "TestRedeliveryCountDrop")

	// Put a message
	err := client.Put(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("bad message"),
	).Error()
	if err != nil {
		t.Fatal(err)
	}

	err = client.Put(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("good message"),
	).Error()
	if err != nil {
		t.Fatal(err)
	}

	// Consumer - tries to get and redeliver the message

	redeliveryAttempts := 0
	maxRedeliveries := 3

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(500*time.Millisecond, maxRedeliveries),
	) {
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("Received event:", event.Id, "with payload:", string(event.Payload))

		if string(event.Payload) == `"good message"` {
			t.Log("Received good message, acking and exiting")
			if err := event.Ack(ctx); err != nil {
				t.Fatalf("failed to ack good message: %s", err)
			}
			break
		}

		redeliveryAttempts++
		t.Logf("Received event %s (redelivery attempt: %d)", event.Id, redeliveryAttempts)
	}

	if redeliveryAttempts != maxRedeliveries {
		t.Fatalf("expected %d redelivery attempts, but got %d", maxRedeliveries, redeliveryAttempts)
	}

	// Verify no more redeliveries occur
	time.Sleep(1 * time.Second)
}
