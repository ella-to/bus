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
	"ella.to/bus/internal/compress"
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

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)))

	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll(eventLogsDir)
	})

	return bus.NewClient(server.URL)
}

func createBusServerWithCompression(t *testing.T, eventLogsDir string, compressor immuta.Compressor) *bus.Client {
	os.RemoveAll(eventLogsDir)

	storage, err := immuta.New(
		immuta.WithLogsDirPath(eventLogsDir),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
		immuta.WithCompression(compressor),
	)
	if err != nil {
		t.Fatal(err)
	}

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)))

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

	for i := 0; i < b.N; i++ {
		buffer.Reset()
		io.Copy(&buffer, &event)
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

	for i := 0; i < b.N; i++ {
		buffer.Reset()
		json.NewEncoder(&buffer).Encode(&event)
	}
}

func BenchmarkDecodeEventUsingWrite(b *testing.B) {
	b.ReportAllocs()

	input := []byte(`{"id":"id-1234","trace_id":"trace-1234","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-17T12:20:45-05:00","payload":{"key": "value"},"index":100}`)

	var event bus.Event

	for i := 0; i < b.N; i++ {
		io.Copy(&event, bytes.NewReader(input))
	}
}

func BenchmarkDecodeEventUsingJSON(b *testing.B) {
	b.ReportAllocs()

	input := []byte(`{"id":"id-1234","trace_id":"trace-1234","subject":"a.b.c","response_subject":"a.b.c.response","created_at":"2025-01-17T12:20:45-05:00","payload":{"key": "value"},"index":100}`)

	var event bus.Event

	for i := 0; i < b.N; i++ {
		json.NewDecoder(bytes.NewReader(input)).Decode(&event)
	}
}

func TestBusWithS2Compression_BasicPutGet(t *testing.T) {
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_BasicPutGet", compress.NewS2Compressor())

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
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_MultipleEvents", compress.NewS2Compressor())

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
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_LargePayload", compress.NewS2Compressor())

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
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_ConcurrentPutGet", compress.NewS2Compressor())

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
	client := createBusServerWithCompression(t, "TestBusWithS2Compression_ComplexPayload", compress.NewS2Compressor())

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

func BenchmarkBusWithS2Compression_Put(b *testing.B) {
	os.RemoveAll("BenchmarkBusWithS2Compression_Put")
	defer os.RemoveAll("BenchmarkBusWithS2Compression_Put")

	storage, err := immuta.New(
		immuta.WithLogsDirPath("BenchmarkBusWithS2Compression_Put"),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
		immuta.WithCompression(compress.NewS2Compressor()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)))
	server := httptest.NewServer(handler)
	defer server.Close()

	client := bus.NewClient(server.URL)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("hello world"))
		if resp.Error() != nil {
			b.Fatal(resp.Error())
		}
	}
}

func BenchmarkBusWithS2Compression_PutLarge(b *testing.B) {
	os.RemoveAll("BenchmarkBusWithS2Compression_PutLarge")
	defer os.RemoveAll("BenchmarkBusWithS2Compression_PutLarge")

	storage, err := immuta.New(
		immuta.WithLogsDirPath("BenchmarkBusWithS2Compression_PutLarge"),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
		immuta.WithCompression(compress.NewS2Compressor()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)))
	server := httptest.NewServer(handler)
	defer server.Close()

	client := bus.NewClient(server.URL)

	// Large repetitive data that compresses well
	largeData := strings.Repeat("test data ", 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(largeData))
		if resp.Error() != nil {
			b.Fatal(resp.Error())
		}
	}
}
