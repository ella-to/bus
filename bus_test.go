package bus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/bus"
)

// newTestClient spins up an isolated dev NATS server backed by memory storage
// and returns a Client wired up to it. The returned cleanup function shuts
// everything down and is safe to call multiple times.
func newTestClient(t *testing.T) (*bus.Client, func()) {
	t.Helper()

	srv, err := bus.NewDevServer(bus.WithDevServerMemStore())
	if err != nil {
		t.Fatalf("dev server: %v", err)
	}

	client, err := bus.NewClient(srv.ClientURL())
	if err != nil {
		srv.Shutdown()
		t.Fatalf("client: %v", err)
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			_ = client.Close()
			srv.Shutdown()
		})
	}
	return client, cleanup
}

func TestPubSubFromOldest(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := client.Put(ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("hello"),
	)
	if err := resp.Error(); err != nil {
		t.Fatalf("put: %v", err)
	}
	if resp.Id == "" {
		t.Fatal("expected non-empty id")
	}
	if resp.Index <= 0 {
		t.Fatalf("expected positive index, got %d", resp.Index)
	}

	for event, err := range client.Get(ctx,
		bus.WithSubject("a.b.c"),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithAckStrategy(bus.AckManual),
	) {
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		var got string
		if err := json.Unmarshal(event.Payload, &got); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if got != "hello" {
			t.Fatalf("unexpected payload %q", got)
		}
		if event.Subject != "a.b.c" {
			t.Fatalf("unexpected subject %q", event.Subject)
		}
		if event.Index <= 0 {
			t.Fatalf("expected positive index, got %d", event.Index)
		}
		if err := event.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
		return
	}
}

func TestWildcardSubscription(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, subj := range []string{"orders.created", "orders.updated", "orders.shipped"} {
		resp := client.Put(ctx, bus.WithSubject(subj), bus.WithData(subj))
		if err := resp.Error(); err != nil {
			t.Fatalf("put %s: %v", subj, err)
		}
	}

	var (
		got = make([]string, 0, 3)
		mu  sync.Mutex
	)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for event, err := range client.Get(subCtx,
			bus.WithSubject("orders.*"),
			bus.WithStartFrom(bus.StartOldest),
		) {
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, event.Subject)
			n := len(got)
			mu.Unlock()
			if n == 3 {
				subCancel()
				return
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for wildcard deliveries")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("expected 3 events, got %d (%v)", len(got), got)
	}
}

func TestMultiSegmentWildcard(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, subj := range []string{"orders.created", "orders.item.added", "orders.item.removed"} {
		if err := client.Put(ctx, bus.WithSubject(subj), bus.WithData(subj)).Error(); err != nil {
			t.Fatalf("put %s: %v", subj, err)
		}
	}

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range client.Get(subCtx,
			bus.WithSubject("orders.>"),
			bus.WithStartFrom(bus.StartOldest),
		) {
			if err != nil {
				return
			}
			if count.Add(1) == 3 {
				subCancel()
				return
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout")
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3, got %d", count.Load())
	}
}

func TestRequestReply(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		for event, err := range client.Get(ctx,
			bus.WithSubject("math.add"),
			bus.WithStartFrom(bus.StartOldest),
			bus.WithAckStrategy(bus.AckManual),
		) {
			if err != nil {
				return
			}
			var req struct{ A, B int }
			if err := json.Unmarshal(event.Payload, &req); err != nil {
				t.Errorf("unmarshal: %v", err)
				return
			}
			if err := event.Ack(ctx, bus.WithData(map[string]int{"result": req.A + req.B})); err != nil {
				t.Errorf("ack: %v", err)
				return
			}
		}
	}()

	// Give the consumer a moment to register before publishing.
	time.Sleep(100 * time.Millisecond)

	resp := client.Put(ctx,
		bus.WithSubject("math.add"),
		bus.WithData(map[string]int{"A": 7, "B": 8}),
		bus.WithRequestReply(),
	)
	if err := resp.Error(); err != nil {
		t.Fatalf("put: %v", err)
	}

	var out struct{ Result int }
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		t.Fatalf("unmarshal payload %s: %v", string(resp.Payload), err)
	}
	if out.Result != 15 {
		t.Fatalf("expected 15, got %d", out.Result)
	}
}

func TestConfirm(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumerReady := make(chan struct{})
	go func() {
		first := true
		for event, err := range client.Get(ctx,
			bus.WithSubject("svc.event"),
			bus.WithStartFrom(bus.StartOldest),
			bus.WithAckStrategy(bus.AckManual),
		) {
			if first {
				close(consumerReady)
				first = false
			}
			if err != nil {
				return
			}
			_ = event.Ack(ctx)
		}
	}()

	// Publish a warmup event so the consumer registers and signals ready.
	if err := client.Put(ctx, bus.WithSubject("svc.event"), bus.WithData("warmup")).Error(); err != nil {
		t.Fatalf("warmup put: %v", err)
	}
	select {
	case <-consumerReady:
	case <-ctx.Done():
		t.Fatal("consumer never ready")
	}

	if err := client.Put(ctx,
		bus.WithSubject("svc.event"),
		bus.WithData("confirm me"),
		bus.WithConfirm(1),
	).Error(); err != nil {
		t.Fatalf("confirm put: %v", err)
	}
}

func TestBatchPut(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := client.Put(ctx,
		bus.Batch(bus.WithSubject("orders.created"), bus.WithData(map[string]string{"id": "1"})),
		bus.Batch(bus.WithSubject("orders.created"), bus.WithData(map[string]string{"id": "2"})),
		bus.Batch(bus.WithSubject("orders.updated"), bus.WithData(map[string]string{"id": "3"})),
	)
	if err := resp.Error(); err != nil {
		t.Fatalf("batch: %v", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	var got int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range client.Get(subCtx,
			bus.WithSubject("orders.>"),
			bus.WithStartFrom(bus.StartOldest),
		) {
			if err != nil {
				return
			}
			if atomic.AddInt32(&got, 1) == 3 {
				subCancel()
				return
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout")
	}
	if atomic.LoadInt32(&got) != 3 {
		t.Fatalf("expected 3 events, got %d", got)
	}
}

func TestBatchMixedNamespacesRejected(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := client.Put(ctx,
		bus.Batch(bus.WithSubject("orders.created"), bus.WithData("x")),
		bus.Batch(bus.WithSubject("users.created"), bus.WithData("x")),
	)
	if resp.Error() == nil {
		t.Fatal("expected mixed-namespace batch to fail")
	}
}

func TestRedeliveryOnMissingAck(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Put(ctx, bus.WithSubject("redel.event"), bus.WithData("retry")).Error(); err != nil {
		t.Fatalf("put: %v", err)
	}

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	var deliveries int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range client.Get(subCtx,
			bus.WithSubject("redel.event"),
			bus.WithStartFrom(bus.StartOldest),
			bus.WithAckStrategy(bus.AckManual),
			bus.WithDelivery(300*time.Millisecond, 3),
		) {
			if err != nil {
				return
			}
			n := atomic.AddInt32(&deliveries, 1)
			if n >= 3 {
				subCancel()
				return
			}
			// intentionally do not ack
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for redelivery")
	}
	if atomic.LoadInt32(&deliveries) < 2 {
		t.Fatalf("expected at least 2 deliveries, got %d", deliveries)
	}
}

func TestExtractMeta(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Put(ctx, bus.WithSubject("meta.event"), bus.WithData("x")).Error(); err != nil {
		t.Fatalf("put: %v", err)
	}

	var captured string
	for event, err := range client.Get(ctx,
		bus.WithSubject("meta.event"),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithExtractMeta(func(m map[string]string) {
			captured = m["consumer-id"]
		}),
	) {
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if event != nil {
			break
		}
	}
	if !strings.HasPrefix(captured, "c_") {
		t.Fatalf("unexpected consumer id %q", captured)
	}
}

func TestPutInvalidSubject(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	resp := client.Put(context.Background(),
		bus.WithSubject("orders.*"),
		bus.WithData("x"),
	)
	if resp.Error() == nil {
		t.Fatal("expected wildcard subject in Put to fail")
	}
}

func TestGetWithoutSubject(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	for _, err := range client.Get(context.Background()) {
		if err == nil {
			t.Fatal("expected an error for missing subject")
		}
		return
	}
}

func TestEventReadWriteRoundTrip(t *testing.T) {
	in := bus.Event{
		Id:        "e_test",
		TraceId:   "t_test",
		Key:       "k_test",
		Subject:   "ns.sub",
		Payload:   json.RawMessage(`{"hello":"world"}`),
		CreatedAt: time.Now().UTC().Truncate(time.Second),
		Index:     42,
	}

	buf, err := io.ReadAll(&in)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var out bus.Event
	if _, err := out.Write(buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if out.Id != in.Id || out.Subject != in.Subject || out.Index != in.Index {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
	if string(out.Payload) != string(in.Payload) {
		t.Fatalf("payload mismatch: %s vs %s", string(out.Payload), string(in.Payload))
	}
}

func TestErrorPayloadSurfacedThroughResponse(t *testing.T) {
	r := &bus.Response{
		Payload: json.RawMessage(`"boom"`),
	}
	err := r.Error()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func ExampleNewClient() {
	srv, err := bus.NewDevServer()
	if err != nil {
		panic(err)
	}
	defer srv.Shutdown()

	client, err := bus.NewClient(srv.ClientURL())
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()

	if err := client.Put(ctx,
		bus.WithSubject("demo.hello"),
		bus.WithData("world"),
	).Error(); err != nil {
		panic(err)
	}

	for event, err := range client.Get(ctx,
		bus.WithSubject("demo.hello"),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithAckStrategy(bus.AckManual),
	) {
		if err != nil {
			panic(err)
		}
		fmt.Println(string(event.Payload))
		_ = event.Ack(ctx)
		break
	}
	// Output: "world"
}
