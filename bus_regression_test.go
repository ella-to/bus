package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/bus"
)

// Regression: acking a REDELIVERED event must stop further redeliveries.
// Previously the server deleted the ack channel on every redelivery timeout
// but never re-registered it, so acks of redelivered events were silently
// dropped and the event was eventually discarded.
func TestAckAfterRedeliveryStopsRedelivery(t *testing.T) {
	client := createBusServer(t, "TestAckAfterRedeliveryStopsRedelivery")

	resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("payload"))
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	receipts := 0
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(400*time.Millisecond, 10),
	) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatalf("failed to get event: %s", err)
		}

		receipts++
		switch receipts {
		case 1:
			// skip the ack so the event gets redelivered
		case 2:
			// ack the REDELIVERED copy; this must be honored
			if err := event.Ack(ctx); err != nil {
				t.Fatalf("failed to ack redelivered event: %s", err)
			}
		default:
			t.Fatalf("event was redelivered after it was acked (receipt %d)", receipts)
		}
	}

	if receipts != 2 {
		t.Fatalf("expected exactly 2 receipts (initial + one redelivery), got %d", receipts)
	}
}

// Starting from an event id must deliver every event AFTER that id.
func TestStartFromEventId(t *testing.T) {
	client := createBusServer(t, "TestStartFromEventId")

	ids := make([]string, 5)
	for i := range ids {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData(i))
		if resp.Error() != nil {
			t.Fatal(resp.Error())
		}
		ids[i] = resp.Id
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var got []string
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithStartFrom(ids[1]),
	) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatalf("failed to get event: %s", err)
		}

		got = append(got, event.Id)
		if len(got) == 3 {
			break
		}
	}

	want := ids[2:]
	if len(got) != len(want) {
		t.Fatalf("expected %d events, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected event %s at position %d, got %s", want[i], i, got[i])
		}
	}
}

// Starting from an event id that is not in the log must surface an error
// instead of silently delivering nothing forever.
func TestStartFromUnknownEventIdErrors(t *testing.T) {
	client := createBusServer(t, "TestStartFromUnknownEventIdErrors")

	for range 3 {
		resp := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("x"))
		if resp.Error() != nil {
			t.Fatal(resp.Error())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithStartFrom("e_doesnotexist"),
	) {
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return // expected
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("timed out without receiving the expected error")
			}
			t.Fatalf("unexpected error: %s", err)
		}
		t.Fatalf("unexpected event delivered: %s", event.Id)
	}

	t.Fatal("iterator ended without the expected error")
}

// A consumer with start=newest must receive every event published after it
// subscribed, including events committed together in one batch. Previously
// the stream position was resolved lazily to the LAST record, skipping
// everything before it in the same commit.
func TestNewestReceivesAllBatchEvents(t *testing.T) {
	client := createBusServer(t, "TestNewestReceivesAllBatchEvents")

	// seed some history that must NOT be delivered
	for range 3 {
		if err := client.Put(context.Background(), bus.WithSubject("a.b.c"), bus.WithData("old")).Error(); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// subscribing happens eagerly inside Get, before the iterator runs
	events := client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithStartFrom(bus.StartNewest))

	resp := client.Put(context.Background(),
		bus.Batch(bus.WithSubject("a.b.c"), bus.WithData("one")),
		bus.Batch(bus.WithSubject("a.b.c"), bus.WithData("two")),
		bus.Batch(bus.WithSubject("a.b.c"), bus.WithData("three")),
	)
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	var got []string
	for event, err := range events {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatalf("failed to get event: %s", err)
		}

		var s string
		if err := json.Unmarshal(event.Payload, &s); err != nil {
			t.Fatalf("failed to unmarshal payload: %s", err)
		}
		got = append(got, s)
		if len(got) == 3 {
			break
		}
	}

	if len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("expected [one two three], got %v", got)
	}
}

// Request/reply must work end-to-end without a _bus_ namespace in storage:
// replies are routed in memory.
func TestRequestReply(t *testing.T) {
	client := createBusServer(t, "TestRequestReply")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// responder
	go func() {
		for event, err := range client.Get(
			ctx,
			bus.WithSubject("a.req"),
			bus.WithAckStrategy(bus.AckManual),
			bus.WithStartFrom(bus.StartOldest),
		) {
			if err != nil {
				return
			}
			if err := event.Ack(ctx, bus.WithData(map[string]string{"reply": "pong"})); err != nil {
				t.Errorf("failed to reply: %s", err)
				return
			}
		}
	}()

	resp := client.Put(ctx,
		bus.WithSubject("a.req"),
		bus.WithData("ping"),
		bus.WithRequestReply(),
	)
	if resp.Error() != nil {
		t.Fatalf("request failed: %s", resp.Error())
	}

	var reply map[string]string
	if err := json.Unmarshal(resp.Payload, &reply); err != nil {
		t.Fatalf("failed to unmarshal reply %q: %s", resp.Payload, err)
	}
	if reply["reply"] != "pong" {
		t.Fatalf("expected reply 'pong', got %q", reply["reply"])
	}
}

// WithConfirm(n) must unblock once n consumers acked the event.
func TestConfirm(t *testing.T) {
	client := createBusServer(t, "TestConfirm")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var acked atomic.Int32

	for i := range 2 {
		go func(i int) {
			for event, err := range client.Get(
				ctx,
				bus.WithSubject("a.job"),
				bus.WithAckStrategy(bus.AckManual),
				bus.WithStartFrom(bus.StartOldest),
			) {
				if err != nil {
					return
				}
				if err := event.Ack(ctx); err != nil {
					t.Errorf("consumer %d failed to ack: %s", i, err)
					return
				}
				acked.Add(1)
			}
		}(i)
	}

	resp := client.Put(ctx,
		bus.WithSubject("a.job"),
		bus.WithData("work"),
		bus.WithConfirm(2),
	)
	if resp.Error() != nil {
		t.Fatalf("confirmed put failed: %s", resp.Error())
	}

	// Put returning means both confirms arrived; the consumers' own counters
	// may lag by a scheduling beat
	deadline := time.Now().Add(2 * time.Second)
	for acked.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := acked.Load(); n != 2 {
		t.Fatalf("expected 2 acks, got %d", n)
	}
}

// Subjects without a namespace and unknown namespaces must yield an error,
// not a panic or a silent hang.
func TestInvalidSubjectsError(t *testing.T) {
	client := createBusServer(t, "TestInvalidSubjectsError")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, subject := range []string{"nodots", "unknown.namespace"} {
		gotErr := false
		for _, err := range client.Get(ctx, bus.WithSubject(subject), bus.WithStartFrom(bus.StartOldest)) {
			if err == nil {
				t.Fatalf("subject %q: expected error, got event", subject)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("subject %q: timed out instead of erroring", subject)
			}
			gotErr = true
			break
		}
		if !gotErr {
			t.Fatalf("subject %q: expected an error", subject)
		}

		if err := client.Put(ctx, bus.WithSubject(subject), bus.WithData("x")).Error(); err == nil {
			t.Fatalf("put to subject %q: expected error", subject)
		}
	}
}

// Keys and trace ids containing JSON-special characters must round-trip.
func TestEscapedFieldsRoundTrip(t *testing.T) {
	client := createBusServer(t, "TestEscapedFieldsRoundTrip")

	key := "weird\"key\\with\nchars"
	resp := client.Put(context.Background(),
		bus.WithSubject("a.b.c"),
		bus.WithKey(key),
		bus.WithTraceId("trace\t\"1\""),
		bus.WithData("x"),
	)
	if resp.Error() != nil {
		t.Fatal(resp.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for event, err := range client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithStartFrom(bus.StartOldest)) {
		if err != nil {
			t.Fatalf("failed to get event: %s", err)
		}
		if event.Key != key {
			t.Fatalf("expected key %q, got %q", key, event.Key)
		}
		if event.TraceId != "trace\t\"1\"" {
			t.Fatalf("expected trace id to round-trip, got %q", event.TraceId)
		}
		return
	}
}

// A batch with an invalid subject must fail cleanly with a single error.
func TestBatchInvalidNamespace(t *testing.T) {
	client := createBusServer(t, "TestBatchInvalidNamespace")

	resp := client.Put(context.Background(),
		bus.Batch(bus.WithSubject("a.b.c"), bus.WithData("ok")),
		bus.Batch(bus.WithSubject("other.b.c"), bus.WithData("bad")),
	)
	if resp.Error() == nil {
		t.Fatal("expected error for mixed-namespace batch")
	}
}

// End-to-end stress: concurrent producers and manual-ack consumers must not
// deadlock or drop events. Run with -race.
func TestConcurrentManualAckStress(t *testing.T) {
	client := createBusServer(t, "TestConcurrentManualAckStress")

	const producers = 4
	const perProducer = 50
	const total = producers * perProducer

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for p := range producers {
		go func(p int) {
			for i := range perProducer {
				err := client.Put(ctx,
					bus.WithSubject(fmt.Sprintf("a.stress.p%d", p)),
					bus.WithData(i),
				).Error()
				if err != nil {
					t.Errorf("producer %d put %d failed: %s", p, i, err)
					return
				}
			}
		}(p)
	}

	count := 0
	for event, err := range client.Get(
		ctx,
		bus.WithSubject("a.stress.>"),
		bus.WithAckStrategy(bus.AckManual),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithDelivery(5*time.Second, 3),
	) {
		if err != nil {
			t.Fatalf("consumer error after %d events: %s", count, err)
		}
		if err := event.Ack(ctx); err != nil {
			t.Fatalf("ack failed after %d events: %s", count, err)
		}
		count++
		if count == total {
			break
		}
	}

	if count != total {
		t.Fatalf("expected %d events, got %d", total, count)
	}
}
