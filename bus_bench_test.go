package bus_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"ella.to/immuta"

	"ella.to/bus"
)

func createBenchServer(tb testing.TB, eventLogsDir string) *bus.Client {
	os.RemoveAll(eventLogsDir)

	storage, err := immuta.New(
		immuta.WithLogsDirPath(eventLogsDir),
		immuta.WithNamespaces("a"),
		immuta.WithFastWrite(true),
		immuta.WithReaderCount(16),
	)
	if err != nil {
		tb.Fatal(err)
	}

	handler := bus.NewHandler(storage, nil)
	server := httptest.NewServer(handler)

	tb.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll(eventLogsDir)
	})

	return bus.NewClient(server.URL)
}

func BenchmarkPut(b *testing.B) {
	client := createBenchServer(b, "BenchmarkPut")
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := client.Put(ctx, bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)).Error(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutParallel(b *testing.B) {
	client := createBenchServer(b, "BenchmarkPutParallel")
	ctx := context.Background()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Put(ctx, bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)).Error(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutBatch100(b *testing.B) {
	client := createBenchServer(b, "BenchmarkPutBatch100")
	ctx := context.Background()

	opts := make([]bus.PutOpt, 0, 100)
	for range 100 {
		opts = append(opts, bus.Batch(bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)))
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := client.Put(ctx, opts...).Error(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "events/sec")
}

// BenchmarkEndToEnd measures publish-to-receive round-trip for a single
// consumer subscribed at newest.
func BenchmarkEndToEnd(b *testing.B) {
	client := createBenchServer(b, "BenchmarkEndToEnd")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan struct{})
	ready := make(chan struct{})

	go func() {
		close(ready)
		for _, err := range client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithStartFrom(bus.StartOldest)) {
			if err != nil {
				return
			}
			received <- struct{}{}
		}
	}()
	<-ready

	b.ReportAllocs()
	for b.Loop() {
		if err := client.Put(ctx, bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)).Error(); err != nil {
			b.Fatal(err)
		}
		<-received
	}
}

// BenchmarkConsumeThroughput measures steady-state delivery throughput: a
// producer batches events in the background while the benchmark loop counts
// received events.
func BenchmarkConsumeThroughput(b *testing.B) {
	client := createBenchServer(b, "BenchmarkConsumeThroughput")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// background producer: batches of 100 until ctx is canceled
	go func() {
		opts := make([]bus.PutOpt, 0, 100)
		for range 100 {
			opts = append(opts, bus.Batch(bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)))
		}
		for ctx.Err() == nil {
			if err := client.Put(ctx, opts...).Error(); err != nil {
				return
			}
		}
	}()

	next, stop := pull2(client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithStartFrom(bus.StartOldest)))
	defer stop()

	b.ReportAllocs()
	for b.Loop() {
		_, err, ok := next()
		if !ok || err != nil {
			b.Fatalf("consumer stopped: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}

// BenchmarkFanout8 measures delivery of one published event to 8 consumers.
func BenchmarkFanout8(b *testing.B) {
	client := createBenchServer(b, "BenchmarkFanout8")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const consumers = 8
	received := make(chan struct{}, consumers)

	for range consumers {
		ready := make(chan struct{})
		go func() {
			close(ready)
			for _, err := range client.Get(ctx, bus.WithSubject("a.b.c"), bus.WithStartFrom(bus.StartOldest)) {
				if err != nil {
					return
				}
				received <- struct{}{}
			}
		}()
		<-ready
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := client.Put(ctx, bus.WithSubject("a.b.c"), bus.WithData(`{"n":1}`)).Error(); err != nil {
			b.Fatal(err)
		}
		for range consumers {
			<-received
		}
	}
}

func BenchmarkRequestReply(b *testing.B) {
	client := createBenchServer(b, "BenchmarkRequestReply")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for event, err := range client.Get(ctx, bus.WithSubject("a.req"), bus.WithStartFrom(bus.StartOldest)) {
			if err != nil {
				return
			}
			if err := event.Ack(ctx, bus.WithData(map[string]int{"n": 1})); err != nil {
				return
			}
		}
	}()

	b.ReportAllocs()
	for b.Loop() {
		resp := client.Put(ctx, bus.WithSubject("a.req"), bus.WithData(`{"q":1}`), bus.WithRequestReply())
		if resp.Error() != nil {
			b.Fatal(resp.Error())
		}
	}
}

// pull2 is a tiny iter.Pull2 replacement that keeps the pulled iterator on a
// dedicated goroutine (handy inside benchmarks).
func pull2(seq func(yield func(*bus.Event, error) bool)) (next func() (*bus.Event, error, bool), stop func()) {
	type item struct {
		event *bus.Event
		err   error
	}
	items := make(chan item)
	done := make(chan struct{})

	go func() {
		defer close(items)
		seq(func(e *bus.Event, err error) bool {
			select {
			case items <- item{e, err}:
				return true
			case <-done:
				return false
			}
		})
	}()

	next = func() (*bus.Event, error, bool) {
		it, ok := <-items
		return it.event, it.err, ok
	}
	stop = func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	return next, stop
}

// TestMillionEventsDelivery publishes one million events and verifies a
// single consumer receives every one of them. Skipped with -short.
func TestMillionEventsDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-event delivery test in short mode")
	}

	client := createBenchServer(t, "TestMillionEventsDelivery")

	const total = 1_000_000
	const batchSize = 1000

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	publishStart := time.Now()
	for i := range total / batchSize {
		opts := make([]bus.PutOpt, 0, batchSize)
		for j := range batchSize {
			opts = append(opts, bus.Batch(
				bus.WithSubject("a.load.test"),
				bus.WithData(fmt.Sprintf(`{"batch":%d,"n":%d}`, i, j)),
			))
		}
		if err := client.Put(ctx, opts...).Error(); err != nil {
			t.Fatalf("batch %d failed: %s", i, err)
		}
	}
	publishDur := time.Since(publishStart)
	t.Logf("published %d events in %s (%.0f events/sec)", total, publishDur, total/publishDur.Seconds())

	consumeStart := time.Now()
	count := 0
	for _, err := range client.Get(
		ctx,
		bus.WithSubject("a.load.test"),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithAckStrategy(bus.AckNone),
	) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatalf("consumer error after %d events: %s", count, err)
		}
		count++
		if count == total {
			break
		}
	}
	consumeDur := time.Since(consumeStart)
	t.Logf("consumed %d events in %s (%.0f events/sec)", count, consumeDur, float64(count)/consumeDur.Seconds())

	if count != total {
		t.Fatalf("expected %d events, got %d", total, count)
	}
}
