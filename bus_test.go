package bus_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http/httptest"
	"os"
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

	handler := bus.NewHandler(storage, task.NewRunner(task.WithWorkerSize(1)))

	server := httptest.NewServer(handler)

	t.Cleanup(func() {
		storage.Close()
		server.Close()
		os.RemoveAll(eventLogsDir)
	})

	return bus.NewClient(server.URL)
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
