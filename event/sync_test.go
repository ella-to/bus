package event_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/bus/bustest"
	"ella.to/bus/event"

	"github.com/stretchr/testify/assert"
)

func TestEventSync(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	client := bustest.GetNatsClient(t)

	s := event.NewSync(
		client,
		event.WithSubject("test.>"),
	)

	var counter atomic.Int32

	// sending first event before registering
	// this simulate the case when the event is sent before the handler is registered
	// or being unprocessed and we want to see if the sync once will catch it or not
	err := client.Put(
		context.Background(),
		bus.WithSubject("test.1"),
		bus.WithData("hello world 1"),
	)
	assert.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(2)

	s.Register("test.1", func(ctx context.Context, evt *bus.Event) error {
		defer wg.Done()
		counter.Add(1)

		fmt.Println("event : ", string(evt.Data))

		return nil
	})

	s.Lock()
	err = s.Once(context.Background(), 1*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, int32(1), counter.Load())

	ctx := context.Background()

	go func() {
		err := s.Continue(ctx)
		if err != nil {
			slog.Error("continue: ", "err", err)
		}
	}()

	time.Sleep(1 * time.Second)

	err = client.Put(
		context.Background(),
		bus.WithSubject("test.1"),
		bus.WithData("hello world 2"),
	)
	assert.NoError(t, err)

	wg.Wait()

	assert.Equal(t, int32(2), counter.Load())
}

func TestEventSyncStart(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	client := bustest.GetNatsClient(t)

	s := event.NewSync(
		client,
		event.WithSubject("test.>"),
	)

	var counter atomic.Int32

	// sending first event before registering
	// this simulate the case when the event is sent before the handler is registered
	// or being unprocessed and we want to see if the sync once will catch it or not
	err := client.Put(
		context.Background(),
		bus.WithSubject("test.1"),
		bus.WithData("hello world 1"),
	)
	assert.NoError(t, err)

	var wg sync.WaitGroup

	wg.Add(2)

	s.Register("test.1", func(ctx context.Context, evt *bus.Event) error {
		defer wg.Done()
		counter.Add(1)

		fmt.Println("event : ", string(evt.Data))

		return nil
	})

	s.Lock()
	err = s.Start(context.Background(), 1*time.Second)
	assert.NoError(t, err)

	err = client.Put(
		context.Background(),
		bus.WithSubject("test.1"),
		bus.WithData("hello world 2"),
	)
	assert.NoError(t, err)

	wg.Wait()

	assert.Equal(t, int32(2), counter.Load())
}
