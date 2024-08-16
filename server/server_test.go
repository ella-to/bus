package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/internal/testutil"
)

func TestBasic(t *testing.T) {
	testutil.PrepareTestServer(t)
}

func TestPut(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := testutil.PrepareTestServer(t)

	evt, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt)
	assert.NoError(t, err)

	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		assert.Equal(t, `"hello"`, string(msg.Events[0].Data))
		msg.Ack(context.Background())
		break
	}
}

func Test1000(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := testutil.PrepareTestServer(t)

	n := 1000

	for i := 0; i < n; i++ {
		evt, err := bus.NewEvent(
			bus.WithSubject("a.b.c"),
			bus.WithData(struct {
				Msg string
			}{
				Msg: fmt.Sprintf("hello: '%d", i),
			}),
		)
		assert.NoError(t, err)

		err = c.Put(context.Background(), evt)
		assert.NoError(t, err)
	}

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++

		msg.Ack(context.Background())

		if count == n {
			break
		}
	}
}

func TestBug3(t *testing.T) {
	c := testutil.PrepareTestServer(t)

	evt1, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 1"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt1)
	assert.NoError(t, err)

	evt2, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 2"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt2)
	assert.NoError(t, err)

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++
		msg.Ack(context.Background())

		if count == 2 {
			break
		}
	}
}

func TestConfirmConsumer(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := testutil.PrepareTestServer(t)

	var hit int64

	go func() {
		ctx := context.TODO()

		// NOTE: we need to make sure consumer always starts reading from the oldest
		// as it could happen that the event can be sent before the consumer starts

		for msg, err := range c.Get(ctx, bus.WithFromOldest(), bus.WithSubject("a.b.c")) {
			atomic.AddInt64(&hit, 1)

			assert.NoError(t, err)
			assert.NotNil(t, msg)
			assert.Len(t, msg.Events, 1)

			evt := msg.Events[0]

			assert.Equal(t, "a.b.c", evt.Subject)
			assert.Equal(t, `"hello"`, string(evt.Data))

			err = msg.Ack(ctx)
			assert.NoError(t, err)

			break
		}
	}()

	evt, err := bus.NewEvent(bus.WithSubject("a.b.c"), bus.WithConfirm(1), bus.WithData("hello"))
	assert.NoError(t, err)

	ctx := context.TODO()
	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	err = c.Put(ctx, evt)

	assert.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&hit))
}

func TestSequences(t *testing.T) {
	c := testutil.PrepareTestServer(t)

	evt1, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 1"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt1)
	assert.NoError(t, err)

	evt2, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 2"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt2)
	assert.NoError(t, err)

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++

		msg.Ack(context.Background())

		if count == 2 {
			break
		}
	}
}

func TestQueue(t *testing.T) {
	c := testutil.PrepareTestServer(t)

	ctx := context.TODO()

	for range 3 {
		evt, err := bus.NewEvent(
			bus.WithSubject("a.b.1"),
			bus.WithData(struct {
				Name string `json:"name"`
			}{
				Name: "John",
			}),
		)
		assert.NoError(t, err)
		err = c.Put(ctx, evt)
		assert.NoError(t, err)
	}

	msg1 := c.Get(ctx, bus.WithSubject("a.b.*"), bus.WithQueue("q1"), bus.WithBatchSize(1), bus.WithFromOldest())
	msg2 := c.Get(ctx, bus.WithSubject("a.b.*"), bus.WithQueue("q1"), bus.WithBatchSize(1), bus.WithFromOldest())

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		count := 0
		for msg, err := range msg1 {
			assert.NoError(t, err)
			err = msg.Ack(ctx)
			assert.NoError(t, err)
			count++

			if count == 2 {
				break
			}
		}
	}()

	go func() {
		defer wg.Done()
		count := 0
		for msg, err := range msg2 {
			assert.NoError(t, err)
			err = msg.Ack(ctx)
			assert.NoError(t, err)

			count++
			if count == 1 {
				break
			}
		}
	}()

	wg.Wait()

	// NOTE: we need to make sure no terminating test too fast
	time.Sleep(1 * time.Second)
}
