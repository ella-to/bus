package storage_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/internal/gen"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
)

func appendNewEvent(t *testing.T, ctx context.Context, conn *sqlite.Conn) string {
	evt1, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData([]byte(`{"key": "value"}`)),
		bus.WithExpiresAt(time.Hour),
	)
	assert.NoError(t, err)
	evt1.Id = gen.NewID()

	err = storage.AppendEvents(ctx, conn, evt1)
	assert.NoError(t, err)

	return evt1.Id
}

func TestConsumerNotify(t *testing.T) {
	var notifyFnCalls int64

	ctx := context.TODO()
	notify := storage.NotifyFunc(func(consumerId string, event *bus.Event) {
		atomic.AddInt64(&notifyFnCalls, 1)
	})

	db := storageSetup(ctx, notify, t, "")
	defer db.Close()

	t.Run("Register Consumer", func(t *testing.T) {
		con, err := bus.NewConsumer(
			bus.WithId("consumer-1"),
			bus.WithSubject("a.b.c"),
			bus.WithBatchSize(1),
		)
		assert.NoError(t, err)

		conn, err := db.Conn(ctx)
		assert.NoError(t, err)
		defer conn.Close()

		err = storage.SaveConsumer(ctx, conn, con)
		assert.NoError(t, err)
	})

	t.Run("Append Event", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		assert.NoError(t, err)
		defer conn.Close()

		appendNewEvent(t, ctx, conn)
	})

	assert.Equal(t, int64(1), atomic.LoadInt64(&notifyFnCalls))
}

func TestConsumerAckEvent(t *testing.T) {
	ctx := context.TODO()

	var notifyFnCalls int64

	db := storageSetup(ctx, func(consumerId string, event *bus.Event) {
		atomic.AddInt64(&notifyFnCalls, 1)
	}, t, "./test.db")
	defer db.Close()

	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	con, err := bus.NewConsumer(
		bus.WithId("consumer-1"),
		bus.WithSubject("a.b.c"),
		bus.WithBatchSize(1),
	)
	assert.NoError(t, err)

	{
		err = storage.SaveConsumer(ctx, conn, con)
		assert.NoError(t, err)
	}

	evt1 := appendNewEvent(t, ctx, conn)

	evt2 := appendNewEvent(t, ctx, conn)

	assert.Equal(t, int64(1), atomic.LoadInt64(&notifyFnCalls))

	err = storage.AckEvent(ctx, conn, con.Id, evt1)
	assert.NoError(t, err)

	assert.Equal(t, int64(2), atomic.LoadInt64(&notifyFnCalls))

	err = storage.AckEvent(ctx, conn, con.Id, evt2)
	assert.NoError(t, err)

	assert.Equal(t, int64(2), atomic.LoadInt64(&notifyFnCalls))
}

func TestEventsBurst(t *testing.T) {
	t.Skip("This test is flaky")
	ctx := context.TODO()
	var notifyFnCalls int64
	var db *sqlite.Database
	db = storageSetup(ctx, func(consumerId string, event *bus.Event) {
		atomic.AddInt64(&notifyFnCalls, 1)

		go func() {
			conn, err := db.Conn(ctx)
			assert.NoError(t, err)
			defer conn.Close()

			err = storage.AckEvent(ctx, conn, consumerId, event.Id)
			assert.NoError(t, err)
		}()

	}, t, "")
	defer db.Close()

	conn, err := db.Conn(ctx)
	assert.NoError(t, err)

	con, err := bus.NewConsumer(
		bus.WithId("consumer-1"),
		bus.WithSubject("a.b.c"),
		bus.WithBatchSize(1),
	)
	assert.NoError(t, err)

	err = storage.SaveConsumer(ctx, conn, con)
	assert.NoError(t, err)
	conn.Close()

	const workers = 2
	const eventsPerWorker = 100

	{
		var wg sync.WaitGroup

		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := db.Conn(ctx)
				if err != nil {
					return
				}
				defer conn.Close()
				assert.NoError(t, err)
				for range eventsPerWorker {
					appendNewEvent(t, ctx, conn)
				}
			}()
		}

		wg.Wait()
	}

	time.Sleep(2 * time.Second)

	assert.Equal(t, int64(workers*eventsPerWorker), atomic.LoadInt64(&notifyFnCalls))
}
