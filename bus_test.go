package bus_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/bus/server"
	"github.com/stretchr/testify/assert"
)

func setupBusServer(t *testing.T) *client.Client {
	os.Remove("./bus.db")
	os.Remove("./bus.db-wal")
	os.Remove("./bus.db-shm")

	handler, err := server.New(
		context.TODO(),

		server.WithStoragePoolSize(10),
		// server.WithStoragePath("./bus.db"),
	)
	assert.NoError(t, err)

	server := httptest.NewServer(handler)
	// t.Cleanup(server.Close)

	c, err := client.New(client.WithAddr(server.URL))
	assert.NoError(t, err)

	return c
}

func TestConfirmConsumer(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := setupBusServer(t)

	var hit int64

	go func() {
		ctx := context.TODO()

		// NOTE: we need to make sure consumer always starts reading from the oldest
		// as it could happen that the event can be sent before the consumer starts

		for msg, err := range c.Get(ctx, bus.WithFromOldest(), bus.WithSubject("a.b.c"), bus.WithManualAck()) {
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

	evt, err := bus.NewEvent(bus.WithSubject("a.b.c"), bus.WithConfirm(1), bus.WithJsonData("hello"))
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = c.Put(ctx, evt)

	assert.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&hit))
}
