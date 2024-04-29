package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/bus/server"
	"github.com/stretchr/testify/assert"
)

func setupBusServer(t *testing.T, dbPath string) *client.Client {
	opt := []server.Opt{
		server.WithStoragePoolSize(10),
	}

	if dbPath != "" {
		os.Remove(fmt.Sprintf("%s", dbPath))
		os.Remove(fmt.Sprintf("%s-wal", dbPath))
		os.Remove(fmt.Sprintf("%s-shm", dbPath))

		opt = append(opt, server.WithStoragePath(dbPath))
	}

	handler, err := server.New(context.Background(), opt...)
	assert.NoError(t, err)

	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
	})

	client, err := client.New(client.WithAddr(server.URL))
	assert.NoError(t, err)

	return client
}

func TestBasicServer(t *testing.T) {
	ctx := context.TODO()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	client := setupBusServer(t, "")

	eventsCount := 100

	for range eventsCount {
		evt, err := bus.NewEvent(
			bus.WithSubject("a.b.1"),
			bus.WithJsonData(struct {
				Name string `json:"name"`
			}{
				Name: "John",
			}),
		)
		assert.NoError(t, err)
		err = client.Put(ctx, evt)
		assert.NoError(t, err)
	}

	//

	msgs := client.Get(
		ctx,

		bus.WithSubject("a.b.*"),
		bus.WithFromOldest(),
		bus.WithManualAck(),
		bus.WithBatchSize(4),
	)

	count := 0
	for msg, err := range msgs {
		assert.NoError(t, err)
		count += len(msg.Events)
		if count == eventsCount {
			break
		}

		err = msg.Ack(ctx)
		assert.NoError(t, err)
	}

	fmt.Println("done")
}

func TestQueue(t *testing.T) {
	client := setupBusServer(t, "")

	ctx := context.TODO()

	for range 3 {
		evt, err := bus.NewEvent(
			bus.WithSubject("a.b.1"),
			bus.WithJsonData(struct {
				Name string `json:"name"`
			}{
				Name: "John",
			}),
		)
		assert.NoError(t, err)
		err = client.Put(ctx, evt)
		assert.NoError(t, err)
	}

	msg1 := client.Get(ctx, bus.WithSubject("a.b.*"), bus.WithQueue("q1"), bus.WithManualAck(), bus.WithBatchSize(1), bus.WithFromOldest())
	msg2 := client.Get(ctx, bus.WithSubject("a.b.*"), bus.WithQueue("q1"), bus.WithManualAck(), bus.WithBatchSize(1), bus.WithFromOldest())

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

}
