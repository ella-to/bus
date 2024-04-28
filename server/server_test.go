package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/bus/server"
	"github.com/stretchr/testify/assert"
)

func TestBasicServer(t *testing.T) {
	ctx := context.TODO()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	os.Remove("./test.db")
	os.Remove("./test.db-wal")
	os.Remove("./test.db-shm")

	handler, err := server.New(ctx, server.WithStoragePoolSize(10), server.WithStoragePath("./test.db"))
	assert.NoError(t, err)

	server := httptest.NewServer(handler)
	defer server.Close()

	client, err := client.New(client.WithAddr(server.URL))
	assert.NoError(t, err)

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
