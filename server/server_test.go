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

	{
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

	{
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

	{
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

	events := client.Get(
		ctx,

		bus.WithSubject("a.b.*"),
		bus.WithFromOldest(),
		// bus.WithManualAck(),
		bus.WithBatchSize(2),
	)

	count := 0
	for evt, err := range events {
		count++

		assert.NoError(t, err)
		assert.Equal(t, "a.b.1", evt.Subject)
		fmt.Println(evt.Id)
		err = evt.Ack(ctx)
		assert.NoError(t, err)

		if count == 4 {
			break
		}
	}

	fmt.Println("done")
}
