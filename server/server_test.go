package server_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/bus/server"
	"ella.to/bus/storage"
	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
)

func prepareTestServer(t *testing.T) *client.Client {
	t.Helper()

	ctx := context.Background()

	storage, err := storage.NewSqlite(
		ctx,
		100,
		sqlite.WithMemory(),
		sqlite.WithPoolSize(10),
	)
	assert.NoError(t, err)

	server := server.New(ctx, storage)
	t.Cleanup(func() {
		server.Close()
	})

	return client.New(httptest.NewServer(server).URL)
}

func TestBasic(t *testing.T) {
	prepareTestServer(t)
}

func TestPut(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := prepareTestServer(t)

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
		break
	}
}

func TestBug3(t *testing.T) {
	c := prepareTestServer(t)

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

		if count == 2 {
			break
		}
	}
}
