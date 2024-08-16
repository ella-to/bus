package storage_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/storage"
)

func createBasicEvents(t *testing.T, n int) []*bus.Event {
	events := make([]*bus.Event, 0, n)

	for i := 0; i < n; i++ {
		event, err := bus.NewEvent(
			bus.WithSubject("a.b.c"),
			bus.WithData(fmt.Sprintf("payload-%d", i)),
			bus.WithExpiresAt(10*time.Second),
		)
		event.Id = bus.GetEventId()
		assert.NoError(t, err)

		events = append(events, event)
	}

	return events
}

func createSqliteStorage(t *testing.T, ctx context.Context, path string) *storage.Sqlite {
	t.Helper()

	if path != "" {
		os.Remove(path)
		os.Remove(path + "-wal")
		os.Remove(path + "-shm")
	}

	storage, err := storage.NewSqlite(ctx, path)
	assert.NoError(t, err)

	t.Cleanup(func() {
		storage.Close()
	})

	return storage
}

func TestSave1000Events(t *testing.T) {
	ctx := context.Background()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	storage := createSqliteStorage(t, ctx, "")

	events := createBasicEvents(t, 1)

	for _, event := range events {
		err := storage.SaveEvent(ctx, event)
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)
}
