package storage_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"ella.to/sqlite"
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

	workerSize := 10

	ops := []sqlite.OptionFunc{
		sqlite.WithPoolSize(workerSize),
	}

	if path != "" {
		os.Remove(path)
		os.Remove(path + "-wal")
		os.Remove(path + "-shm")
		ops = append(ops, sqlite.WithFile(path))
	} else {
		ops = append(ops, sqlite.WithMemory())
	}

	storage, err := storage.NewSqlite(ctx, workerSize, ops...)
	assert.NoError(t, err)

	t.Cleanup(func() {
		storage.Close()
	})

	return storage
}
