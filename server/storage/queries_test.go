package storage_test

import (
	"context"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
)

func storageSetup(ctx context.Context, notify storage.NotifyFunc, t *testing.T, path string) *sqlite.Database {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	dbOpts := []sqlite.OptionFunc{
		sqlite.WithPoolSize(1),
	}

	if path != "" {
		dbOpts = append(dbOpts, sqlite.WithFile(path))
		t.Cleanup(func() {
			os.Remove(path)
		})
	} else {
		dbOpts = append(dbOpts, sqlite.WithMemory())
	}

	db, err := storage.New(ctx, notify, dbOpts...)
	assert.NoError(t, err)

	err = sqlite.Migration(context.TODO(), db, storage.MigrationFiles, "schema")
	assert.NoError(t, err)

	return db
}

func TestSaveConsumer(t *testing.T) {
	ctx := context.TODO()
	notify := storage.NotifyFunc(func(consumerId string, event *bus.Event) {
		log.Printf("consumerId: %s, event: %v", consumerId, event)
	})

	db := storageSetup(ctx, notify, t, "")
	defer db.Close()

	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	c, err := bus.NewConsumer(
		bus.WithSubject("a.b.c"),
		bus.WithDurable(),
	)
	assert.NoError(t, err)

	err = storage.SaveConsumer(ctx, conn, c)
	assert.NoError(t, err)
}

func TestAppendEvents(t *testing.T) {
	ctx := context.TODO()
	notify := storage.NotifyFunc(func(consumerId string, event *bus.Event) {
		log.Printf("consumerId: %s, event: %v", consumerId, event)
	})

	db := storageSetup(ctx, notify, t, "")
	defer db.Close()

	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	evt, err := bus.NewEvent(
		bus.WithSubject("subject"),
		bus.WithData([]byte(`{"key": "value"}`)),
		bus.WithExpiresAt(time.Hour),
	)
	assert.NoError(t, err)

	err = storage.AppendEvents(ctx, conn, evt)
	assert.NoError(t, err)
}
