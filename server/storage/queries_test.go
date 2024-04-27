package storage_test

import (
	"context"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/bus/server/storage"
	"github.com/stretchr/testify/assert"
)

func TestAppendEvents(t *testing.T) {
	ctx := context.TODO()

	db := createTestStorage(t, "")

	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	consumer, err := bus.NewConsumer(
		bus.WithId("consumer-1"),
		bus.WithBatchSize(1),
		bus.WithSubject("a.b.*"),
	)
	assert.NoError(t, err)

	err = storage.SaveConsumer(ctx, conn, consumer)
	assert.NoError(t, err)

	now := time.Now().Add(1 * time.Hour)

	err = storage.AppendEvents(ctx, conn, &bus.Event{
		Id:        "event-1",
		Subject:   "a.b.c",
		Size:      4,
		Data:      []byte("data"),
		ExpiresAt: &now,
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	consumerIds, err := storage.LoadConsumerIdsByEventId(ctx, conn, "event-1")
	assert.NoError(t, err)

	assert.Equal(t, []string{"consumer-1"}, consumerIds)
}
