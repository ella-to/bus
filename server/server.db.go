package server

import (
	"context"
	"errors"

	"ella.to/bus"
	"ella.to/bus/server/storage"
	"ella.to/sqlite"
)

func (h *Handler) AppendEvents(ctx context.Context, event *bus.Event) (err error) {
	h.batch.Add(event)
	return
}

func (h *Handler) DeleteConsumer(ctx context.Context, id string) (err error) {
	h.dbw.Submit(func(conn *sqlite.Conn) {
		err = storage.DeleteConsumer(ctx, conn, id)
	})

	return
}

func (h *Handler) AckEvent(ctx context.Context, consumerId, eventId string) (err error) {
	h.dbw.Submit(func(conn *sqlite.Conn) {
		err = storage.AckEvent(ctx, conn, consumerId, eventId)
	})

	return
}

func (h *Handler) GetLastEventId(ctx context.Context, pos string) (lastEventId string, err error) {
	h.dbw.Submit(func(conn *sqlite.Conn) {
		switch pos {
		case "oldest":
			lastEventId = ""
			return
		case "newest":
			lastEvent, err := storage.LoadLastEvent(ctx, conn)
			if errors.Is(err, storage.ErrEventNotFound) {
				lastEventId = ""
				return
			} else if err != nil {
				return
			} else {
				lastEventId = lastEvent.Id
				return
			}
		default:
			lastEventId = pos
			return
		}
	})
	return
}

func (h *Handler) LoadConsumerById(ctx context.Context, id string) (consumer *bus.Consumer, err error) {
	h.dbw.Submit(func(conn *sqlite.Conn) {
		consumer, err = storage.LoadConsumerById(ctx, conn, id)
	})
	return
}

func (h *Handler) CreateConsumer(ctx context.Context, consumer *bus.Consumer) (err error) {
	h.dbw.Submit(func(conn *sqlite.Conn) {
		err = storage.SaveConsumer(ctx, conn, consumer)
	})

	return
}
