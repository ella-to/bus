package server

import (
	"context"
	"errors"

	"ella.to/bus.go"
	"ella.to/bus.go/internal/db"
	"ella.to/bus.go/internal/sqlite"
)

func (s *Server) appendEvents(ctx context.Context, evt *bus.Event) (err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		err = db.AppendEvents(ctx, conn, evt)
	})
	return
}

func (s *Server) ackEvent(ctx context.Context, consumerId string, eventId string) (err error) {
	// NOTE: this is a ticked operation to speed up the process
	// of acking events, tick can be configured by setting server.WithAckTick
	s.tick(consumerId, func() {
		s.dbw.Submit(func(conn *sqlite.Conn) {
			err = db.AckEvent(ctx, conn, consumerId, eventId)
		})
	})
	return
}

func (s *Server) getNextEvent(ctx context.Context, consumerId string, lastEventId string) (event *bus.Event, err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		event, err = db.LoadNotAckedEvent(ctx, conn, consumerId, lastEventId)
	})
	return
}

func (s *Server) createOrGetConsumer(ctx context.Context, id string, subject string, queueName string, lastEventId string) (consumer *bus.Consumer, err error) {
	consumer, err = s.loadConsumerById(ctx, id)
	if err == nil {
		return
	} else if !errors.Is(err, db.ErrConsumerNotFound) {
		return nil, err
	}

	err = nil

	consumer, err = bus.NewConsumer(
		bus.WithDurable(id),
		bus.WithSubject(subject),
		bus.WithQueue(queueName),
		bus.WithFromEventId(lastEventId),
	)
	if err != nil {
		return nil, err
	}

	s.dbw.Submit(func(conn *sqlite.Conn) {
		err = db.SaveConsumer(ctx, conn, consumer)
		if err != nil {
			consumer = nil
		}
	})

	return
}

func (s *Server) deleteConsumer(ctx context.Context, id string) (err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		err = db.DeleteConsumerById(ctx, conn, id)
	})
	return
}

func (s *Server) createOrGetQueue(ctx context.Context, name string, lastEventId string, pos string) (queue *bus.Queue, err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		var dbQueue *bus.Queue
		dbQueue, err = db.LoadQueueByName(ctx, conn, name)
		if err == nil {
			if dbQueue.LastEventId != pos && pos != "normal" {
				err = errors.New("queue already exists and position cannot be changed")
				return
			}

			queue = dbQueue
			return
		} else if !errors.Is(err, db.ErrQueueNotFound) {
			return
		}

		newQueue := &bus.Queue{
			Name:        name,
			LastEventId: lastEventId,
		}

		err = db.SaveQueue(ctx, conn, newQueue)
		if err != nil {
			return
		}

		queue = newQueue
	})
	return
}

func (s *Server) getLastEventId(ctx context.Context, pos string) (lastEventId string, err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		// at this point, queue does not exist, we need to create one
		// we need to translate pos into last_event_id
		switch pos {
		case "all":
			lastEventId = ""
			return
		case "normal":
			lastEvent, err := db.LoadLastEvent(ctx, conn)
			if errors.Is(err, db.ErrEventNotFound) {
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

func (s *Server) loadConsumerById(ctx context.Context, id string) (consumer *bus.Consumer, err error) {
	s.dbw.Submit(func(conn *sqlite.Conn) {
		consumer, err = db.LoadConsumerById(ctx, conn, id)
	})
	return
}
