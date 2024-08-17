package server

import (
	"context"
	"errors"
	"log/slog"

	"ella.to/bus"
	"ella.to/bus/storage"
)

func (s *Server) putEvent(ctx context.Context, event *bus.Event) (err error) {
	slog.Debug("put event", "event_id", event.Id, "subject", event.Subject)

	err = s.storage.SaveEvent(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) registerConsumer(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error) {
	slog.Debug("register consumer", "consumer_id", consumer.Id, "subject", consumer.Subject)

	if consumer.LastEventId == bus.Oldest {
		consumer.LastEventId = ""
	} else if consumer.LastEventId == bus.Newest {
		lastEventId, err := s.storage.LoadLastEventId(ctx)
		if err != nil {
			return nil, err
		}
		consumer.LastEventId = lastEventId
	}

	consumer.Online = true

	// Need to check if consumer is durable and has been registered before
	// Also we need to check if the consumer is alive and a new consumer is registered
	// with the same id, which means we need to return an error

	if consumer.Type == bus.Durable {
		dbConsumer, err := s.storage.LoadConsumerById(ctx, consumer.Id)
		if err != nil && !errors.Is(err, storage.ErrConsumerNotFound) {
			return nil, err
		}

		if dbConsumer != nil {
			if dbConsumer.Type != bus.Durable {
				return nil, errors.New("consumer already exists")
			}

			consumer.Type = dbConsumer.Type
			consumer.LastEventId = dbConsumer.LastEventId
			consumer.BatchSize = dbConsumer.BatchSize
			consumer.AckedCount = dbConsumer.AckedCount
			consumer.Subject = dbConsumer.Subject
			consumer.UpdatedAt = dbConsumer.UpdatedAt
		}
	}

	err := s.storage.SaveConsumer(ctx, consumer)
	if err != nil {
		return nil, err
	}

	if consumer.QueueName == "" {
		// Put the new one
		s.consumers.Put(consumer.Subject, consumer.Id)
	} else {
		s.queuedConsumers.Put(consumer.Subject, consumer.QueueName)
	}

	// Create new batch event channel
	batchEvents := make(chan []*bus.Event, 10)
	s.consumersBatchEvents[consumer.Id] = batchEvents

	return batchEvents, nil
}

// pushEvent will be called in 2 cases:
//
// 1. when a new event is published, eventId is not empty and consumerId is empty
// 2. when a consumer is registered, eventId is emtpy and consumerId is not
func (s *Server) pushEvent(ctx context.Context, consumerId string, eventId string) {
	if eventId != "" {
		slog.Debug("push event", "event_id", eventId)

		event, err := s.storage.LoadEventById(ctx, eventId)
		if err != nil {
			slog.Error("failed to load event during pushEvent", "event_id", eventId, "error", err)
			return
		}

		consumers := s.consumers.Get(event.Subject)
		for _, id := range consumers {
			batchEvents, ok := s.consumersBatchEvents[id]
			if !ok {
				slog.Error("failed to get batch events channel during pushEvent", "consumer_id", id)
				continue
			}

			batchEvents <- []*bus.Event{event}
		}

		queueNames := s.queuedConsumers.Get(event.Subject)
		for _, queueName := range queueNames {
			consumer, err := s.storage.LoadNextQueueConsumerByName(ctx, queueName)
			if err != nil {
				continue
			}

			batchEvents, ok := s.consumersBatchEvents[consumer.Id]
			if !ok {
				continue
			}

			batchEvents <- []*bus.Event{event}
		}
	} else if consumerId != "" {
		slog.Debug("push consumer", "consumer_id", consumerId)

		consumer, err := s.storage.LoadConsumerById(ctx, consumerId)
		if err != nil {
			slog.Error("failed to load consumer during pushEvent", "consumer_id", consumerId)
			return
		}

		if consumer.QueueName != "" {
			consumer, err := s.storage.LoadNextQueueConsumerByName(ctx, consumer.QueueName)
			if err != nil {
				slog.Error("failed to load next queue consumer during pushEvent", "queue_name", consumer.QueueName)
				return
			}

			events, err := s.storage.LoadEventsByConsumerId(ctx, consumer.Id)
			if err != nil {
				slog.Error("failed to load events during pushEvent", "consumer_id", consumer.Id)
				return
			}

			if len(events) == 0 {
				return
			}

			batchEvents, ok := s.consumersBatchEvents[consumer.Id]
			if !ok {
				slog.Error("failed to get batch events channel during pushEvent", "consumer_id", consumer.Id)
				return
			}

			batchEvents <- events
		} else {

			events, err := s.storage.LoadEventsByConsumerId(ctx, consumer.Id)
			if err != nil {
				slog.Error("failed to load events during pushEvent", "consumer_id", consumer.Id)
				return
			}

			if len(events) == 0 {
				return
			}

			batchEvents, ok := s.consumersBatchEvents[consumer.Id]
			if !ok {
				slog.Error("failed to get batch events channel during pushEvent", "consumer_id", consumer.Id)
				return
			}

			batchEvents <- events
		}
	} else {
		// NOTE: this should never happen, we only log it here if this happens
		slog.Error("this should never happend in pushEvent")
	}
}

func (s *Server) ackEvent(ctx context.Context, consumerId string, eventId string) (err error) {
	slog.Debug("ack event", "consumer_id", consumerId, "event_id", eventId)
	//
	// Update the consumer acked counts and last event id
	// and if consumer is part of a queue, update all consumers last event id
	//
	err = s.storage.UpdateConsumerAck(ctx, consumerId, eventId)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) deleteConsumer(ctx context.Context, consumerId string) (err error) {
	slog.Debug("delete consumer", "consumer_id", consumerId)

	err = s.storage.DeleteConsumer(ctx, consumerId)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) deleteExpiredEvents(ctx context.Context) (err error) {
	err = s.storage.DeleteExpiredEvents(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) offlineConsumer(ctx context.Context, consumerId string) (err error) {
	consumer, err := s.storage.LoadConsumerById(ctx, consumerId)
	if err != nil {
		return err
	}

	consumer.Online = false

	err = s.storage.SaveConsumer(ctx, consumer)
	if err != nil {
		return err
	}

	return nil
}
