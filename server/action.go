package server

import (
	"context"
	"log/slog"

	"ella.to/bus"
)

func (s *Server) putEvent(ctx context.Context, event *bus.Event) (err error) {
	err = s.storage.SaveEvent(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) registerConsumer(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error) {
	if consumer.LastEventId == "oldest" {
		consumer.LastEventId = ""
	} else if consumer.LastEventId == "latest" {
		lastEventId, err := s.storage.LoadLastEventId(ctx)
		if err != nil {
			return nil, err
		}
		consumer.LastEventId = lastEventId
	}

	err := s.storage.SaveConsumer(ctx, consumer)
	if err != nil {
		return nil, err
	}

	if consumer.QueueName == "" {
		// Delete the old consumer with the same id
		s.consumers.Del(consumer.Subject, consumer.Id, func(a, b string) bool {
			return a == b
		})

		// Put the new one
		s.consumers.Put(consumer.Subject, consumer.Id)
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
func (s *Server) pushEvent(ctx context.Context, eventId string, consumerId string) {
	if eventId != "" {
		event, err := s.storage.LoadEventById(ctx, eventId)
		if err != nil {
			slog.Error("failed to load event during pushEvent", "event_id", eventId)
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
	} else if consumerId != "" {
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

func (s *Server) ackEvent(ctx context.Context, consumerId, eventId string) (err error) {
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
	err = s.storage.DeleteConsumer(ctx, consumerId)
	if err != nil {
		return err
	}

	return nil
}
