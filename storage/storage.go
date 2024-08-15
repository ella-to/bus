package storage

import (
	"context"
	"errors"

	"ella.to/bus"
)

var (
	ErrEventNotFound    = errors.New("event not found")
	ErrConsumerNotFound = errors.New("consumer not found")
)

type Storage interface {
	SaveEvent(ctx context.Context, event *bus.Event) error
	SaveConsumer(ctx context.Context, consumer *bus.Consumer) error
	LoadEventById(ctx context.Context, eventId string) (*bus.Event, error)
	LoadConsumerById(ctx context.Context, consumerId string) (*bus.Consumer, error)
	LoadNextQueueConsumerByName(ctx context.Context, queueName string) (*bus.Consumer, error)
	LoadEventsByConsumerId(ctx context.Context, consumerId string) ([]*bus.Event, error)
	LoadLastEventId(ctx context.Context) (string, error)
	UpdateConsumerAck(ctx context.Context, consumerId string, eventId string) error
	DeleteConsumer(ctx context.Context, consumerId string) error
	Close() error
}
