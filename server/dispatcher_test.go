package server

import (
	"context"
	"log/slog"
	"testing"

	"ella.to/bus"
)

func TestDispatcher(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	putEventFunc := func(ctx context.Context, evt *bus.Event) error {
		return nil
	}
	registerConsumerFunc := func(ctx context.Context, consumer *bus.Consumer) (<-chan []*bus.Event, error) {
		return nil, nil
	}
	pushEventFunc := func(ctx context.Context, consumerId string, eventId string) {}
	ackEventFunc := func(ctx context.Context, consumerId string, eventId string) error {
		return nil
	}
	deleteConsumerFunc := func(ctx context.Context, consumerId string) error {
		return nil
	}
	deleteExpiredEvents := func(ctx context.Context) error {
		return nil
	}

	dispatcher := NewDispatcher(
		1,
		1,

		withPutEventFunc(putEventFunc),
		withRegisterConsumerFunc(registerConsumerFunc),
		withPushEventFunc(pushEventFunc),
		withAckEventFunc(ackEventFunc),
		withDeleteConsumerFunc(deleteConsumerFunc),
		withDeleteExpiredEventsFunc(deleteExpiredEvents),
	)

	n := 100

	for i := 0; i < n; i++ {
		dispatcher.PutEvent(context.Background(), &bus.Event{})
	}

	dispatcher.Close()
}
