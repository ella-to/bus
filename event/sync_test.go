package event_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/bus/bustest"
	"ella.to/bus/event"

	"github.com/stretchr/testify/assert"
)

func TestEventSync(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	client := bustest.GetNatsClient(t)

	s := event.NewSync(client, event.WithSubject("test.>"))

	s.Register("test.1", func(ctx context.Context, evt *bus.Event) error {
		return nil
	})

	s.Lock()
	err := s.Once(context.Background(), 1*time.Second)
	assert.NoError(t, err)

	ctx := context.Background()

	go func() {
		err := s.Continue(ctx)
		if err != nil {
			slog.Error("continue: ", "err", err)
		}
	}()
	assert.NoError(t, err)

	err = client.Put(
		context.Background(),
		bus.WithSubject("test.1"),
		bus.WithData(
			struct {
				Name string `json:"name"`
			}{
				Name: "test name",
			},
		),
	)
	assert.NoError(t, err)
}
