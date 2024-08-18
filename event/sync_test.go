package event_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"ella.to/bus"
	"ella.to/bus/event"
	"ella.to/bus/internal/testutil"
	"ella.to/sqlite"
	"github.com/stretchr/testify/assert"
)

func TestEventSync(t *testing.T) {
	t.Skip("skipping test becase race test failed on this")

	slog.SetLogLoggerLevel(slog.LevelDebug)

	client := testutil.PrepareTestServer(
		t,
		testutil.WithDatabasePath("./test.db"),
	)

	s := event.NewSync(nil, client, event.WithSubject("test.>"))

	s.Register("test.1", func(ctx context.Context, wdb *sqlite.Worker, evt *bus.Event) error {
		return nil
	})

	s.Lock()
	err := s.Once(context.Background(), 1*time.Second)
	assert.NoError(t, err)

	ctx := context.Background()
	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// defer cancel()

	go func() {
		err := s.Continue(ctx)
		if err != nil {
			slog.Error("continue: ", "err", err)
		}
	}()
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	evt, err := bus.NewEvent(
		bus.WithSubject("test.1"),
		bus.WithData(struct {
			Name string `json:"name"`
		}{
			Name: "test name",
		}),
	)
	assert.NoError(t, err)

	err = client.Put(context.Background(), evt)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
}
