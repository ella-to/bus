package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/internal/testutil"
)

func TestBasic(t *testing.T) {
	testutil.PrepareTestServer(t)
}

func TestPut(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := testutil.PrepareTestServer(t)

	evt, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt)
	assert.NoError(t, err)

	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		assert.Equal(t, `"hello"`, string(msg.Events[0].Data))
		break
	}
}

func Test1000(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	c := testutil.PrepareTestServer(t)

	n := 1000

	for i := 0; i < n; i++ {
		evt, err := bus.NewEvent(
			bus.WithSubject("a.b.c"),
			bus.WithData(struct {
				Msg string
			}{
				Msg: fmt.Sprintf("hello: '%d", i),
			}),
		)
		assert.NoError(t, err)

		err = c.Put(context.Background(), evt)
		assert.NoError(t, err)
	}

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++

		if count == n {
			break
		}
	}
}

func TestBug3(t *testing.T) {
	c := testutil.PrepareTestServer(t)

	evt1, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 1"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt1)
	assert.NoError(t, err)

	evt2, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 2"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt2)
	assert.NoError(t, err)

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++

		if count == 2 {
			break
		}
	}
}

func TestQueue(t *testing.T) {
	c := testutil.PrepareTestServer(t)

	evt1, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 1"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt1)
	assert.NoError(t, err)

	evt2, err := bus.NewEvent(
		bus.WithSubject("a.b.c"),
		bus.WithData("hello 2"),
	)
	assert.NoError(t, err)

	err = c.Put(context.Background(), evt2)
	assert.NoError(t, err)

	count := 0
	for msg, err := range c.Get(
		context.Background(),
		bus.WithSubject("a.>"),
		bus.WithFromOldest(),
	) {
		assert.NoError(t, err)
		assert.Equal(t, 1, len(msg.Events))
		count++

		if count == 2 {
			break
		}
	}
}
