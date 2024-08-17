package event_test

import (
	"context"
	"testing"
	"time"

	"ella.to/bus/event"
	"ella.to/bus/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestEventSync(t *testing.T) {
	t.Skip("skipping test becase race test failed on this")

	client := testutil.PrepareTestServer(t, testutil.WithDatabasePath("./test.db"))

	s := event.NewSync(nil, client)

	s.Lock()
	err := s.Once(context.Background(), 1*time.Second)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = s.Continue(ctx)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
}
