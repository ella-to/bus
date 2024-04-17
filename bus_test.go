package bus_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/client"
	"ella.to/bus/server"
)

func runBusServer(t testing.TB, ctx context.Context) *client.Client {
	busServer, err := server.New(
		ctx,
		// server.WithDBPath("./test.db"),
	)
	assert.NoError(t, err)

	httpServer := httptest.NewServer(busServer)
	t.Cleanup(func() {
		busServer.Close()
	})

	busClient, err := client.New(client.WithAddr(httpServer.URL))
	assert.NoError(t, err)

	return busClient
}

func TestRequestReply(t *testing.T) {
	ctx := context.Background()

	busClient := runBusServer(t, ctx)

	type Req struct {
	}

	type Resp struct {
	}

	var func1Called bool
	var func2Called bool

	bus.Reply(ctx, busClient, "test.a", func(context.Context, Req) (Resp, error) {
		func1Called = true
		return Resp{}, nil
	})

	bus.Reply(ctx, busClient, "test.a", func(context.Context, Req) (Resp, error) {
		func2Called = true
		return Resp{}, nil
	})

	fn := bus.Request[Req, Resp](busClient, "test.a")

	resp, err := fn(ctx, Req{})

	assert.NoError(t, err)
	assert.Equal(t, Resp{}, resp)

	resp, err = fn(ctx, Req{})

	assert.NoError(t, err)
	assert.Equal(t, Resp{}, resp)

	assert.True(t, func1Called)
	assert.True(t, func2Called)
}

func TestRequestReplyUnderLoad(t *testing.T) {
	ctx := context.Background()

	busClient := runBusServer(t, ctx)

	type Req struct {
	}

	type Resp struct {
	}

	var count1 int64
	var count2 int64

	bus.Reply(ctx, busClient, "test.a", func(context.Context, Req) (Resp, error) {
		atomic.AddInt64(&count1, 1)
		return Resp{}, nil
	})

	bus.Reply(ctx, busClient, "test.a", func(context.Context, Req) (Resp, error) {
		atomic.AddInt64(&count2, 1)
		return Resp{}, nil
	})

	fn := bus.Request[Req, Resp](busClient, "test.a")

	const loadCount = 10
	const workerCount = 5
	const totalCount = int64(loadCount * workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()

			for i := 0; i < loadCount; i++ {
				resp, err := fn(ctx, Req{})
				assert.NoError(t, err)
				assert.Equal(t, Resp{}, resp)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, totalCount, count1+count2)
}
