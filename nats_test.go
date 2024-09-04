package bus_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"ella.to/bus"
	"ella.to/bus/bustest"
)

func TestNatsServerBootup(t *testing.T) {
	bustest.StartDefaultNatsServer(t, 1)
}

func TestNatsClientConnect(t *testing.T) {
	ctx := context.Background()

	url := bustest.StartDefaultNatsServer(t, 1)

	client, err := bus.NewNatsClient(
		ctx,
		bus.WithNatsURL(url),
		bus.WithNatsStream("test", "test.>"),
	)
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestNatsSimplePut(t *testing.T) {
	ctx := context.Background()

	client := bustest.GetNatsClient(t)

	err := client.Put(ctx, bus.WithSubject("test.1"), bus.WithData("hello world"))
	assert.NoError(t, err)
}

func TestNatsPutGet(t *testing.T) {
	ctx := context.Background()

	client := bustest.GetNatsClient(t)

	err := client.Put(
		ctx,
		bus.WithSubject("test.1"),
		bus.WithData("hello world"),
	)
	assert.NoError(t, err)

	for msg, err := range client.Get(
		ctx,
		bus.WithSubject("test.1"),
		bus.WithBatchSize(1),
		bus.WithFromOldest(),
	) {

		var result string
		err = msg.Unmarshal(&result)
		assert.NoError(t, err)

		assert.NoError(t, err)
		assert.Equal(t, "hello world", result)

		err = msg.Ack()
		assert.NoError(t, err)
		break
	}
}

func TestNatsPutGetConfirm(t *testing.T) {
	ctx := context.Background()

	client := bustest.GetNatsClient(t)

	var requestNumConfirms int32 = 10

	var count atomic.Int32

	for range requestNumConfirms {
		go func() {
			for msg, err := range client.Get(
				ctx,
				bus.WithSubject("test.1"),
				bus.WithBatchSize(1),
				bus.WithFromOldest(),
			) {
				var result string
				err = msg.Unmarshal(&result)
				assert.NoError(t, err)

				assert.NoError(t, err)
				assert.Equal(t, "hello world", result)

				count.Add(1)
				err = msg.Ack()
				assert.NoError(t, err)
				break
			}
		}()
	}

	err := client.Put(
		ctx,
		bus.WithSubject("test.1"),
		bus.WithData("hello world"),
		bus.WithConfirm(int(requestNumConfirms)),
	)
	assert.NoError(t, err)

	assert.Equal(t, requestNumConfirms, count.Load())
}

func TestNatsRequestReply(t *testing.T) {
	ctx := context.Background()

	client := bustest.GetNatsClient(t)

	type request struct {
		A int
		B int
	}

	type reply struct {
		Result int
	}

	err := client.Reply(ctx, "test.add", func(ctx context.Context, req json.RawMessage) (any, error) {
		var r request
		err := json.Unmarshal(req, &r)
		if err != nil {
			return nil, err
		}

		return reply{Result: r.A + r.B}, nil
	})
	assert.NoError(t, err)

	for range 100 {
		result, err := client.Request(ctx, "test.add", request{A: 1, B: 2})
		assert.NoError(t, err)

		var r reply
		err = json.Unmarshal(result, &r)
		assert.NoError(t, err)

		assert.Equal(t, 3, r.Result)
	}
}
