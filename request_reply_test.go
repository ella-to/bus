package bus_test

import (
	"context"
	"fmt"
	"testing"

	"ella.to/bus"
	"github.com/stretchr/testify/assert"
)

func TestRequestReply(t *testing.T) {
	client := setupBusServer(t)

	type Req struct {
		A int
		B int
	}

	type Resp struct {
		Result int
	}

	bus.Reply(context.TODO(), client, "func.div", func(ctx context.Context, req *Req) (*Resp, error) {
		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}

		return &Resp{Result: req.A / req.B}, nil
	})

	fn := bus.Request[*Req, *Resp](client, "func.div")

	req := &Req{A: 4, B: 2}
	resp, err := fn(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, 2, resp.Result)

	req = &Req{A: 4, B: 0}
	resp, err = fn(context.Background(), req)
	assert.Error(t, err)
	assert.Equal(t, "division by zero", err.Error())
	assert.Nil(t, resp)
}
