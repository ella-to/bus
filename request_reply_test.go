package bus_test

import (
	"context"
	"encoding/json"
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

	subject := "func.div"

	bus.Reply(context.TODO(), client, subject, func(ctx context.Context, in json.RawMessage) (resp any, err error) {
		req := &Req{}
		err = json.Unmarshal(in, req)
		if err != nil {
			return nil, err
		}

		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}

		return &Resp{Result: req.A / req.B}, nil
	})

	fn := bus.Request(client, subject)

	req := &Req{A: 4, B: 2}
	resp := &Resp{}
	rawResp, err := fn(context.Background(), req)
	err = json.Unmarshal(rawResp, resp)
	assert.NoError(t, err)

	assert.NoError(t, err)
	assert.Equal(t, 2, resp.Result)

	req = &Req{A: 4, B: 0}
	resp = &Resp{}
	rawResp, err = fn(context.Background(), req)
	assert.Nil(t, rawResp)
	assert.Error(t, err)
	assert.Equal(t, "division by zero", err.Error())
}
