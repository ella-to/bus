package adaptor

import (
	"context"
	"encoding/json"

	"ella.to/bus"
)

//
// Ella is an adaptor for the compatibility of the bus.Stream and ella compiler for rpc calls
//

type Ella struct {
	stream bus.Stream
}

// var _ rpcAdaptor = (*Ella)(nil)

func (b *Ella) Request(ctx context.Context, topic string, in any) (json.RawMessage, error) {
	fn := bus.Request(b.stream, topic)
	return fn(ctx, in)
}

func (b *Ella) Reply(ctx context.Context, topic string, fn func(ctx context.Context, data json.RawMessage) (out any, err error)) error {
	bus.Reply(ctx, b.stream, topic, fn)
	return nil
}

func NewElla(stream bus.Stream) *Ella {
	return &Ella{stream: stream}
}
