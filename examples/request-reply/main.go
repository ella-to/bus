package main

import (
	"context"

	"ella.to/bus.go"
	"ella.to/bus.go/client"
)

type Req struct {
	A int
	B int
}

type Resp struct {
	Result int
}

func main() {
	c := client.New("http://localhost:2021")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus.Reply(ctx, c, "func.sum", func(ctx context.Context, req Req) (Resp, error) {
		return Resp{Result: req.A + req.B}, nil
	})

	fn := bus.Request[Req, Resp](c, "func.sum")

	resp, err := fn(ctx, Req{A: 1, B: 2})
	if err != nil {
		panic(err)
	}

	println(resp.Result)
}
