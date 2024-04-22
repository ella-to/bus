package main

import (
	"context"
	"fmt"

	"ella.to/bus"
	"ella.to/bus/client"

	"ella.to/bus/examples/request-reply/data"
)

func main() {
	c, err := client.New(client.WithAddr("http://localhost:2021"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus.Reply(ctx, c, "func.div", func(ctx context.Context, req *data.Req) (*data.Resp, error) {
		fmt.Println("Got a request")
		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return &data.Resp{Result: req.A / req.B}, nil
	})

	select {}
}
