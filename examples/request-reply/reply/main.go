package main

import (
	"context"
	"encoding/json"
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

	bus.Reply(ctx, c, "func.div", func(ctx context.Context, in json.RawMessage) (any, error) {
		fmt.Println("Got a request")

		req := &data.Req{}
		err := json.Unmarshal(in, req)
		if err != nil {
			return nil, err
		}

		if req.B == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return &data.Resp{Result: req.A / req.B}, nil
	})

	select {}
}
