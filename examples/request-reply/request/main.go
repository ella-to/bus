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

	fn := bus.Request[*data.Req, *data.Resp](c, "func.div")

	for range 1000 {
		req := &data.Req{A: 4, B: 2}
		resp, err := fn(ctx, req)
		if err != nil {
			fmt.Printf("%s = %s\n", req, err)
		} else {
			fmt.Printf("%s = %s\n", req, resp)
		}
	}
}
