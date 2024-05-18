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

	fn := bus.Request(c, "func.div")

	for range 1000 {
		req := &data.Req{A: 4, B: 2}
		resp := &data.Resp{}
		err := fn(ctx, req, resp)
		if err != nil {
			fmt.Printf("%s = %s\n", req, err)
		} else {
			fmt.Printf("%s = %s\n", req, resp)
		}
	}
}
