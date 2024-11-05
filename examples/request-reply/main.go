package main

import (
	"context"
	"encoding/json"

	"ella.to/bus"
)

type Request struct {
	A int
	B int
}

type Response struct {
	Result int
}

func main() {
	client := bus.NewClient("http://localhost:2021")

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for event, err := range client.Get(ctx, bus.WithSubject("math.add"), bus.WithStartFrom(bus.StartOldest)) {
			if err != nil {
				panic(err)
			}

			req := &Request{}
			if err := json.Unmarshal(event.Payload, req); err != nil {
				panic(err)
			}

			resp := &Response{
				Result: req.A + req.B,
			}

			err = event.Ack(ctx, bus.WithData(resp))
			if err != nil {
				panic(err)
			}
		}
	}()

	resp := client.Put(
		ctx,
		bus.WithSubject("math.add"),
		bus.WithData(Request{A: 1, B: 2}),
		bus.WithRequestReply(),
	)

	if resp.Error() != nil {
		panic(resp.Error())
	}

	var response Response
	err := json.Unmarshal(resp.Payload, &response)
	if err != nil {
		panic(err)
	}

	println(response.Result)
}
