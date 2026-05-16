// Request/Reply example: a service consumes math.add events and replies
// with the sum, while the requester awaits the response.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

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
	srv, err := bus.NewDevServer()
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Shutdown()

	client, err := bus.NewClient(srv.ClientURL())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		for event, err := range client.Get(ctx,
			bus.WithSubject("math.add"),
			bus.WithStartFrom(bus.StartOldest),
			bus.WithAckStrategy(bus.AckManual),
		) {
			if err != nil {
				return
			}
			var req Request
			if err := json.Unmarshal(event.Payload, &req); err != nil {
				log.Printf("bad request: %v", err)
				continue
			}
			resp := Response{Result: req.A + req.B}
			if err := event.Ack(ctx, bus.WithData(resp)); err != nil {
				log.Printf("ack error: %v", err)
			}
		}
	}()

	resp := client.Put(ctx,
		bus.WithSubject("math.add"),
		bus.WithData(Request{A: 1, B: 2}),
		bus.WithRequestReply(),
	)
	if err := resp.Error(); err != nil {
		log.Fatal(err)
	}

	var out Response
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1 + 2 = %d\n", out.Result)
}
