// Confirm example: the publisher waits for one consumer ack before its
// Put call returns.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"ella.to/bus"
)

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
			bus.WithSubject("a.b.c"),
			bus.WithStartFrom(bus.StartOldest),
			bus.WithAckStrategy(bus.AckManual),
		) {
			if err != nil {
				return
			}
			fmt.Printf("subscriber got: %s\n", string(event.Payload))
			if err := event.Ack(ctx); err != nil {
				log.Printf("ack error: %v", err)
			}
			return
		}
	}()

	if err := client.Put(ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("hello world"),
		bus.WithConfirm(1),
	).Error(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("publisher confirmed")
}
