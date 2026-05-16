// Pub/Sub example: a publisher emits one event, and a subscriber receives
// the very same event from the start of the stream.
package main

import (
	"context"
	"fmt"
	"log"

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

	ctx := context.Background()

	resp := client.Put(ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("hello world"),
	)
	if err := resp.Error(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("published %s (index=%d)\n", resp.Id, resp.Index)

	for event, err := range client.Get(ctx,
		bus.WithSubject("a.b.c"),
		bus.WithStartFrom(bus.StartOldest),
		bus.WithAckStrategy(bus.AckManual),
	) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("received: %s\n", string(event.Payload))
		if err := event.Ack(ctx); err != nil {
			log.Fatal(err)
		}
		break
	}
}
