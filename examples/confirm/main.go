package main

import (
	"context"

	"ella.to/bus"
)

func main() {
	client := bus.NewClient("http://localhost:2021")

	ctx := context.Background()

	go func() {
		for event, err := range client.Get(
			ctx,
			bus.WithSubject("a.b.c"),
			bus.WithStartFrom(bus.StartOldest),
		) {
			if err != nil {
				panic(err)
			}

			// do something with the event
			// e.g. print the data
			println(string(event.Payload))

			// ack the event, this will unblock the publisher
			// that has 1 confirmation waiting.
			if err := event.Ack(ctx); err != nil {
				panic(err)
			}

			// since there is only one event, we can break the loop
			break
		}
	}()

	// publish an event to subject "a.b.c" with data "hello world"
	// Since we want to confirm the event, we need to pass bus.WithConfirm(1) option.
	// this will wait for 1 confirmation before returning.
	err := client.Put(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("hello world"),
		bus.WithConfirm(1),
	).Error()
	if err != nil {
		panic(err)
	}
}
