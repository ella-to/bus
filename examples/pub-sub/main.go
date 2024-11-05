package main

import (
	"context"

	"ella.to/bus"
)

func main() {
	client := bus.NewClient("http://localhost:2021")

	ctx := context.Background()

	// publish an event to subject "a.b.c" with data "hello world"
	err := client.Put(
		ctx,
		bus.WithSubject("a.b.c"),
		bus.WithData("hello world"),
	).Error()
	if err != nil {
		panic(err)
	}

	// subscribe to subject "a.b.c" and since subscription is blocking
	// we can use range to iterate over the events. For every event we
	// need to ack it. If ack is not called, the event will be redelivered.
	// Since an event is already published, we start from the oldest event by passing bus.WithStartFrom(bus.StartOldest) options.
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

		// ack the event
		if err := event.Ack(ctx); err != nil {
			panic(err)
		}

		// since there is only one event, we can break the loop
		break
	}
}
