package main

import (
	"context"
	"fmt"

	"ella.to/bus.go"
	"ella.to/bus.go/client"
)

func main() {
	ctx := context.Background()

	addr := "http://localhost:2021"

	c := client.New(addr)

	evt, err := bus.NewEvent(bus.WithSubject("a.b.c"))
	if err != nil {
		panic(err)
	}
	c.Publish(ctx, evt)

	msgs, err := c.Consume(ctx, bus.WithSubject("a.b.c"), bus.WithFromBeginning())
	if err != nil {
		panic(err)
	}

	for msg, err := range msgs {
		fmt.Println(msg, err)
	}
}
