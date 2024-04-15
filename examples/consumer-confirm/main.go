package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ella.to/bus.go"
	"ella.to/bus.go/client"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	const confirmRequired = 2

	c := client.New("http://localhost:2021")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range confirmRequired {
		go func() {
			defer func() {
				slog.Debug("closing consumer", "consumer", i)
			}()

			for msg, err := range c.Consume(ctx, bus.WithSubject("a.b.c"), bus.WithFromBeginning()) {
				if err != nil {
					fmt.Println(msg, err)
				}
			}
		}()
	}

	evt, err := bus.NewEvent(bus.WithSubject("a.b.c"), bus.WithData("hello"), bus.WithConfirm(confirmRequired))
	if err != nil {
		panic(err)
	}

	err = c.Publish(ctx, evt)
	if err != nil {
		panic(err)
	}

	cancel()

	time.Sleep(1 * time.Second)
}
