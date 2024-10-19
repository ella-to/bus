package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"ella.to/immuta"

	"ella.to/bus"
)

// Use this program to print the events from the events log file.
// Usage:
// 		go run cmd/read-events/main.go ./events.log

func main() {
	if len(os.Args) < 2 {
		log.Fatal("missing filepath argument")
	}

	filepath := os.Args[1]

	im, err := immuta.New(immuta.WithFastWrite(filepath))
	if err != nil {
		log.Fatal(err)
	}

	defer im.Close()

	stream, err := im.Stream(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Done()

	var count int64
	var size int64
	for {
		r, s, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		var event bus.Event

		json.NewDecoder(r).Decode(&event)

		count++
		size += s

		fmt.Printf("% 6d: %s\n", count, event.String())
	}

	fmt.Println("-----")
	fmt.Printf("total events:\t%d\n", count)
	fmt.Printf("total size:\t%d bytes\n", size)
	fmt.Println("-----")
}
