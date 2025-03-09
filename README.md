```
██████╗░██╗░░░██╗░██████╗
██╔══██╗██║░░░██║██╔════╝
██████╦╝██║░░░██║╚█████╗░
██╔══██╗██║░░░██║░╚═══██╗
██████╦╝╚██████╔╝██████╔╝
╚═════╝░░╚═════╝░╚═════╝░
```

# Bus: A Persistent and High-Performance Message Bus

`bus` is a robust, persistent message bus designed to streamline event handling with simplicity and flexibility. Based on [task](https://ella.to/task), a task runner, [solid](https://ella.to/solid), a signal/broadcast library, and [immuta](https://ella.to/immuta), an append-only log, `bus` delivers high performance, intuitive APIs, and resilient message persistence.

## Key Features

- ✅ **Persistent Event Storage** - Ensures message durability and reliability with a persistent log.
- ✅ **Pattern Matching on Subjects** - Supports wildcard patterns like `a.*.b` or `a.>` to route events dynamically.
- ✅ **Request/Reply Pattern** - Easily implement request-response communication with in-built support.
- ✅ **HTTP and Server-Sent Events (SSE)** - Uses both standard HTTP and SSE for flexible, web-friendly transport.
- ✅ **Multi-Consumer Confirmation** - Allows publishers to confirm when an event is acknowledged by a specified number of consumers.
- ✅ **Ergonomic, Idiomatic API** - Designed with simplicity, adhering closely to Golang conventions for ease of use.
- ✅ **High Performance** - Optimized for rapid event persistence and delivery.
- ✅ **Redelivery and Acknowledgement** - Provides automatic message redelivery and various acknowledgement strategies for reliability.
- ✅ **CLI for Debugging** - Comes with a command-line interface to publish, consume, and debug events easily.

## Installation

To install `bus`, use:

```shell
go get ella.to/bus@v0.3.6
```

to install a cli, run the following

```shell
go install ella.to/bus/cmd/bus@v0.3.6
```

and to run the server using docker, simply use the provided docker-compose and run it

```
docker-compose up
```

## Namespaces

Namespaces have been introduced to efficiently organize events by ensuring that not all events are saved in a single file. Each namespace has its own dedicated file. All namespaces must be defined when starting the Bus server by using the `--namespaces` flag.

### What Are Namespaces?

Namespaces are essentially the first segment of a topic. For example, in the topic a.b.c, the namespace is a.

The Bus server also includes a special namespace called `_bus_`, reserved for internal bus operations. It is strongly recommended not to consume events from the `_bus_` namespace.

### Best Practices for Namespaces

When defining namespaces, consider your business logic and choose meaningful names that clearly represent their purpose. For instance:

- If the Bus is used to handle RPC calls, a good namespace might be `rpc`.
- For user-related operations, you might use user.

### Key Features and Limitations

Event Sequencing Within Namespaces: The Bus guarantees the sequence of events stored within a single namespace.
No Cross-Namespace Sequencing Guarantee: The Bus does not guarantee the sequence of messages stored across different namespaces.
By following these guidelines, you can keep your Bus server organized and aligned with your application's goals.

## Basic Example

At its core, bus is a pub/sub library, enabling asynchronous communication between publishers and subscribers. Here’s how to publish an event after creating a client

```golang
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
```

## More Examples

for more examples, checkout examples folder

# Reference

logo was created using https://fsymbols.com/generators/carty
