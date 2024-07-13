```
______
| ___ \
| |_/ /_   _ ___
| ___ \ | | / __|
| |_/ / |_| \__ \
\____/ \__,_|___/

```

# Intorduction

Bus is my powerful event bus focus on simplicity and based on Golang new iterate feature

# Features

- [x] Based on the new iterate feature in Golang 1.22
- [x] Support Request/Reply
- [x] Support Durable stream
- [x] Support consume batching
- [x] Support Manual Acking
- [x] Support Group Consumers using queue's name
- [x] Support confirmation that at least n number of consumers acking the event
- [x] The bus server exposes all features over `http` and `SSE`, and currently only Go, the client, is implemented. However, any other client can be written quite easily.
- [x] Support in memory server for testing purposes, only in Golang

# Usage

## Bus Server

There are 2 ways bus server can be executed. using docker and docker-compose, run it as a library imported to your go program which can be used for testing purposes.

### Docker and Docker-Compose

simply copy/paste the following docker-compose and run it using `docker-compose up -d` command. By deafult bus server uses 2021 port

```yaml
version: "3.5"

services:
  bus:
    image: ellato/bus:latest

    environment:
      - BUS_LOG_LEVEL=INFO
      - BUS_SERVER_ADDR=0.0.0.0:2021
      - BUS_STORAGE_POOL_SIZE=10
      # if you want to use in memory
      # dont set BUS_STORAGE_PATH
      - BUS_STORAGE_PATH=/storage/bus.db
      - BUS_WORKER_BUFFER_SIZE=1000
      - BUS_CLEAN_EXPIRED_EVENTS_FREQ=30s

    # if ports is removed, then the bus service can
    # only be accessed from within the network bus
    # make sure to add the bus network to the other services
    ports:
      - "2021:2021"

    # make sure to use the right network
    networks:
      - bus

    volumes:
      - ./storage:/storage

networks:
  bus:
    name: bus_net
    driver: bridge
```

### Run as Library with your Go program

bus server implements `http.Handler` which can be used with go standard library. The following example demonstrates a way of running bus server

```go
package main

import (
    "net/http/httptest"

    "ella.to/bus/server"
)

func runBusServer(ctx context.Context) (url string, err error) {
    // Refer to documentation about the server options
    busServer, err := server.New(ctx, server.WithStoragePoolSize(10))
	if err != nil {
		return "", err
	}

	server := httptest.NewServer(busServer)
	go func() {
		<-ctx.Done()
		server.Close()
	}()

	slog.Info("dev bus server started", "addr", server.URL)

	return server.URL, nil
}
```

## Bus Client

Currently, only Go client is provided, however any other client on different languages can be implemented since the protocol is HTTP and SSE. For furthur information, pelase refer to implementation of client.

> Note: in order to use this library, Go 1.22+ is required and prior or Go 1.22, `GOEXPERIMENT=rangefunc` environment variable must be provided since bus is using new iterator feature.

- first get the bus golang client sdk

```
go get ella.to/bus
```

- then create a bus client as follows

```golang

import (
    "ella.to/bus/client"
)

const BusServerURL = "http://localhost:2021"

func main() {
    busClient, err := client.New(
        client.WithAddr(BusServerURL)
    )
    if err != nil {
        panic(err)
    }

    _ = busClient
}

```

- bus implements 4 single method interfaces, `Putter`, `Getter`, `Acker` and `Closer`.
