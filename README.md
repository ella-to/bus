# bus (NATS engine)

A drop-in implementation of the [`ella.to/bus`](https://github.com/ella-to/bus)
public API powered by [NATS.io](https://nats.io) / JetStream.

The exposed types — `Event`, `Response`, `Putter`, `Getter`, `Acker`, `Client`
and every `With...` option — keep the same names and semantics as upstream so
existing call sites can switch engines simply by re-importing this module.
The persistent append-only log, custom HTTP server and SSE transport are
replaced with a NATS connection plus a JetStream stream per namespace.

```text
┌──────────────┐      ┌──────────────────────────────────────────────┐
│ bus.Client   │ ───▶ │ NATS server  ─ Core NATS pub/sub  (inboxes)  │
│ (your code)  │ ◀─── │              ─ JetStream streams  (events)   │
└──────────────┘      └──────────────────────────────────────────────┘
```

## Install

```bash
go get ella.to/bus
```

The module needs a NATS server with JetStream enabled. For tests and local
development a fully embedded server ships in this package — no extra binaries
or containers required.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "ella.to/bus"
)

func main() {
    srv, err := bus.NewDevServer()
    if err != nil { log.Fatal(err) }
    defer srv.Shutdown()

    client, err := bus.NewClient(srv.ClientURL())
    if err != nil { log.Fatal(err) }
    defer client.Close()

    ctx := context.Background()

    if err := client.Put(ctx,
        bus.WithSubject("orders.created"),
        bus.WithData(map[string]string{"id": "1"}),
    ).Error(); err != nil {
        log.Fatal(err)
    }

    for event, err := range client.Get(ctx,
        bus.WithSubject("orders.*"),
        bus.WithStartFrom(bus.StartOldest),
        bus.WithAckStrategy(bus.AckManual),
    ) {
        if err != nil { log.Fatal(err) }
        fmt.Printf("got %s -> %s\n", event.Subject, event.Payload)
        _ = event.Ack(ctx)
        break
    }
}
```

Pointing the client at a real NATS cluster is the same call with a different
URL:

```go
client, err := bus.NewClient("nats://nats.example.com:4222",
    bus.WithNatsOptions(nats.Token("..."), nats.Name("my-service")),
)
```

## Public API

### Client construction

| Function / Option                                | Purpose                                                                  |
| ------------------------------------------------ | ------------------------------------------------------------------------ |
| `NewClient(url, opts...)`                        | Connects to NATS and returns a ready-to-use Client.                      |
| `WithNatsOptions(opts...nats.Option)`            | Forwards `nats.Option`s to the underlying connection.                    |
| `WithNatsConn(*nats.Conn)`                       | Reuse an existing NATS connection (caller manages its lifecycle).        |
| `WithDefaultStreamConfig(jetstream.StreamConfig)`| Baseline used when ensuring a per-namespace stream.                      |
| `WithStreamConfigurer(fn)`                       | Hook to mutate the StreamConfig (per-namespace) before it's created.     |

The Client is also a `Putter`, `Getter` and `Acker`. `Client.Close()` closes
the connection if `NewClient` opened it; if it was passed via `WithNatsConn`,
the caller stays in charge.

### Putter — `client.Put(ctx, opts...) *Response`

| Option                              | Notes                                                            |
| ----------------------------------- | ---------------------------------------------------------------- |
| `WithSubject(subject)`              | Required. Validated for `*` / `>` (publishers must be concrete). |
| `WithData(any)`                     | JSON-encoded payload (string / []byte / number / struct / etc.). |
| `WithKey(key)`                      | Stored on the event; useful for downstream deduping.             |
| `WithTraceId(id)` / `WithId(id)`    | Override the auto-generated trace id / event id.                 |
| `WithCreatedAt(t)`                  | Override the auto-set `time.Now()`.                              |
| `WithRequestReply()`                | RPC-style: waits for one reply.                                  |
| `WithConfirm(n)`                    | Waits for `n` consumer acks before returning.                    |
| `Batch(items...)`                   | Publishes a batch of events sharing one namespace.               |

The returned `*Response` carries `Id`, `Index` (JetStream sequence), `CreatedAt`
and (for request/reply) the reply `Payload`. `Response.Error()` returns nil on
success, or a non-nil error if the engine failed or the reply payload itself
is a non-JSON string.

### Getter — `client.Get(ctx, opts...) iter.Seq2[*Event, error]`

| Option                              | Notes                                                            |
| ----------------------------------- | ---------------------------------------------------------------- |
| `WithSubject(subject)`              | Required. May contain `*` and `>` wildcards.                     |
| `WithStartFrom(StartOldest/Newest/"e_id")` | Where to start reading. The `"e_id"` form starts *after* that event. |
| `WithAckStrategy(AckManual/AckNone)`| Manual ack uses JetStream `AckExplicit`; none uses `AckNone`.    |
| `WithDelivery(d, n)`                | AckWait + MaxDeliver. `n <= 0` means redeliver indefinitely.     |
| `WithExtractMeta(fn)`               | Receives `{"consumer-id": "c_..."}` once the consumer is set up. |

The iterator yields each `*Event` together with a non-nil error when the
underlying transport hiccups. Breaking out of the loop (or cancelling the
context) tears down the JetStream consumer.

### Acker

Each yielded event carries an internal acker tied to the specific JetStream
delivery, so the canonical path is:

```go
event.Ack(ctx)                       // simple ack
event.Ack(ctx, bus.WithData(reply))  // ack + reply (used in request/reply)
```

`Client.Ack(ctx, consumerId, eventId)` is preserved on the interface but is
intentionally a no-op for the NATS engine — there is no way to retroactively
locate a delivery from those values once it has been dropped, so always ack
through the event itself.

## How upstream concepts map onto NATS

| Upstream bus concept              | NATS-backed implementation                                       |
| --------------------------------- | ---------------------------------------------------------------- |
| Append-only log per namespace     | One JetStream stream `BUS_<namespace>` with subjects `<ns>.>`    |
| Event id                          | `e_<xid>` set on the `Bus-Event-Id` header + `Nats-Msg-Id` (dedup)|
| Event index                       | JetStream stream sequence number                                 |
| Subject pattern matching          | NATS subject wildcards (`*`, `>`)                                |
| Manual ack + redelivery           | `AckExplicitPolicy` + `AckWait` + `MaxDeliver`                   |
| Auto ack (`AckNone`)              | `AckNonePolicy`                                                  |
| Batch publish                     | Sequential `js.PublishMsg` calls under a single namespace check  |
| Request/Reply (`_bus_.<id>`)      | Core NATS subscription on the inbox, no JetStream persistence    |
| Confirm (`WithConfirm(n)`)        | Same inbox, but the publisher waits for `n` acks                 |
| `WithExtractMeta`                 | Returns the auto-generated consumer id (`c_<xid>`)               |

Streams are created lazily the first time a namespace is touched, and the
defaults (`FileStorage`, `LimitsPolicy`) can be customised via
`WithDefaultStreamConfig` or `WithStreamConfigurer`.

## Embedded dev server

```go
srv, err := bus.NewDevServer(
    bus.WithDevServerHost("127.0.0.1"),        // default
    bus.WithDevServerPort(-1),                 // -1 = random free port
    bus.WithDevServerStoreDir("/tmp/store"),   // pin the JetStream store dir
)
defer srv.Shutdown()

url := srv.ClientURL()    // pass to NewClient
nc  := srv.Server()       // *natsserver.Server for advanced cases
```

`NewDevServer` boots a real `nats-server` in-process with JetStream enabled
and waits until it's accepting connections. It's the same engine you'd run
in production — perfect for integration tests without spinning up Docker.

## Examples

- [`examples/pub-sub`](examples/pub-sub) — basic publish + subscribe
- [`examples/request-reply`](examples/request-reply) — RPC over the bus
- [`examples/confirm`](examples/confirm) — publisher waits for N acks

Run any of them directly:

```bash
go run ./examples/pub-sub
go run ./examples/request-reply
go run ./examples/confirm
```

## Deploying a real NATS server

The [`deploy/`](deploy) folder ships ready-to-use docker-compose
configurations:

- `deploy/docker-compose.yml` — single-node NATS + JetStream
- `deploy/docker-compose.cluster.yml` — 3-node HA cluster
- `deploy/nats.conf` and `deploy/cluster/*.conf` — server configurations

```bash
cd deploy
docker compose up -d
# then point your client at nats://127.0.0.1:4222
```

See [`deploy/README.md`](deploy/README.md) for cluster topology, replication,
TLS / auth pointers and more.

## Testing

```bash
go test ./...
```

The test suite uses `bus.NewDevServer()`, which boots a real NATS server with
JetStream enabled in a freshly-allocated temp directory. The directory is
removed automatically on `Shutdown()`, so each test gets fully isolated state
without any global pollution.

## Limitations vs upstream

- **`WithStartFrom("e_<id>")`** is implemented by asking JetStream to deliver
  every message in the stream and skipping on the client side until the
  boundary event is seen — so positioning is exact, but the cost grows with
  the size of the stream. Prefer `StartOldest` / `StartNewest` when you can.
- **`Client.Ack(ctx, consumerId, eventId)`** is a no-op (see Acker section).
- **At-rest encryption** and **duplicate cache TTL** are not implemented in
  this engine — configure JetStream's own encryption / `AllowDirect` /
  `Discard` policies via `WithDefaultStreamConfig` if needed.
- The NATS engine has its own subject restrictions (no spaces, etc.); they
  are at least as strict as the upstream rules.

## License

MIT — see upstream `ella.to/bus` for the original work.
