```
██████╗░██╗░░░██╗░██████╗
██╔══██╗██║░░░██║██╔════╝
██████╦╝██║░░░██║╚█████╗░
██╔══██╗██║░░░██║░╚═══██╗
██████╦╝╚██████╔╝██████╔╝
╚═════╝░░╚═════╝░╚═════╝░
```

# Bus: A Persistent and High-Performance Message Bus

<div align="center">

[![Go Reference](https://pkg.go.dev/badge/ella.to/bus.svg)](https://pkg.go.dev/ella.to/bus)
[![Go Report Card](https://goreportcard.com/badge/ella.to/bus)](https://goreportcard.com/report/ella.to/bus)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight, persistent message bus built for simplicity and performance. Perfect for microservices, real-time applications, and event-driven architectures.

[Quick Start](#quick-start) • [Features](#key-features) • [Examples](#examples) • [API Reference](HTTP_API.md) • [CLI](#cli-commands)

</div>

---

## Why Bus?

- **Zero dependencies** on external services like Redis, Kafka, or RabbitMQ
- **Persistent by default** - events survive server restarts
- **HTTP/SSE transport** - works with any language, no special client needed
- **Single binary** - easy deployment with Docker or standalone

## Key Features

| Feature | Description |
|---------|-------------|
| 📦 **Persistent Storage** | Events are durably stored in append-only logs |
| 🎯 **Pattern Matching** | Subscribe to `user.*`, `order.>`, or exact subjects |
| 🔄 **Request/Reply** | Built-in RPC-style communication |
| 🔐 **Encryption** | Optional at-rest encryption with NaCl |
| 🔁 **Redelivery** | Automatic retry for unacknowledged messages |
| ✅ **Confirmations** | Wait for N consumers to acknowledge |
| 🌐 **SSE Streaming** | Real-time events via Server-Sent Events |
| 🛠️ **CLI Tools** | Debug, dump, restore, and manage events |

---

## Quick Start

### 1. Install

**Go SDK:**
```bash
go get ella.to/bus@v0.5.3
```

**CLI:**
```bash
go install ella.to/bus/cmd/bus@v0.5.3
```

**Docker:**
```bash
docker pull ellato/bus:v0.5.3
```

### 2. Start the Server

Using Docker Compose (recommended):
```bash
# Create docker-compose.yml
cat > docker-compose.yml << 'EOF'
services:
  bus:
    image: ellato/bus:v0.5.3
    environment:
      - BUS_ADDR=0.0.0.0:2021
      - BUS_PATH=/storage/events.log
      - BUS_NAMESPACES=app,notifications,orders
    ports:
      - "2021:2021"
    volumes:
      - ./storage:/storage
EOF

docker-compose up -d
```

Or using the CLI:
```bash
bus server --addr :2021 --path ./data --namespaces app,notifications,orders
```

### 3. Publish and Subscribe

```go
package main

import (
    "context"
    "fmt"

    "ella.to/bus"
)

func main() {
    client := bus.NewClient("http://localhost:2021")
    ctx := context.Background()

    // Publish an event
    resp := client.Put(ctx,
        bus.WithSubject("app.users.created"),
        bus.WithData(map[string]string{"user_id": "123", "name": "Alice"}),
    )
    if resp.Error() != nil {
        panic(resp.Error())
    }
    fmt.Printf("Published: %s\n", resp.Id)

    // Subscribe to events
    for event, err := range client.Get(ctx,
        bus.WithSubject("app.users.*"),
        bus.WithStartFrom(bus.StartOldest),
    ) {
        if err != nil {
            panic(err)
        }
        fmt.Printf("Received: %s\n", event.Payload)
        event.Ack(ctx)
        break
    }
}
```

---

## Examples

### Pub/Sub Pattern

The simplest pattern - publish events and subscribe to them:

```go
// Publisher
client.Put(ctx,
    bus.WithSubject("notifications.email"),
    bus.WithData(map[string]string{
        "to":      "user@example.com",
        "subject": "Welcome!",
    }),
)

// Subscriber (can be in a different service)
for event, err := range client.Get(ctx,
    bus.WithSubject("notifications.*"),
    bus.WithStartFrom(bus.StartNewest),
    bus.WithAckStrategy(bus.AckManual),
    bus.WithDelivery(5*time.Second, 3), // retry 3 times, 5s apart
) {
    if err != nil {
        log.Printf("Error: %v", err)
        continue
    }
    
    // Process the notification
    sendEmail(event.Payload)
    
    // Acknowledge to prevent redelivery
    event.Ack(ctx)
}
```

### Request/Reply Pattern

Implement RPC-style communication:

```go
// Service (handles math.add requests)
go func() {
    for event, err := range client.Get(ctx,
        bus.WithSubject("math.add"),
        bus.WithStartFrom(bus.StartOldest),
    ) {
        if err != nil {
            continue
        }
        
        var req struct{ A, B int }
        json.Unmarshal(event.Payload, &req)
        
        // Reply with result
        event.Ack(ctx, bus.WithData(map[string]int{
            "result": req.A + req.B,
        }))
    }
}()

// Client (makes the request)
resp := client.Put(ctx,
    bus.WithSubject("math.add"),
    bus.WithData(map[string]int{"A": 10, "B": 20}),
    bus.WithRequestReply(),
)

var result struct{ Result int }
json.Unmarshal(resp.Payload, &result)
fmt.Println(result.Result) // 30
```

### Publisher Confirmation

Wait for consumers to acknowledge before continuing:

```go
// Subscriber must be running first
go func() {
    for event, _ := range client.Get(ctx,
        bus.WithSubject("critical.events"),
        bus.WithStartFrom(bus.StartOldest),
    ) {
        processEvent(event)
        event.Ack(ctx) // This unblocks the publisher
    }
}()

// Publisher waits for 1 consumer to ack
err := client.Put(ctx,
    bus.WithSubject("critical.events"),
    bus.WithData("important data"),
    bus.WithConfirm(1), // Wait for 1 acknowledgment
).Error()
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BUS_ADDR` | Server listen address | `:2021` |
| `BUS_PATH` | Storage directory path | `./data` |
| `BUS_NAMESPACES` | Comma-separated namespace list | *required* |
| `BUS_SECRET_KEY` | Encryption key (enables encryption) | *disabled* |
| `BUS_BLOCK_SIZE` | Encryption block size | `4096` |
| `BUS_LOG_LEVEL` | Log level: `DEBUG`, `INFO`, `WARN`, `ERROR` | `INFO` |

### Namespaces

Namespaces organize events into separate files for better performance. The namespace is the first segment of a subject:

```
subject: "orders.created"
          └──────┬──────┘
          namespace: "orders"
```

**Rules:**
- All namespaces must be declared at server startup
- `_bus_` is reserved for internal operations
- Events in different namespaces have independent ordering

```bash
# Start with multiple namespaces
bus server --namespaces orders,users,notifications,analytics
```

### Subject Patterns

| Pattern | Matches | Example |
|---------|---------|---------|
| `orders.created` | Exact match only | `orders.created` |
| `orders.*` | Single segment wildcard | `orders.created`, `orders.updated` |
| `orders.>` | Multi-segment wildcard | `orders.created`, `orders.item.added` |

---

## Encryption

Bus supports optional at-rest encryption using NaCl (XSalsa20-Poly1305):

```bash
# Enable encryption with a secret key
bus server --namespaces app --secret-key "your-secret-key-here" --block-size 4096
```

Or via environment:
```yaml
environment:
  - BUS_SECRET_KEY=your-secret-key-here
  - BUS_BLOCK_SIZE=4096
```

**Notes:**
- The secret key is hashed with SHA-256 to produce a 32-byte key
- Block size affects performance: larger blocks = better throughput, more memory
- Recommended block sizes: 4096 (default), 8192, or 16384

---

## CLI Commands

### Server

```bash
# Start the server
bus server --addr :2021 --path ./data --namespaces app,orders

# With encryption
bus server --namespaces app --secret-key "my-key"
```

### Publish Events

```bash
# Simple publish
bus put --subject "app.test" --data '{"hello": "world"}'

# With trace ID
bus put --subject "app.test" --data "test" --trace-id "req-123"
```

### Subscribe to Events

```bash
# Subscribe from oldest
bus get --subject "app.*" --start oldest

# With manual ack
bus get --subject "app.critical" --ack manual --redelivery 10s
```

### Acknowledge Events

```bash
bus ack --consumer-id c_xxx --event-id e_yyy
```

### Debug & Maintenance

```bash
# Debug/inspect events
bus debug --path ./data

# Dump events to file
bus dump --path ./data --output events.json

# Restore from dump
bus restore --path ./data --input events.json

# Copy events between servers
bus copy --from ./data --to ./backup
```

---

## HTTP API

Bus exposes a simple HTTP API. See [HTTP_API.md](HTTP_API.md) for complete documentation.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST /` | Publish event | Body: JSON with `subject` and `payload` |
| `GET /?subject=...` | Subscribe (SSE) | Returns Server-Sent Events stream |
| `PUT /?consumer_id=...&event_id=...` | Acknowledge | Confirms message receipt |

### JavaScript/Browser Example

```javascript
// Publish
await fetch('http://localhost:2021/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
        subject: 'chat.room1',
        payload: { user: 'Alice', message: 'Hello!' }
    })
});

// Subscribe
const events = new EventSource('http://localhost:2021/?subject=chat.*&start=newest');
events.addEventListener('msg', (e) => {
    const data = JSON.parse(e.data);
    console.log('Message:', data.payload);
});
```

---

## Architecture

Bus is built on top of:

- **[immuta](https://ella.to/immuta)** - Append-only log storage
- **[task](https://ella.to/task)** - Task runner for concurrent operations
- **[solid](https://ella.to/solid)** - Signal/broadcast primitives
- **[sse](https://ella.to/sse)** - Server-Sent Events implementation

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│   Server    │────▶│   Immuta    │
│  (HTTP/SSE) │◀────│  (Handler)  │◀────│  (Storage)  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │   Crypto    │
                    │ (Optional)  │
                    └─────────────┘
```

---

## Production Deployment

### Docker Compose (Recommended)

```yaml
services:
  bus:
    image: ellato/bus:v0.5.3
    restart: unless-stopped
    environment:
      - BUS_ADDR=0.0.0.0:2021
      - BUS_PATH=/storage
      - BUS_NAMESPACES=orders,users,notifications
      - BUS_SECRET_KEY=${BUS_SECRET_KEY}  # From .env file
      - BUS_LOG_LEVEL=INFO
    ports:
      - "2021:2021"
    volumes:
      - bus_data:/storage
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:2021/"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  bus_data:
```

### Health Check

```bash
# Simple health check - server returns 400 for GET without subject
curl -s -o /dev/null -w "%{http_code}" http://localhost:2021/
```

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<div align="center">

Logo created using [fsymbols.com](https://fsymbols.com/generators/carty)

</div>
