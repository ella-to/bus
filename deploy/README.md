# Deploy

Compose files and NATS configurations for running the bus engine outside of
the embedded `bus.NewDevServer`. Two topologies are provided:

| File                              | Topology                              | When to use                          |
| --------------------------------- | ------------------------------------- | ------------------------------------ |
| `docker-compose.yml`              | Single NATS node with JetStream       | Local dev, CI, small services        |
| `docker-compose.cluster.yml`      | 3-node JetStream cluster (HA)         | Production / staging that needs HA   |

Both compose files read overrides from `.env` (copy `.env.example` to `.env`
to customise image versions, ports, and storage paths).

## Layout

```
deploy/
├── .env.example                # configurable defaults
├── docker-compose.yml          # single node
├── docker-compose.cluster.yml  # 3-node cluster
├── nats.conf                   # config used by the single node
└── cluster/
    ├── nats-1.conf
    ├── nats-2.conf
    └── nats-3.conf
```

## Single-node

```bash
cd deploy
cp .env.example .env  # optional, only if you want to tweak ports etc.
docker compose up -d
```

Then point your client at the published port:

```go
client, err := bus.NewClient("nats://127.0.0.1:4222")
```

The HTTP monitoring endpoint is exposed on
[http://127.0.0.1:8222](http://127.0.0.1:8222) — `/varz`, `/jsz`, `/connz`,
`/healthz`, etc.

JetStream data lives under `./data` on the host so it survives container
restarts. Override the location with `NATS_DATA_DIR` in `.env`.

### Inspecting the running cluster

`nats-box` is included as an opt-in service via the `tools` profile. It
gives you the official `nats` CLI without installing anything on the host:

```bash
docker compose --profile tools run --rm natsbox nats stream ls
docker compose --profile tools run --rm natsbox nats consumer ls BUS_orders
```

## 3-node cluster

```bash
cd deploy
docker compose -f docker-compose.cluster.yml up -d
```

The cluster publishes one client port per node (`4222`, `4223`, `4224` by
default). Connect to any of them — typically you list all three so the
client can fail over:

```go
client, err := bus.NewClient("nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224")
```

To make every stream this client creates use replication factor 3, configure
`WithDefaultStreamConfig` (or `WithStreamConfigurer`) at construction time:

```go
import "github.com/nats-io/nats.go/jetstream"

client, err := bus.NewClient(
    "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224",
    bus.WithStreamConfigurer(func(_ string, cfg *jetstream.StreamConfig) {
        cfg.Replicas = 3
    }),
)
```

The HTTP monitoring endpoints are at `:8222`, `:8223`, `:8224`. JetStream
data is kept in named Docker volumes (`nats1-data`, `nats2-data`,
`nats3-data`) — destroying them resets the cluster state.

## Sanity check

Once a deployment is up, the `examples/` programs work against it as long as
you change `bus.NewDevServer()` to point the client at the deployed URL:

```go
client, err := bus.NewClient("nats://127.0.0.1:4222")
```

A quick end-to-end check using `nats-box`:

```bash
# Publish a single event and read it back, both via the bus.Client. From
# the host:
NATS_URL=nats://127.0.0.1:4222 go run ../examples/pub-sub
```

## Hardening notes

The provided configs are intentionally minimal so you can layer your own
operational concerns on top. A few common knobs:

- **TLS / mTLS**: add a `tls { ... }` block to each `nats.conf`, mount the
  certs into the container, and update the published ports accordingly.
- **Authentication**: enable `accounts` / `users` (or NKEYs / JWT) in the
  config and pass `nats.UserCredentials("/path/to/file.creds")` to
  `bus.WithNatsOptions`.
- **Resource limits**: `max_memory_store` / `max_file_store` cap how much
  JetStream is allowed to use. `max_payload`, `max_pending`, and
  `max_connections` cap client behaviour.
- **Disable the monitoring port** in production unless you've put it on a
  trusted network or behind authentication (`http_port` ⇒ `-1`).

For more, see the upstream
[NATS configuration reference](https://docs.nats.io/running-a-nats-service/configuration)
and the
[JetStream operations guide](https://docs.nats.io/nats-concepts/jetstream).
