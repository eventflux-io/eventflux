# EventFlux

[![Build](https://github.com/eventflux-io/eventflux/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/eventflux-io/eventflux/actions/workflows/docker-publish.yml)
[![Rust](https://github.com/eventflux-io/eventflux/actions/workflows/rust.yml/badge.svg)](https://github.com/eventflux-io/eventflux/actions/workflows/rust.yml)
[![GHCR](https://img.shields.io/badge/ghcr.io-eventflux--io%2Feventflux-green)](https://ghcr.io/eventflux-io/eventflux)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

Stream processing engine built in Rust. Write SQL, process events, skip the infrastructure headache.

## Why EventFlux

You need to detect patterns in event streams, aggregate metrics, or react to conditions in real-time.

Your options today:

- **Flink** — needs Kubernetes, 4GB+ JVM heap, dedicated ops
- **Kafka Streams** — needs a Kafka cluster plus Java expertise
- **Build it yourself** — months of work

For 100k events/sec, that's overkill.

EventFlux runs as a single binary. No cluster. No JVM. No YAML manifests. Just SQL.

## Quick Start

```bash
# Docker
docker run -v ./app.sql:/app.sql ghcr.io/eventflux-io/eventflux /app.sql

# Or build from source (--recursive pulls in the vendored SQL parser submodule)
git clone --recursive https://github.com/eventflux-io/eventflux.git
cd eventflux
cargo build --release --features connectors-all
./target/release/run_eventflux app.sql
```

Already cloned without `--recursive`? Fetch the submodule before building:

```bash
git submodule update --init --recursive
```

### Connector feature flags

The default build is fully minimal — core engine plus the built-in `timer`
source and `log` sink, no external connectors. Each connector is a cargo
feature named after its SQL extension name:

| Feature | Connector | In default build? |
|---------|-----------|-------------------|
| `file` | File source (replay + follow) + sink (rotation) | No |
| `http` | HTTP source (polling + webhook) + sink | No |
| `kafka` | Kafka source + sink (needs cmake to build) | No |
| `rabbitmq` | RabbitMQ source + sink | No |
| `websocket` | WebSocket source + sink | No |
| `connectors-all` | All of the above | No |

Docker images are built with `connectors-all`, so they include everything.
Building from source, pick what you need:

```bash
cargo build --release --features rabbitmq          # just RabbitMQ
cargo build --release --features connectors-all    # everything
```

### Prerequisites (for building)

- Rust 1.85+
- Protocol Buffer compiler (for gRPC features)
- Git submodules initialized (see above) — the SQL parser is a vendored submodule

## Example

```sql
CREATE
STREAM Trades (symbol STRING, price DOUBLE, volume INT);

SELECT symbol, AVG(price), SUM(volume)
FROM Trades WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT
INTO Summaries;
```

That's it. No boilerplate. No config files. Just SQL.

## How It Compares

|               | EventFlux     | Flink              | Kafka Streams |
|---------------|---------------|--------------------|---------------|
| Deployment    | Single binary | Kubernetes cluster | Kafka cluster |
| Memory        | 50-100MB      | 4GB+ JVM           | 1GB+ JVM      |
| Language      | SQL           | Java/SQL           | Java          |
| Setup time    | Minutes       | Hours/days         | Hours         |
| Scale ceiling | ~500k eps     | Millions+          | Millions+     |

**Choose EventFlux** when you want simple deployment and SQL-first development.

**Choose Flink** when you need massive scale or batch+stream processing.

## Documentation

Full docs at **[eventflux.io](https://eventflux.io)**:

- [Getting Started](https://eventflux.io/docs/getting-started/installation) — install and run your first query
- [SQL Reference](https://eventflux.io/docs/sql-reference/queries) — windows, joins, patterns, aggregations
- [Connectors](https://eventflux.io/docs/connectors/overview) — File, HTTP, Kafka, RabbitMQ, WebSocket
- [Examples](https://eventflux.io/docs/demo/crypto-trading) — real-world use cases
- [Architecture](https://eventflux.io/docs/architecture/overview) — how it works under the hood

## IDE Support

**[EventFlux Studio](https://marketplace.visualstudio.com/items?itemName=eventflux.eventflux-studio)** — VS Code extension with:

- Syntax highlighting for `.eventflux` files
- Schema-aware autocomplete
- Real-time error diagnostics
- Query visualization

Install from VS Code marketplace or search "EventFlux Studio" in extensions.

## Performance

- 1M+ events/sec on a single node
- Sub-millisecond latency
- Zero GC pauses
- Starts in milliseconds

## When to Use

**Good fit:** IoT backends, e-commerce tracking, analytics pipelines, SaaS telemetry, fraud detection.

**Not a fit:** You need 100+ connectors, or you're already running Flink at scale.

## Status

Active development. Core CEP works. 1,400+ tests passing. See [ROADMAP.md](ROADMAP.md) for details.

## Contributing

```bash
cargo nextest run   # run tests
just lint           # clippy
cargo fmt           # format
just check-all      # all quality checks
```

See [DEV_GUIDE.md](DEV_GUIDE.md) for setup.

## License

[Apache-2.0](LICENSE)
