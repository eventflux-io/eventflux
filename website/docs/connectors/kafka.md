---
sidebar_position: 2
title: Kafka Connector
description: Consume from and produce to Apache Kafka topics
---

# Kafka Connector

The Kafka connector enables EventFlux to consume records from Kafka topics and publish processed results back to Kafka. It supports JSON, CSV, and bytes formats, consumer groups with configurable offset semantics, and SASL/SSL security.

Built on librdkafka (via the `rdkafka` crate), the connector inherits its battle-tested networking: automatic reconnection, internal retries, and producer-side batching all come from librdkafka's background threads — no extra runtime inside EventFlux.

## Prerequisites

### Building with Kafka Support

The Kafka connector is feature-gated. Build EventFlux with the `kafka` feature (Docker images already include all connectors). Compiling librdkafka requires **cmake and a C toolchain**:

```bash
cargo build --release --features kafka
# or: cargo build --release --features connectors-all
```

### Starting Kafka

The easiest way to run a single-node broker is Docker (KRaft mode, no ZooKeeper):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
```

### Creating Topics

EventFlux validates at startup that configured topics exist (fail-fast):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic trades-in
```

## Kafka Source

The Kafka source joins a consumer group and delivers record payloads into the pipeline.

### SQL Syntax

```sql
CREATE STREAM TradeInput (
    symbol STRING,
    price DOUBLE,
    volume INT
) WITH (
    type = 'source',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'localhost:9092',
    "kafka.topic" = 'trades-in',
    "kafka.group.id" = 'eventflux-trades',
    "kafka.enable.auto.commit" = 'false'
);
```

### Source Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `kafka.bootstrap.servers` | Yes | - | Comma-separated broker list |
| `kafka.topic` | Yes | - | Topic(s) to consume, comma-separated for multiple |
| `kafka.group.id` | No | `eventflux-<topic>` | Consumer group id (deterministic default, so offsets survive restarts) |
| `kafka.auto.offset.reset` | No | `latest` | Start position without committed offsets: `earliest` or `latest` |
| `kafka.enable.auto.commit` | No | `true` | `false` = commit after pipeline delivery (**at-least-once**) |
| `kafka.poll.timeout.ms` | No | `100` | Poll timeout; bounds shutdown latency |
| `kafka.security.protocol` | No | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `kafka.sasl.mechanism` | No | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `kafka.sasl.username` / `kafka.sasl.password` | No | - | SASL credentials |
| `kafka.ssl.ca.location` | No | - | CA certificate path |
| `kafka.rdkafka.<prop>` | No | - | Escape hatch: passed verbatim to librdkafka (e.g. `kafka.rdkafka.fetch.min.bytes`) |
| `error.strategy` | No | - | Error handling: `drop`, `retry`, `dlq`, `fail` |

### Delivery Semantics

| Configuration | Guarantee |
|---------------|-----------|
| `kafka.enable.auto.commit = true` (default) | ~at-most-once: librdkafka commits periodically, possibly before processing completes |
| `kafka.enable.auto.commit = false` | **at-least-once**: a record's offset is committed only after the pipeline accepted it (or the error strategy disposed of it via drop/DLQ). Crash before commit → redelivery on restart |

With the `fail` error strategy, an unprocessable record is *not* committed, so it is redelivered after restart.

Records with empty payloads (tombstones) are skipped.

## Kafka Sink

The Kafka sink publishes each event as its own record (one event = one message).

### SQL Syntax

```sql
CREATE STREAM TradeOutput (
    symbol STRING,
    price DOUBLE,
    price_category STRING
) WITH (
    type = 'sink',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'localhost:9092',
    "kafka.topic" = 'trades-out',
    "kafka.acks" = 'all',
    "kafka.compression" = 'lz4'
);
```

### Sink Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `kafka.bootstrap.servers` | Yes | - | Comma-separated broker list |
| `kafka.topic` | Yes | - | Target topic |
| `kafka.key` | No | - | Static record key (keyless records use the default partitioner) |
| `kafka.acks` | No | `all` | Durability vs latency: `0`, `1`, `all` |
| `kafka.compression` | No | `none` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `kafka.linger.ms` | No | `5` | Producer batching delay |
| `kafka.batch.size` | No | librdkafka default | Max batch bytes |
| `kafka.enable.idempotence` | No | `false` | No duplicates on producer retry (requires `kafka.acks = 'all'`) |
| `kafka.message.timeout.ms` | No | `30000` | Total time to deliver a record before it's reported failed |
| `kafka.delivery.sync` | No | `false` | `true`: `publish()` waits for delivery confirmation per record (strict, slow) |
| `kafka.flush.timeout.ms` | No | `30000` | Bounds the shutdown flush, sync-mode flushes, and queue-full backpressure |
| `kafka.queue.full.strategy` | No | `block` | Local queue full: `block` (backpressure) or `error` (fail fast) |
| `kafka.rdkafka.<prop>` | No | - | Escape hatch: passed verbatim to librdkafka |

### Delivery Semantics

- **Default (async)**: `publish()` enqueues into librdkafka's local queue; delivery happens in the background with internal retries until `kafka.message.timeout.ms`. Failures are counted and logged. This keeps pipeline throughput decoupled from broker round-trips.
- **Sync** (`kafka.delivery.sync = true`): each publish waits for its delivery report — strict confirmation for low-throughput, high-assurance pipelines.
- **Shutdown**: in-flight records are flushed (bounded by `kafka.flush.timeout.ms`); lifetime produced/failed counters are logged.
- `kafka.acks = all` + `kafka.enable.idempotence = true` gives at-least-once without producer-side duplicates.

## Complete End-to-End Example

See [`examples/kafka.eventflux`](https://github.com/eventflux-io/eventflux/blob/main/examples/kafka.eventflux) for a runnable pipeline (Kafka → filter + CASE → Kafka), including broker setup and console producer/consumer commands for testing.

## Error Handling

The source integrates with EventFlux's error handling strategies (`WITH`
clause options on the source stream):

| Option | Default | Description |
|--------|---------|-------------|
| `error.strategy` | `drop` | What to do when the pipeline rejects a record: `drop`, `retry`, `dlq`, `fail` |
| `error.retry.max-attempts` | `3` | Max retries before falling back to drop |
| `error.retry.initial-delay-ms` | `100` | First retry delay |
| `error.retry.max-delay-ms` | `10000` | Retry delay ceiling |
| `error.retry.backoff-multiplier` | `2.0` | Exponential backoff factor |
| `error.dlq.stream` | - | Dead-letter stream name (required when strategy is `dlq`) |

Strategy semantics and their offset interaction:

- `drop`: skip the failing record (offset committed)
- `retry`: retry delivery with exponential backoff
- `dlq`: route the raw payload to a dead-letter stream (offset committed)
- `fail`: stop the source; the record is **not** committed and is redelivered after restart

## More Configuration Examples

### At-least-once source with retry and a dead-letter queue

```sql
CREATE STREAM Orders (order_id STRING, amount DOUBLE) WITH (
    type = 'source',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'localhost:9092',
    "kafka.topic" = 'orders',
    "kafka.group.id" = 'eventflux-orders',
    "kafka.auto.offset.reset" = 'earliest',
    "kafka.enable.auto.commit" = 'false',      -- at-least-once
    "error.strategy" = 'retry',
    "error.retry.max-attempts" = '5',
    "error.retry.initial-delay-ms" = '200',
    "error.retry.max-delay-ms" = '5000',
    "error.retry.backoff-multiplier" = '2.0'
);
```

### Secured cluster (SASL/SSL)

`kafka.sasl.*` options require a `sasl_*` security protocol. Use environment
variable substitution for secrets rather than inlining them:

```sql
CREATE STREAM SecureInput (id STRING, value DOUBLE) WITH (
    type = 'source',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'broker1:9093,broker2:9093',
    "kafka.topic" = 'secure-events',
    "kafka.security.protocol" = 'sasl_ssl',
    "kafka.sasl.mechanism" = 'SCRAM-SHA-256',
    "kafka.sasl.username" = '${KAFKA_USERNAME}',
    "kafka.sasl.password" = '${KAFKA_PASSWORD}',
    "kafka.ssl.ca.location" = '/etc/ssl/certs/kafka-ca.pem'
);
```

### High-throughput sink with idempotence and librdkafka tuning

`kafka.enable.idempotence` requires `kafka.acks = 'all'`. Any librdkafka
setting without a first-class option goes through the `kafka.rdkafka.*`
passthrough:

```sql
CREATE STREAM MetricsOut (name STRING, value DOUBLE) WITH (
    type = 'sink',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'localhost:9092',
    "kafka.topic" = 'metrics',
    "kafka.acks" = 'all',
    "kafka.enable.idempotence" = 'true',
    "kafka.compression" = 'zstd',
    "kafka.linger.ms" = '50',
    "kafka.batch.size" = '131072',
    "kafka.rdkafka.queue.buffering.max.messages" = '500000'
);
```

### Strict per-record confirmation sink

For low-throughput pipelines where every publish must be confirmed before
the next event is processed:

```sql
CREATE STREAM AuditLog (user_id STRING, action STRING) WITH (
    type = 'sink',
    extension = 'kafka',
    format = 'json',
    "kafka.bootstrap.servers" = 'localhost:9092',
    "kafka.topic" = 'audit-log',
    "kafka.key" = 'audit',
    "kafka.delivery.sync" = 'true',
    "kafka.flush.timeout.ms" = '10000',
    "kafka.queue.full.strategy" = 'error'
);
```

## Connection Validation

Both source and sink validate at application startup that brokers are reachable and topics exist (10s timeout). The application does not start otherwise:

```
Configuration error: Kafka topic 'trades-in' does not exist (brokers: 'localhost:9092')
```

## Not Yet Supported

- Exactly-once semantics (Kafka transactions) — needs checkpoint-coordinated commits
- Per-event record keys / topic routing (static `kafka.key` only)
- Kafka headers ↔ event metadata
- Avro / Schema Registry (no Avro mapper yet)
