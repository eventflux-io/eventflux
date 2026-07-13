# Kafka Connector Specification for EventFlux

Status: **IMPLEMENTED (2026-07-12)** — source + sink + factories, registration, feature gating,
integration tests, CI broker service, example, docs. Deferred items tracked below.
Prerequisite (completed): **`feat/connector-infra/README.md`** — landed as eventflux-io/eventflux
#115/#116/#117.

Review decisions (2026-07-10): fully minimal default feature set (`default = []`, no connectors);
`map_event` signature change approved; CI broker `apache/kafka:3.9.1`.

This document specifies the Kafka source and sink implementations for EventFlux, following the
patterns established by the RabbitMQ connector (`feat/rabbitmq/README.md`). Cross-cutting
infrastructure fixes discovered during the same review are specified separately in
`feat/connector-infra/README.md`; this spec assumes they are in place.

## Overview

EventFlux will provide native integration with Apache Kafka through:

- **Kafka Source**: Consumes records from Kafka topics (consumer groups, offset management)
- **Kafka Sink**: Produces records to Kafka topics (partitioning, delivery reports)

Both implementations use the `rdkafka` crate (bindings to librdkafka), behind a `kafka` cargo
feature.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Topic   │────▶│    EventFlux    │────▶│   Kafka Topic   │
│    (Source)     │     │    Pipeline     │     │     (Sink)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Threading Model — no tokio runtime needed

The RabbitMQ connector must bridge the sync `Source`/`Sink` traits to `lapin`'s async API, which
forces `thread::spawn → tokio::runtime::Runtime → block_on` and the
`Handle::try_current()/block_in_place` fallback chains seen throughout `rabbitmq_sink.rs`.

Kafka avoids all of this. librdkafka runs its own background threads (network I/O, batching,
reconnection), and `rdkafka` exposes synchronous APIs on top:

| Component | rdkafka API | Model |
|-----------|-------------|-------|
| Source | `BaseConsumer<CustomContext>` | Dedicated thread, `consumer.poll(100ms)` loop — mirrors the existing 100ms `running`-flag check pattern |
| Sink | `ThreadedProducer<DeliveryContext>` | `publish()` = non-blocking enqueue; librdkafka batches and delivers; delivery reports arrive via callback on the producer's poll thread |

```
Source:  thread::spawn → BaseConsumer::poll(100ms) loop → bytes → SourceCallback → SourceMapper → Events
Sink:    Events → SinkMapper → bytes → ThreadedProducer::send() → librdkafka batching → Kafka
```

Benefits over the RabbitMQ approach:
- No nested-runtime handling, no `block_in_place`, no stored `runtime`/`runtime_handle` fields
- Automatic reconnection with exponential backoff (librdkafka built-in) — the RabbitMQ source
  thread simply exits on connection errors; Kafka gets resilience for free
- Producer-side batching (`linger.ms`, `batch.size`) without blocking the pipeline per message —
  the RabbitMQ sink synchronously awaits a publisher confirm per publish, capping throughput at
  ~1 message per broker RTT

## Kafka Source

Consumes records from one or more Kafka topics and delivers payload bytes to the EventFlux
pipeline via `SourceCallback`.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `kafka.bootstrap.servers` | Yes | - | Comma-separated broker list (`host1:9092,host2:9092`) |
| `kafka.topic` | Yes | - | Topic(s) to consume, comma-separated for multiple |
| `kafka.group.id` | No | `eventflux-<topic>` | Consumer group id. Default is deterministic so offsets survive restarts |
| `kafka.auto.offset.reset` | No | `latest` | Where to start without committed offsets (`earliest`, `latest`) |
| `kafka.enable.auto.commit` | No | `true` | `true`: librdkafka commits periodically (at-most-once-ish). `false`: commit after successful delivery to pipeline (at-least-once) |
| `kafka.poll.timeout.ms` | No | `100` | Poll timeout; bounds shutdown latency |
| `kafka.security.protocol` | No | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `kafka.sasl.mechanism` | No | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `kafka.sasl.username` | No | - | SASL username |
| `kafka.sasl.password` | No | - | SASL password (use `${ENV_VAR}` substitution; never logged) |
| `kafka.ssl.ca.location` | No | - | CA certificate path |
| `kafka.rdkafka.<prop>` | No | - | Escape hatch: passed verbatim to librdkafka (e.g. `kafka.rdkafka.fetch.min.bytes`). Same pattern as Flink's `properties.*` |
| `error.strategy` | No | - | Error handling strategy (drop, retry, dlq, fail) |
| `error.retry.max-attempts` | No | 3 | Max retry attempts |
| `error.retry.initial-delay-ms` | No | 100 | Initial retry delay in ms |
| `error.retry.max-delay-ms` | No | 10000 | Max retry delay in ms |
| `error.retry.backoff-multiplier` | No | 2.0 | Backoff multiplier |
| `error.dlq.stream` | No | - | DLQ stream name (required when strategy=dlq) |

Reserved keys (`bootstrap.servers`, `group.id`, offset/commit settings) may not be overridden via
`kafka.rdkafka.*` — config parsing rejects them with a clear error.

### Consume loop semantics

Mirrors the RabbitMQ source's retry/ack pattern, with Kafka commit semantics:

```
while running:
    match consumer.poll(poll_timeout):
        None            → continue (check running flag)
        Some(Err(e))    → route through SourceErrorContext (retry/drop/dlq/fail)
        Some(Ok(msg))   →
            loop:                                   # retry loop, same as RabbitMQ
                match callback.on_data(msg.payload()):
                    Ok  → if !auto_commit: consumer.commit_message(msg, Async)
                          reset error counter; break
                    Err → action = error_ctx.handle_error_with_action(fallback_event, e)
                          Retry → continue
                          Drop | SendToDlq → if !auto_commit: commit (skip record); break
                          Fail  → do NOT commit; stop source

on shutdown: final commit (Sync) if !auto_commit, then consumer unsubscribe/close
```

- **At-least-once** (`kafka.enable.auto.commit=false`): a record's offset is committed only after
  `on_data` succeeds (or the error strategy explicitly disposes of it via Drop/DLQ). Crash before
  commit → redelivery on restart.
- **DLQ fallback event**: raw payload wrapped as `AttributeValue::Bytes`, identical to RabbitMQ.
- Records with empty/null payloads (tombstones) are skipped with a debug log in v1.
- Consumer group rebalances are logged via a custom `ConsumerContext` (`pre_rebalance` /
  `post_rebalance` hooks).

### `KafkaSource` structure

```rust
pub struct KafkaSourceConfig { /* fields per config table; from_properties() parsing */ }

pub struct KafkaSource {
    config: KafkaSourceConfig,
    worker: SourceWorker,        // lifecycle: spawn / signal / bounded join (connector-infra Part 2)
    error_ctx: Option<SourceErrorContext>,
    metrics: Arc<SourceMetrics>, // records_received, records_failed (AtomicU64)
}

impl Source for KafkaSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>);  // worker.start(|running| poll loop)
    fn stop(&mut self);          // worker.stop() — synchronous, joined, idempotent
    fn clone_box(&self) -> Box<dyn Source>;
    fn set_error_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>);
    fn validate_connectivity(&self) -> Result<(), EventFluxError>;
}
```

`validate_connectivity()`: create a consumer, `fetch_metadata(Some(topic), 10s)` per configured
topic; error if brokers unreachable or a topic does not exist. No queue-declare complications —
topic auto-creation is a broker-side setting, so unlike RabbitMQ there is no client-side
declare/passive-declare dance.

### `KafkaSourceFactory`

```rust
impl SourceFactory for KafkaSourceFactory {
    fn name(&self) -> &'static str { "kafka" }
    fn supported_formats(&self) -> &[&str] { &["json", "csv", "bytes"] }
    fn required_parameters(&self) -> &[&str] { &["kafka.bootstrap.servers", "kafka.topic"] }
    fn optional_parameters(&self) -> &[&str] { /* per config table + error.* */ }
    fn create_initialized(&self, config) -> Result<Box<dyn Source>, EventFluxError>;
    fn clone_box(&self) -> Box<dyn SourceFactory>;
}
```

Note: the existing placeholder stub in `example_factories.rs` claims `avro` support — there is no
avro mapper in the registry, so the real factory advertises only formats that actually exist
(json, csv, bytes). Avro + Schema Registry is deferred.

## Kafka Sink

Produces formatted event payloads to a Kafka topic.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `kafka.bootstrap.servers` | Yes | - | Comma-separated broker list |
| `kafka.topic` | Yes | - | Target topic |
| `kafka.key` | No | - | Static record key (Phase 1). Per-event keys are Phase 2 (see Improvements) |
| `kafka.acks` | No | `all` | `0`, `1`, `all` — durability vs latency |
| `kafka.compression` | No | `none` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `kafka.linger.ms` | No | `5` | Batching delay |
| `kafka.batch.size` | No | librdkafka default | Max batch bytes |
| `kafka.enable.idempotence` | No | `false` | Exactly-once producer semantics (no duplicates on retry) |
| `kafka.message.timeout.ms` | No | `30000` | Total time to deliver a record before it's reported failed |
| `kafka.delivery.sync` | No | `false` | `true`: `publish()` blocks until the delivery report for that record (strict, slow). `false`: async enqueue + background delivery reports |
| `kafka.flush.timeout.ms` | No | `30000` | Max wait for in-flight records during `stop()` |
| `kafka.queue.full.strategy` | No | `block` | When librdkafka's local queue is full: `block` (backpressure, bounded retry) or `error` (fail fast into pipeline error handling) |
| `kafka.security.protocol` / `kafka.sasl.*` / `kafka.ssl.*` | No | - | Same as source |
| `kafka.rdkafka.<prop>` | No | - | Escape hatch, same as source |

### Publish semantics

- **Default (async)**: `publish()` enqueues into librdkafka's local queue and returns. Delivery
  reports arrive on the `ThreadedProducer` poll thread via a `ProducerContext::delivery()`
  callback, which increments failure counters and logs errors. This is the standard model for
  Kafka sinks (Flink/Kafka Connect) and keeps pipeline throughput decoupled from broker RTT.
- **Sync mode** (`kafka.delivery.sync=true`): `publish()` waits for that record's delivery report;
  errors propagate to the caller (`SinkCallbackAdapter` currently logs them). Use for
  low-throughput, strict pipelines.
- **`stop()`**: `producer.flush(flush_timeout)`; logs a warning with the count of undelivered
  records if the timeout is exceeded, then logs lifetime counters (produced, failed).
- `validate_connectivity()`: `fetch_metadata(Some(topic), 10s)`; error if brokers unreachable or
  topic missing.

### `KafkaSink` structure

```rust
pub struct KafkaSinkConfig { /* fields per config table; from_properties() parsing */ }

pub struct KafkaSink {
    config: KafkaSinkConfig,
    producer: Arc<Mutex<Option<ThreadedProducer<DeliveryContext>>>>,  // created in start()
    metrics: Arc<SinkMetrics>,  // records_produced, records_failed (AtomicU64)
}

impl Sink for KafkaSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError>;
    fn start(&self);   // builds ClientConfig, creates ThreadedProducer
    fn stop(&self);    // flush(timeout), log stats, drop producer
    fn clone_box(&self) -> Box<dyn Sink>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError>;
}
```

`KafkaSinkFactory` mirrors `RabbitMQSinkFactory` (name `"kafka"`, formats json/csv/bytes,
required `kafka.bootstrap.servers` + `kafka.topic`).

## Delivery Guarantees

| Configuration | Guarantee |
|---------------|-----------|
| Source: `enable.auto.commit=true` (default) | Approx. at-most-once (offsets may be committed before pipeline processing completes) |
| Source: `enable.auto.commit=false` | At-least-once (commit after successful delivery) |
| Sink: `acks=all` (default) + async delivery | At-least-once to Kafka; delivery failures logged/counted, not retried by EventFlux (librdkafka retries internally until `message.timeout.ms`) |
| Sink: `+ enable.idempotence=true` | At-least-once without producer-side duplicates |
| End-to-end exactly-once (Kafka transactions) | **Deferred** — requires integrating producer transactions with EventFlux checkpointing |

## Format Support

Same mapper infrastructure as RabbitMQ: `json`, `csv`, `bytes` for both source and sink.
Record **keys, headers, and timestamps are not exposed to the pipeline in v1** (the
`SourceCallback` interface carries payload bytes only) — see Improvements below.

## SQL Usage Example

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

CREATE STREAM TradeOutput (
    symbol STRING,
    price DOUBLE,
    volume INT,
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

Example file: `examples/kafka.eventflux` (mirroring `examples/rabbitmq.eventflux`, with
`docker run -p 9092:9092 apache/kafka:3.9.1` prerequisites).

## Dependency & Feature Gating

Kafka is the first connector to use the **generic connector feature-gating framework** specified
in `feat/connector-infra/README.md` Part 3. Summary as applied to Kafka:

```toml
[features]
kafka = ["dep:rdkafka"]                                   # named after the extension name
connectors-all = ["rabbitmq", "websocket", "kafka"]       # umbrella includes kafka

[dependencies]
rdkafka = { version = "0.37", features = ["cmake-build", "ssl-vendored"], optional = true }
```

- **Not in `default`** — per the framework rule, the default set is fully minimal
  (`default = []`); no connectors ship in default builds.
- All gating points follow the framework's per-connector checklist (module decl, re-exports,
  registration, integration tests, examples note, cargo-machete metadata).
- A build without the feature reports the framework's helpful error:
  "Extension 'kafka' ... rebuild with `--features kafka` (or `--features connectors-all`)".
- Release/Docker builds use `--features connectors-all`, so shipped artifacts include Kafka.
- `ssl-vendored` statically links OpenSSL → SSL/SASL-SCRAM work without system OpenSSL.
  GSSAPI/Kerberos is excluded (heavy system deps; niche).
- Version note: pin to the latest rdkafka at implementation time; verify MSRV 1.85 compatibility.

## Error Handling

Identical integration to RabbitMQ: `ErrorConfigBuilder` / `SourceErrorContext`, strategies
drop / retry / dlq / fail, DLQ junction wired post-creation by `stream_initializer` via
`set_error_dlq_junction()`. The source factory uses the topic name as the error-context stream
identifier (RabbitMQ uses the queue name).

Additional Kafka-specific handling:
- librdkafka transient errors (broker down, timeouts) are retried internally; the source only
  surfaces persistent errors to the error strategy.
- Fatal consumer errors (e.g. unknown topic after metadata refresh, auth failure) map to
  `EventFluxError::ConnectionUnavailable` / `configuration` respectively.

## Testing Plan

### Unit tests (no broker) — in-module, mirroring RabbitMQ

- Config parsing: required/optional params, defaults, invalid values, reserved `kafka.rdkafka.*`
  keys rejected, SASL config validation (mechanism requires protocol, etc.)
- Factory metadata: name, formats, required/optional parameter lists
- Factory create: missing params error, valid config succeeds, error.* config accepted
- Clone semantics: cloned source/sink has config but no live state

### Integration tests (broker required) — `tests/kafka_integration.rs`, `#[ignore]`-gated

Following `tests/rabbitmq_integration.rs` structure:
- Source: consume produced records end-to-end (JSON format)
- Sink: publish and verify via a raw rdkafka consumer
- Round-trip: sink → topic → source through an EventFlux app
- Manual commit: verify offset advances only after successful processing; verify redelivery
  after simulated failure with `error.strategy=retry`
- `validate_connectivity`: unreachable broker fails fast (<15s), missing topic fails
- Graceful shutdown: `stop()` flushes producer, joins consumer thread

### CI

Add a Kafka service container to `.github/workflows/rust.yml` (single-node KRaft):

```yaml
kafka:
  image: apache/kafka:3.9.1
  ports:
    - 9092:9092
  options: >-
    --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
    --health-interval 10s
    --health-timeout 10s
    --health-retries 10
```

The main test job builds with `--features connectors-all` (per the connector-infra CI strategy);
integration tests via `cargo nextest run --test kafka_integration --run-ignored ignored-only`
(same pattern as RabbitMQ). cmake is preinstalled on `ubuntu-latest` runners.

## Implementation Plan

| Phase | Scope |
|-------|-------|
| 0 | **Prerequisite, separate PRs**: `feat/connector-infra/README.md` — sink batch fix, source thread join, feature-gating framework |
| 1 | `kafka` cargo feature + `kafka_source.rs` (config, source, factory, unit tests) |
| 2 | `kafka_sink.rs` (config, sink, factory, delivery context, unit tests) |
| 3 | Registration in `EventFluxContext`, remove Kafka stub from `example_factories.rs`, update stub-based tests in `extension/mod.rs` |
| 4 | Integration tests + CI service (apache/kafka:3.9.1, confirmed) + `examples/kafka.eventflux` |
| 5 | Docs: this file updated to "Implemented", ROADMAP.md, website connector docs, README feature-flags table entry |
| 6 (follow-up) | Keyed publish interface (Improvement 2 below), per-event keys via `kafka.key.field` |

### Files

| File | Description |
|------|-------------|
| `src/core/stream/input/source/kafka_source.rs` | Kafka source and factory |
| `src/core/stream/output/sink/kafka_sink.rs` | Kafka sink and factory |
| `tests/kafka_integration.rs` | Integration tests |
| `examples/kafka.eventflux` | Example query file |

## Technical Improvements (found reviewing connector infrastructure)

Numbered for review; each is independent. Items 1, 3, and 8 are now **specified in
`feat/connector-infra/README.md`** and land before Kafka; 2 is a Kafka follow-up phase; the
rest are noted for the backlog.

### 1. `SinkCallbackAdapter` silently drops batched events (bug) — MOVED to connector-infra spec

Specified in `feat/connector-infra/README.md` Part 1 (with the full per-mapper behavior audit:
JSON drops N−1 events, CSV concatenates rows into one message, Bytes hard-errors on batches,
Passthrough bincodes the batch). Fix: per-event adapter iteration + per-event
`SinkMapper::map_event` contract. Kafka's one-record-per-event semantics depend on this.

### 2. `Sink::publish(&[u8])` carries no event context — no per-event Kafka keys/headers

The sink sees opaque bytes, so it cannot extract a partition key or headers from the event.
Backward-compatible extension:

```rust
fn publish_event(&self, _event: &Event, payload: &[u8]) -> Result<(), EventFluxError> {
    self.publish(payload)   // default: ignore event context
}
```

`SinkCallbackAdapter` calls `publish_event`; existing sinks are untouched. The Kafka sink
overrides it to resolve `kafka.key.field` → record key. Requires the factory to know the field's
position: have `stream_initializer` (which holds the `StreamDefinition`) inject schema metadata
into the properties map (e.g. `_schema.fields=symbol:STRING,price:DOUBLE,...`) before
`create_initialized()` — a generic mechanism any connector can use. Same path later enables
Kafka headers and source-side key/timestamp exposure.

### 3. Source `stop()` never joins the consumer thread — MOVED to connector-infra spec

Specified in `feat/connector-infra/README.md` Part 2: all three existing sources (RabbitMQ,
WebSocket, Timer) discard their `JoinHandle`; fix adds handle retention + a shared
`join_with_timeout` helper. Kafka uses that helper from day one.

### 4. No reconnect/supervision for source connection loss

If the RabbitMQ broker connection drops, the source thread logs and exits — the stream is dead
until app restart, with nothing above it noticing. librdkafka reconnects internally so Kafka is
immune, but the framework-level gap remains: a supervision policy (restart with backoff, surface
"source dead" state, health hook) belongs in the runtime, not in each connector. Ties into
ROADMAP Priority 9/10 (observability, production hardening).

### 5. One tokio runtime per connector instance

Each RabbitMQ/WebSocket source thread creates its own `tokio::runtime::Runtime`, and each sink
holds an owned runtime + handle fallback chain. N connectors → N runtimes and thread pools.
Standard fix: one shared I/O runtime on `EventFluxContext` that async connectors get a `Handle`
to. (Kafka needs none, but the RabbitMQ/WebSocket code would shrink substantially.)

### 6. No connector metrics

No counters exist for records received/produced/failed — invisible in production and needed by
ROADMAP Priority 9. Kafka connector ships with `AtomicU64` counters (received, produced, failed,
last-error timestamp) from day one, logged at `stop()`; wiring into a metrics endpoint comes with
the observability milestone. Consumer lag is available via rdkafka's statistics callback when
metrics land.

### 7. `validate_connectivity()` boilerplate duplication

Every connector re-implements "connect with 10s timeout, map to ConnectionUnavailable, cleanup".
The 10s value is hardcoded. Worth a shared helper with a configurable timeout
(`connect.timeout.ms`) once a third network connector exists — Kafka keeps its implementation
self-contained but accepts the timeout property.

### 8. Unregistered-extension errors don't mention feature flags — MOVED to connector-infra spec

Specified in `feat/connector-infra/README.md` Part 3 (`UNAVAILABLE_EXTENSIONS` table +
stream_initializer hint). A binary built without the feature reports the rebuild instruction
instead of a bare `Unknown source type: kafka`.

### 9. Config parsing returns `Result<_, String>`

`*Config::from_properties()` returns `Result<Self, String>` mapped to
`EventFluxError::configuration` at the factory boundary. Kafka follows the same pattern for
consistency; unifying on `EventFluxError` end-to-end is a mechanical cleanup for later.

### 10. Placeholder stub drift

`example_factories.rs`'s `KafkaSourceFactory` advertises `avro` (no avro mapper exists) and uses
`kafka.timeout` (real config uses librdkafka names). The stub and its tests in
`extension/mod.rs` are replaced in Phase 3 — keeping placeholder stubs for implemented
connectors invites exactly this drift.

## Deferred (post-v1) — tracked as GitHub issues

| Feature | Reason | Issue |
|---------|--------|-------|
| Exactly-once (Kafka transactions) | Needs checkpoint-coordinated commit protocol | eventflux-io/eventflux#125 |
| Per-event keys / topic routing / headers | Needs `publish_event` + schema injection (Improvement 2) | eventflux-io/eventflux#126 |
| Avro / Schema Registry | No avro mapper in registry yet | eventflux-io/eventflux#127 |
| Consumer lag + connector metrics | Lands with observability milestone (statistics callback) | eventflux-io/eventflux#128 |
| Regex topic subscription | Niche; `subscribe` with explicit list covers v1 | eventflux-io/eventflux#129 |
| Shared-helper adoption in older sources | Kept out of the Kafka PR (single purpose) | eventflux-io/eventflux#130 |
| Source supervision on connection loss | Runtime-level design needed | eventflux-io/eventflux#131 |
