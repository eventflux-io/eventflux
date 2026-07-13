# Connector Infrastructure: Bug Fixes & Feature Gating Specification

Status: **SPEC — APPROVED, ready for implementation**
Prerequisite for: `feat/kafka/README.md` (Kafka connector)

Review decisions (2026-07-10):
- `SinkMapper::map_event` signature change approved — no backward-compatibility constraints at
  this stage
- Feature gating approved with a **fully minimal default set** (`default = []`)
- CI Kafka broker: `apache/kafka:3.9.1`

This document specifies fixes for correctness bugs in the connector base/adapter layer, and a
generic feature-gating framework for connectors. Both were identified while reviewing the
RabbitMQ connector as the template for Kafka; both must land before Kafka implementation starts.

---

## Part 1: Bug — Sink batch handling loses or corrupts events

### Evidence

`SinkCallbackAdapter::receive_events()` (`src/core/stream/output/sink/mod.rs:49`) maps an entire
`&[Event]` batch into **one** payload and makes **one** `publish()` call:

```rust
fn receive_events(&self, events: &[Event]) {
    let payload = match self.mapper.lock().unwrap().map(events) { ... };  // whole batch → 1 payload
    if let Err(e) = self.sink.lock().unwrap().publish(&payload) { ... }   // 1 publish
}
```

Multi-event batches genuinely occur: `callback_processor.rs:82` calls
`receive_events(&events_vec)` with all events a query emitted in one processing pass (e.g. a
batch window flush).

Each sink mapper handles the batch differently — none correctly for record-oriented transports:

| Mapper | Batch of N events becomes | Verdict |
|--------|---------------------------|---------|
| `JsonSinkMapper` (`json_mapper.rs:287`) | 1 message containing only `events[0]` — comment says "batching can be added later" | **Silent data loss of N−1 events** |
| `CsvSinkMapper` (`csv_mapper.rs:355`) | 1 message containing N newline-joined rows | No loss, but 1 transport message ≠ 1 event; a JSON/CSV consumer of a queue/topic expects one record per message |
| `BytesSinkMapper` (`bytes_mapper.rs:129`) | Hard error (`MappingFailed`) — refuses batches | Whole batch fails; the error text itself asks callers to "ensure events are sent individually" |
| `PassthroughMapper` (`output/mapper.rs:67`) | 1 bincode blob of `Vec<Event>` | Internal binary format (LogSink pairing); acceptable but inconsistent |

So today, publishing a 3-event batch through a RabbitMQ sink with `format='json'` silently
delivers 1 message and discards 2 events. **Passing tests don't catch this because tests emit
single events.**

### Fix design

**Chosen approach: per-event iteration in the adapter, per-event mapper contract.**

1. `SinkCallbackAdapter::receive_events()` iterates the batch — one `map` + one `publish` per
   event. One event = one transport message (Kafka record, AMQP message, WebSocket frame). This
   matches every real message transport's data model.

2. `SinkMapper` trait gets an honest, per-event signature:

   ```rust
   pub trait SinkMapper: Debug + Send + Sync {
       /// Map ONE event to one transport payload.
       fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError>;
       fn clone_box(&self) -> Box<dyn SinkMapper>;
   }
   ```

   The `&[Event]` signature invited exactly the divergence in the table above. Per the project's
   no-backward-compatibility philosophy, change the contract rather than documenting around it.

3. Mapper updates (all four call sites):
   - `JsonSinkMapper`: drop the `events[0]` selection — body already processes one event
   - `CsvSinkMapper`: serialize one row; header handling moves to per-message (header + row per
     message, or drop header support for stream sinks — decide at implementation; headers in a
     message stream are unusual and CSV consumers of queues rarely want them repeated)
   - `BytesSinkMapper`: remove the batch-rejection branch — the constraint is now structural
   - `PassthroughMapper`: `bincode::serialize(&event)`; `LogSink` consumers updated accordingly

4. Error handling per event: a mapper/publish failure for event *i* logs and continues with
   event *i+1* (today one failure drops the whole batch). Failure counting hooks come later with
   connector metrics.

**Alternative considered — adapter iterates, keep `&[Event]` signature**: smaller diff, but
leaves a trait contract nobody exercises with N>1, and the mapper divergence intact. Rejected;
if a future sink genuinely wants transport-level batching (HTTP batch POST, file), that batching
belongs in the sink (buffer + flush), not in the formatting layer.

### Tests

- Adapter: batch of 3 events through a recording mock sink → exactly 3 publishes, in order
- Adapter: mapper failure on event 2 of 3 → events 1 and 3 still published
- Each mapper: `map_event` output equivalence with current single-event output (regression)
- Integration: RabbitMQ round-trip test extended to send a multi-event batch and assert N
  messages arrive (extends `tests/rabbitmq_integration.rs`)

---

## Part 2: Bug — Source threads are never joined on stop()

### Evidence

All three sources spawn their worker thread and discard the `JoinHandle`:

- `RabbitMQSource::stop()` (`rabbitmq_source.rs:721`) — sets `AtomicBool`, returns immediately
- `WebSocketSource::stop()` (`websocket_source.rs:558`) — same
- `TimerSource::stop()` (`timer_source.rs:296`) — same

Consequences: `stop()` returns while the worker may still be mid-callback — events can arrive
after shutdown began; app teardown can race connector cleanup (channel/connection close runs
concurrently with runtime destruction); tests that stop-then-assert are inherently racy.

### Fix design (revised at review: abstract, don't repeat)

Rather than each source hand-rolling a `running: Arc<AtomicBool>` + `JoinHandle` pair (a pattern
every future source would have to remember to copy correctly), the lifecycle is captured once in
a **`SourceWorker`** struct in `source/mod.rs` that sources embed by composition:

```rust
pub struct SourceWorker {
    name: &'static str,                 // for shutdown log messages
    running: Arc<AtomicBool>,           // polled by the worker loop
    handle: Option<JoinHandle<()>>,     // joined in stop()
}

impl SourceWorker {
    pub fn new(name: &'static str) -> Self;
    /// Sets the flag, spawns the thread; closure receives the running flag
    pub fn start<F: FnOnce(Arc<AtomicBool>) + Send + 'static>(&mut self, body: F);
    /// Signals + joins (bounded by SOURCE_STOP_GRACE_PERIOD = 5s). Idempotent.
    pub fn stop(&mut self);
}
impl Drop for SourceWorker { /* stop() — RAII: dropped ⇒ shut down */ }
impl Clone for SourceWorker { /* fresh, unstarted worker */ }
```

A source then holds `worker: SourceWorker`, spawns with `self.worker.start(|running| ...)`, and
`Source::stop()` is one line: `self.worker.stop()`. The join cannot be forgotten because it
lives inside the abstraction — and `Drop` covers even a source that is never stopped.

- `join_with_timeout` (watcher thread + channel; std has no native timed join) stays available
  but `SourceWorker` is the intended API. Kafka uses `SourceWorker` from day one.
- Applies to: RabbitMQSource, WebSocketSource, TimerSource.
- The `Source::stop()` trait doc states the contract (synchronous, idempotent, no callbacks
  after return) and points to `SourceWorker`; `docs/writing_extensions.md` gains a
  "Source Extensions" section documenting the pattern for new connector authors.

### Tests

- Per source: `start()` then `stop()` → worker observed terminated (flag set by worker's last
  instruction; assert after `stop()` returns)
- `stop()` without `start()` → no-op, no panic
- Double `stop()` → idempotent

---

## Part 3: Generic connector feature gating

### Verdict and rationale

**Adopt per-connector cargo features.** It directly serves the project identity (lightweight
single binary, minimal deps, zero external dependencies for core operation):

- A core build should not pay for connectors it doesn't use — in binary size, compile time, or
  build prerequisites
- Kafka makes this acute: `rdkafka` compiles librdkafka via cmake + a C toolchain. Imposing that
  on every `cargo build` (contributors, CI matrix, WASM-adjacent targets) is unacceptable
- The project already has this precedent for infra integrations: `kubernetes`, `consul`, `etcd`,
  `vault`, `cloud-native` umbrella — connectors follow the same pattern
- Cost is a `#[cfg]` discipline and a CI check to prevent gating rot — both cheap and specified
  below

### Feature scheme

```toml
[features]
default = []   # fully minimal — core engine only (Timer/Log baseline, no external connectors)

# Connectors (one feature per connector, named exactly after the extension name)
rabbitmq  = ["dep:lapin"]
websocket = ["dep:tokio-tungstenite", "dep:tungstenite"]
kafka     = ["dep:rdkafka"]
# future: http = ["dep:reqwest"], file = [], ...

# Umbrella
connectors-all = ["rabbitmq", "websocket", "kafka"]
```

**Rules (documented in CLAUDE.md / IMPLEMENTATION_GUIDE.md when implemented):**

1. **One feature per connector, named exactly after its registered extension name** (the string
   used in SQL `WITH (extension = '...')`). This makes the SQL name ↔ feature name ↔ docs
   mapping trivial.
2. **Fully minimal default set: `default = []`** (decided at review). No connector ships in the
   default build — every external connector is opt-in, regardless of whether it is pure Rust.
   This maximizes the lightweight identity: fastest `cargo build`, smallest core binary, and one
   uniform rule with no per-dependency judgment calls. It also matches the existing
   `default = []` in Cargo.toml.
3. **`connectors-all` umbrella** aggregates every connector. Release artifacts and Docker images
   build with `--features connectors-all` so shipped binaries include everything.
4. **Timer and Log stay ungated** — zero-dependency, used pervasively in tests and examples;
   they are the "core operation" baseline.
5. Existing unconditionally-compiled connectors (RabbitMQ, WebSocket) are **retrofitted** under
   this scheme. Note the behavior change: a plain `cargo build` will no longer include them —
   accepted at review (early stage, no backward-compatibility constraints). Anyone building from
   source for messaging use cases passes `--features connectors-all` (documented in README).

### Gating checklist (per connector)

Every gated connector touches exactly these points — this is the template Kafka and all future
connectors follow:

| Point | Mechanism |
|-------|-----------|
| Dependency | `optional = true` + `dep:` in the feature |
| Module declaration | `#[cfg(feature = "x")] pub mod x_source;` in `source/mod.rs` (same for sink) |
| Re-exports | `#[cfg(feature = "x")]` on any `pub use` |
| Registration | `#[cfg(feature = "x")]` on the `add_source_factory`/`add_sink_factory` lines in `register_default_extensions()` |
| Integration tests | `#![cfg(feature = "x")]` at top of `tests/x_integration.rs` |
| Unit tests referencing the connector | gate or relocate into the connector module |
| Examples | header comment noting required feature |
| cargo-machete metadata | add the optional dep to the ignored list if needed |

### Unknown-extension error hint

A binary built without a feature must fail helpfully, not with a bare `Unknown source type:
kafka`. Add a compile-time table of known-but-not-compiled connectors:

```rust
/// Extensions that exist in the codebase but were excluded from this build.
const UNAVAILABLE_EXTENSIONS: &[(&str, &str)] = &[
    #[cfg(not(feature = "kafka"))]
    ("kafka", "kafka"),
    #[cfg(not(feature = "rabbitmq"))]
    ("rabbitmq", "rabbitmq"),
    #[cfg(not(feature = "websocket"))]
    ("websocket", "websocket"),
];
```

On failed factory lookup in `stream_initializer`, consult the table and emit:

> Extension 'kafka' is supported by EventFlux but was not included in this build.
> Rebuild with `--features kafka` (or `--features connectors-all`).

Unknown names not in the table keep the current error (with the existing available-extensions
listing).

### CI strategy

Additions to `.github/workflows/rust.yml`:

1. **Main test job builds with `--features connectors-all`** — full coverage remains the norm
   (requires cmake for rdkafka; preinstalled on `ubuntu-latest`)
2. **Gating-honesty checks**: `cargo check` (minimal default build must compile and is the
   lightweight baseline we advertise) plus `cargo check --features <connector>` for each
   connector alone — catches missing `#[cfg]`s and accidental cross-feature dependencies
   cheaply, without a full test matrix
3. Docker publish workflow builds with `--features connectors-all`

### Documentation requirements

- **README.md**: "Building" section gains a feature-flags table (connector → feature → in
  default? → extra prerequisites)
- **Website** (`website/`): connector docs each state their feature flag and build command
- **IMPLEMENTATION_GUIDE.md**: "Adding a Connector" gains the gating checklist above
- **ROADMAP.md**: connector table gains a "Feature flag" column

---

## Out of scope (tracked, not in this spec)

Noted during the same review; deliberately deferred to keep this spec shippable:

| Item | Where it lives |
|------|----------------|
| `publish_event(&Event, payload)` keyed-publish interface + schema injection | `feat/kafka/README.md` Improvement 2 (driven by Kafka partition keys) |
| Source supervision/restart policy on connection loss | ROADMAP P10 (production hardening) |
| Shared tokio runtime for async connectors | Backlog — RabbitMQ/WebSocket cleanup, not needed by Kafka |
| Connector metrics (records in/out/failed, lag) | ROADMAP P9 (observability); Kafka ships local counters meanwhile |
| Shared `validate_connectivity` timeout helper | When a third network connector lands |
| `from_properties` `Result<_, String>` → `EventFluxError` | Mechanical cleanup, anytime |

---

## Implementation order & PR breakdown

| PR | Content | Risk |
|----|---------|------|
| 1 | Part 1: adapter iteration + `SinkMapper::map_event` migration + tests — **submitted: eventflux-io/eventflux#115** | Medium — touches 4 mappers, adapter, LogSink; behavior change for batched output is the point |
| 2 | Part 2: `SourceWorker` lifecycle abstraction + tests (3 sources) — **submitted: eventflux-io/eventflux#116** | Low |
| 3 | Part 3: feature scheme in Cargo.toml, retrofit rabbitmq/websocket, error hint, CI checks, docs — **submitted: eventflux-io/eventflux#117**. Review addition: ALL check surfaces (CI, pre-push hook, justfile, CONTRIBUTING) build with gates enabled so gated code is never silently skipped; CI runs the test suite in both minimal and connectors-all configurations | Low — mostly mechanical |

Each PR: `cargo fmt`, `cargo clippy`, full `cargo test`, plus the RabbitMQ integration suite for
PRs 1–2 (`cargo test --test rabbitmq_integration -- --ignored`, broker required).

After PR 3 lands, Kafka implementation (`feat/kafka/README.md`) starts on a clean base.
