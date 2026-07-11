---
sidebar_position: 4
title: Custom Connectors
description: Write your own sources and sinks for EventFlux
---

# Custom Connectors

EventFlux connectors are ordinary Rust types implementing the `Source` and
`Sink` traits, registered through factories. This page covers the contracts a
connector must honor and the helpers that make them easy to honor.

The in-tree RabbitMQ connector
(`src/core/stream/input/source/rabbitmq_source.rs` and
`src/core/stream/output/sink/rabbitmq_sink.rs`) is a complete reference
implementation of everything below.

## Architecture

```
Source:  external system → bytes → SourceCallback → SourceMapper → Events → pipeline
Sink:    pipeline → Events → per event: SinkMapper → bytes → Sink::publish() → external system
```

Connectors deal only in **bytes** — parsing and serialization are the mappers'
job. This keeps transports and formats independently pluggable.

## Writing a Source

A source implements the `Source` trait:

```rust
pub trait Source: Debug + Send + Sync {
    fn start(&mut self, callback: Arc<dyn SourceCallback>);
    fn stop(&mut self);
    fn clone_box(&self) -> Box<dyn Source>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError> { Ok(()) }
    fn set_error_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {}
}
```

### The stop() contract — use SourceWorker

`stop()` must be **synchronous**: when it returns, the worker thread has
terminated and no callback can fire afterwards. Repeated calls must be no-ops.

Don't hand-roll this with an `AtomicBool` and a discarded thread handle —
embed a `SourceWorker`, which captures the lifecycle once:

```rust
use eventflux::core::stream::input::source::{Source, SourceCallback, SourceWorker};

pub struct MySource {
    config: MyConfig,
    worker: SourceWorker,
}

impl MySource {
    pub fn new(config: MyConfig) -> Self {
        Self { config, worker: SourceWorker::new("MySource") }
    }
}

impl Source for MySource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();
        self.worker.start(move |running| {
            while running.load(Ordering::SeqCst) {
                // read from the external system, then:
                //     callback.on_data(&bytes)?
                // Poll `running` at least every ~100ms so stop() is fast.
            }
        });
    }

    fn stop(&mut self) {
        self.worker.stop(); // signals the flag + joins the thread (bounded)
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(Self {
            config: self.config.clone(),
            worker: self.worker.clone(), // clones as a fresh, unstarted worker
        })
    }
}
```

`SourceWorker` guarantees:

- **`stop()` joins the worker**, bounded by a 5s grace period
  (`SOURCE_STOP_GRACE_PERIOD`) — a worker stuck in slow I/O is detached with a
  warning instead of hanging shutdown
- **Idempotent** — safe without `start()`, safe to call twice
- **Drop-safe (RAII)** — a source dropped without being stopped still shuts
  its worker down
- **Clone-safe** — a clone starts unstarted; runtime state never leaks

Your only obligation is inside the loop: **poll the `running` flag at least
every ~100ms**. Use a poll/read timeout around blocking I/O, and
`sleep_while_running(&running, interval)` instead of `thread::sleep` for waits
between iterations, so long intervals never delay shutdown.

The runtime stops all sources **in parallel** during application shutdown, so
a correct `stop()` is all you need — total shutdown stays bounded by one grace
period regardless of how many sources an app runs.

### validate_connectivity()

Called at application startup (fail-fast): verify the external system is
reachable and configured resources exist, with a bounded timeout. If it
returns an error, the application does not start.

## Writing a Sink

```rust
pub trait Sink: Debug + Send + Sync {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError>;
    fn start(&self) {}
    fn stop(&self) {}
    fn clone_box(&self) -> Box<dyn Sink>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError> { Ok(()) }
}
```

`publish()` receives **one pre-formatted payload per event** — the runtime
maps each event through the sink mapper and calls `publish()` once per event
(one event = one transport message). A sink that wants transport-level
batching (e.g. batched HTTP POSTs) should buffer internally and flush in
`stop()`.

## Factories and Registration

Factories carry the metadata the SQL layer validates against — supported
formats, required and optional parameters — and build fully-initialized
instances from the `WITH` clause properties:

```rust
impl SourceFactory for MySourceFactory {
    fn name(&self) -> &'static str { "mysource" }        // WITH (extension = 'mysource')
    fn supported_formats(&self) -> &[&str] { &["json", "csv", "bytes"] }
    fn required_parameters(&self) -> &[&str] { &["mysource.host"] }
    fn optional_parameters(&self) -> &[&str] { &["mysource.port"] }
    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> { /* parse + validate */ }
    fn clone_box(&self) -> Box<dyn SourceFactory> { Box::new(self.clone()) }
}
```

Register with the manager, and the connector becomes usable from SQL:

```rust
manager.context().add_source_factory("mysource".to_string(), Box::new(MySourceFactory));
```

```sql
CREATE STREAM Input (value INT) WITH (
    type = 'source',
    extension = 'mysource',
    format = 'json',
    "mysource.host" = 'localhost'
);
```

## Error Handling Integration

Sources can integrate with EventFlux's error handling strategies
(`error.strategy` = `drop` / `retry` / `dlq` / `fail`) via
`SourceErrorContext`. The pattern — retry loop around `callback.on_data`,
acknowledge only after success, DLQ fallback events from raw bytes — is shown
end-to-end in the RabbitMQ source.

## Contributing a Connector Upstream

In-tree connectors are gated behind a cargo feature named after the extension
(the default build is minimal; `connectors-all` enables everything). The
complete per-connector gating checklist — dependency, feature, `#[cfg]`
points, registration, the unavailable-extension error hint, tests, CI, docs —
lives in
[`docs/writing_extensions.md`](https://github.com/eventflux-io/eventflux/blob/main/docs/writing_extensions.md)
in the repository.

## Dynamic Extensions

Connectors can also ship as separate crates compiled to dynamic libraries and
loaded at runtime via `manager.set_extension(...)`, exporting registration
callbacks like `register_sources` / `register_sinks`. See
[`docs/writing_extensions.md`](https://github.com/eventflux-io/eventflux/blob/main/docs/writing_extensions.md)
for the callback list and a minimal crate example.
