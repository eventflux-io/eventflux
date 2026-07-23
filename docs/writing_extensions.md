# Writing Extensions

This guide explains how to extend the EventFlux Rust runtime with custom
components.  Extensions are registered with a `EventFluxManager` and become
available to applications via annotations like `@source`, `@sink` or `@store`.

## Table Extensions

A table extension implements the `Table` trait and provides a corresponding
`TableFactory`.  The factory is registered under a unique name:

```rust
use eventflux::core::extension::TableFactory;
use eventflux::core::table::Table;

pub struct MyTable;
impl Table for MyTable { /* ... */ }

#[derive(Clone)]
pub struct MyTableFactory;
impl TableFactory for MyTableFactory {
    fn name(&self) -> &'static str { "myStore" }
    fn create(
        &self,
        name: String,
        props: std::collections::HashMap<String, String>,
        ctx: std::sync::Arc<eventflux::core::config::eventflux_context::EventFluxContext>,
    ) -> Result<std::sync::Arc<dyn Table>, String> {
        Ok(std::sync::Arc::new(MyTable))
    }
    fn clone_box(&self) -> Box<dyn TableFactory> { Box::new(self.clone()) }
}
```

After registering the factory with a `EventFluxManager`, applications can define a
table using `@store(type='myStore')`.

### Compiled Conditions

To execute query conditions efficiently, tables can translate expressions into a
custom representation by implementing `compile_condition` and
`compile_update_set`.  The returned structures implement the marker traits
`CompiledCondition` and `CompiledUpdateSet` which the runtime passes back to the
same table instance for `find`, `update` and `delete` operations.

A table may also implement `compile_join_condition` to optimise stream-table
joins.  The provided `JdbcTable` demonstrates translating expressions into SQL
for execution inside a database, while `CacheTable` performs in-memory matching
using compiled values.

Extensions for other storage engines can follow these examples to provide their
own compiled condition formats.

## Source Extensions

A source extension implements the `Source` trait and provides a `SourceFactory`
(see the RabbitMQ connector in `src/core/stream/input/source/rabbitmq_source.rs`
for a complete reference implementation).

### Reserved injected properties

Property keys beginning with an underscore are **reserved for the engine** —
never define your own. `stream_initializer` injects them into the map passed
to `create_initialized()`:

| Key | Value |
|-----|-------|
| `_format` | The stream's declared `format` (e.g. `json`), when one is set |

Connectors may read these to derive format-dependent settings — the HTTP sink
derives its default Content-Type from `_format` (see
`connector_util::INJECTED_FORMAT_KEY`).

### Worker thread lifecycle: use `SourceWorker`

`Source::stop()` has a hard contract: **it must be synchronous**. When `stop()`
returns, the worker thread must have terminated — no `SourceCallback` may fire
afterwards, and repeated `stop()` calls must be no-ops. This is what makes
application shutdown deterministic (no events delivered into a runtime that is
tearing down).

Do not hand-roll a `running: Arc<AtomicBool>` + `JoinHandle` pair. Embed a
`SourceWorker` (from `core::stream::input::source`), which captures the pattern
once and cannot be misused:

```rust
use eventflux::core::stream::input::source::{Source, SourceCallback, SourceWorker};

pub struct MySource {
    config: MyConfig,
    worker: SourceWorker,
}

impl MySource {
    pub fn new(config: MyConfig) -> Self {
        Self {
            config,
            worker: SourceWorker::new("MySource"),
        }
    }
}

impl Source for MySource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();
        self.worker.start(move |running| {
            while running.load(Ordering::SeqCst) {
                // Read from the external system and deliver bytes:
                //     callback.on_data(&bytes)
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

What `SourceWorker` guarantees:

- `stop()` signals the running flag and joins the thread, bounded by
  `SOURCE_STOP_GRACE_PERIOD` (5s) — a worker stuck in slow I/O is detached
  with a warning instead of hanging shutdown
- `stop()` is idempotent (safe without `start()`, safe to call twice)
- `Drop` calls `stop()` — a source dropped without being stopped still shuts
  its worker down
- `Clone` yields a fresh, unstarted worker, so runtime state never leaks into
  clones

The runtime stops sources in parallel during bulk shutdown, so a correct
`stop()` is all a source needs to provide — total shutdown stays bounded by
one grace period regardless of source count.

The only obligation left on the implementer is inside the loop: **poll the
`running` flag at least every ~100ms** — use a poll/read timeout around
blocking I/O (as the RabbitMQ and WebSocket sources do), and
`sleep_while_running(&running, interval)` instead of `thread::sleep` for waits
between iterations (as TimerSource does), so long intervals never delay
shutdown.

## Connector Feature Gating

Every in-tree connector is gated behind a cargo feature **named exactly after
its registered extension name** (the string used in SQL
`WITH (extension = '...')`). The default build is fully minimal — no
connectors — and release/Docker builds use `--features connectors-all`.

A feature may own a *family* of related extensions (e.g. a future
`http-response` sink under the `http` feature) — the SQL extension names stay
distinct, but they gate together.

When adding a new in-tree connector, touch exactly these points:

| Point | Mechanism |
|-------|-----------|
| Dependency | `optional = true` in `[dependencies]`, `dep:` in the feature |
| Feature | `[features]` entry named after the extension; add it to `connectors-all` |
| Module declaration | `#[cfg(feature = "x")] pub mod x_source;` in `source/mod.rs` (same for the sink) |
| Registration | `#[cfg(feature = "x")]` on the import and `add_source_factory`/`add_sink_factory` lines in `register_default_extensions()` |
| Unavailable-extension hint | add the name to `GATED_OUT_EXTENSIONS` in `stream_initializer.rs` so builds without the feature report "rebuild with `--features x`" instead of "extension not found" |
| Integration tests | `#![cfg(feature = "x")]` at the top of `tests/x_integration.rs` |
| CI | add `cargo check --all-targets --features x` to the per-connector gating checks in `.github/workflows/rust.yml`, `justfile` (`check-minimal`), and the CONTRIBUTING.md command list — all three must stay in sync |
| Docs | feature-flags table in README.md; example files note the required feature |

## Dynamic Extensions

Extensions can also be distributed as separate crates compiled into dynamic
libraries.  When the `EventFluxManager` loads a library via `set_extension` it will
try to invoke a set of registration callbacks if they are exported:

```text
register_extension
register_windows
register_functions
register_sources
register_sinks
register_stores
register_source_mappers
register_sink_mappers
```

Each callback should have the type `unsafe extern "C" fn(&EventFluxManager)` and
can register any number of factories.  Only the callbacks required by your
extension need to be implemented.  A minimal crate might look like:

```rust
use eventflux::core::eventflux_manager::EventFluxManager;

#[no_mangle]
pub extern "C" fn register_functions(manager: &EventFluxManager) {
    manager.add_scalar_function_factory("myFn".to_string(), Box::new(MyFn));
}

#[no_mangle]
pub extern "C" fn register_windows(manager: &EventFluxManager) {
    manager.add_window_factory("myWindow".to_string(), Box::new(MyWindowFactory));
}
```

Compile the crate as a `cdylib` and load it at runtime:

```bash
cargo build -p my_extension
```

```rust
manager.set_extension("ext", "./target/debug/libmy_extension.so".into()).unwrap();
```
