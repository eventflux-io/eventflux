# EventFlux Development Guide

This guide covers building, testing, running, and contributing to EventFlux.

## Table of Contents

- [Building](#building)
- [Running](#running)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Extensions](#extensions)
- [Contributing](#contributing)

---

## Building

### Prerequisites

- Rust 1.85 or later
- Protocol Buffer Compiler (for gRPC features)
- Git submodules (the SQL parser is vendored — see below)

MSRV is enforced via `Cargo.toml` (`package.rust-version`) and CI. If you don’t want to install Rust locally, use the
official Docker image (`ghcr.io/eventflux-io/eventflux:latest`) for running `.eventflux` queries.

```bash
# Install protoc (required for gRPC transport)
# macOS
brew install protobuf

# Ubuntu/Debian
apt-get install protobuf-compiler

# Verify
protoc --version
```

### Git Submodules (required)

EventFlux depends on a **vendored fork** of `datafusion-sqlparser-rs`, wired in as a git submodule at
`vendor/datafusion-sqlparser-rs`. `Cargo.toml` references it via a `path` dependency:

```toml
sqlparser = { path = "vendor/datafusion-sqlparser-rs" }
```

The fork lives at [`eventflux-io/datafusion-sqlparser-rs`](https://github.com/eventflux-io/datafusion-sqlparser-rs)
and is pinned to the `eventflux-extensions` branch, which carries EventFlux's streaming SQL extensions (native
`WINDOW` clause, `EventFluxDialect`, etc.). **`cargo build` fails with a missing-manifest error if the submodule
is not checked out**, because the `path` target is an empty directory.

```bash
# Preferred: clone with submodules in one step
git clone --recursive git@github.com:eventflux-io/eventflux.git

# Already cloned without --recursive? Initialize the submodule:
git submodule update --init --recursive

# Verify it is populated (should list Cargo.toml, src/, etc.)
ls vendor/datafusion-sqlparser-rs
```

Updating the pinned parser version (maintainers):

```bash
# Pull the latest commit on the tracked branch into the submodule
git submodule update --remote vendor/datafusion-sqlparser-rs

# Commit the new submodule pointer in the parent repo
git add vendor/datafusion-sqlparser-rs
git commit -m "Update vendored sqlparser submodule"
```

### Build Commands

```bash
# Development build (minimal — no external connectors)
cargo build

# Development build with all connectors (RabbitMQ, WebSocket, ...)
cargo build --features connectors-all

# Release build (shipped artifacts include all connectors)
cargo build --release --features connectors-all

# Check for compilation errors
cargo check

# Format code
cargo fmt

# Run linter (CI runs this with connectors enabled)
cargo clippy --all-targets --features connectors-all -- -D warnings -A clippy::nursery
```

Connectors are cargo features named after their SQL extension name
(`rabbitmq`, `websocket`, ...; umbrella: `connectors-all`). The default build
is fully minimal. See the feature-flags table in [README.md](README.md) and
the gating checklist in
[docs/writing_extensions.md](docs/writing_extensions.md).

---

## Running

### CLI Runner

Execute EventFluxQL files using the `run_eventflux` binary:

```bash
cargo run --bin run_eventflux examples/sample.eventflux
```

Available flags:

```
--persistence-dir <dir>   # Enable file persistence
--sqlite <db>             # Use SQLite persistence
--extension <lib>         # Load a dynamic extension library (repeatable)
--config <file>           # Provide a custom configuration
```

### Examples

```bash
# Simple filter
cargo run --bin run_eventflux examples/simple_filter.eventflux

# Time window
cargo run --bin run_eventflux examples/time_window.eventflux

# Partitioning
cargo run --bin run_eventflux examples/partition.eventflux

# Triggers
cargo run --bin run_eventflux examples/trigger.eventflux

# Extensions
cargo run --bin run_eventflux examples/extension.eventflux
```

### Running with Configuration

```bash
# Development
eventflux run app.sql --config config-dev.toml

# Production
export KAFKA_USER="admin"
export KAFKA_PASSWORD="secret"
eventflux run app.sql --config config-prod.toml
```

---

## Testing

### Running Tests

Connector tests only compile with their feature enabled — run the full suite
with `--features connectors-all` so nothing is silently skipped:

```bash
# Run all tests (including connector code)
cargo test --features connectors-all

# Minimal-build tests (no connectors — guards the lightweight baseline)
cargo test

# Run with output visible
cargo test --features connectors-all -- --nocapture

# Run specific test
cargo test --features connectors-all test_name

# Run tests matching pattern
cargo test --features connectors-all pattern
```

### Performance Tests

Performance tests are excluded from normal test runs. Enable with the `perf-tests` feature:

```bash
# Run all performance tests
cargo test --features perf-tests --test performance_tests

# Run specific performance test
cargo test --features perf-tests --test performance_tests test_junction_backpressure
```

### Redis Tests

Redis tests skip automatically if Redis is not available:

```bash
# Start Redis (Docker)
docker compose up -d redis

# Run Redis tests
cargo test redis

# Run Redis persistence tests
cargo test redis_persistence

# Run Redis backend tests
cargo test redis_backend
```

### Transport Tests

```bash
# TCP transport tests
cargo test distributed_tcp_integration

# gRPC transport tests
cargo test distributed_grpc_integration

# All distributed tests
cargo test distributed
```

### Benchmarks

```bash
cargo bench
```

---

## Project Structure

```
eventflux/
├── src/
│   ├── core/                    # Runtime execution engine
│   │   ├── ai/                  # AI/LLM integration (planned)
│   │   ├── config/              # Configuration management
│   │   ├── distributed/         # Distributed processing
│   │   ├── event/               # Event types and handling
│   │   ├── executor/            # Expression executors
│   │   ├── persistence/         # State persistence
│   │   ├── query/               # Query processors
│   │   ├── stream/              # Stream handling
│   │   └── table/               # Table implementations
│   ├── query_api/               # AST and query structures
│   └── sql_compiler/            # SQL parser
├── tests/                       # Integration tests
├── examples/                    # Example EventFluxQL files
├── proto/                       # Protocol buffer definitions
├── feat/                        # Feature documentation
└── docs/                        # Additional documentation
```

### Module Overview

- `query_api`: Defines AST for EventFlux applications, streams, queries, expressions
- `sql_compiler`: SQL parser with EventFlux streaming extensions (vendored sqlparser-rs)
- `core`: Runtime execution including processors, executors, state management

---

## Configuration

### Configuration Layers

EventFlux merges configuration from 4 layers (highest to lowest priority):

1. SQL WITH clause - Runtime overrides
2. TOML `[streams.StreamName]` - Stream-specific config
3. TOML `[application]` - Application defaults
4. Rust defaults - Framework defaults

### SQL WITH Clause

```sql
CREATE
STREAM Orders (
    orderId VARCHAR,
    amount DOUBLE,
    timestamp BIGINT
) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'kafka.brokers' = 'localhost:9092',
    'kafka.topic' = 'orders',
    'format' = 'json'
);
```

Key properties:

- `'type'` - Required for streams: `'source'` or `'sink'`
- `'extension'` - Required: connector type (`'kafka'`, `'http'`, `'mysql'`, `'file'`)
- `'format'` - Data mapper (`'json'`, `'avro'`, `'csv'`, `'protobuf'`)

### TOML Configuration

Development (config-dev.toml):

```toml
[application]
name = "OrderProcessing"
buffer_size = 8192

[streams.Orders]
type = "source"
extension = "kafka"
format = "json"

[streams.Orders.kafka]
brokers = "localhost:9092"
topic = "orders"
```

Production (config-prod.toml):

```toml
[application]
name = "OrderProcessing-Prod"

[streams.Orders]
type = "source"
extension = "kafka"
format = "json"

[streams.Orders.kafka]
brokers = "prod1:9092,prod2:9092,prod3:9092"
topic = "orders"
group = "production-app"

[streams.Orders.kafka.security]
protocol = "SASL_SSL"
username = "${KAFKA_USER}"
password = "${KAFKA_PASSWORD}"
```

See [feat/configuration/CONFIGURATION.md](feat/configuration/CONFIGURATION.md) for complete reference.

### Async Stream Configuration

```sql
CREATE
STREAM HighThroughputStream (
    symbol STRING,
    price DOUBLE,
    volume BIGINT
) WITH (
    'async.buffer_size' = '1024',
    'async.workers' = '2',
    'async.batch_size_max' = '10'
);
```

Properties:

- `async.enabled` - Enable async processing (true/false)
- `async.buffer_size` - Queue buffer size
- `async.workers` - Throughput estimation hint
- `async.batch_size_max` - Batch processing size

See [ASYNC_STREAMS_GUIDE.md](ASYNC_STREAMS_GUIDE.md) for details.

---

## Extensions

### Registering Tables

```rust
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::table::{InMemoryTable, Table};
use eventflux::core::event::value::AttributeValue;
use std::sync::Arc;

let manager = EventFluxManager::new();
let ctx = manager.eventflux_context();
let table: Arc<dyn Table> = Arc::new(InMemoryTable::new());
table.insert( & [AttributeValue::Int(1)]);
ctx.add_table("MyTable".to_string(), table);
```

### Registering User-Defined Functions

```rust
use eventflux::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;

#[derive(Debug, Clone)]
struct CounterFn;

impl ScalarFunctionExecutor for CounterFn {
    fn init(&mut self, _args: &Vec<Box<dyn ExpressionExecutor>>, _ctx: &Arc<EventFluxAppContext>) -> Result<(), String> {
        Ok(())
    }
    fn get_name(&self) -> String {
        "counter".to_string()
    }
    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor> {
        Box::new(self.clone())
    }
}

let manager = EventFluxManager::new();
manager.add_scalar_function_factory("counter".to_string(), Box::new(CounterFn));
```

### Registering Windows and Aggregators

```rust
use eventflux::core::extension::{WindowProcessorFactory, AttributeAggregatorFactory};

let manager = EventFluxManager::new();
manager.add_window_factory("myWindow".to_string(), Box::new(MyWindowFactory));
manager.add_attribute_aggregator_factory("myAgg".to_string(), Box::new(MyAggFactory));
```

### Dynamic Extension Loading

Extensions can be compiled as separate crates and loaded at runtime:

```rust
let manager = EventFluxManager::new();
let lib_path = custom_dyn_ext::library_path();
manager
.set_extension("custom", lib_path.to_str().unwrap().to_string())
.unwrap();
```

The library should export registration functions:

```
register_extension
register_windows
register_functions
register_sources
register_sinks
register_stores
register_source_mappers
register_sink_mappers
```

Each function has signature `unsafe extern "C" fn(&EventFluxManager)`.

Build as cdylib:

```bash
cargo build -p my_extension
./target/debug/libmy_extension.{so|dylib|dll}
```

See [docs/writing_extensions.md](docs/writing_extensions.md) for complete guide.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide.

### Development Setup

1. Fork the repository
2. Clone your fork with submodules: `git clone --recursive <your-fork-url>`
   (or run `git submodule update --init --recursive` after cloning)
3. Install pre-commit hooks:
   ```bash
   pip install pre-commit
   pre-commit install --install-hooks
   ```
4. Create a feature branch
5. Make changes
6. Run quality checks:
   ```bash
   just check-all
   cargo nextest run
   cargo test --doc
   ```
7. Submit pull request

### Code Style

- Follow Rust idioms
- Use `cargo fmt` for formatting
- Address `cargo clippy` warnings
- Add tests for new functionality
- Document public APIs

### Commit Messages

- Use imperative mood ("Add feature" not "Added feature")
- Keep first line under 60 characters
- Start with capital letter
- Do not mention AI assistance in commits

### Testing Requirements

- Unit tests for new functions
- Integration tests for new features
- Performance tests for hot paths (when applicable)

### Documentation

- Update relevant documentation in `feat/` directories
- Add examples for new features
- Keep CLAUDE.md updated for AI-assisted development

---

## Docker Setup

### Redis for Development

```bash
# Start Redis
docker compose up -d redis

# Verify
redis-cli ping

# Stop
docker compose down
```

### Redis Configuration

```rust
use eventflux::core::persistence::RedisPersistenceStore;
use eventflux::core::distributed::RedisConfig;

let config = RedisConfig {
url: "redis://localhost:6379".to_string(),
max_connections: 10,
connection_timeout_ms: 5000,
key_prefix: "eventflux:".to_string(),
ttl_seconds: Some(3600),
};

let store = RedisPersistenceStore::new_with_config(config) ?;
```

See [DOCKER_SETUP.md](DOCKER_SETUP.md) for details.

---

## Debugging and Profiling

### CPU Profiling

```bash
cargo build --release
perf record --call-graph=dwarf target/release/run_eventflux query.eventflux
perf report
```

### Memory Profiling

```bash
valgrind --tool=massif target/release/run_eventflux query.eventflux
ms_print massif.out.*
```

### Flamegraph

```bash
cargo flamegraph --bin run_eventflux -- query.eventflux
```

### Lock Contention

```bash
perf lock record target/release/run_eventflux query.eventflux
perf lock report
```

---

## Additional Resources

- [ROADMAP.md](ROADMAP.md) - Verified roadmap with completed and upcoming work
- [CLAUDE.md](CLAUDE.md) - AI-assisted development guide
- [feat/](feat/) - Feature documentation and architecture
