---
sidebar_position: 1
title: Installation
description: Install EventFlux in your Rust project
---

# Installation

EventFlux can be installed from source or added as a dependency to your Rust project.

## Prerequisites

- **Rust 1.85+** (stable)
- **Cargo** (comes with Rust)

MSRV is enforced via `Cargo.toml` (`package.rust-version`) and CI. If you don’t want to install Rust locally, use the
official Docker image (`ghcr.io/eventflux-io/eventflux:latest`).

:::tip Verify Rust Installation
```bash
rustc --version
cargo --version
```
:::

## From Source

Clone and build the repository (`--recursive` pulls in the vendored SQL parser submodule):

```bash
git clone --recursive https://github.com/eventflux-io/eventflux.git
cd eventflux
cargo build --release --features connectors-all
```

### Connector Feature Flags

The default build is **fully minimal** — the core engine plus the built-in
`timer` source and `log` sink, with no external connectors. Each connector is
a cargo feature named exactly after its SQL extension name:

| Feature | Connector | In default build? |
|---------|-----------|-------------------|
| `http` | HTTP source (polling + webhook) + sink | No |
| `kafka` | Kafka source + sink (needs cmake to build) | No |
| `rabbitmq` | RabbitMQ source + sink | No |
| `websocket` | WebSocket source + sink | No |
| `connectors-all` | All connectors | No |

```bash
cargo build --release                             # minimal: core engine only
cargo build --release --features rabbitmq         # just RabbitMQ
cargo build --release --features connectors-all   # everything
```

Docker images (`ghcr.io/eventflux-io/eventflux`) are built with
`connectors-all`, so they include every connector out of the box.

:::tip Helpful errors
If a query references a connector that wasn't compiled in, EventFlux tells
you exactly how to fix it:

```
Extension 'rabbitmq' is supported by EventFlux but was not included in this
build. Rebuild with `--features rabbitmq` (or `--features connectors-all`).
```
:::

### Running Tests

Verify your installation by running the test suite:

```bash
cargo test --features connectors-all
```

You should see **3,000+ passing tests**.

### Build Artifacts

After building, you'll find:
- `target/release/libeventflux.rlib` - Library
- `target/release/run_eventflux` - CLI binary

## As a Dependency

Add EventFlux to your `Cargo.toml`. Enable the connector features your
application needs (none are enabled by default):

```toml
[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git", features = ["connectors-all"] }
```

Or with a specific revision and only the connectors you use:

```toml
[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git", rev = "main", features = ["rabbitmq"] }
```

## Project Structure

After installation, your project structure should look like:

```
my-project/
├── Cargo.toml
├── src/
│   └── main.rs
```

With `Cargo.toml`:

```toml
[package]
name = "my-eventflux-app"
version = "0.1.0"
edition = "2021"

[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git" }
```

## Verify Installation

Create a simple test file to verify everything works:

```rust title="src/main.rs"
use eventflux::prelude::*;

fn main() {
    let manager = EventFluxManager::new();
    println!("EventFlux initialized successfully!");
}
```

Run it:

```bash
cargo run
```

## Optional Features

Beyond connectors, EventFlux exposes additional opt-in cargo features:

| Feature | Purpose |
|---------|---------|
| `rabbitmq`, `websocket`, `connectors-all` | External connectors (see table above) |
| `kubernetes`, `consul`, `etcd`, `vault`, `cloud-native` | Cloud-native integrations for distributed deployments |
| `perf-tests` | Performance test suite |

## Troubleshooting

### Common Issues

**Compilation takes too long**

Enable incremental compilation:

```toml title="Cargo.toml"
[profile.dev]
incremental = true
```

**Out of memory during compilation**

Reduce parallelism:

```bash
CARGO_BUILD_JOBS=2 cargo build
```

**Missing system dependencies**

On Linux, ensure you have:

```bash
# Ubuntu/Debian
sudo apt-get install build-essential pkg-config

# Fedora
sudo dnf install gcc pkg-config
```

## Next Steps

Once installed, proceed to the [Quick Start](/docs/getting-started/quick-start) guide to build your first streaming application.
