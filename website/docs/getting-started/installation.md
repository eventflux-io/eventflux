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

Clone and build the repository:

```bash
git clone https://github.com/eventflux-io/eventflux.git
cd eventflux
cargo build --release
```

### Running Tests

Verify your installation by running the test suite:

```bash
cargo test
```

You should see **1,400+ passing tests**.

### Build Artifacts

After building, you'll find:
- `target/release/libeventflux.rlib` - Library
- `target/release/run_eventflux` - CLI binary

## As a Dependency

Add EventFlux to your `Cargo.toml`:

```toml
[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git" }
```

Or with a specific revision:

```toml
[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git", rev = "main" }
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

## Optional Dependencies

For specific features, you may need additional dependencies:

| Feature | Dependency | Purpose |
|---------|------------|---------|
| Redis State | `redis` | Distributed state backend |
| Async Runtime | `tokio` | Async event processing |
| Serialization | `serde` | Event serialization |

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
