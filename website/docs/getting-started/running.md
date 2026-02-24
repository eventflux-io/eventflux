---
sidebar_position: 3
title: Running EventFlux
description: Multiple ways to run EventFlux applications - CLI, Rust API, and Docker
---

# Running EventFlux

EventFlux provides multiple ways to run streaming applications depending on your use case. This guide covers all execution methods with working examples.

## Prerequisites

Before running EventFlux applications, ensure you have:

1. **Rust toolchain** (1.85+): Install from [rustup.rs](https://rustup.rs/)
2. **RabbitMQ** (for connector examples): See [RabbitMQ setup](#rabbitmq-setup)

## Method 1: Command Line Interface (CLI)

The simplest way to run EventFlux is using the built-in CLI binary.

### Basic Usage

```bash
# Run a query file
cargo run --bin run_eventflux <path-to-query-file>

# Example with the RabbitMQ to Log example
cargo run --bin run_eventflux examples/rabbitmq_to_log.eventflux
```

### CLI Options

```bash
cargo run --bin run_eventflux -- --help
```

| Option | Description |
|--------|-------------|
| `<eventflux_file>` | Path to the EventFlux query file (required) |
| `-c, --config <PATH>` | Path to YAML configuration file |
| `--set <KEY=VALUE>` | Override any config value using dot-notation (can be used multiple times) |
| `-e, --extension <PATH>` | Dynamic extension library to load (can be used multiple times) |

### Examples

```bash
# Basic execution
cargo run --bin run_eventflux examples/simple_filter.eventflux

# With file-based persistence (using --set)
cargo run --bin run_eventflux examples/rabbitmq.eventflux \
  --set eventflux.persistence.type=file \
  --set eventflux.persistence.path=./snapshots

# With SQLite persistence (using --set)
cargo run --bin run_eventflux examples/rabbitmq.eventflux \
  --set eventflux.persistence.type=sqlite \
  --set eventflux.persistence.path=./eventflux.db

# With custom configuration file
cargo run --bin run_eventflux examples/rabbitmq.eventflux \
  --config ./config/eventflux.yaml

# Combine config file with overrides
cargo run --bin run_eventflux examples/rabbitmq.eventflux \
  --config ./config/base.yaml \
  --set eventflux.runtime.performance.thread_pool_size=16

# With dynamic extension
cargo run --bin run_eventflux examples/extension.eventflux \
  -e ./target/release/libcustom_ext.so
```

### Config Override Examples

The `--set` flag supports dot-notation paths to override any configuration value:

```bash
# Persistence settings
--set eventflux.persistence.type=sqlite
--set eventflux.persistence.path=./data.db
--set eventflux.persistence.enabled=true

# Runtime performance tuning
--set eventflux.runtime.performance.thread_pool_size=8
--set eventflux.runtime.performance.event_buffer_size=500000
--set eventflux.runtime.performance.batch_processing=true

# Runtime mode
--set eventflux.runtime.mode=distributed

# Metadata
--set metadata.name=my-production-app
--set metadata.environment=production
```

### Release Build (Recommended for Production)

```bash
# Build release binary
cargo build --release

# Run with release binary
./target/release/run_eventflux examples/rabbitmq_to_log.eventflux
```

## Method 2: Rust API (Programmatic)

For embedding EventFlux in your Rust applications, use the Rust API.

### Basic Example

```rust
use eventflux::core::eventflux_manager::EventFluxManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create the manager
    let manager = EventFluxManager::new();

    // Define the application using SQL
    let query = r#"
        CREATE STREAM SensorInput (
            sensor_id STRING,
            temperature DOUBLE,
            timestamp LONG
        );

        CREATE STREAM HighTempAlerts (
            sensor_id STRING,
            temperature DOUBLE,
            timestamp LONG
        );

        INSERT INTO HighTempAlerts
        SELECT sensor_id, temperature, timestamp
        FROM SensorInput
        WHERE temperature > 100.0;
    "#;

    // Create the runtime (does NOT start automatically)
    let runtime = manager
        .create_eventflux_app_runtime_from_string(query)
        .await?;

    // Add output callback before starting
    runtime.add_callback(
        "HighTempAlerts",
        Box::new(|events| {
            for event in events {
                println!("ALERT: {:?}", event);
            }
        }),
    )?;

    // Start the runtime
    runtime.start()?;

    println!("Running app '{}'. Press Ctrl+C to exit.", runtime.name);

    // Keep running until interrupted
    tokio::signal::ctrl_c().await?;
    runtime.shutdown();

    Ok(())
}
```

### With RabbitMQ Source and Log Sink

```rust
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::stream::output::LogStreamCallback;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = EventFluxManager::new();

    let query = r#"
        CREATE STREAM EventInput (
            id STRING,
            name STRING,
            value DOUBLE
        ) WITH (
            type = 'source',
            extension = 'rabbitmq',
            format = 'json',
            "rabbitmq.host" = 'localhost',
            "rabbitmq.queue" = 'event-queue'
        );

        CREATE STREAM EventOutput (
            id STRING,
            name STRING,
            value DOUBLE
        ) WITH (
            type = 'sink',
            extension = 'log'
        );

        INSERT INTO EventOutput
        SELECT id, name, value FROM EventInput;
    "#;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(query)
        .await?;

    // Add log callback to all output streams
    for stream_id in runtime.stream_junction_map.keys() {
        runtime.add_callback(
            stream_id,
            Box::new(LogStreamCallback::new(stream_id.clone())),
        )?;
    }

    runtime.start()?;

    println!("Running '{}'. Press Ctrl+C to exit.", runtime.name);
    tokio::signal::ctrl_c().await?;
    runtime.shutdown();

    Ok(())
}
```

### Using API Instead of SQL

For complete programmatic control, build the application using the API:

```rust
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::definition::{
    Attribute, StreamDefinition
};
use eventflux::query_api::execution::{
    Query, QueryInput, QueryOutput, Selector
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = EventFluxManager::new();

    // Build application programmatically
    let mut app = EventFluxApp::new("MyApp".to_string());

    // Define input stream
    let input_stream = StreamDefinition::new(
        "SensorInput".to_string(),
        vec![
            Attribute::new("sensor_id".to_string(), "STRING".to_string()),
            Attribute::new("temperature".to_string(), "DOUBLE".to_string()),
        ],
    );
    app.add_stream_definition(input_stream);

    // Define output stream
    let output_stream = StreamDefinition::new(
        "FilteredOutput".to_string(),
        vec![
            Attribute::new("sensor_id".to_string(), "STRING".to_string()),
            Attribute::new("temperature".to_string(), "DOUBLE".to_string()),
        ],
    );
    app.add_stream_definition(output_stream);

    // Define query (filter temperature > 100)
    let query = Query::new(
        QueryInput::from_stream("SensorInput"),
        vec![
            Selector::attribute("sensor_id"),
            Selector::attribute("temperature"),
        ],
        QueryOutput::to_stream("FilteredOutput"),
    )
    .with_filter("temperature > 100.0");

    app.add_query(query);

    // Create runtime from API app
    let runtime = manager
        .create_eventflux_app_runtime_from_api(Arc::new(app), None)
        .await?;

    runtime.start()?;

    // Send events programmatically
    runtime.send_event("SensorInput", vec![
        "sensor-1".into(),
        95.0.into(),
    ])?;

    runtime.send_event("SensorInput", vec![
        "sensor-2".into(),
        105.0.into(),  // This triggers the filter
    ])?;

    // Collect output
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    runtime.shutdown();

    Ok(())
}
```

### With Configuration File

```rust
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::config::ConfigManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from file
    let config_manager = ConfigManager::from_file("config/eventflux.yaml");

    // Create manager with configuration
    let mut manager = EventFluxManager::new();
    manager.set_config_manager(config_manager)?;

    let query = std::fs::read_to_string("examples/rabbitmq.eventflux")?;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(&query)
        .await?;

    runtime.start()?;

    // ... rest of the code
    Ok(())
}
```

## Method 3: Docker

Run EventFlux in a containerized environment for production deployments.

### Using Pre-built Image (Recommended)

The easiest way to run EventFlux is using the pre-built Docker image from GitHub Container Registry:

```bash
# Pull the latest image
docker pull ghcr.io/eventflux-io/eventflux:latest

# Run with your query file
docker run --rm \
  -v $(pwd)/my_query.eventflux:/app/query.eventflux:ro \
  --network host \
  ghcr.io/eventflux-io/eventflux:latest /app/query.eventflux
```

**No Rust toolchain required!** Just Docker Desktop and your `.eventflux` query file.

:::note Docker Compose version
This repository uses Docker Compose v2 (`docker compose ...`). If you only have legacy `docker-compose`, install the
Compose v2 plugin (recommended) or adapt the commands accordingly.
:::

#### Quick Start Example

```bash
# 1. Create a simple query file
cat > my_app.eventflux << 'EOF'
CREATE STREAM EventInput (
    id STRING,
    name STRING,
    value DOUBLE
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'host.docker.internal',
    "rabbitmq.queue" = 'event-queue'
);

CREATE STREAM EventOutput (
    id STRING,
    name STRING,
    value DOUBLE
) WITH (
    type = 'sink',
    extension = 'log'
);

INSERT INTO EventOutput
SELECT id, name, value FROM EventInput;
EOF

# 2. Run EventFlux
docker run --rm \
  -v $(pwd)/my_app.eventflux:/app/query.eventflux:ro \
  --network host \
  ghcr.io/eventflux-io/eventflux:latest /app/query.eventflux
```

#### Available Tags

| Tag | Description |
|-----|-------------|
| `latest` | Most recent stable build |
| `0.1.0` | Version 0.1.0 release |

### Building Your Own Image (Optional)

If you’re reading this on the website and just want to run EventFlux, prefer the pre-built image:
`ghcr.io/eventflux-io/eventflux:latest`.

Building locally is mainly for contributors who are modifying the engine (or testing a fork/PR). The repository already
includes a production-ready `Dockerfile` at the repo root. Build it with any local tag:

```bash
docker build -t eventflux:local .
```

### Running with Docker

```bash
# Run with a query file mounted
docker run --rm \
  -v $(pwd)/examples:/app/queries:ro \
  --network host \
  eventflux:local /app/queries/rabbitmq_to_log.eventflux

# With file-based persistence
docker run --rm \
  -v $(pwd)/examples:/app/queries:ro \
  -v $(pwd)/data:/app/data \
  --network host \
  eventflux:local /app/queries/rabbitmq.eventflux \
    --set eventflux.persistence.type=file \
    --set eventflux.persistence.path=/app/data/snapshots

# With SQLite persistence
docker run --rm \
  -v $(pwd)/examples:/app/queries:ro \
  -v $(pwd)/data:/app/data \
  --network host \
  eventflux:local /app/queries/rabbitmq.eventflux \
    --set eventflux.persistence.type=sqlite \
    --set eventflux.persistence.path=/app/data/eventflux.db
```

### Docker Compose Example

The repository includes a full `docker-compose.yml` with RabbitMQ, Redis, and EventFlux pre-configured:

```bash
# Clone the repository
git clone https://github.com/eventflux-io/eventflux.git
cd eventflux

# Start backing services (RabbitMQ + Redis)
docker compose up -d rabbitmq redis

# If you already have RabbitMQ/Redis running, you can avoid port conflicts:
#   RABBITMQ_AMQP_PORT=5673 RABBITMQ_MANAGEMENT_PORT=15673 REDIS_PORT=6380 docker compose up -d rabbitmq redis

# Optional: enable Redis Commander UI (runs under an extra profile)
docker compose --profile tools up -d redis-commander

# Run EventFlux (CLI-style container that executes a query file)
# By default the repo wires `eventflux` to run `examples/rabbitmq_to_log_docker.eventflux`.
docker compose --profile eventflux run --rm --no-deps eventflux /app/queries/rabbitmq_to_log_docker.eventflux

# View EventFlux logs
docker compose logs -f rabbitmq redis

# Stop all services + clean network
docker compose down
```

Or create a minimal `docker-compose.yml` (requires Docker Compose v2+):

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 10s
      timeout: 5s
      retries: 5

  eventflux:
    image: ghcr.io/eventflux-io/eventflux:latest
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./my_query.eventflux:/app/query.eventflux:ro
    command: /app/query.eventflux
    environment:
      - RUST_LOG=info
```

## RabbitMQ Setup

For examples using RabbitMQ, start the broker first:

```bash
# Start RabbitMQ with management UI
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Access the management UI at [http://localhost:15672](http://localhost:15672) with credentials `guest/guest`.

### Create Test Queue

For the `rabbitmq_to_log.eventflux` example:

1. Open RabbitMQ Management UI
2. Go to **Queues** > **Add a new queue**
3. Name: `event-queue`
4. Click **Add queue**

### Publish Test Messages

1. Go to **Queues** > `event-queue` > **Publish message**
2. Payload:
   ```json
   {"id":"evt-001","name":"temperature","value":23.5}
   ```
3. Click **Publish message**

You should see the event logged in the EventFlux console output.

## Complete Example: End-to-End

Here's a complete walkthrough:

```bash
# 1. Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 2. Wait for RabbitMQ to be ready (about 30 seconds)
sleep 30

# 3. Create the queue (via rabbitmqadmin or Management UI)
# Management UI: http://localhost:15672 (guest/guest)
# Create queue named: event-queue

# 4. Run EventFlux
cargo run --bin run_eventflux examples/rabbitmq_to_log.eventflux

# 5. In another terminal, publish a test message via Management UI
# or use the rabbitmqadmin CLI:
# rabbitmqadmin publish exchange=amq.default routing_key=event-queue \
#   payload='{"id":"test-1","name":"sensor","value":42.0}'

# 6. Observe the output in the EventFlux terminal:
# [LOG] Event { data: ["test-1", "sensor", 42.0] }
```

## Troubleshooting

### Connection Refused to RabbitMQ

```
Error: Failed to connect to RabbitMQ at localhost:5672
```

**Solution**: Ensure RabbitMQ is running:
```bash
docker ps | grep rabbitmq
```

### Queue Not Found

```
Error: Queue 'event-queue' does not exist
```

**Solution**: Create the queue in RabbitMQ Management UI before starting EventFlux.

### Permission Denied (Docker)

```
Error: Permission denied reading /app/queries/...
```

**Solution**: Ensure volumes are mounted with correct permissions:
```bash
docker run -v $(pwd)/examples:/app/queries:ro ...
```

## Next Steps

- **[SQL Reference](/docs/sql-reference/queries)** - Learn the complete query syntax
- **[RabbitMQ Connector](/docs/connectors/rabbitmq)** - Advanced RabbitMQ configuration
- **[Configuration](/docs/rust-api/configuration)** - Configure runtime behavior
