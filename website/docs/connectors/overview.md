---
sidebar_position: 1
title: Connectors Overview
description: Connect EventFlux to external systems with Sources, Sinks, and Mappers
---

# Connectors Overview

EventFlux provides a powerful connector system that enables integration with external messaging systems and data stores. Connect your streaming pipelines to message brokers, databases, and other services using SQL-native syntax.

## Architecture

The connector system consists of three main components:

```
┌─────────────────────────────────────────────────────────────────┐
│                        EventFlux Engine                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────┐    ┌──────────┐    ┌─────────┐    ┌──────────┐   │
│   │ Source  │───▶│  Mapper  │───▶│  Query  │───▶│  Mapper  │──▶│
│   │         │    │ (decode) │    │ Engine  │    │ (encode) │   │
│   └─────────┘    └──────────┘    └─────────┘    └──────────┘   │
│        ▲                                              │         │
│        │                                              ▼         │
│   ┌─────────┐                                    ┌─────────┐   │
│   │RabbitMQ │                                    │   Sink  │   │
│   │WebSocket│                                    │         │   │
│   │ Kafka   │                                    └─────────┘   │
│   └─────────┘                                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Sources

**Sources** consume data from external systems and feed events into the processing pipeline. They handle:

- Connection management and reconnection
- Message acknowledgment
- Backpressure handling
- Format conversion via mappers

### Sinks

**Sinks** publish processed events to external systems. They handle:

- Connection management
- Delivery guarantees
- Format serialization via mappers

**One event = one message**: each event is serialized and published as its own
transport message (AMQP message, WebSocket frame, ...). A failure to serialize
or publish one event never drops the other events in the same batch.

### Mappers

**Mappers** transform between raw bytes and structured events:

- **Source Mappers**: Decode incoming bytes (JSON, CSV, bytes) into event attributes
- **Sink Mappers**: Encode event attributes into output format (JSON, CSV, bytes)

## SQL Syntax

### Defining a Source Stream

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'source',
    extension = 'connector_name',
    format = 'mapper_name',
    "connector.option1" = 'value1',
    "connector.option2" = 'value2'
);
```

### Defining a Sink Stream

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'sink',
    extension = 'connector_name',
    format = 'mapper_name',
    "connector.option1" = 'value1',
    "connector.option2" = 'value2'
);
```

## Building with Connectors

Connectors are **opt-in cargo features**, named exactly after their SQL
extension name. The default build is fully minimal (core engine plus the
built-in `timer` source and `log` sink):

```bash
cargo build --release --features file              # just File
cargo build --release --features http              # just HTTP
cargo build --release --features kafka             # just Kafka
cargo build --release --features rabbitmq          # just RabbitMQ
cargo build --release --features websocket         # just WebSocket
cargo build --release --features connectors-all    # everything
```

Docker images are built with `connectors-all` and include every connector.

If a query references a connector that wasn't compiled in, EventFlux fails
fast with an actionable error:

```
Extension 'rabbitmq' is supported by EventFlux but was not included in this
build. Rebuild with `--features rabbitmq` (or `--features connectors-all`).
```

## Available Connectors

| Connector | Source | Sink | Feature flag | Status | Description |
|-----------|--------|------|--------------|--------|-------------|
| **Timer** | Yes | — | (always built) | Production Ready | Periodic tick source |
| **Log** | — | Yes | (always built) | Production Ready | Logging sink for debugging |
| **File** | Yes | Yes | `file` | Production Ready | Replay/follow files, rotating output |
| **HTTP** | Yes | Yes | `http` | Production Ready | REST polling, webhooks in/out |
| **Kafka** | Yes | Yes | `kafka` | Production Ready | Apache Kafka streaming |
| **RabbitMQ** | Yes | Yes | `rabbitmq` | Production Ready | AMQP 0-9-1 message broker |
| **WebSocket** | Yes | Yes | `websocket` | Production Ready | Real-time bidirectional streaming |

## Available Mappers

| Mapper | Source | Sink | Status | Description |
|--------|--------|------|--------|-------------|
| **JSON** | Yes | Yes | Production Ready | JSON serialization |
| **CSV** | Yes | Yes | Production Ready | CSV parsing/formatting |
| **Bytes** | Yes | Yes | Production Ready | Raw binary passthrough |
| **Avro** | Planned | Planned | Roadmap | Apache Avro format |
| **Protobuf** | Planned | Planned | Roadmap | Protocol Buffers |

## Complete Example

Here's a complete example showing RabbitMQ source and sink with JSON mapping:

```sql
-- Input: Consume JSON events from RabbitMQ queue
CREATE STREAM OrderInput (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    product STRING
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.port" = '5672',
    "rabbitmq.queue" = 'orders',
    "rabbitmq.username" = 'guest',
    "rabbitmq.password" = 'guest'
);

-- Output: Publish enriched events to RabbitMQ exchange
CREATE STREAM OrderOutput (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    product STRING,
    priority STRING
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.exchange" = 'processed-orders',
    "rabbitmq.routing.key" = 'high-value'
);

-- Processing: Filter high-value orders and add priority
INSERT INTO OrderOutput
SELECT
    order_id,
    customer_id,
    amount,
    product,
    CASE
        WHEN amount > 1000 THEN 'HIGH'
        WHEN amount > 100 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS priority
FROM OrderInput
WHERE amount > 50;
```

## Extension Registration

All built-in connectors and mappers are registered automatically. Custom extensions can be added programmatically:

```rust
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::extension::{SourceFactory, SinkFactory};

let mut manager = EventFluxManager::new();

// Register custom source factory
manager.context().add_source_factory(
    "custom".to_string(),
    Box::new(MyCustomSourceFactory)
);

// Register custom sink factory
manager.context().add_sink_factory(
    "custom".to_string(),
    Box::new(MyCustomSinkFactory)
);
```

## Next Steps

- **[File Connector](/docs/connectors/file)** - Replay and follow files, rotating line-delimited output
- **[HTTP Connector](/docs/connectors/http)** - REST polling, webhook ingestion and delivery
- **[Kafka Connector](/docs/connectors/kafka)** - Consume from and produce to Apache Kafka topics
- **[RabbitMQ Connector](/docs/connectors/rabbitmq)** - Connect to RabbitMQ message broker
- **[WebSocket Connector](/docs/connectors/websocket)** - Connect to WebSocket endpoints for real-time streaming
- **[Mappers Reference](/docs/connectors/mappers)** - JSON, CSV, and bytes format handling
