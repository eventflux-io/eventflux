---
sidebar_position: 5
title: RabbitMQ Connector
description: Connect EventFlux to RabbitMQ message broker for real-time event streaming
---

# RabbitMQ Connector

The RabbitMQ connector enables EventFlux to consume events from RabbitMQ queues and publish processed results to RabbitMQ exchanges. It supports JSON, CSV, and bytes message formats with automatic serialization/deserialization.

## Prerequisites

### Building with RabbitMQ Support

The RabbitMQ connector is feature-gated. Build EventFlux with the `rabbitmq`
feature (Docker images already include all connectors):

```bash
cargo build --release --features rabbitmq
# or: cargo build --release --features connectors-all
```

### Starting RabbitMQ

The easiest way to run RabbitMQ is with Docker:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Access the management UI at [http://localhost:15672](http://localhost:15672) with credentials `guest/guest`.

### Creating Queues and Exchanges

Before running EventFlux queries, create the required infrastructure:

**Option 1: Via Management UI**

1. Go to **Exchanges** → **Add a new exchange**
   - Name: `my-input-exchange`
   - Type: `direct`

2. Go to **Queues** → **Add a new queue**
   - Name: `my-input-queue`

3. Go to **Exchanges** → Click your exchange → **Bindings**
   - To queue: `my-input-queue`
   - Routing key: `events`

**Option 2: Via rabbitmqadmin CLI**

```bash
# Create exchange and queue
rabbitmqadmin declare exchange name=my-input-exchange type=direct
rabbitmqadmin declare queue name=my-input-queue

# Bind queue to exchange
rabbitmqadmin declare binding \
  source=my-input-exchange \
  destination=my-input-queue \
  routing_key=events
```

## RabbitMQ Source

The RabbitMQ source consumes messages from a queue and converts them into events for processing.

### SQL Syntax

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',                    -- or 'csv', 'bytes'
    "rabbitmq.host" = 'hostname',       -- Required
    "rabbitmq.queue" = 'queue-name',    -- Required
    "rabbitmq.port" = '5672',           -- Optional (default: 5672)
    "rabbitmq.username" = 'guest',      -- Optional (default: guest)
    "rabbitmq.password" = 'guest',      -- Optional (default: guest)
    "rabbitmq.vhost" = '/',             -- Optional (default: /)
    "rabbitmq.auto.ack" = 'true',       -- Optional (default: true)
    "rabbitmq.prefetch" = '10',         -- Optional (default: 10)
    "rabbitmq.declare.queue" = 'true',  -- Optional (default: true)
    "rabbitmq.consumer.tag" = 'my-tag'  -- Optional (auto-generated)
);
```

### Source Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `rabbitmq.host` | Yes | - | RabbitMQ broker hostname or IP |
| `rabbitmq.queue` | Yes | - | Queue name to consume from |
| `rabbitmq.port` | No | `5672` | RabbitMQ AMQP port |
| `rabbitmq.username` | No | `guest` | Authentication username |
| `rabbitmq.password` | No | `guest` | Authentication password |
| `rabbitmq.vhost` | No | `/` | Virtual host |
| `rabbitmq.auto.ack` | No | `true` | Auto-acknowledge messages. Set to `false` for at-least-once delivery with manual acknowledgment |
| `rabbitmq.prefetch` | No | `10` | Prefetch count for flow control (QoS) |
| `rabbitmq.declare.queue` | No | `true` | Whether to auto-declare the queue if it doesn't exist. Set to `false` when queue must already exist |
| `rabbitmq.consumer.tag` | No | auto | Consumer tag identifier. Auto-generated if not specified |

### Source Example

```sql
-- Consume trade events from RabbitMQ
CREATE STREAM TradeInput (
    symbol STRING,
    price DOUBLE,
    volume INT,
    timestamp LONG
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'trades',
    "rabbitmq.prefetch" = '100'
);

-- Process the incoming events
SELECT symbol, AVG(price) AS avg_price
FROM TradeInput
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO TradeStats;
```

## RabbitMQ Sink

The RabbitMQ sink publishes processed events to an exchange for downstream consumers.

### SQL Syntax

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'json',                       -- or 'csv', 'bytes'
    "rabbitmq.host" = 'hostname',          -- Required
    "rabbitmq.exchange" = 'exchange-name', -- Required
    "rabbitmq.port" = '5672',              -- Optional (default: 5672)
    "rabbitmq.routing.key" = '',           -- Optional (default: "")
    "rabbitmq.username" = 'guest',         -- Optional (default: guest)
    "rabbitmq.password" = 'guest',         -- Optional (default: guest)
    "rabbitmq.vhost" = '/',                -- Optional (default: /)
    "rabbitmq.content.type" = 'application/json', -- Optional
    "rabbitmq.mandatory" = 'false',        -- Optional (default: false)
    "rabbitmq.persistent" = 'true',        -- Optional (default: true)
    "rabbitmq.declare.exchange" = 'false', -- Optional (default: false)
    "rabbitmq.exchange.type" = 'direct'    -- Optional (default: direct)
);
```

### Sink Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `rabbitmq.host` | Yes | - | RabbitMQ broker hostname or IP |
| `rabbitmq.exchange` | Yes | - | Exchange name to publish to |
| `rabbitmq.port` | No | `5672` | RabbitMQ AMQP port |
| `rabbitmq.routing.key` | No | `""` | Routing key for messages |
| `rabbitmq.username` | No | `guest` | Authentication username |
| `rabbitmq.password` | No | `guest` | Authentication password |
| `rabbitmq.vhost` | No | `/` | Virtual host |
| `rabbitmq.content.type` | No | `application/octet-stream` | Message content type header |
| `rabbitmq.mandatory` | No | `false` | Mandatory flag. When `true`, unroutable messages trigger a return instead of being silently dropped |
| `rabbitmq.persistent` | No | `true` | Whether messages should be persistent (survive broker restarts) |
| `rabbitmq.declare.exchange` | No | `false` | Whether to auto-declare the exchange if it doesn't exist |
| `rabbitmq.exchange.type` | No | `direct` | Exchange type when declaring: `direct`, `fanout`, `topic`, or `headers` |

### Sink Example

```sql
-- Output stream to RabbitMQ
CREATE STREAM AlertOutput (
    symbol STRING,
    alert_type STRING,
    message STRING,
    timestamp LONG
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.exchange" = 'alerts',
    "rabbitmq.routing.key" = 'high-priority',
    "rabbitmq.content.type" = 'application/json'
);

-- Generate alerts for price spikes
INSERT INTO AlertOutput
SELECT
    symbol,
    'PRICE_SPIKE' AS alert_type,
    CONCAT('Price spiked to ', CAST(price AS STRING)) AS message,
    timestamp
FROM TradeInput
WHERE price > 1000;
```

## Complete End-to-End Example

This example demonstrates a complete pipeline: RabbitMQ → Process → RabbitMQ.

```sql
-- ============================================
-- INPUT: Consume stock trades from RabbitMQ
-- ============================================
CREATE STREAM TradeInput (
    symbol STRING,
    price DOUBLE,
    volume INT
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.port" = '5672',
    "rabbitmq.queue" = 'trade-input-queue',
    "rabbitmq.username" = 'guest',
    "rabbitmq.password" = 'guest'
);

-- ============================================
-- OUTPUT: Publish processed trades to RabbitMQ
-- ============================================
CREATE STREAM TradeOutput (
    symbol STRING,
    price DOUBLE,
    volume INT,
    price_category STRING
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.port" = '5672',
    "rabbitmq.exchange" = 'trade-output-exchange',
    "rabbitmq.routing.key" = 'processed',
    "rabbitmq.username" = 'guest',
    "rabbitmq.password" = 'guest'
);

-- ============================================
-- PROCESSING: Filter and classify trades
-- ============================================
INSERT INTO TradeOutput
SELECT
    symbol,
    price,
    volume,
    CASE
        WHEN price < 150.0 THEN 'low'
        WHEN price < 300.0 THEN 'medium'
        ELSE 'high'
    END AS price_category
FROM TradeInput
WHERE volume > 1000;
```

### Testing the Pipeline

1. **Start RabbitMQ:**
   ```bash
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

2. **Create infrastructure via Management UI** ([http://localhost:15672](http://localhost:15672)):
   - Exchange: `trade-input-exchange` (direct)
   - Queue: `trade-input-queue`
   - Bind with routing key: `trades`
   - Exchange: `trade-output-exchange` (direct)
   - Queue: `trade-output-queue`
   - Bind with routing key: `processed`

3. **Run EventFlux:**
   ```bash
   cargo run --features rabbitmq --bin run_eventflux your_query.eventflux
   ```

4. **Publish test messages** via Management UI:
   - Go to `trade-input-exchange` → Publish message
   - Routing key: `trades`
   - Payload:
     ```json
     {"symbol":"AAPL","price":185.50,"volume":5000}
     ```

5. **Check output** in `trade-output-queue`:
   ```json
   {"field_0":"AAPL","field_1":185.5,"field_2":5000,"field_3":"medium"}
   ```

## Error Handling

### Connection Failures

The connector validates connectivity during stream initialization:

```rust
// Connectivity is validated when the app starts
// If the queue/exchange doesn't exist, initialization fails with a clear error
```

Error messages include:
- `Failed to connect to RabbitMQ at host:port`
- `Queue 'queue-name' does not exist`
- `Exchange 'exchange-name' does not exist`

### Reconnection

The source connector automatically attempts to reconnect on connection loss. The sink will report errors for failed publishes.

### Message Acknowledgment

By default (`rabbitmq.auto.ack = true`), messages are acknowledged immediately upon receipt.

For at-least-once processing, set `rabbitmq.auto.ack = false`:
- **Success**: Message is ACKed after successful processing
- **Error with retry**: Message is NACKed with requeue=true to retry
- **Error with drop**: Message is ACKed (removed from queue)
- **Error with DLQ**: Message is ACKed after sending to dead-letter queue
- **Error with fail**: Message is NACKed with requeue=true, then source stops

This integrates with error handling strategies configured via `error.*` properties.

## Performance Tuning

### Prefetch Count

The `rabbitmq.prefetch` option controls how many messages are buffered locally:

```sql
-- Higher prefetch for throughput
"rabbitmq.prefetch" = '1000'

-- Lower prefetch for latency-sensitive apps
"rabbitmq.prefetch" = '1'
```

### Batching (Sink)

The sink publishes messages individually. For high-throughput scenarios, consider:
- Using persistent connections (handled automatically)
- Grouping related events via windowing before publishing

## Troubleshooting

### Connection Refused

```
Error: Failed to connect to RabbitMQ at localhost:5672
```

**Solutions:**
- Verify RabbitMQ is running: `docker ps | grep rabbitmq`
- Check port accessibility: `nc -zv localhost 5672`
- Verify firewall settings

### Queue Not Found

```
Error: Queue 'my-queue' does not exist
```

**Solutions:**
- Create the queue before starting EventFlux
- Check queue name spelling in Management UI
- Verify you're connecting to the correct vhost

### Authentication Failed

```
Error: Access refused - Login was refused
```

**Solutions:**
- Verify username/password
- Note: `guest` user only works for localhost by default
- Create a new user for remote connections

### No Messages Received

**Checklist:**
- Is the queue bound to the exchange?
- Is the routing key correct?
- Are messages being published to the exchange?
- Check stream field types match JSON field types

## Rust API Usage

For programmatic access, you can use the RabbitMQ connector directly:

```rust
use eventflux::core::stream::input::source::rabbitmq_source::{
    RabbitMQSource, RabbitMQSourceFactory
};
use eventflux::core::stream::output::sink::rabbitmq_sink::{
    RabbitMQSink, RabbitMQSinkFactory
};
use eventflux::core::extension::{SourceFactory, SinkFactory};
use std::collections::HashMap;

// Create source
let mut config = HashMap::new();
config.insert("rabbitmq.host".to_string(), "localhost".to_string());
config.insert("rabbitmq.queue".to_string(), "my-queue".to_string());

let factory = RabbitMQSourceFactory;
let source = factory.create_initialized(&config)?;

// Validate connectivity
source.validate_connectivity()?;

// Create sink
let mut sink_config = HashMap::new();
sink_config.insert("rabbitmq.host".to_string(), "localhost".to_string());
sink_config.insert("rabbitmq.exchange".to_string(), "my-exchange".to_string());

let sink_factory = RabbitMQSinkFactory;
let sink = sink_factory.create_initialized(&sink_config)?;
```

## See Also

- [Connectors Overview](/docs/connectors/overview) - Architecture and concepts
- [Mappers Reference](/docs/connectors/mappers) - JSON, CSV, and bytes format handling
- [SQL Reference](/docs/sql-reference/queries) - Query language documentation
