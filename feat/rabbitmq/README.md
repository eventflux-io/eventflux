# RabbitMQ Integration for EventFlux

This document describes the RabbitMQ source and sink implementations for EventFlux.

## Overview

EventFlux provides native integration with RabbitMQ through:
- **RabbitMQ Source**: Consumes messages from RabbitMQ queues
- **RabbitMQ Sink**: Publishes events to RabbitMQ exchanges

Both implementations use the `lapin` crate for AMQP 0-9-1 protocol support.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  RabbitMQ Queue │────▶│    EventFlux    │────▶│ RabbitMQ Exchange│
│   (Source)      │     │    Pipeline     │     │    (Sink)       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Threading Model

Both source and sink use synchronous `Source`/`Sink` traits with internal tokio runtime for async `lapin` operations. This follows the existing `TimerSource` pattern:

```
thread::spawn → tokio::runtime::Runtime → lapin async operations
```

This approach:
- Maintains compatibility with existing EventFlux architecture
- Provides graceful shutdown via `Arc<AtomicBool>`
- Isolates async operations from the sync trait interface

## RabbitMQ Source

Consumes messages from a RabbitMQ queue and delivers them to the EventFlux pipeline.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `rabbitmq.host` | Yes | - | Broker hostname |
| `rabbitmq.queue` | Yes | - | Queue name to consume from |
| `rabbitmq.port` | No | 5672 | Broker port |
| `rabbitmq.vhost` | No | "/" | Virtual host |
| `rabbitmq.username` | No | "guest" | Authentication username |
| `rabbitmq.password` | No | "guest" | Authentication password |
| `rabbitmq.prefetch` | No | 10 | Consumer prefetch count |
| `rabbitmq.consumer.tag` | No | auto | Consumer tag |
| `rabbitmq.auto.ack` | No | true | Auto-acknowledge messages |
| `error.strategy` | No | - | Error handling strategy (drop, retry, dlq, fail) |
| `error.retry.max-attempts` | No | 3 | Max retry attempts |
| `error.retry.initial-delay-ms` | No | 100 | Initial retry delay in ms |
| `error.retry.max-delay-ms` | No | 10000 | Max retry delay in ms |
| `error.retry.backoff-multiplier` | No | 2.0 | Backoff multiplier |
| `error.dlq.stream` | No | - | DLQ stream name (required when strategy=dlq) |

### Usage Example

```rust
use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSource;
use std::collections::HashMap;

let mut config = HashMap::new();
config.insert("rabbitmq.host".to_string(), "localhost".to_string());
config.insert("rabbitmq.queue".to_string(), "events".to_string());
config.insert("rabbitmq.prefetch".to_string(), "100".to_string());

let source = RabbitMQSource::from_properties(&config, None, "EventStream")?;
```

### Factory

```rust
use eventflux::core::stream::input::source::rabbitmq_source::RabbitMQSourceFactory;
use eventflux::core::extension::SourceFactory;

let factory = RabbitMQSourceFactory;
let source = factory.create_initialized(&config)?;
```

## RabbitMQ Sink

Publishes formatted event data to a RabbitMQ exchange.

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `rabbitmq.host` | Yes | - | Broker hostname |
| `rabbitmq.exchange` | Yes | - | Exchange name to publish to |
| `rabbitmq.port` | No | 5672 | Broker port |
| `rabbitmq.vhost` | No | "/" | Virtual host |
| `rabbitmq.username` | No | "guest" | Authentication username |
| `rabbitmq.password` | No | "guest" | Authentication password |
| `rabbitmq.routing.key` | No | "" | Routing key for messages |
| `rabbitmq.persistent` | No | true | Message persistence (delivery mode 2) |
| `rabbitmq.content.type` | No | - | Content-Type header |
| `rabbitmq.declare.exchange` | No | false | Declare exchange if missing |
| `rabbitmq.exchange.type` | No | "direct" | Exchange type when declaring |

### Usage Example

```rust
use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSink;
use eventflux::core::stream::output::sink::Sink;
use std::collections::HashMap;

let mut config = HashMap::new();
config.insert("rabbitmq.host".to_string(), "localhost".to_string());
config.insert("rabbitmq.exchange".to_string(), "events".to_string());
config.insert("rabbitmq.routing.key".to_string(), "processed".to_string());
config.insert("rabbitmq.content.type".to_string(), "application/json".to_string());

let sink = RabbitMQSink::from_properties(&config)?;

// Start connection
sink.start();

// Publish message
sink.publish(b"{\"event\": \"test\"}")?;

// Stop connection
sink.stop();
```

### Factory

```rust
use eventflux::core::stream::output::sink::rabbitmq_sink::RabbitMQSinkFactory;
use eventflux::core::extension::SinkFactory;

let factory = RabbitMQSinkFactory;
let sink = factory.create_initialized(&config)?;
```

## Format Support

Both source and sink support the following formats through the mapper infrastructure:

| Format | Mapper | Description |
|--------|--------|-------------|
| `json` | `JsonSourceMapper` / `JsonSinkMapper` | JSON object mapping |
| `csv` | `CsvSourceMapper` / `CsvSinkMapper` | CSV text mapping |

### JSON Format Example

When using JSON format, events are serialized/deserialized according to the stream schema:

```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "volume": 1000
}
```

## Error Handling

Both implementations integrate with M5 error handling:

```rust
config.insert("error.strategy".to_string(), "retry".to_string());
config.insert("error.retry.max-attempts".to_string(), "3".to_string());
```

Supported strategies:
- `drop`: Drop failed messages
- `retry`: Retry with backoff
- `dlq`: Send to dead-letter queue
- `fail`: Stop processing on error

## Connection Validation

Both source and sink implement `validate_connectivity()` for fail-fast behavior:

```rust
// Source validates broker and queue exist
let source = RabbitMQSource::from_properties(&config, None, "Stream")?;
source.validate_connectivity()?;

// Sink validates broker and exchange exist
let sink = RabbitMQSink::from_properties(&config)?;
sink.validate_connectivity()?;
```

## Testing

### Unit Tests (No broker required)

```bash
cargo test rabbitmq --lib
```

### Integration Tests (Requires broker)

Start RabbitMQ:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Run tests:
```bash
cargo test --test rabbitmq_integration -- --ignored
```

## Future Improvements

The following are documented for future implementation:

1. **Async Traits**: Create `AsyncSource`/`AsyncSink` traits for native async support
2. **Connection Pooling**: Use `deadpool-lapin` for connection management
3. **Publisher Confirms**: Add configurable publish confirmation mode
4. **Consumer Acknowledgments**: Add manual ack modes with requeue options
5. **TLS Support**: Add SSL/TLS configuration options
6. **Cluster Support**: Handle multiple broker nodes for high availability

## Files

| File | Description |
|------|-------------|
| `src/core/stream/input/source/rabbitmq_source.rs` | RabbitMQ source and factory |
| `src/core/stream/output/sink/rabbitmq_sink.rs` | RabbitMQ sink and factory |
| `tests/rabbitmq_integration.rs` | Integration tests |
| `examples/rabbitmq.eventflux` | Example query file |
