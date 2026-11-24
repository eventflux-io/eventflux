# Sink Architecture

## Overview

Sinks publish formatted event data to external systems. The architecture separates formatting concerns from transport concerns through a two-layer design.

## Architecture Layers

### Layer 1: SinkMapper - Formatting

SinkMapper converts Events to bytes in specific formats (JSON, CSV, XML, binary).

Location: `src/core/stream/output/mapper.rs`

```rust
pub trait SinkMapper: Debug + Send + Sync {
    fn map(&self, events: &[Event]) -> Vec<u8>;
    fn clone_box(&self) -> Box<dyn SinkMapper>;
}
```

### Layer 2: Sink - Transport

Sink receives formatted bytes and publishes to external systems (HTTP, Kafka, files).

Location: `src/core/stream/output/sink/sink_trait.rs`

```rust
pub trait Sink: Debug + Send + Sync {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError>;
    fn start(&self) {}
    fn stop(&self) {}
    fn clone_box(&self) -> Box<dyn Sink>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError> { Ok(()) }
}
```

## Data Flow

```
StreamJunction → Events → SinkMapper::map() → Vec<u8> → Sink::publish() → External System
```

Step-by-step:

1. StreamJunction produces Events from query processing
2. SinkCallbackAdapter receives Events via StreamCallback trait
3. SinkMapper transforms Events to bytes (JSON/CSV/XML/binary)
4. Sink publishes bytes to external system
5. External system receives formatted data

## SinkCallbackAdapter

Bridges StreamCallback interface (Events) with Sink interface (bytes).

Location: `src/core/stream/output/sink/mod.rs`

```rust
pub struct SinkCallbackAdapter {
    pub sink: Arc<Mutex<Box<dyn Sink>>>,
    pub mapper: Arc<Mutex<Box<dyn SinkMapper>>>,
}

impl StreamCallback for SinkCallbackAdapter {
    fn receive_events(&self, events: &[Event]) {
        let payload = self.mapper.lock().unwrap().map(events);
        if let Err(e) = self.sink.lock().unwrap().publish(&payload) {
            log::error!("Sink publish failed: {}", e);
        }
    }
}
```

## PassthroughMapper

Default mapper when no format is specified. Uses bincode for efficient binary serialization.

Location: `src/core/stream/output/mapper.rs`

```rust
impl SinkMapper for PassthroughMapper {
    fn map(&self, events: &[Event]) -> Vec<u8> {
        bincode::serialize(events).unwrap_or_else(|e| {
            log::error!("Failed to serialize events: {}", e);
            vec![]
        })
    }
}
```

Use case: Debug sinks (LogSink) that need to deserialize Events back.

## Lifecycle Management

SinkStreamHandler manages sink lifecycle.

Location: `src/core/stream/handler/mod.rs:144`

```rust
pub struct SinkStreamHandler {
    sink: Arc<Mutex<Box<dyn Sink>>>,
    mapper: Option<Arc<Mutex<Box<dyn SinkMapper>>>>,
    stream_id: String,
    is_running: AtomicBool,
}
```

Operations:
- `start()` - Calls `Sink::start()`, idempotent
- `stop()` - Calls `Sink::stop()`, flushes pending events
- `is_running()` - Check current state

## Implementing a Sink Extension

### Step 1: Implement Sink Trait

Example: HTTP sink publishing JSON to REST API

```rust
use crate::core::stream::output::sink::Sink;
use crate::core::exception::EventFluxError;

#[derive(Debug)]
pub struct HttpSink {
    url: String,
    method: String,
    client: reqwest::blocking::Client,
}

impl Sink for HttpSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let response = self.client
            .request(self.method.parse().unwrap(), &self.url)
            .body(payload.to_vec())
            .header("Content-Type", "application/json")
            .send()
            .map_err(|e| EventFluxError::connection_unavailable(
                format!("HTTP request failed: {}", e)
            ))?;

        if !response.status().is_success() {
            return Err(EventFluxError::app_runtime(
                format!("HTTP {} returned status {}", self.method, response.status())
            ));
        }

        Ok(())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let response = self.client
            .head(&self.url)
            .timeout(Duration::from_secs(10))
            .send()
            .map_err(|e| EventFluxError::configuration(
                format!("Cannot reach {}: {}", self.url, e)
            ))?;

        if !response.status().is_success() && !response.status().is_client_error() {
            return Err(EventFluxError::configuration(
                format!("HTTP endpoint not reachable: {}", response.status())
            ));
        }

        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(HttpSink {
            url: self.url.clone(),
            method: self.method.clone(),
            client: reqwest::blocking::Client::new(),
        })
    }
}
```

### Step 2: Implement SinkFactory

Location: `src/core/extension/mod.rs:94`

```rust
use crate::core::extension::SinkFactory;

#[derive(Debug, Clone)]
pub struct HttpSinkFactory;

impl SinkFactory for HttpSinkFactory {
    fn name(&self) -> &'static str {
        "http"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "xml", "text"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["http.url"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["http.method", "http.headers", "http.timeout"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let url = config.get("http.url")
            .ok_or_else(|| EventFluxError::missing_parameter("http.url"))?;

        let method = config.get("http.method")
            .cloned()
            .unwrap_or_else(|| "POST".to_string());

        Ok(Box::new(HttpSink {
            url: url.clone(),
            method: method.to_uppercase(),
            client: reqwest::blocking::Client::new(),
        }))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}
```

### Step 3: Register Factory

```rust
let mut context = EventFluxContext::new();
context.register_sink_factory(Box::new(HttpSinkFactory));
```

### Step 4: Use in SQL

```sql
CREATE SINK STREAM HttpOutputStream (id string, value double)
WITH (
    type = 'sink',
    extension = 'http',
    format = 'json',
    http.url = 'https://api.example.com/events',
    http.method = 'POST'
);
```

## Implementing a SinkMapper Extension

### Step 1: Implement SinkMapper Trait

Example: CSV mapper

```rust
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::event::event::Event;

#[derive(Debug)]
pub struct CsvSinkMapper {
    delimiter: String,
    include_header: bool,
}

impl SinkMapper for CsvSinkMapper {
    fn map(&self, events: &[Event]) -> Vec<u8> {
        let mut output = Vec::new();

        // Write header if needed
        if self.include_header && !events.is_empty() {
            let header = events[0].data
                .iter()
                .enumerate()
                .map(|(i, _)| format!("field_{}", i))
                .collect::<Vec<_>>()
                .join(&self.delimiter);
            output.extend_from_slice(header.as_bytes());
            output.push(b'\n');
        }

        // Write data rows
        for event in events {
            let row = event.data
                .iter()
                .map(|attr| format!("{}", attr))
                .collect::<Vec<_>>()
                .join(&self.delimiter);
            output.extend_from_slice(row.as_bytes());
            output.push(b'\n');
        }

        output
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(CsvSinkMapper {
            delimiter: self.delimiter.clone(),
            include_header: self.include_header,
        })
    }
}
```

### Step 2: Implement SinkMapperFactory

Location: `src/core/extension/mod.rs:188`

```rust
#[derive(Debug, Clone)]
pub struct CsvSinkMapperFactory;

impl SinkMapperFactory for CsvSinkMapperFactory {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["csv.delimiter", "csv.header"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        let delimiter = config.get("csv.delimiter")
            .cloned()
            .unwrap_or_else(|| ",".to_string());

        let include_header = config.get("csv.header")
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(true);

        Ok(Box::new(CsvSinkMapper {
            delimiter,
            include_header,
        }))
    }

    fn clone_box(&self) -> Box<dyn SinkMapperFactory> {
        Box::new(self.clone())
    }
}
```

### Step 3: Register Mapper Factory

```rust
context.register_sink_mapper_factory(Box::new(CsvSinkMapperFactory));
```

## Built-in Implementations

### LogSink

Debug sink that logs events to console and collects them in memory.

Location: `src/core/stream/output/sink/log_sink.rs`

Features:
- Deserializes bytes back to Events using PassthroughMapper
- Logs to console with configurable prefix
- Stores events in Vec for testing
- Thread-safe via Mutex

Usage:
```rust
let sink = LogSink::new();
let collected = sink.events.clone();
// Events are collected in `collected` as they arrive
```

## Configuration Sources

Sinks can be configured from two sources:

### YAML/TOML Configuration

```yaml
streams:
  HttpOutputStream:
    sink:
      type: http
      format: json
      http.url: https://api.example.com/events
      http.method: POST
```

Connection keys sit next to `type` and `format`. Merged with any SQL WITH clause by
`ElementConfigResolver` (`src/core/config/element_config_resolver.rs`), then attached
by `EventFluxAppRuntime::auto_attach_streams()`.

### SQL WITH Clause

```sql
CREATE SINK STREAM HttpOutputStream (id string, value double)
WITH (
    type = 'sink',
    extension = 'http',
    format = 'json',
    http.url = 'https://api.example.com/events'
);
```

Processed by: `EventFluxAppRuntime::attach_single_stream_from_sql_sink()`
Location: `src/core/eventflux_app_runtime.rs:1051`

SQL configuration has higher priority than YAML when both exist.

## Error Handling

Sink implementations should handle errors appropriately:

- Connection failures: Return `EventFluxError::ConnectionUnavailable`
- Configuration errors: Return `EventFluxError::InvalidParameter`
- Runtime errors: Return `EventFluxError::AppRuntime`

The SinkCallbackAdapter logs errors but continues processing to prevent one failed publish from stopping the entire pipeline.

## Thread Safety

All Sink and SinkMapper implementations must be Send + Sync. The runtime wraps sinks in Arc<Mutex<>> for thread-safe access.

## Testing

Example integration test showing complete flow:

Location: `tests/source_sink.rs`

```rust
// Create sink with adapter
let sink = LogSink::new();
let collected = sink.events.clone();
let sink_adapter = SinkCallbackAdapter {
    sink: Arc::new(Mutex::new(Box::new(sink))),
    mapper: Arc::new(Mutex::new(Box::new(PassthroughMapper::new()))),
};

// Subscribe to junction
let callback = Arc::new(Mutex::new(Box::new(sink_adapter) as Box<dyn StreamCallback>));
let cb_processor = CallbackProcessor::new(callback, app_ctx, query_ctx);
junction.lock().unwrap().subscribe(Arc::new(Mutex::new(cb_processor)));

// Events flow through pipeline
// Verify events arrived in collected Vec
```

## Reference Implementations

Working examples with full implementation:

1. LogSink: `src/core/stream/output/sink/log_sink.rs`
2. HttpSink placeholder: `src/core/extension/example_factories.rs:216`
3. PassthroughMapper: `src/core/stream/output/mapper.rs:33`

## Factory Registration

Factories are registered in EventFluxContext and accessed via stream initializer.

Location: `src/core/stream/stream_initializer.rs`

The initializer:
1. Looks up factory by extension name
2. Validates required parameters present
3. Calls `create_initialized()` with configuration
4. Returns initialized Sink ready for use
