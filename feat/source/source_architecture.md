# Source Architecture

## Overview

Sources read data from external systems and deliver it to the event processing pipeline. The architecture separates reading concerns from parsing concerns through a callback-based design.

## Architecture Layers

### Layer 1: Source - Transport

Source reads raw bytes from external systems (Kafka, HTTP, files) and delivers via callback.

Location: `src/core/stream/input/source/mod.rs`

```rust
pub trait Source: Debug + Send + Sync {
    fn start(&mut self, callback: Arc<dyn SourceCallback>);
    fn stop(&mut self);
    fn clone_box(&self) -> Box<dyn Source>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError> { Ok(()) }
}
```

### Layer 2: SourceMapper - Parsing

SourceMapper converts bytes from specific formats (JSON, CSV, XML, binary) into Events.

Location: `src/core/stream/input/mapper.rs`

```rust
pub trait SourceMapper: Debug + Send + Sync {
    fn map(&self, input: &[u8]) -> Vec<Event>;
    fn clone_box(&self) -> Box<dyn SourceMapper>;
}
```

## Data Flow

```
External System → Source::read() → Vec<u8> → SourceCallback::on_data() → SourceMapper::map() → Events → InputHandler
```

Step-by-step:

1. Source reads bytes from external system (Kafka messages, HTTP responses, file contents)
2. Source calls callback with raw bytes
3. SourceCallbackAdapter receives bytes via SourceCallback trait
4. SourceMapper parses bytes into Events
5. InputHandler receives Events and sends to StreamJunction
6. Events flow through query processing pipeline

## SourceCallback

Callback interface for delivering data from sources.

Location: `src/core/stream/input/source/mod.rs:15`

```rust
pub trait SourceCallback: Debug + Send + Sync {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError>;
}
```

Sources call `on_data()` whenever new data is available from the external system.

## SourceCallbackAdapter

Bridges SourceCallback interface (bytes) with SourceMapper and InputHandler.

Location: `src/core/stream/input/source/mod.rs:97`

```rust
pub struct SourceCallbackAdapter {
    mapper: Arc<Mutex<Box<dyn SourceMapper>>>,
    handler: Arc<Mutex<InputHandler>>,
}

impl SourceCallback for SourceCallbackAdapter {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
        let events = self.mapper.lock().unwrap().map(data);

        for event in events {
            self.handler.lock().unwrap()
                .send_single_event(event)
                .map_err(|e| EventFluxError::app_runtime(
                    format!("Failed to send event: {}", e)
                ))?;
        }

        Ok(())
    }
}
```

## PassthroughMapper

Default mapper when no format is specified. Uses bincode for efficient binary deserialization.

Location: `src/core/stream/input/mapper.rs`

```rust
impl SourceMapper for PassthroughMapper {
    fn map(&self, input: &[u8]) -> Vec<Event> {
        bincode::deserialize(input).unwrap_or_else(|e| {
            log::error!("Failed to deserialize events: {}", e);
            vec![]
        })
    }
}
```

Use case: Debug sources (TimerSource) that produce Events internally and serialize them.

Helper method for sources:
```rust
let bytes = PassthroughMapper::serialize(&[event])?;
callback.on_data(&bytes)?;
```

## Lifecycle Management

SourceStreamHandler manages source lifecycle.

Location: `src/core/stream/handler/mod.rs:34`

```rust
pub struct SourceStreamHandler {
    source: Arc<Mutex<Box<dyn Source>>>,
    mapper: Option<Arc<Mutex<Box<dyn SourceMapper>>>>,
    input_handler: Arc<Mutex<InputHandler>>,
    stream_id: String,
    is_running: AtomicBool,
}
```

Operations:
- `start()` - Creates SourceCallbackAdapter and calls `Source::start()`, idempotent
- `stop()` - Calls `Source::stop()`, graceful shutdown
- `is_running()` - Check current state

Handler creates the callback automatically:
```rust
let mapper = self.mapper.clone().unwrap_or_else(|| {
    Arc::new(Mutex::new(Box::new(PassthroughMapper::new())))
});

let callback = Arc::new(SourceCallbackAdapter::new(
    mapper,
    Arc::clone(&self.input_handler),
));

self.source.lock().unwrap().start(callback);
```

## Implementing a Source Extension

### Step 1: Implement Source Trait

Example: Kafka source reading messages from topic

```rust
use crate::core::stream::input::source::{Source, SourceCallback};
use crate::core::exception::EventFluxError;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

#[derive(Debug)]
pub struct KafkaSource {
    consumer: StreamConsumer,
    topic: String,
    running: Arc<AtomicBool>,
}

impl Source for KafkaSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        self.running.store(true, Ordering::SeqCst);

        let consumer = self.consumer.clone();
        let topic = self.topic.clone();
        let running = self.running.clone();

        thread::spawn(move || {
            consumer.subscribe(&[&topic]).unwrap();

            while running.load(Ordering::SeqCst) {
                match consumer.recv().await {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            if let Err(e) = callback.on_data(payload) {
                                log::error!("Callback error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Kafka error: {}", e);
                    }
                }
            }
        });
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let metadata = self.consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| EventFluxError::configuration(
                format!("Cannot reach Kafka brokers: {}", e)
            ))?;

        if !metadata.topics().iter().any(|t| t.name() == self.topic) {
            return Err(EventFluxError::configuration(
                format!("Topic '{}' does not exist", self.topic)
            ));
        }

        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(KafkaSource {
            consumer: self.consumer.clone(),
            topic: self.topic.clone(),
            running: Arc::new(AtomicBool::new(false)),
        })
    }
}
```

### Step 2: Implement SourceFactory

Location: `src/core/extension/mod.rs:48`

```rust
use crate::core::extension::SourceFactory;

#[derive(Debug, Clone)]
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "avro", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["kafka.bootstrap.servers", "kafka.topic"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "kafka.consumer.group",
            "kafka.timeout",
            "kafka.security.protocol",
            "kafka.sasl.mechanism"
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        let brokers = config.get("kafka.bootstrap.servers")
            .ok_or_else(|| EventFluxError::missing_parameter("kafka.bootstrap.servers"))?;

        let topic = config.get("kafka.topic")
            .ok_or_else(|| EventFluxError::missing_parameter("kafka.topic"))?;

        let consumer_group = config.get("kafka.consumer.group")
            .cloned()
            .unwrap_or_else(|| format!("eventflux-{}", topic));

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", consumer_group)
            .set("enable.auto.commit", "true")
            .create()
            .map_err(|e| EventFluxError::configuration(
                format!("Failed to create Kafka consumer: {}", e)
            ))?;

        Ok(Box::new(KafkaSource {
            consumer,
            topic: topic.clone(),
            running: Arc::new(AtomicBool::new(false)),
        }))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}
```

### Step 3: Register Factory

```rust
let mut context = EventFluxContext::new();
context.register_source_factory(Box::new(KafkaSourceFactory));
```

### Step 4: Use in SQL

```sql
CREATE SOURCE STREAM KafkaInputStream (id string, value double)
WITH (
    type = 'source',
    extension = 'kafka',
    format = 'json',
    kafka.bootstrap.servers = 'localhost:9092',
    kafka.topic = 'input-events',
    kafka.consumer.group = 'my-app'
);
```

## Implementing a SourceMapper Extension

### Step 1: Implement SourceMapper Trait

Example: JSON mapper parsing JSON bytes to Events

```rust
use crate::core::stream::input::mapper::SourceMapper;
use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;

#[derive(Debug)]
pub struct JsonSourceMapper {
    fail_on_missing: bool,
}

impl SourceMapper for JsonSourceMapper {
    fn map(&self, input: &[u8]) -> Vec<Event> {
        let json_str = match std::str::from_utf8(input) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Invalid UTF-8: {}", e);
                return vec![];
            }
        };

        let json_value: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Invalid JSON: {}", e);
                return vec![];
            }
        };

        // Handle array of events or single event
        let events = match json_value {
            serde_json::Value::Array(arr) => arr,
            single => vec![single],
        };

        events.into_iter()
            .filter_map(|ev| self.parse_event(&ev))
            .collect()
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(JsonSourceMapper {
            fail_on_missing: self.fail_on_missing,
        })
    }
}

impl JsonSourceMapper {
    fn parse_event(&self, json: &serde_json::Value) -> Option<Event> {
        let obj = json.as_object()?;

        let mut data = Vec::new();
        for (key, value) in obj {
            let attr = match value {
                serde_json::Value::String(s) => AttributeValue::String(s.clone()),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        AttributeValue::Long(i)
                    } else if let Some(f) = n.as_f64() {
                        AttributeValue::Double(f)
                    } else {
                        continue;
                    }
                }
                serde_json::Value::Bool(b) => AttributeValue::Bool(*b),
                _ => continue,
            };
            data.push(attr);
        }

        Some(Event::new_with_data(0, data))
    }
}
```

### Step 2: Implement SourceMapperFactory

Location: `src/core/extension/mod.rs:151`

```rust
#[derive(Debug, Clone)]
pub struct JsonSourceMapperFactory;

impl SourceMapperFactory for JsonSourceMapperFactory {
    fn name(&self) -> &'static str {
        "json"
    }

    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["json.fail-on-missing-attribute", "json.enclosing-element"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        let fail_on_missing = config.get("json.fail-on-missing-attribute")
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(false);

        Ok(Box::new(JsonSourceMapper { fail_on_missing }))
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}
```

### Step 3: Register Mapper Factory

```rust
context.register_source_mapper_factory(Box::new(JsonSourceMapperFactory));
```

## Built-in Implementations

### TimerSource

Debug source that generates tick events at regular intervals.

Location: `src/core/stream/input/source/timer_source.rs`

Features:
- Generates Events internally with timestamp
- Serializes to bytes using PassthroughMapper::serialize()
- Spawns background thread for tick generation
- Supports error handling integration (M5)

Usage:
```rust
let source = TimerSource::new(100); // 100ms interval
let callback = Arc::new(SourceCallbackAdapter::new(mapper, input_handler));
source.start(callback);
```

Implementation pattern for internal Event generation:
```rust
let event = Event::new_with_data(0, vec![AttributeValue::String("tick".to_string())]);

match PassthroughMapper::serialize(&[event.clone()]) {
    Ok(bytes) => {
        if let Err(e) = callback.on_data(&bytes) {
            log::error!("Callback error: {}", e);
        }
    }
    Err(e) => {
        log::error!("Serialization error: {}", e);
    }
}
```

## Configuration Sources

Sources can be configured from two sources:

### YAML/TOML Configuration

```yaml
streams:
  KafkaInputStream:
    source:
      type: kafka
      format: json
      kafka.bootstrap.servers: localhost:9092
      kafka.topic: input-events
      kafka.consumer.group: my-app
```

Connection keys sit next to `type` and `format`. Merged with any SQL WITH clause by
`ElementConfigResolver` (`src/core/config/element_config_resolver.rs`), then attached
by `EventFluxAppRuntime::auto_attach_streams()`.

### SQL WITH Clause

```sql
CREATE SOURCE STREAM KafkaInputStream (id string, value double)
WITH (
    type = 'source',
    extension = 'kafka',
    format = 'json',
    kafka.bootstrap.servers = 'localhost:9092',
    kafka.topic = 'input-events'
);
```

Processed by: `EventFluxAppRuntime::attach_single_stream_from_sql_source()`
Location: `src/core/eventflux_app_runtime.rs:989`

SQL configuration has higher priority than YAML when both exist.

## Error Handling

Source implementations should handle errors appropriately:

- Connection failures: Log error, continue trying (sources are continuous)
- Parse errors: Log error, skip malformed data
- Callback errors: Log error, continue processing

The SourceCallbackAdapter propagates errors from InputHandler but sources should handle callback errors gracefully to avoid stopping data ingestion.

## Thread Safety

All Source and SourceMapper implementations must be Send + Sync. Sources typically spawn background threads for data reading. The runtime wraps sources in Arc<Mutex<>> for thread-safe access.

## Startup Validation

Sources implement `validate_connectivity()` to verify external systems before starting.

Called during application initialization:
```rust
let source = factory.create_initialized(&config)?;
source.validate_connectivity()?; // Fail-fast if not ready
```

This prevents application from starting with unreachable data sources.

## Testing

Example integration test showing complete flow:

Location: `tests/source_sink.rs`

```rust
// Create source with callback adapter
let source_callback = Arc::new(SourceCallbackAdapter::new(
    Arc::new(Mutex::new(Box::new(PassthroughMapper::new()))),
    Arc::clone(&input_handler),
));

let mut source = TimerSource::new(10);
source.start(source_callback);

// Wait for events
std::thread::sleep(Duration::from_millis(50));
source.stop();

// Events have flowed through pipeline to sink
```

## Reference Implementations

Working examples with full implementation:

1. TimerSource: `src/core/stream/input/source/timer_source.rs`
2. KafkaSource placeholder: `src/core/extension/example_factories.rs:86`
3. PassthroughMapper: `src/core/stream/input/mapper.rs:33`

## Factory Registration

Factories are registered in EventFluxContext and accessed via stream initializer.

Location: `src/core/stream/stream_initializer.rs`

The initializer:
1. Looks up factory by extension name
2. Validates required parameters present
3. Calls `create_initialized()` with configuration
4. Validates connectivity via `validate_connectivity()`
5. Returns initialized Source ready for use

## Integration with InputHandler

InputHandler receives Events from SourceCallbackAdapter and processes them:

Location: `src/core/stream/input/input_handler.rs`

```rust
pub fn send_single_event(&mut self, event: Event) -> Result<(), String> {
    self.processor.lock()
        .unwrap()
        .send_single_event(event, self.stream_index)
}
```

The processor is typically a StreamJunctionInputProcessor that publishes to StreamJunction, starting the query processing pipeline.

## Active vs Passive Sources

Sources are active components that push data to the pipeline. They:
- Run in background threads
- Call callback when data available
- Continue until stop() is called

This contrasts with Tables (passive components) which are queried on demand.
