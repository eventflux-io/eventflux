# WebSocket Source and Sink for EventFlux

This document describes the implementation plan for WebSocket source and sink extensions for EventFlux.

## Overview

EventFlux will provide native WebSocket integration through:
- **WebSocket Source**: Connects to WebSocket endpoints and consumes messages
- **WebSocket Sink**: Publishes events to WebSocket endpoints

Both implementations will use `tokio-tungstenite` for async WebSocket support.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  WebSocket URL  │────▶│    EventFlux    │────▶│  WebSocket URL  │
│    (Source)     │     │    Pipeline     │     │     (Sink)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Threading Model

Following the established RabbitMQ pattern:
- Sync `Source`/`Sink` traits with internal tokio runtime for async operations
- Graceful shutdown via `Arc<AtomicBool>` running flag
- Connection management with auto-reconnection

```
thread::spawn → tokio::runtime::Runtime → tungstenite async operations
```

---

## WebSocket Source

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `websocket.url` | Yes | - | WebSocket URL (ws:// or wss://) |
| `websocket.reconnect` | No | true | Auto-reconnect on disconnect |
| `websocket.reconnect.delay.ms` | No | 1000 | Initial reconnect delay |
| `websocket.reconnect.max.delay.ms` | No | 30000 | Max reconnect delay (exponential backoff) |
| `websocket.reconnect.max.attempts` | No | -1 | Max reconnect attempts (-1 = unlimited) |
| `websocket.headers.*` | No | - | Custom headers (e.g., `websocket.headers.Authorization`) |
| `websocket.subprotocol` | No | - | WebSocket subprotocol for negotiation |
| `error.strategy` | No | - | Error handling strategy (drop, retry, dlq, fail) |
| `error.retry.*` | No | - | Retry configuration options |
| `error.dlq.stream` | No | - | DLQ stream name |

### Implementation Structure

```rust
// src/core/stream/input/source/websocket_source.rs

/// Configuration for WebSocket source
#[derive(Debug, Clone)]
pub struct WebSocketSourceConfig {
    /// WebSocket URL (ws:// or wss://)
    pub url: String,
    /// Enable auto-reconnect (default: true)
    pub reconnect: bool,
    /// Initial reconnect delay in ms (default: 1000)
    pub reconnect_delay_ms: u64,
    /// Max reconnect delay in ms (default: 30000)
    pub reconnect_max_delay_ms: u64,
    /// Max reconnect attempts (-1 = unlimited, default: -1)
    pub reconnect_max_attempts: i32,
    /// Custom headers for connection
    pub headers: HashMap<String, String>,
    /// WebSocket subprotocol
    pub subprotocol: Option<String>,
}

/// WebSocket source that connects to an endpoint and receives messages
#[derive(Debug)]
pub struct WebSocketSource {
    config: WebSocketSourceConfig,
    running: Arc<AtomicBool>,
    error_ctx: Option<SourceErrorContext>,
}

impl Source for WebSocketSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>);
    fn stop(&mut self);
    fn clone_box(&self) -> Box<dyn Source>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError>;
    fn set_error_dlq_junction(&mut self, stream_name: &str, junction: Arc<Mutex<InputHandler>>) -> Result<(), EventFluxError>;
}

/// Factory for creating WebSocket source instances
#[derive(Debug, Clone)]
pub struct WebSocketSourceFactory;

impl SourceFactory for WebSocketSourceFactory {
    fn name(&self) -> &'static str { "websocket" }
    fn supported_formats(&self) -> &[&str] { &["json", "text", "bytes"] }
    fn required_parameters(&self) -> &[&str] { &["websocket.url"] }
    fn optional_parameters(&self) -> &[&str];
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError>;
}
```

### Core Logic Flow

```
1. start() called with SourceCallback
2. Spawn thread with tokio runtime
3. Connect to WebSocket URL using tokio-tungstenite
4. Enter message loop:
   a. Receive message (with timeout for shutdown check)
   b. Handle message types:
      - Text: callback.on_data(text.as_bytes())
      - Binary: callback.on_data(&binary)
      - Ping: Send Pong (handled by tungstenite)
      - Pong: Update last pong time
      - Close: Exit loop or reconnect
   c. Check running flag
   d. Handle errors with M5 error context
5. On disconnect: Reconnect with exponential backoff if enabled
6. On stop(): Set running = false, close connection gracefully
```

### Reconnection Strategy

```rust
async fn connect_with_retry(&self) -> Result<WebSocketStream, EventFluxError> {
    let mut attempts = 0;
    let mut delay = self.config.reconnect_delay_ms;

    loop {
        match connect_async(&self.config.url).await {
            Ok((stream, _)) => return Ok(stream),
            Err(e) => {
                attempts += 1;
                if self.config.reconnect_max_attempts >= 0
                   && attempts >= self.config.reconnect_max_attempts {
                    return Err(EventFluxError::ConnectionUnavailable { ... });
                }

                log::warn!("[WebSocketSource] Reconnecting in {}ms (attempt {})",
                          delay, attempts);
                tokio::time::sleep(Duration::from_millis(delay)).await;

                // Exponential backoff with cap
                delay = (delay * 2).min(self.config.reconnect_max_delay_ms);
            }
        }
    }
}
```

---

## WebSocket Sink

### Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `websocket.url` | Yes | - | WebSocket URL (ws:// or wss://) |
| `websocket.reconnect` | No | true | Auto-reconnect on disconnect |
| `websocket.reconnect.delay.ms` | No | 1000 | Initial reconnect delay |
| `websocket.reconnect.max.delay.ms` | No | 30000 | Max reconnect delay |
| `websocket.message.type` | No | text | Message type: "text" or "binary" |
| `websocket.headers.*` | No | - | Custom headers |
| `websocket.subprotocol` | No | - | WebSocket subprotocol |

### Implementation Structure

```rust
// src/core/stream/output/sink/websocket_sink.rs

/// Configuration for WebSocket sink
#[derive(Debug, Clone)]
pub struct WebSocketSinkConfig {
    pub url: String,
    pub reconnect: bool,
    pub reconnect_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub message_type: MessageType, // Text or Binary
    pub headers: HashMap<String, String>,
    pub subprotocol: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageType {
    Text,
    Binary,
}

/// WebSocket sink that publishes messages to an endpoint
#[derive(Debug)]
pub struct WebSocketSink {
    config: WebSocketSinkConfig,
    state: Arc<Mutex<Option<WebSocketConnectionState>>>,
    runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    runtime_handle: Arc<Mutex<Option<tokio::runtime::Handle>>>,
}

impl Sink for WebSocketSink {
    fn start(&self);
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError>;
    fn stop(&self);
    fn clone_box(&self) -> Box<dyn Sink>;
    fn validate_connectivity(&self) -> Result<(), EventFluxError>;
}

/// Factory for creating WebSocket sink instances
#[derive(Debug, Clone)]
pub struct WebSocketSinkFactory;

impl SinkFactory for WebSocketSinkFactory {
    fn name(&self) -> &'static str { "websocket" }
    fn supported_formats(&self) -> &[&str] { &["json", "text", "bytes"] }
    fn required_parameters(&self) -> &[&str] { &["websocket.url"] }
    fn optional_parameters(&self) -> &[&str];
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Sink>, EventFluxError>;
}
```

### Core Logic Flow

```
1. start() called
2. Create tokio runtime (or use existing handle)
3. Connect to WebSocket URL
4. Store connection state

5. publish(payload) called
6. Check connection alive
7. Send message (Text or Binary based on config)
8. Handle send errors (reconnect if needed)

9. stop() called
10. Send Close frame
11. Close connection gracefully
```

---

## Factory Registration

### Register in EventFluxContext

Add to `src/core/config/eventflux_context.rs`:

```rust
// In register_default_extensions():
self.add_source_factory(
    "websocket".to_string(),
    Box::new(WebSocketSourceFactory)
);
self.add_sink_factory(
    "websocket".to_string(),
    Box::new(WebSocketSinkFactory)
);
```

### Update mod.rs Files

**`src/core/stream/input/source/mod.rs`:**
```rust
pub mod websocket_source;
```

**`src/core/stream/output/sink/mod.rs`:**
```rust
pub mod websocket_sink;
```

---

## Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
tokio-tungstenite = { version = "0.24", features = ["native-tls"] }
# tungstenite is a transitive dependency, but we may need it explicitly
# for message types
tungstenite = "0.24"
```

**Note**: We already have `tokio` as a dependency. The `native-tls` feature enables secure WebSocket (wss://) connections.

---

## Usage Examples

### Source Example

```sql
@source(
    type='websocket',
    websocket.url='wss://stream.binance.com:9443/ws/btcusdt@trade',
    @map(type='json')
)
CREATE STREAM RawTrades (
    e STRING,
    s STRING,
    p STRING,
    q STRING,
    T BIGINT
);
```

### Sink Example

```sql
@sink(
    type='websocket',
    websocket.url='wss://my-server.com/events',
    websocket.message.type='text',
    @map(type='json')
)
CREATE STREAM OutputEvents (
    symbol STRING,
    price DOUBLE,
    volume DOUBLE
);
```

---

## Testing

### Unit Tests

**`src/core/stream/input/source/websocket_source.rs`:**
- `test_config_from_properties_required_only()`
- `test_config_from_properties_all_options()`
- `test_config_missing_url()`
- `test_config_invalid_reconnect_delay()`
- `test_websocket_url_parsing()`
- `test_factory_metadata()`
- `test_factory_create()`
- `test_source_clone()`

**`src/core/stream/output/sink/websocket_sink.rs`:**
- `test_config_from_properties_required_only()`
- `test_config_from_properties_all_options()`
- `test_config_missing_url()`
- `test_message_type_parsing()`
- `test_factory_metadata()`
- `test_factory_create()`
- `test_sink_clone()`

### Integration Tests

**`tests/websocket_integration.rs`:**
- `test_websocket_source_connect_disconnect()` - requires mock server
- `test_websocket_source_reconnect()` - test reconnection logic
- `test_websocket_sink_publish()` - requires mock server
- `test_websocket_round_trip()` - source → processing → sink

**Note**: Integration tests may use a mock WebSocket server or be marked `#[ignore]` for CI environments without network access.

---

## Implementation Checklist

### Source Implementation
- [x] Create `WebSocketSourceConfig` struct with `from_properties()`
- [x] Create `WebSocketSource` implementing `Source` trait
- [x] Implement `start()` with connection loop
- [x] Implement `stop()` with graceful shutdown
- [x] Implement `validate_connectivity()` for fail-fast
- [x] Implement reconnection with exponential backoff
- [x] Create `WebSocketSourceFactory`
- [x] Add unit tests
- [x] Register factory in `EventFluxContext`

### Sink Implementation
- [x] Create `WebSocketSinkConfig` struct with `from_properties()`
- [x] Create `WebSocketSink` implementing `Sink` trait
- [x] Implement `start()` with connection
- [x] Implement `publish()` with message sending
- [x] Implement `stop()` with graceful close
- [x] Implement `validate_connectivity()`
- [x] Create `WebSocketSinkFactory`
- [x] Add unit tests
- [x] Register factory in `EventFluxContext`

---

## Files to Create/Modify

### New Files
| File | Description |
|------|-------------|
| `src/core/stream/input/source/websocket_source.rs` | WebSocket source implementation |
| `src/core/stream/output/sink/websocket_sink.rs` | WebSocket sink implementation |
| `tests/websocket_integration.rs` | Integration tests |
| `website/docs/connectors/websocket.md` | Documentation |

### Modified Files
| File | Change |
|------|--------|
| `src/core/stream/input/source/mod.rs` | Add `pub mod websocket_source;` |
| `src/core/stream/output/sink/mod.rs` | Add `pub mod websocket_sink;` |
| `src/core/config/eventflux_context.rs` | Register factories |
| `Cargo.toml` | Add `tokio-tungstenite` dependency |
| `website/docs/connectors/overview.md` | Add WebSocket to list |
| `website/sidebars.js` | Add WebSocket doc link |

---

## Notes on RabbitMQ Patterns Applied

The WebSocket implementation follows established RabbitMQ patterns:

1. **Config struct**: Separate `*Config` struct with `from_properties()` for parsing
2. **Threading model**: Sync traits with internal tokio runtime
3. **Graceful shutdown**: `Arc<AtomicBool>` running flag
4. **Error handling**: Integration with M5 `SourceErrorContext`
5. **Factory pattern**: Implement `SourceFactory`/`SinkFactory` traits
6. **Connectivity validation**: `validate_connectivity()` for fail-fast
7. **Clone support**: `clone_box()` for trait object cloning
8. **Comprehensive tests**: Unit tests for config parsing, factory creation

---

## Future Enhancements (V2)

The following features are deferred to V2. GitHub issues should be created to track these:

### 1. Subscription Messages
**Issue**: Support `websocket.subscribe.message` for APIs requiring subscription after connect

Some WebSocket APIs (Coinbase, Kraken) require sending a subscribe message after connecting:
```json
{"type": "subscribe", "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}
```

**Proposed Configuration:**
```sql
@source(
    type='websocket',
    websocket.url='wss://ws-feed.exchange.coinbase.com',
    websocket.subscribe.message='{"type":"subscribe","channels":[{"name":"ticker","product_ids":["BTC-USD"]}]}',
    @map(type='json')
)
```

**GitHub Issue**: `feat(websocket): Support subscription messages for WebSocket source`

### 2. Binary/Compressed Data
**Issue**: Support decompression for compressed WebSocket messages

Some APIs send compressed (gzip/deflate) binary data to reduce bandwidth.

**Proposed Configuration:**
```sql
@source(
    type='websocket',
    websocket.url='wss://api.example.com/stream',
    websocket.compression='gzip',
    @map(type='json')
)
```

**GitHub Issue**: `feat(websocket): Support compressed binary messages in WebSocket source`

---

## Combined Streams Support

Binance supports combined streams via URL parameters:
```
wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
```

This is **naturally supported** in V1 - just use the combined URL. The JSON messages include a `stream` field indicating the source:
```json
{
  "stream": "btcusdt@trade",
  "data": {"e":"trade", "s":"BTCUSDT", "p":"50000.00", ...}
}
```

The schema remains consistent across streams, so no special handling is needed.
