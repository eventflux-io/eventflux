---
sidebar_position: 6
title: WebSocket Connector
description: Connect EventFlux to WebSocket endpoints for real-time bidirectional streaming
---

# WebSocket Connector

The WebSocket connector enables EventFlux to consume real-time events from WebSocket endpoints and publish processed results to WebSocket servers. It supports JSON, text, and binary message formats with automatic serialization/deserialization.

## Overview

WebSocket is ideal for:
- **Real-time data feeds**: Cryptocurrency exchanges, stock tickers, sports scores
- **Live streaming APIs**: Social media feeds, IoT sensor data
- **Bidirectional communication**: Chat applications, collaborative tools
- **Low-latency messaging**: Gaming, trading systems

## Prerequisites

The WebSocket connector is feature-gated. Build EventFlux with the
`websocket` feature (Docker images already include all connectors):

```bash
cargo build --release --features websocket
# or: cargo build --release --features connectors-all
```

## WebSocket Source

The WebSocket source connects to a WebSocket endpoint and consumes incoming messages as events for processing.

### SQL Syntax

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',                              -- or 'text', 'bytes'
    "websocket.url" = 'wss://example.com/stream', -- Required
    "websocket.reconnect" = 'true',               -- Optional (default: true)
    "websocket.reconnect.delay.ms" = '1000',      -- Optional (default: 1000)
    "websocket.reconnect.max.delay.ms" = '30000', -- Optional (default: 30000)
    "websocket.reconnect.max.attempts" = '-1',    -- Optional (default: -1, unlimited)
    "websocket.headers.Authorization" = 'Bearer token', -- Optional custom headers
    "websocket.subprotocol" = 'graphql-ws'        -- Optional subprotocol
);
```

### Source Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `websocket.url` | Yes | - | WebSocket URL (must start with `ws://` or `wss://`) |
| `websocket.reconnect` | No | `true` | Auto-reconnect on disconnect |
| `websocket.reconnect.delay.ms` | No | `1000` | Initial reconnect delay in milliseconds |
| `websocket.reconnect.max.delay.ms` | No | `30000` | Maximum reconnect delay (exponential backoff cap) |
| `websocket.reconnect.max.attempts` | No | `-1` | Maximum reconnect attempts. `-1` for unlimited |
| `websocket.headers.*` | No | - | Custom HTTP headers for the connection (e.g., `websocket.headers.Authorization`) |
| `websocket.subprotocol` | No | - | WebSocket subprotocol for negotiation (e.g., `graphql-ws`) |

### Source Example: Binance Trade Stream

```sql
-- Consume real-time trades from Binance WebSocket API
CREATE STREAM BinanceTrades (
    e STRING,           -- Event type
    s STRING,           -- Symbol
    p STRING,           -- Price
    q STRING,           -- Quantity
    T LONG              -- Trade time
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://stream.binance.com:9443/ws/btcusdt@trade'
);

-- Calculate 1-minute VWAP (Volume Weighted Average Price)
SELECT
    s AS symbol,
    SUM(CAST(p AS DOUBLE) * CAST(q AS DOUBLE)) / SUM(CAST(q AS DOUBLE)) AS vwap,
    SUM(CAST(q AS DOUBLE)) AS total_volume
FROM BinanceTrades
WINDOW TUMBLING(1 min)
GROUP BY s
INSERT INTO TradeStats;
```

### Source Example: Authenticated WebSocket

```sql
-- Connect to authenticated WebSocket endpoint
CREATE STREAM PrivateEvents (
    event_type STRING,
    data STRING,
    timestamp LONG
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://api.example.com/private/stream',
    "websocket.headers.Authorization" = 'Bearer eyJhbGciOiJIUzI1NiIs...',
    "websocket.headers.X-API-Key" = 'your-api-key',
    "websocket.subprotocol" = 'v1.json'
);
```

## WebSocket Sink

The WebSocket sink publishes processed events to a WebSocket endpoint.

### SQL Syntax

```sql
CREATE STREAM StreamName (
    field1 TYPE,
    field2 TYPE,
    ...
) WITH (
    type = 'sink',
    extension = 'websocket',
    format = 'json',                              -- or 'text', 'bytes'
    "websocket.url" = 'wss://example.com/events', -- Required
    "websocket.reconnect" = 'true',               -- Optional (default: true)
    "websocket.reconnect.delay.ms" = '1000',      -- Optional (default: 1000)
    "websocket.reconnect.max.delay.ms" = '30000', -- Optional (default: 30000)
    "websocket.reconnect.max.attempts" = '3',     -- Optional (default: 3, -1 for unlimited)
    "websocket.message.type" = 'text',            -- Optional (default: text)
    "websocket.headers.Authorization" = 'Bearer token', -- Optional custom headers
    "websocket.subprotocol" = 'graphql-ws'        -- Optional subprotocol
);
```

### Sink Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `websocket.url` | Yes | - | WebSocket URL (must start with `ws://` or `wss://`) |
| `websocket.reconnect` | No | `true` | Auto-reconnect on disconnect |
| `websocket.reconnect.delay.ms` | No | `1000` | Initial reconnect delay in milliseconds |
| `websocket.reconnect.max.delay.ms` | No | `30000` | Maximum reconnect delay (exponential backoff cap) |
| `websocket.reconnect.max.attempts` | No | `3` | Maximum reconnect attempts. `-1` for unlimited |
| `websocket.message.type` | No | `text` | Message type: `text` (UTF-8 string) or `binary` |
| `websocket.headers.*` | No | - | Custom HTTP headers for the connection |
| `websocket.subprotocol` | No | - | WebSocket subprotocol for negotiation |

### Sink Example

```sql
-- Output stream to WebSocket server
CREATE STREAM AlertOutput (
    symbol STRING,
    alert_type STRING,
    price DOUBLE,
    timestamp LONG
) WITH (
    type = 'sink',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://dashboard.example.com/alerts',
    "websocket.message.type" = 'text'
);

-- Send price spike alerts
INSERT INTO AlertOutput
SELECT
    symbol,
    'PRICE_SPIKE' AS alert_type,
    price,
    event_timestamp() AS timestamp
FROM TradeStream
WHERE price_change_pct > 5.0;
```

## Complete End-to-End Example

This example demonstrates a complete pipeline: WebSocket → Process → WebSocket.

```sql
-- ============================================
-- INPUT: Consume cryptocurrency trades
-- ============================================
CREATE STREAM CryptoTrades (
    symbol STRING,
    price DOUBLE,
    quantity DOUBLE,
    side STRING
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://stream.crypto-exchange.com/trades',
    "websocket.reconnect" = 'true',
    "websocket.reconnect.max.attempts" = '10'
);

-- ============================================
-- OUTPUT: Publish aggregated stats
-- ============================================
CREATE STREAM TradeStats (
    symbol STRING,
    avg_price DOUBLE,
    total_volume DOUBLE,
    trade_count LONG,
    window_end LONG
) WITH (
    type = 'sink',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://analytics.example.com/stats',
    "websocket.headers.Authorization" = 'Bearer analytics-token'
);

-- ============================================
-- PROCESSING: Aggregate trades per minute
-- ============================================
INSERT INTO TradeStats
SELECT
    symbol,
    AVG(price) AS avg_price,
    SUM(quantity) AS total_volume,
    COUNT(*) AS trade_count,
    event_timestamp() AS window_end
FROM CryptoTrades
WINDOW TUMBLING(1 min)
GROUP BY symbol;
```

## Multi-Stream WebSocket URLs

Some WebSocket APIs support multiple streams via URL parameters. For example, Binance combined streams:

```sql
-- Subscribe to multiple trading pairs in one connection
CREATE STREAM MultiPairTrades (
    stream STRING,  -- Indicates which stream: "btcusdt@trade" or "ethusdt@trade"
    data OBJECT     -- Nested trade data
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade'
);
```

The response includes a `stream` field identifying the source:
```json
{
  "stream": "btcusdt@trade",
  "data": {"e":"trade", "s":"BTCUSDT", "p":"50000.00", "q":"0.5", ...}
}
```

## Reconnection Behavior

### Source Reconnection

When `websocket.reconnect = true` (default), the **source** reconnects with exponential backoff:

1. **Connection Lost**: Automatic reconnection with exponential backoff
2. **Server Close Frame**: Reconnect after configured delay
3. **Network Error**: Retry with increasing delays up to `websocket.reconnect.max.delay.ms`
4. **Max Attempts**: Controlled by `websocket.reconnect.max.attempts` (`-1` = unlimited)

```
Attempt 1: Wait 1000ms  → Connect
Attempt 2: Wait 2000ms  → Connect
Attempt 3: Wait 4000ms  → Connect
...
Attempt N: Wait 30000ms → Connect (capped at max delay)
```

### Sink Reconnection

The **sink** has different reconnection behavior:

- **During `start()`**: Single connection attempt (no retry)
- **During `publish()`**: Reconnection attempts with exponential backoff, controlled by `websocket.reconnect.max.attempts`
- Default is **3 attempts**; set to `-1` for unlimited retries
- After max attempts reached, `publish()` returns an error

This design ensures sink operations don't block indefinitely on connection failures by default, while allowing users to configure unlimited retries for high-availability scenarios.

### Disabling Reconnection

For fail-fast scenarios:

```sql
-- Source: Stop immediately on disconnect
CREATE STREAM CriticalFeed (
    ...
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://critical-api.example.com/feed',
    "websocket.reconnect" = 'false'  -- Stop on disconnect
);

-- Sink: Fail publish immediately if disconnected
CREATE STREAM CriticalOutput (
    ...
) WITH (
    type = 'sink',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://critical-api.example.com/events',
    "websocket.reconnect" = 'false'  -- Error immediately if not connected
);
```

## Error Handling

### Connection Failures

The connector validates connectivity during stream initialization:

```
Error: Failed to connect to WebSocket wss://example.com/stream: Connection refused
```

**Solutions:**
- Verify the WebSocket URL is correct
- Check network connectivity
- Ensure the server is running and accessible
- Verify TLS certificates for `wss://` URLs

### Authentication Errors

```
Error: Failed to connect to WebSocket: 401 Unauthorized
```

**Solutions:**
- Check header values: `websocket.headers.Authorization`
- Verify API key or token is valid
- Ensure subprotocol matches server expectations

### Message Handling

- **Text Messages**: Passed to mapper for decoding
- **Binary Messages**: Passed to mapper as raw bytes
- **Ping/Pong**: Handled automatically (keepalive)
- **Close Frames**: Trigger reconnection if enabled

## Performance Considerations

### Low-Latency Applications

For latency-sensitive use cases:

```sql
-- Minimal configuration for fastest response
CREATE STREAM TickerFeed (
    price DOUBLE,
    timestamp LONG
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'json',
    "websocket.url" = 'wss://fast-feed.example.com/ticker'
    -- Use defaults for minimal overhead
);
```

### High-Throughput Scenarios

For high message rates:
- The connector processes messages as they arrive
- Consider windowed aggregations to reduce output volume
- Use binary format for reduced parsing overhead

```sql
-- Binary format for high-frequency data
CREATE STREAM RawFeed (
    payload BYTES
) WITH (
    type = 'source',
    extension = 'websocket',
    format = 'bytes',
    "websocket.url" = 'wss://high-freq.example.com/raw'
);
```

## Troubleshooting

### Connection Timeout

```
Error: Connection to WebSocket wss://example.com/stream timed out after 10 seconds
```

**Solutions:**
- Verify network connectivity to the host
- Check firewall rules for WebSocket traffic
- Ensure the server accepts connections

### SSL/TLS Errors

```
Error: Failed to connect to WebSocket: certificate verify failed
```

**Solutions:**
- Use `ws://` for non-TLS connections (development only)
- Verify the server certificate is valid
- Check system CA certificates are up to date

### No Messages Received

**Checklist:**
- Is the WebSocket URL correct?
- Does the server require a subscription message after connect?
- Are headers/authentication configured correctly?
- Check server-side logs for connection issues
- Verify stream field types match JSON structure

### Subprotocol Mismatch

```
Error: Failed to connect: subprotocol not supported
```

**Solutions:**
- Verify `websocket.subprotocol` matches server expectations
- Remove subprotocol config if server doesn't require it
- Check API documentation for supported subprotocols

## Rust API Usage

For programmatic access, use the WebSocket connector directly:

```rust
use eventflux::core::stream::input::source::websocket_source::{
    WebSocketSource, WebSocketSourceFactory
};
use eventflux::core::stream::output::sink::websocket_sink::{
    WebSocketSink, WebSocketSinkFactory
};
use eventflux::core::extension::{SourceFactory, SinkFactory};
use std::collections::HashMap;

// Create source
let mut config = HashMap::new();
config.insert("websocket.url".to_string(),
    "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string());

let factory = WebSocketSourceFactory;
let source = factory.create_initialized(&config)?;

// Validate connectivity before starting
source.validate_connectivity()?;

// Create sink
let mut sink_config = HashMap::new();
sink_config.insert("websocket.url".to_string(),
    "wss://my-server.com/events".to_string());
sink_config.insert("websocket.message.type".to_string(),
    "text".to_string());

let sink_factory = WebSocketSinkFactory;
let sink = sink_factory.create_initialized(&sink_config)?;
```

## Comparison with Other Connectors

| Feature | WebSocket | RabbitMQ |
|---------|-----------|----------|
| Protocol | WebSocket (HTTP upgrade) | AMQP 0-9-1 |
| Direction | Bidirectional | Queue-based |
| Message Persistence | No (real-time only) | Yes (durable queues) |
| Delivery Guarantee | At-most-once | At-least-once with ACK |
| Use Case | Real-time streams, APIs | Message queuing, task distribution |
| Reconnection | Built-in | Built-in |

## See Also

- [Connectors Overview](/docs/connectors/overview) - Architecture and concepts
- [RabbitMQ Connector](/docs/connectors/rabbitmq) - Message queue integration
- [Mappers Reference](/docs/connectors/mappers) - JSON, CSV, and bytes format handling
- [SQL Reference](/docs/sql-reference/queries) - Query language documentation
