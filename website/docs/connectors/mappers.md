---
sidebar_position: 3
title: Mappers Reference
description: JSON, CSV, and Bytes format handling for EventFlux connectors
---

# Mappers Reference

Mappers transform between raw bytes and structured EventFlux events. They handle serialization (encoding events to bytes) and deserialization (parsing bytes into events) for connector communication.

## Overview

EventFlux provides three built-in mappers:

| Mapper | Format | Source | Sink | Use Case |
|--------|--------|--------|------|----------|
| **JSON** | `format = 'json'` | Yes | Yes | REST APIs, message queues |
| **CSV** | `format = 'csv'` | Yes | Yes | Log files, data exports |
| **Bytes** | `format = 'bytes'` | Yes | Yes | Binary protocols (protobuf, msgpack), raw passthrough |

### Sink mapping contract: one event = one message

On the sink side, each event is mapped to its **own payload** and published as
its **own transport message**. Even when a query emits several events at once
(e.g. a batch window flush), the sink publishes one message per event — no
events are ever merged into or dropped from a message. A mapping failure for
one event is logged and does not affect the remaining events.

## JSON Mapper

The JSON mapper is the most common choice for message queue integration. It handles automatic type conversion between JSON and EventFlux types.

### Type Mapping

| EventFlux Type | JSON Type | Example |
|----------------|-----------|---------|
| `STRING` | string | `"hello"` |
| `INT` | number | `42` |
| `LONG` | number | `9223372036854775807` |
| `FLOAT` | number | `3.14` |
| `DOUBLE` | number | `3.141592653589793` |
| `BOOL` | boolean | `true` |

### Source Mapper (JSON → Events)

The JSON source mapper parses incoming JSON messages and extracts field values based on the stream schema.

#### JSON Object Input

For JSON objects, fields are extracted by name or by position:

**Stream Definition:**
```sql
CREATE STREAM TradeInput (
    symbol STRING,
    price DOUBLE,
    volume INT
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'trades'
);
```

**Input JSON:**
```json
{"symbol": "AAPL", "price": 185.50, "volume": 5000}
```

**Resulting Event:**
```
["AAPL", 185.50, 5000]
```

#### Field Name Matching

The mapper attempts to match JSON fields to stream fields by:
1. Exact field name match
2. Case-insensitive match
3. Positional order (fallback)

**Example with Different Naming:**
```json
{"Symbol": "AAPL", "PRICE": 185.50, "Volume": 5000}
```
Still maps correctly due to case-insensitive matching.

### Sink Mapper (Events → JSON)

The JSON sink mapper serializes events into JSON objects using generic field names.

**Stream Definition:**
```sql
CREATE STREAM TradeOutput (
    symbol STRING,
    price DOUBLE,
    category STRING
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.exchange" = 'processed-trades'
);
```

**Event Data:**
```
["AAPL", 185.50, "high"]
```

**Output JSON:**
```json
{"field_0": "AAPL", "field_1": 185.5, "field_2": "high"}
```

:::note Field Naming
Currently, the JSON sink mapper uses generic field names (`field_0`, `field_1`, etc.). Named field support is planned for a future release.
:::

### JSON Configuration

The JSON mapper uses sensible defaults and requires no additional configuration:

```sql
CREATE STREAM MyStream (...)
WITH (
    format = 'json'  -- That's all you need!
);
```

### Handling Nested JSON

Currently, the JSON mapper handles flat JSON objects. For nested structures, the nested value is serialized as a string:

**Input:**
```json
{
  "id": 123,
  "metadata": {"source": "api", "version": 2}
}
```

**Stream Definition:**
```sql
CREATE STREAM Events (
    id INT,
    metadata STRING  -- Nested object becomes string
);
```

**Resulting Event:**
```
[123, "{\"source\":\"api\",\"version\":2}"]
```

## CSV Mapper

The CSV mapper handles comma-separated (or custom delimiter) data formats.

### Source Mapper (CSV → Events)

Parses CSV rows into events based on positional field mapping.

**Stream Definition:**
```sql
CREATE STREAM LogInput (
    timestamp STRING,
    level STRING,
    message STRING
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'csv',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'logs'
);
```

**Input CSV:**
```
2024-01-15T10:30:00Z,INFO,Application started
```

**Resulting Event:**
```
["2024-01-15T10:30:00Z", "INFO", "Application started"]
```

### Sink Mapper (Events → CSV)

Serializes events into CSV format.

**Event Data:**
```
["2024-01-15T10:30:00Z", "ERROR", "Connection failed"]
```

**Output CSV:**
```
2024-01-15T10:30:00Z,ERROR,Connection failed
```

### CSV Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `csv.delimiter` | `,` | Field delimiter character |
| `csv.quote` | `"` | Quote character for fields with delimiters |
| `csv.escape` | `\` | Escape character |

**Custom Delimiter Example:**
```sql
CREATE STREAM TabDelimited (
    col1 STRING,
    col2 STRING,
    col3 STRING
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'csv',
    "csv.delimiter" = '\t',  -- Tab-separated
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'tsv-data'
);
```

### CSV Type Conversion

All CSV fields are parsed as strings initially, then converted to the declared stream type:

| Declared Type | Conversion |
|--------------|------------|
| `STRING` | No conversion |
| `INT` | `str.parse::<i32>()` |
| `LONG` | `str.parse::<i64>()` |
| `FLOAT` | `str.parse::<f32>()` |
| `DOUBLE` | `str.parse::<f64>()` |
| `BOOL` | `"true"/"false"` (case-insensitive) |

## Bytes Mapper

The Bytes mapper provides raw binary passthrough for scenarios where you need to preserve data exactly without any parsing or transformation. This is ideal for binary protocols like Protocol Buffers, MessagePack, or any pre-formatted payload.

### Key Features

- **Zero-copy passthrough**: Binary data is preserved exactly without UTF-8 conversion
- **Cloneable**: Unlike opaque object types, bytes data can be cloned and serialized
- **Round-trip safe**: Data passes through the system unchanged

### Source Mapper (Bytes → Events)

The bytes source mapper stores incoming binary data in a single `OBJECT` typed field:

**Stream Definition:**
```sql
CREATE STREAM RawMessages (
    payload OBJECT
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'bytes',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'binary-queue'
);
```

**Input (binary):**
```
\x00\x01\xff\xfe\x80\x90  -- Any binary data
```

**Resulting Event:**
```
[<bytes:6>]  -- Stored as AttributeValue::Bytes
```

### Sink Mapper (Events → Bytes)

The bytes sink mapper extracts raw bytes from the specified field and outputs
them unchanged. Because every event maps to its own message (see the
[sink mapping contract](#sink-mapping-contract-one-event--one-message)),
binary payloads are never concatenated:

**Event Data:**
```
[<bytes:6>]
```

**Output:**
```
\x00\x01\xff\xfe\x80\x90  -- Exact original bytes
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `bytes.field-index` | `0` | Which field to extract (for sink mapper) |

### Use Cases

1. **Protocol Buffers**: Pass protobuf-encoded messages through EventFlux for routing
2. **MessagePack**: Handle msgpack serialization at the application layer
3. **Custom Binary Formats**: Proprietary protocols that should pass unchanged
4. **Payload Forwarding**: Route messages between queues without parsing

### Example: Binary Message Router

```sql
-- Source: receive protobuf messages
CREATE STREAM ProtoInput (
    payload OBJECT
) WITH (
    type = 'source',
    extension = 'rabbitmq',
    format = 'bytes',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'proto-input'
);

-- Sink: forward to another queue unchanged
CREATE STREAM ProtoOutput (
    payload OBJECT
) WITH (
    type = 'sink',
    extension = 'rabbitmq',
    format = 'bytes',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.exchange" = 'proto-output'
);

-- Simple passthrough query
INSERT INTO ProtoOutput
SELECT payload
FROM ProtoInput;
```

### Handling Binary Data in Queries

When bytes data is converted to string (e.g., in logging or debugging), it appears as `<bytes:N>` where N is the byte count:

```sql
-- This will show "<bytes:1024>" not the actual binary content
SELECT cast(payload, 'string') as debug_info
FROM RawMessages;
```

:::caution Binary Limitations
The bytes mapper is for passthrough only. You cannot:
- Parse or extract fields from binary data
- Perform string operations on binary payloads
- Mix bytes with structured data in the same mapper

For parsing binary formats, implement a custom mapper with your serialization library.
:::

## Custom Mappers

You can implement custom mappers for specialized formats like Avro, Protobuf, or proprietary formats.

### Implementing a Source Mapper

```rust
use eventflux::core::stream::mapper::{SourceMapper, SourceMapperFactory};
use eventflux::core::event::value::AttributeValue;
use eventflux::core::exception::EventFluxError;
use std::collections::HashMap;

#[derive(Debug)]
pub struct MySourceMapper {
    // Configuration fields
}

impl SourceMapper for MySourceMapper {
    fn map(&self, data: &[u8]) -> Result<Vec<AttributeValue>, EventFluxError> {
        // Parse bytes into AttributeValue vector
        let parsed = parse_my_format(data)?;
        Ok(parsed)
    }
}

#[derive(Debug, Clone)]
pub struct MySourceMapperFactory;

impl SourceMapperFactory for MySourceMapperFactory {
    fn name(&self) -> &'static str {
        "myformat"
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        Ok(Box::new(MySourceMapper::new(config)?))
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}
```

### Implementing a Sink Mapper

```rust
use eventflux::core::stream::mapper::{SinkMapper, SinkMapperFactory};
use eventflux::core::event::value::AttributeValue;
use eventflux::core::exception::EventFluxError;
use std::collections::HashMap;

#[derive(Debug)]
pub struct MySinkMapper {
    // Configuration fields
}

impl SinkMapper for MySinkMapper {
    fn map(&self, data: &[AttributeValue]) -> Result<Vec<u8>, EventFluxError> {
        // Serialize AttributeValue vector to bytes
        let serialized = serialize_my_format(data)?;
        Ok(serialized)
    }
}

#[derive(Debug, Clone)]
pub struct MySinkMapperFactory;

impl SinkMapperFactory for MySinkMapperFactory {
    fn name(&self) -> &'static str {
        "myformat"
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        Ok(Box::new(MySinkMapper::new(config)?))
    }

    fn clone_box(&self) -> Box<dyn SinkMapperFactory> {
        Box::new(self.clone())
    }
}
```

### Registering Custom Mappers

```rust
use eventflux::core::eventflux_manager::EventFluxManager;

let mut manager = EventFluxManager::new();

// Register source mapper factory
manager.context().add_source_mapper_factory(
    "myformat".to_string(),
    Box::new(MySourceMapperFactory)
);

// Register sink mapper factory
manager.context().add_sink_mapper_factory(
    "myformat".to_string(),
    Box::new(MySinkMapperFactory)
);
```

Then use in SQL:
```sql
CREATE STREAM MyStream (...) WITH (
    format = 'myformat',
    ...
);
```

## Best Practices

### JSON Mapper

1. **Match field types carefully** - Ensure JSON number types align with declared INT/LONG/FLOAT/DOUBLE
2. **Handle nulls** - Missing fields result in default values or errors
3. **Keep structures flat** - Nested objects require STRING fields

### CSV Mapper

1. **Escape delimiters** - Ensure fields containing delimiters are quoted
2. **Consistent column order** - CSV is positional, maintain order
3. **Type safety** - Validate numeric strings parse correctly

### Performance

1. **JSON is slightly slower** than CSV due to parsing overhead
2. **Pre-validate formats** in tests before production
3. **Consider compression** at the transport layer for high-volume streams

## Troubleshooting

### JSON Parse Errors

```
Error: Failed to parse JSON: expected value at line 1 column 1
```

**Causes:**
- Empty message body
- Invalid JSON syntax
- Binary data sent to JSON mapper

**Solutions:**
- Validate JSON with `jq` or online validators
- Check message content type
- Verify producer is sending valid JSON

### Type Conversion Errors

```
Error: Cannot convert "abc" to INT
```

**Causes:**
- JSON string value for numeric field
- CSV field with non-numeric content

**Solutions:**
- Change stream field type to STRING
- Fix producer to send correct types
- Add data validation at source

### Missing Fields

```
Error: Required field 'price' not found in JSON
```

**Solutions:**
- Ensure all declared fields exist in input
- Make optional fields nullable (future feature)
- Use default values in processing

## See Also

- [Connectors Overview](/docs/connectors/overview) - Architecture and concepts
- [RabbitMQ Connector](/docs/connectors/rabbitmq) - RabbitMQ integration
- [SQL Reference](/docs/sql-reference/queries) - Query language documentation
