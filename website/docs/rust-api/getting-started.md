---
sidebar_position: 1
title: Rust API Getting Started
description: Embed EventFlux in your Rust applications
---

# Rust API Getting Started

This guide shows you how to embed EventFlux in your Rust applications for programmatic stream processing.

## Installation

Add EventFlux to your `Cargo.toml`:

```toml
[dependencies]
eventflux = { git = "https://github.com/eventflux-io/eventflux.git" }
```

Or install from source:

```bash
git clone https://github.com/eventflux-io/eventflux.git
cd eventflux
cargo build --release
```

## Basic Usage

### Creating an EventFlux Manager

The `EventFluxManager` is the entry point for all EventFlux operations:

```rust
use eventflux::prelude::*;

fn main() -> Result<(), EventFluxError> {
    // Create a new manager instance
    let manager = EventFluxManager::new();

    // Manager is now ready to create runtimes
    Ok(())
}
```

### Defining and Running a Query

```rust
use eventflux::prelude::*;

fn main() -> Result<(), EventFluxError> {
    let manager = EventFluxManager::new();

    // Define the EventFlux application
    let app_definition = r#"
        DEFINE STREAM TemperatureReadings (
            sensor_id STRING,
            temperature DOUBLE,
            timestamp LONG
        );

        SELECT sensor_id, temperature
        FROM TemperatureReadings
        WHERE temperature > 100.0
        INSERT INTO HighTemperatureAlerts;
    "#;

    // Create the runtime
    let runtime = manager.create_runtime(app_definition)?;

    // Start the runtime
    runtime.start();

    Ok(())
}
```

### Sending Events

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
  <TabItem value="single" label="Single Event" default>

```rust
use eventflux::prelude::*;

// Create an event with the event! macro
let event = event![
    "sensor_001",  // sensor_id
    105.5,         // temperature
    1699900000i64  // timestamp
];

// Send to the input stream
runtime.send("TemperatureReadings", event)?;
```

  </TabItem>
  <TabItem value="batch" label="Batch Events">

```rust
use eventflux::prelude::*;

// Send multiple events efficiently
let events = vec![
    event!["sensor_001", 105.5, 1699900000i64],
    event!["sensor_002", 98.2, 1699900001i64],
    event!["sensor_001", 110.3, 1699900002i64],
];

for event in events {
    runtime.send("TemperatureReadings", event)?;
}
```

  </TabItem>
</Tabs>

### Receiving Output Events

```rust
use eventflux::prelude::*;

// Register a callback for the output stream
runtime.on_output("HighTemperatureAlerts", |event| {
    let sensor_id: &str = event.get(0)?;
    let temperature: f64 = event.get(1)?;

    println!("ALERT: {} has temperature {}", sensor_id, temperature);
    Ok(())
})?;
```

## Working with Windows

### Time-Based Windows

```rust
let app = r#"
    DEFINE STREAM Trades (
        symbol STRING,
        price DOUBLE,
        volume INT
    );

    SELECT symbol,
           AVG(price) AS avg_price,
           SUM(volume) AS total_volume
    FROM Trades
    WINDOW TUMBLING(5 sec)
    GROUP BY symbol
    INSERT INTO TradeStats;
"#;

let runtime = manager.create_runtime(app)?;

// Events will be grouped into 5-second windows
runtime.send("Trades", event!["AAPL", 150.25, 100])?;
```

### Advancing Time

For testing or replay scenarios, you can manually advance time:

```rust
// Advance the event time to trigger window evaluation
runtime.advance_time(1699900005)?;  // 5 seconds later
```

## Error Handling

EventFlux uses a custom error type for comprehensive error handling:

```rust
use eventflux::error::EventFluxError;

match runtime.send("Input", event) {
    Ok(()) => println!("Event sent successfully"),
    Err(EventFluxError::StreamNotFound(name)) => {
        eprintln!("Stream '{}' does not exist", name);
    }
    Err(EventFluxError::TypeMismatch { expected, got }) => {
        eprintln!("Type error: expected {}, got {}", expected, got);
    }
    Err(e) => eprintln!("Unexpected error: {}", e),
}
```

### Error Types

| Error | Description |
|-------|-------------|
| `ParseError` | Invalid query syntax |
| `StreamNotFound` | Referenced stream doesn't exist |
| `TypeMismatch` | Event attribute type doesn't match schema |
| `BufferFull` | Event queue is full (non-blocking mode) |
| `RuntimeError` | General execution error |

## Complete Example

Here's a complete example with multiple queries and output handling:

```rust
use eventflux::prelude::*;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = EventFluxManager::new();

    let app = r#"
        DEFINE STREAM SensorReadings (
            sensor_id STRING,
            temperature DOUBLE,
            humidity DOUBLE,
            timestamp LONG
        );

        -- Alert on high temperature
        SELECT sensor_id, temperature, timestamp
        FROM SensorReadings
        WHERE temperature > 100.0
        INSERT INTO HighTempAlerts;

        -- Compute 5-minute averages
        SELECT sensor_id,
               AVG(temperature) AS avg_temp,
               AVG(humidity) AS avg_humidity,
               COUNT(*) AS reading_count
        FROM SensorReadings
        WINDOW TUMBLING(5 min)
        GROUP BY sensor_id
        INSERT INTO SensorStats;
    "#;

    let runtime = manager.create_runtime(app)?;

    // Collect alerts
    let alerts = Arc::new(Mutex::new(Vec::new()));
    let alerts_clone = alerts.clone();

    runtime.on_output("HighTempAlerts", move |event| {
        let sensor_id: String = event.get(0)?;
        let temperature: f64 = event.get(1)?;
        alerts_clone.lock().unwrap().push((sensor_id, temperature));
        Ok(())
    })?;

    runtime.start();

    // Send test events
    runtime.send("SensorReadings", event!["s1", 95.0, 50.0, 1000i64])?;
    runtime.send("SensorReadings", event!["s2", 105.0, 45.0, 1001i64])?;
    runtime.send("SensorReadings", event!["s1", 110.0, 55.0, 1002i64])?;

    // Check alerts
    let alerts = alerts.lock().unwrap();
    println!("Received {} alerts", alerts.len());

    Ok(())
}
```

## Lifecycle Management

EventFlux runtimes support full lifecycle control: start, shutdown, restart, and state management.

### Shutdown and Restart

A runtime can be restarted after shutdown. The same runtime instance transitions through `Created → Running → Stopped → Running` states:

```rust
let runtime = manager
    .create_eventflux_app_runtime_from_string(query)
    .await?;

runtime.start()?;

// Process events...
runtime.send_event("Input", vec!["sensor-1".into(), 105.0.into()])?;

// Shutdown (auto-persists state if persistence store is configured)
runtime.shutdown();

// Restart (auto-restores state if a persisted revision exists)
runtime.start()?;

// Continue processing — state (windows, aggregators) is preserved
runtime.send_event("Input", vec!["sensor-2".into(), 110.0.into()])?;

runtime.shutdown();
```

Key behaviors:
- **`shutdown()`** is idempotent — calling it twice is safe (second call is a no-op)
- **`start()`** is idempotent — calling it on a running runtime is a no-op
- Callbacks registered before the first start are preserved across restarts

### Auto-Persist and Auto-Restore

When a persistence store is configured, the runtime automatically:
- **Persists** all state (window buffers, aggregator accumulators) on `shutdown()`
- **Restores** state from the last saved revision on `start()`

```rust
use eventflux::core::persistence::{InMemoryPersistenceStore, PersistenceStore};

// Configure persistence store on the manager
let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
manager.set_persistence_store(Arc::clone(&store));

let runtime = manager
    .create_eventflux_app_runtime_from_string(query)
    .await?;

runtime.start()?;

// Send events — aggregation state accumulates
runtime.send_event("Input", vec![10.into()])?;
runtime.send_event("Input", vec![20.into()])?;

// Shutdown auto-persists: sum=30, window=[10, 20]
runtime.shutdown();

// Restart auto-restores: state is back to sum=30, window=[10, 20]
runtime.start()?;

// Next event builds on restored state
runtime.send_event("Input", vec![5.into()])?;
// Output: sum=35, window=[10, 20, 5]

runtime.shutdown();
```

### Clear State

Use `clear_state()` between `shutdown()` and `start()` to discard all accumulated state and start fresh:

```rust
runtime.shutdown();

// Clear all persisted revisions and in-memory state
runtime.clear_state();

// Restart with clean state — no old aggregation values
runtime.start()?;
```

:::note
`clear_state()` only works when the runtime is in the `Stopped` state. It removes both persisted revisions from the store and in-memory state (window buffers, aggregator accumulators, per-partition group states).
:::

## Next Steps

- [Configuration](/docs/rust-api/configuration) - Customize runtime behavior
- [Testing](/docs/rust-api/testing) - Test your streaming applications
- [SQL Reference](/docs/sql-reference/queries) - Full query language documentation
