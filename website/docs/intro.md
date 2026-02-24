---
sidebar_position: 1
title: Introduction
description: Get started with EventFlux, the Rust-native Complex Event Processing engine
---

# Introduction to EventFlux

EventFlux is a **high-performance Complex Event Processing (CEP) engine** built from the ground up in Rust. It processes streaming data in real-time using familiar SQL syntax with powerful streaming extensions.

:::tip Why EventFlux?
EventFlux delivers **over 1 million events per second** with zero garbage collection pauses, making it ideal for latency-sensitive applications like financial trading, IoT analytics, and real-time fraud detection.
:::

## Key Features

### Proven & Tested

EventFlux has **1,400+ passing tests** covering all core functionality:

| Feature | Status | Tests |
|---------|--------|-------|
| **SQL Parser** | Production Ready | Native streaming extensions |
| **14 Window Types** | Production Ready | 200+ tests |
| **Pattern Matching** | Production Ready | 370+ tests |
| **Joins** | Production Ready | INNER, LEFT, RIGHT, FULL |
| **Aggregations** | Production Ready | COUNT, SUM, AVG, MIN, MAX, STDDEV |
| **State Management** | Production Ready | Checkpointing, Recovery |
| **RabbitMQ Connector** | Production Ready | Source & Sink with JSON/CSV |

### SQL-First Design

Write queries in familiar SQL with streaming extensions:

```sql
DEFINE STREAM SensorReadings (
    sensor_id STRING,
    temperature DOUBLE,
    timestamp LONG
);

SELECT sensor_id,
       AVG(temperature) AS avg_temp,
       MAX(temperature) AS max_temp
FROM SensorReadings
WINDOW TUMBLING(5 min)
GROUP BY sensor_id
HAVING MAX(temperature) > 100
INSERT INTO Alerts;
```

### Comprehensive Window Support

EventFlux supports **14 window types** for different streaming scenarios:

| Window Type | Description | Use Case |
|-------------|-------------|----------|
| **Tumbling** | Fixed, non-overlapping | Periodic reports |
| **Sliding** | Overlapping with slide | Moving averages |
| **Length** | Count-based | Last N events |
| **LengthBatch** | Count batches | Batch processing |
| **Time** | Continuous time | Rolling windows |
| **TimeBatch** | Time batches | Periodic snapshots |
| **Session** | Gap-based | User sessions |
| **ExternalTime** | Event time | Out-of-order events |
| **Delay** | Time delay | Late arrivals |
| **Unique** | Latest per unique key | Deduplication (keep latest) |
| **FirstUnique** | First per unique key | Deduplication (keep first) |
| **Expression** | Expression-based count | Dynamic count limits |
| **Frequent** | Most frequent events | Top-N analysis |
| **LossyFrequent** | Approximate frequency | High-volume frequency estimation |

### Pattern Matching

Detect complex event sequences with temporal constraints:

```sql
-- Detect temperature spike pattern
FROM SensorReadings
MATCH (e1=TempReading -> e2=TempReading -> e3=TempReading)
WITHIN 5 MINUTES
FILTER e2.temperature > e1.temperature
   AND e3.temperature > e2.temperature
SELECT e1.sensor_id, e1.temperature AS start_temp, e3.temperature AS end_temp
INSERT INTO TempSpikes;
```

### Blazing Performance

- **Lock-free pipelines** using crossbeam queues
- **Zero-allocation hot paths** with object pooling
- **No garbage collection** pauses
- **Rust's memory safety** without runtime overhead

## Quick Example

Here's a complete example that filters and aggregates sensor data:

```rust
use eventflux::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = EventFluxManager::new();

    let app = r#"
        DEFINE STREAM Input (sensor_id STRING, value DOUBLE);

        SELECT sensor_id, AVG(value) AS avg_value
        FROM Input
        WINDOW TUMBLING(1 min)
        GROUP BY sensor_id
        INSERT INTO Output;
    "#;

    let runtime = manager.create_runtime(app)?;
    runtime.start();

    // Send events
    runtime.send("Input", event!["sensor-1", 25.5])?;
    runtime.send("Input", event!["sensor-1", 26.0])?;
    runtime.send("Input", event!["sensor-2", 18.3])?;

    Ok(())
}
```

## Next Steps

import DocCardList from '@theme/DocCardList';

<DocCardList />

:::info Ready to dive in?
- **[Installation](/docs/getting-started/installation)** - Set up EventFlux in your project
- **[Quick Start](/docs/getting-started/quick-start)** - Build your first streaming application
- **[SQL Reference](/docs/sql-reference/queries)** - Complete query language documentation
:::

## Community

- **GitHub**: [eventflux-io/eventflux](https://github.com/eventflux-io/eventflux)
- **Discussions**: [GitHub Discussions](https://github.com/eventflux-io/eventflux/discussions)
- **Discord**: Join our community chat

:::caution Work in Progress
EventFlux is under active development. APIs may change between releases. We recommend pinning to specific versions in production.
:::
