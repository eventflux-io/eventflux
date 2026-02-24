---
sidebar_position: 7
title: Triggers
description: Time-based event generation with triggers for scheduling and batch processing
---

# Triggers

Triggers provide time-based event generation in EventFlux. They fire events at specified times or intervals, enabling scheduled operations, heartbeats, and batch processing coordination.

## Trigger Syntax

```sql
CREATE TRIGGER TriggerName AT timing_specification;
```

## Trigger Types

EventFlux supports three types of triggers:

| Type | Description | Use Case |
|------|-------------|----------|
| **Start** | Fires once at application start | Initialization, one-time setup |
| **Periodic** | Fires at regular intervals | Heartbeats, polling, batch coordination |
| **Cron** | Fires on cron schedule | Scheduled jobs, time-of-day events |

---

## Start Trigger

Fires exactly once when the application starts.

```sql
CREATE TRIGGER AppStartTrigger AT START;
```

**Use Cases:**
- Application initialization
- One-time data loading
- Startup notifications

---

## Periodic Trigger

Fires at regular time intervals.

```sql
-- Every 5 seconds
CREATE TRIGGER HeartbeatTrigger AT EVERY 5 SECONDS;

-- Every 100 milliseconds
CREATE TRIGGER FastPollTrigger AT EVERY 100 MILLISECONDS;

-- Every 1 minute
CREATE TRIGGER MinuteTrigger AT EVERY 1 MINUTE;

-- Every 2 hours
CREATE TRIGGER HourlyTrigger AT EVERY 2 HOURS;

-- Every day
CREATE TRIGGER DailyTrigger AT EVERY 1 DAY;
```

### Time Units

Supported time units:

| Unit | Aliases | Example |
|------|---------|---------|
| `MILLISECONDS` | `MILLISECOND` | `AT EVERY 50 MILLISECONDS` |
| `SECONDS` | `SECOND` | `AT EVERY 5 SECONDS` |
| `MINUTES` | `MINUTE` | `AT EVERY 1 MINUTE` |
| `HOURS` | `HOUR` | `AT EVERY 2 HOURS` |
| `DAYS` | `DAY` | `AT EVERY 1 DAY` |
| `WEEKS` | `WEEK` | `AT EVERY 1 WEEK` |

**Note:** Milliseconds is the minimum precision. Sub-millisecond units (NANOSECONDS, MICROSECONDS) and variable-length units (YEAR, MONTH) are not supported.

---

## Cron Trigger

Fires according to a cron expression for complex scheduling.

```sql
-- Every second
CREATE TRIGGER EverySecond AT CRON '*/1 * * * * *';

-- Every minute
CREATE TRIGGER EveryMinute AT CRON '0 * * * * *';

-- Every hour at minute 0
CREATE TRIGGER EveryHour AT CRON '0 0 * * * *';

-- Daily at midnight
CREATE TRIGGER DailyMidnight AT CRON '0 0 0 * * *';

-- Every weekday at 9 AM
CREATE TRIGGER WeekdayMorning AT CRON '0 0 9 * * 1-5';
```

### Cron Expression Format

EventFlux uses 6-field cron expressions:

```
┌──────────── second (0-59)
│ ┌────────── minute (0-59)
│ │ ┌──────── hour (0-23)
│ │ │ ┌────── day of month (1-31)
│ │ │ │ ┌──── month (1-12)
│ │ │ │ │ ┌── day of week (0-6, Sunday=0)
│ │ │ │ │ │
* * * * * *
```

| Field | Values | Special Characters |
|-------|--------|-------------------|
| Second | 0-59 | `*` `,` `-` `/` |
| Minute | 0-59 | `*` `,` `-` `/` |
| Hour | 0-23 | `*` `,` `-` `/` |
| Day of Month | 1-31 | `*` `,` `-` `/` |
| Month | 1-12 | `*` `,` `-` `/` |
| Day of Week | 0-6 | `*` `,` `-` `/` |

---

## Using Triggers as Stream Sources

Triggers can be used as input sources in queries. When a trigger fires, it emits an event that can be processed by downstream queries.

### Basic Usage

```sql
-- Create a periodic trigger
CREATE TRIGGER HeartbeatTrigger AT EVERY 1 SECOND;

-- Create output stream
CREATE STREAM TimestampStream (ts BIGINT);

-- Use trigger as source
INSERT INTO TimestampStream
SELECT currentTimeMillis() AS ts FROM HeartbeatTrigger;
```

### Trigger Event Schema

Triggers emit events with a single attribute:

| Attribute | Type | Description |
|-----------|------|-------------|
| `TRIGGERED_TIME` | `LONG` | Timestamp when the trigger fired (milliseconds) |

### Batch Processing Coordination

Triggers are commonly used to coordinate batch window processing:

```sql
-- Input data stream
CREATE STREAM SensorData (sensor_id STRING, temperature DOUBLE);

-- Batch trigger every 10 seconds
CREATE TRIGGER BatchTrigger AT EVERY 10 SECONDS;

-- Output aggregated results
CREATE STREAM BatchResults (sensor_id STRING, avg_temp DOUBLE);

-- Aggregate data with time-based batching
INSERT INTO BatchResults
SELECT sensor_id, AVG(temperature) AS avg_temp
FROM SensorData
WINDOW('timeBatch', 10 SECONDS)
GROUP BY sensor_id;
```

---

## Examples

### Heartbeat Monitoring

Generate heartbeat events for health monitoring:

```sql
CREATE TRIGGER Heartbeat AT EVERY 30 SECONDS;
CREATE STREAM HeartbeatEvents (timestamp BIGINT, status STRING);

INSERT INTO HeartbeatEvents
SELECT currentTimeMillis() AS timestamp,
       'ALIVE' AS status
FROM Heartbeat;
```

### Scheduled Data Fetch

Trigger periodic data polling:

```sql
CREATE TRIGGER PollTrigger AT EVERY 5 MINUTES;
CREATE STREAM PollRequests (request_time BIGINT);

INSERT INTO PollRequests
SELECT currentTimeMillis() AS request_time
FROM PollTrigger;
```

### Daily Report Generation

Schedule daily report triggers:

```sql
-- Trigger at midnight every day
CREATE TRIGGER DailyReport AT CRON '0 0 0 * * *';
CREATE STREAM ReportTriggers (report_date STRING);

INSERT INTO ReportTriggers
SELECT 'daily_report' AS report_date
FROM DailyReport;
```

### Multiple Triggers

An application can have multiple triggers for different purposes:

```sql
-- Startup initialization
CREATE TRIGGER AppStart AT START;

-- Fast polling for real-time data
CREATE TRIGGER FastPoll AT EVERY 100 MILLISECONDS;

-- Slower metrics collection
CREATE TRIGGER MetricsCollection AT EVERY 1 MINUTE;

-- Daily cleanup job
CREATE TRIGGER DailyCleanup AT CRON '0 0 2 * * *';
```

---

## Best Practices

:::tip Trigger Design Guidelines

1. **Choose appropriate intervals** - Match trigger frequency to your actual needs
2. **Use cron for complex schedules** - Better than multiple periodic triggers
3. **Consider system load** - Very fast triggers (< 100ms) may impact performance
4. **Name triggers descriptively** - Makes debugging and monitoring easier

:::

:::caution Performance Considerations

- Periodic triggers with very short intervals (< 10ms) may cause high CPU usage
- Each trigger maintains its own scheduler thread
- Cron triggers are evaluated once per second minimum

:::

## Trigger Comparison

| Feature | Start | Periodic | Cron |
|---------|-------|----------|------|
| Fires once | Yes | No | No |
| Regular interval | No | Yes | Depends |
| Complex schedule | No | No | Yes |
| Time-of-day | No | No | Yes |
| Weekday support | No | No | Yes |

---

## Programmatic API

Triggers can also be created using the Rust API:

```rust
use eventflux::query_api::definition::TriggerDefinition;
use eventflux::query_api::expression::constant::TimeUtil;

// Start trigger
let start_trigger = TriggerDefinition::id("StartTrigger".to_string())
    .at("start".to_string());

// Periodic trigger (50ms interval)
let periodic_trigger = TriggerDefinition::id("PeriodicTrigger".to_string())
    .at_every_time_constant(TimeUtil::millisec(50))
    .unwrap();

// Cron trigger
let cron_trigger = TriggerDefinition::id("CronTrigger".to_string())
    .at("*/1 * * * * *".to_string());

// Add to application
app.add_trigger_definition(start_trigger);
app.add_trigger_definition(periodic_trigger);
app.add_trigger_definition(cron_trigger);
```

---

## Next Steps

- **[Windows](/docs/sql-reference/windows)** - Combine triggers with window-based aggregations
- **[Aggregations](/docs/sql-reference/aggregations)** - Aggregate data from trigger-driven batches
- **[Functions](/docs/sql-reference/functions)** - Use time functions with triggers
