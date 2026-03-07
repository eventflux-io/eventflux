---
sidebar_position: 3
title: Aggregations
description: Aggregate functions and GROUP BY in EventFlux
---

# Aggregations

Aggregations allow you to compute summary statistics over groups of events. EventFlux provides a comprehensive set of aggregate functions that work with windows and GROUP BY.

## Aggregate Functions

### Core Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all events | `COUNT(*)` |
| `COUNT(attr)` | Count non-null values | `COUNT(price)` |
| `SUM(attr)` | Sum of values | `SUM(volume)` |
| `AVG(attr)` | Average of values | `AVG(price)` |
| `MIN(attr)` | Minimum value in window | `MIN(temperature)` |
| `MAX(attr)` | Maximum value in window | `MAX(temperature)` |
| `FIRST(attr)` | First value in window | `FIRST(price)` |
| `LAST(attr)` | Last/most recent value in window | `LAST(price)` |

### Statistical Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `STDDEV(attr)` | Standard deviation (Welford's algorithm) | `STDDEV(price)` |
| `VARIANCE(attr)` | Variance | `VARIANCE(latency)` |

### Forever Aggregates

These aggregates track all-time values across all windows and never reset:

| Function | Description | Example |
|----------|-------------|---------|
| `MINFOREVER(attr)` | All-time minimum (never resets) | `MINFOREVER(price)` |
| `MAXFOREVER(attr)` | All-time maximum (never resets) | `MAXFOREVER(price)` |

```sql
-- Track all-time high and low prices alongside windowed stats
SELECT symbol,
       MIN(price) AS window_low,      -- Resets each window
       MAX(price) AS window_high,     -- Resets each window
       MINFOREVER(price) AS all_time_low,   -- Never resets
       MAXFOREVER(price) AS all_time_high   -- Never resets
FROM Trades
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO PriceExtremes;
```

### Boolean Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `AND(attr)` | Logical AND across window (true if all true) | `AND(is_valid)` |
| `OR(attr)` | Logical OR across window (true if any true) | `OR(is_fraud)` |

```sql
-- Check if all transactions in window are valid, or if any are fraudulent
SELECT merchant_id,
       AND(is_valid) AS all_valid,       -- true only if ALL are valid
       OR(is_fraud) AS any_fraud,        -- true if ANY is fraudulent
       COUNT(*) AS txn_count
FROM Transactions
WINDOW TUMBLING(1 min)
GROUP BY merchant_id
INSERT INTO MerchantHealth;
```

### Distinct Aggregates

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(DISTINCT attr)` | Count unique values | `COUNT(DISTINCT user_id)` |
| `DISTINCTCOUNT(attr)` | Count unique values (alias) | `DISTINCTCOUNT(user_id)` |
| `SUM(DISTINCT attr)` | Sum unique values | `SUM(DISTINCT amount)` |

## Basic Usage

### Simple Aggregation

```sql
SELECT COUNT(*) AS total_events,
       SUM(volume) AS total_volume,
       AVG(price) AS avg_price
FROM Trades
WINDOW TUMBLING(1 min)
INSERT INTO TradeStats;
```

### With GROUP BY

```sql
SELECT symbol,
       COUNT(*) AS trade_count,
       SUM(volume) AS total_volume,
       AVG(price) AS avg_price,
       MIN(price) AS low_price,
       MAX(price) AS high_price
FROM Trades
WINDOW TUMBLING(5 min)
GROUP BY symbol
INSERT INTO SymbolStats;
```

### Multiple GROUP BY Columns

```sql
SELECT region,
       product_category,
       COUNT(*) AS order_count,
       SUM(amount) AS total_sales,
       AVG(amount) AS avg_order_value
FROM Orders
WINDOW TUMBLING(1 hour)
GROUP BY region, product_category
INSERT INTO RegionalSales;
```

## HAVING Clause

Filter groups based on aggregate conditions:

```sql
SELECT symbol,
       AVG(price) AS avg_price,
       COUNT(*) AS trade_count,
       SUM(volume) AS total_volume
FROM Trades
WINDOW TUMBLING(5 min)
GROUP BY symbol
HAVING COUNT(*) > 10
   AND AVG(price) > 50
   AND SUM(volume) > 1000
INSERT INTO ActiveStocks;
```

### Complex HAVING Conditions

```sql
SELECT sensor_id,
       AVG(temperature) AS avg_temp,
       MAX(temperature) AS max_temp,
       MIN(temperature) AS min_temp
FROM SensorReadings
WINDOW TUMBLING(10 min)
GROUP BY sensor_id
HAVING MAX(temperature) - MIN(temperature) > 20  -- High variance
    OR MAX(temperature) > 100                     -- Exceeds threshold
INSERT INTO AnomalySensors;
```

## Aggregation Examples

### Financial Analytics

```sql
-- OHLC (Open, High, Low, Close) calculation
SELECT symbol,
       FIRST(price) AS open,         -- First price in window
       MAX(price) AS high,           -- Highest price
       MIN(price) AS low,            -- Lowest price
       LAST(price) AS close,         -- Last price in window
       SUM(volume) AS total_volume,
       COUNT(*) AS tick_count,
       STDDEV(price) AS price_volatility
FROM MarketTicks
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO OHLCBars;
```

### All-Time Tracking

```sql
-- Track session highs/lows alongside window stats
SELECT symbol,
       LAST(price) AS current_price,
       MINFOREVER(price) AS session_low,    -- All-time low (never resets)
       MAXFOREVER(price) AS session_high,   -- All-time high (never resets)
       MIN(price) AS window_low,            -- Window low (resets)
       MAX(price) AS window_high            -- Window high (resets)
FROM Trades
WINDOW SLIDING(5 min)
GROUP BY symbol
INSERT INTO SessionStats;
```

### IoT Monitoring

```sql
-- Sensor health metrics
SELECT device_id,
       sensor_type,
       COUNT(*) AS reading_count,
       AVG(value) AS avg_value,
       STDDEV(value) AS value_stddev,
       MAX(value) - MIN(value) AS value_range
FROM SensorData
WINDOW TUMBLING(5 min)
GROUP BY device_id, sensor_type
INSERT INTO DeviceHealth;
```

### User Analytics

```sql
-- Session metrics per user
SELECT user_id,
       COUNT(*) AS page_views,
       COUNT(DISTINCT page_url) AS unique_pages,
       SUM(time_on_page) AS total_time
FROM PageViews
WINDOW SESSION(30 min)
GROUP BY user_id
INSERT INTO SessionMetrics;
```

### Performance Monitoring

```sql
-- API latency percentiles (approximation)
SELECT endpoint,
       COUNT(*) AS request_count,
       AVG(latency_ms) AS avg_latency,
       MIN(latency_ms) AS min_latency,
       MAX(latency_ms) AS max_latency,
       STDDEV(latency_ms) AS latency_stddev
FROM APIRequests
WINDOW TUMBLING(1 min)
GROUP BY endpoint
HAVING COUNT(*) > 100
INSERT INTO LatencyStats;
```

## Expressions with Aggregates

### Computed Aggregate Expressions

```sql
SELECT symbol,
       SUM(price * volume) / SUM(volume) AS vwap,  -- Volume-weighted average price
       MAX(price) - MIN(price) AS price_range,
       (MAX(price) - MIN(price)) / AVG(price) * 100 AS volatility_pct
FROM Trades
WINDOW TUMBLING(5 min)
GROUP BY symbol
INSERT INTO PriceAnalysis;
```

### Conditional Aggregation

```sql
SELECT symbol,
       COUNT(*) AS total_trades,
       SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
       SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_count,
       SUM(CASE WHEN side = 'BUY' THEN volume ELSE 0 END) AS buy_volume,
       SUM(CASE WHEN side = 'SELL' THEN volume ELSE 0 END) AS sell_volume
FROM Trades
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO BuySellAnalysis;
```

## Aggregation without Windows

When used without windows, aggregations apply to all events seen so far (running aggregates):

```sql
-- Running totals (use with caution - unbounded state)
SELECT symbol,
       COUNT(*) AS cumulative_trades,
       SUM(volume) AS cumulative_volume
FROM Trades
GROUP BY symbol
INSERT INTO RunningTotals;
```

:::caution Unbounded State
Aggregations without windows maintain state indefinitely. Use windows to bound state growth in production systems.
:::

## Best Practices

:::tip Aggregation Guidelines

1. **Always use windows** for bounded memory usage
2. **Filter before aggregating** to reduce computation
3. **Use appropriate precision** for floating-point aggregates
4. **Consider cardinality** of GROUP BY columns
5. **Monitor state size** for high-cardinality groupings

:::

### Efficient Query Design

```sql
-- Good: Filter early, then aggregate
SELECT symbol,
       AVG(price) AS avg_price
FROM Trades
WHERE volume > 100  -- Filter first
WINDOW TUMBLING(1 min)
GROUP BY symbol
INSERT INTO Stats;

-- Less efficient: Aggregate all, then filter
SELECT symbol,
       AVG(price) AS avg_price
FROM Trades
WINDOW TUMBLING(1 min)
GROUP BY symbol
HAVING AVG(price) > 0  -- Should have filtered earlier
INSERT INTO Stats;
```

## Next Steps

- **[Windows](/docs/sql-reference/windows)** - Window types for aggregations
- **[Joins](/docs/sql-reference/joins)** - Combining streams
- **[Functions](/docs/sql-reference/functions)** - Scalar functions
