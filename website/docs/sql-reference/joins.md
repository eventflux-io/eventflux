---
sidebar_position: 4
title: Joins
description: Stream-to-stream and stream-to-table joins in EventFlux
---

# Joins

Joins allow you to combine events from multiple streams or enrich streams with table data. EventFlux supports all standard SQL join types.

## Join Types

| Type | Description | Matching |
|------|-------------|----------|
| `JOIN` / `INNER JOIN` | Only matching events | Both sides required |
| `LEFT JOIN` | All left events | Right may be null |
| `RIGHT JOIN` | All right events | Left may be null |
| `FULL JOIN` | All events | Either side may be null |

## Stream-to-Stream Joins

### Basic Join Syntax

```sql
SELECT a.column1, b.column2
FROM StreamA AS a
WINDOW TUMBLING(duration)
JOIN StreamB AS b
  ON a.key = b.key
INSERT INTO Output;
```

### Inner Join

Returns only matching events from both streams:

```sql
DEFINE STREAM Trades (symbol STRING, price DOUBLE, volume INT);
DEFINE STREAM Quotes (symbol STRING, bid DOUBLE, ask DOUBLE);

SELECT t.symbol,
       t.price AS trade_price,
       q.bid,
       q.ask,
       t.price - q.bid AS spread
FROM Trades AS t
WINDOW TUMBLING(1 sec)
JOIN Quotes AS q
  ON t.symbol = q.symbol
INSERT INTO TradeQuoteMatches;
```

### Left Join

Returns all events from the left stream, with matching right events (or null):

```sql
SELECT t.symbol,
       t.price,
       t.volume,
       COALESCE(r.risk_score, 0) AS risk_score
FROM Trades AS t
WINDOW TUMBLING(5 sec)
LEFT JOIN RiskScores AS r
  ON t.symbol = r.symbol
INSERT INTO TradesWithRisk;
```

### Right Join

Returns all events from the right stream:

```sql
SELECT COALESCE(t.trade_id, 'NO_TRADE') AS trade_id,
       o.order_id,
       o.symbol,
       o.quantity
FROM Trades AS t
WINDOW TUMBLING(10 sec)
RIGHT JOIN Orders AS o
  ON t.order_id = o.order_id
INSERT INTO OrderFillStatus;
```

### Full Outer Join

Returns all events from both streams:

```sql
SELECT COALESCE(a.symbol, b.symbol) AS symbol,
       a.price AS price_a,
       b.price AS price_b
FROM ExchangeA AS a
WINDOW TUMBLING(1 sec)
FULL JOIN ExchangeB AS b
  ON a.symbol = b.symbol
INSERT INTO CrossExchangePrices;
```

## Multiple Join Conditions

```sql
SELECT t.symbol,
       t.price,
       q.bid,
       q.ask
FROM Trades AS t
WINDOW TUMBLING(1 sec)
JOIN Quotes AS q
  ON t.symbol = q.symbol
 AND t.exchange = q.exchange
INSERT INTO MatchedData;
```

## Stream-to-Table Joins

Enrich streaming data with reference tables:

```sql
DEFINE STREAM Orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT
);

DEFINE TABLE Customers (
    customer_id STRING,
    name STRING,
    tier STRING
);

DEFINE TABLE Products (
    product_id STRING,
    name STRING,
    price DOUBLE
);

-- Enrich orders with customer and product info
SELECT o.order_id,
       c.name AS customer_name,
       c.tier AS customer_tier,
       p.name AS product_name,
       o.quantity,
       o.quantity * p.price AS total_value
FROM Orders AS o
JOIN Customers AS c
  ON o.customer_id = c.customer_id
JOIN Products AS p
  ON o.product_id = p.product_id
INSERT INTO EnrichedOrders;
```

## Stream and Table Aliases

Aliases provide shorthand names for streams and tables in joins, making queries more readable and enabling self-joins.

### Basic Alias Syntax

Use `AS` to assign an alias to a stream or table:

```sql
SELECT s.symbol, s.price, t.description
FROM StockStream AS s
WINDOW TUMBLING(1 sec)
JOIN StockTable AS t
  ON s.symbol = t.symbol
INSERT INTO EnrichedStock;
```

### Self-Joins

Aliases are required when joining a stream or table with itself. This is useful for comparing events within the same stream:

```sql
DEFINE STREAM StockStream (symbol STRING, price DOUBLE, volume INT);

-- Compare consecutive price changes for the same symbol
SELECT a.symbol,
       a.price AS price1,
       b.price AS price2,
       b.price - a.price AS price_diff
FROM StockStream AS a
WINDOW TUMBLING(5 sec)
JOIN StockStream AS b
  ON a.symbol = b.symbol
WHERE b.price > a.price
INSERT INTO PriceIncreases;
```

### Mixed Alias Usage

You can alias only the streams/tables you need:

```sql
-- Alias left stream only
SELECT s.symbol, s.price, StockTable.description
FROM StockStream AS s
WINDOW TUMBLING(1 sec)
JOIN StockTable
  ON s.symbol = StockTable.symbol
INSERT INTO Output;

-- Alias right stream only
SELECT StockStream.symbol, t.description
FROM StockStream
WINDOW TUMBLING(1 sec)
JOIN StockTable AS t
  ON StockStream.symbol = t.symbol
INSERT INTO Output;
```

### Alias Scope

- Aliases are scoped to the query where they are defined
- Once aliased, use the alias (not the original name) to reference columns
- Aliases work in SELECT, ON, WHERE, GROUP BY, and HAVING clauses

```sql
SELECT a.symbol,
       a.price,
       b.bid,
       b.ask
FROM Trades AS a
WINDOW TUMBLING(1 sec)
JOIN Quotes AS b
  ON a.symbol = b.symbol
WHERE a.price > b.ask  -- Use aliases in WHERE
GROUP BY a.symbol      -- Use aliases in GROUP BY
INSERT INTO Analysis;
```

## Join with Aggregations

Combine joins with window aggregations:

```sql
SELECT a.symbol,
       SUM(a.volume) AS total_volume_a,
       SUM(b.volume) AS total_volume_b,
       AVG(a.price) AS avg_price_a,
       AVG(b.price) AS avg_price_b
FROM ExchangeA AS a
WINDOW TUMBLING(1 min)
JOIN ExchangeB AS b
  ON a.symbol = b.symbol
GROUP BY a.symbol
INSERT INTO VolumeComparison;
```

## Join Examples

### Price Arbitrage Detection

```sql
-- Find arbitrage opportunities across exchanges
SELECT a.symbol,
       a.price AS price_a,
       b.price AS price_b,
       ABS(a.price - b.price) AS spread,
       ABS(a.price - b.price) / a.price * 100 AS spread_pct
FROM ExchangeA AS a
WINDOW TUMBLING(1 sec)
JOIN ExchangeB AS b
  ON a.symbol = b.symbol
WHERE ABS(a.price - b.price) / a.price > 0.001
INSERT INTO ArbitrageOpportunities;
```

### Order-Trade Matching

```sql
-- Match orders with their executions
SELECT o.order_id,
       o.symbol,
       o.quantity AS ordered_qty,
       COALESCE(SUM(t.quantity), 0) AS filled_qty,
       o.quantity - COALESCE(SUM(t.quantity), 0) AS remaining_qty
FROM Orders AS o
WINDOW TUMBLING(1 min)
LEFT JOIN Trades AS t
  ON o.order_id = t.order_id
GROUP BY o.order_id, o.symbol, o.quantity
INSERT INTO OrderStatus;
```

### Multi-Source Sensor Fusion

```sql
-- Combine readings from multiple sensor types
SELECT t.device_id,
       t.temperature,
       h.humidity,
       p.pressure,
       t.timestamp
FROM TemperatureSensors AS t
WINDOW TUMBLING(10 sec)
LEFT JOIN HumiditySensors AS h
  ON t.device_id = h.device_id
LEFT JOIN PressureSensors AS p
  ON t.device_id = p.device_id
INSERT INTO FusedSensorData;
```

### Customer 360 View

```sql
-- Combine transaction with customer profile
SELECT t.transaction_id,
       t.amount,
       t.timestamp,
       c.name,
       c.segment,
       c.lifetime_value,
       CASE
           WHEN t.amount > c.avg_transaction * 3 THEN 'HIGH'
           ELSE 'NORMAL'
       END AS risk_flag
FROM Transactions AS t
JOIN CustomerProfiles AS c
  ON t.customer_id = c.customer_id
INSERT INTO EnrichedTransactions;
```

## Join Behavior

### Window Requirements

Stream-to-stream joins require a window to bound the join:

```sql
-- Window defines the join scope
FROM StreamA AS a
WINDOW TUMBLING(5 sec)  -- Events within 5-second windows are joined
JOIN StreamB AS b
  ON a.key = b.key
```

### Null Handling

```sql
-- Handle nulls from outer joins
SELECT COALESCE(a.value, 0) AS value_a,
       COALESCE(b.value, 0) AS value_b,
       COALESCE(a.value, 0) + COALESCE(b.value, 0) AS total
FROM StreamA AS a
WINDOW TUMBLING(1 sec)
FULL JOIN StreamB AS b
  ON a.key = b.key
INSERT INTO Combined;
```

### WHERE on Joins

`WHERE` runs on each joined row, after the join. It can use columns and aliases from both sides:

```sql
SELECT b.symbol, b.price AS bid, a.price AS ask
FROM Bids AS b
JOIN Asks AS a
  ON b.symbol = a.symbol
WHERE b.price >= a.price
INSERT INTO Crossed;
```

On an outer join, this follows standard SQL. A `WHERE` condition on the side that has no match sees `NULL` there, so it drops the unmatched row. Put the condition in `ON` to keep the unmatched rows.

## Best Practices

:::tip Join Optimization

1. **Use appropriate window sizes** - Smaller windows reduce memory usage
2. **Know where WHERE runs** - It filters joined rows after the join, so it does not shrink the join state
3. **Index join keys** - For table joins, ensure keys are indexed
4. **Monitor state size** - Joins maintain state for the window duration

:::

:::caution Performance Considerations

- **Cardinality explosion** - Be careful with many-to-many joins
- **Memory usage** - Large windows with high-throughput streams consume more memory
- **Late arrivals** - Consider delay windows for out-of-order events

:::

## Next Steps

- **[Windows](/docs/sql-reference/windows)** - Window types for joins
- **[Patterns](/docs/sql-reference/patterns)** - Pattern detection across streams
- **[Aggregations](/docs/sql-reference/aggregations)** - Aggregate joined data
