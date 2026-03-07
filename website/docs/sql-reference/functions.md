---
sidebar_position: 6
title: Functions
description: Built-in functions reference for EventFlux
---

# Functions Reference

EventFlux provides a comprehensive set of built-in functions for data transformation, mathematical operations, string manipulation, and more.

## Function Types

EventFlux has two distinct types of functions:

### Scalar Functions (Stateless)

Scalar functions operate on individual values and return a single result. They are **stateless** - each invocation is independent and doesn't remember previous calls.

- Execute once per event
- No memory of previous events
- Can be used anywhere in SELECT, WHERE, HAVING clauses
- Examples: `UPPER()`, `ABS()`, `CONCAT()`, `ROUND()`

```sql
-- Scalar functions transform individual values
SELECT symbol,
       UPPER(name) AS upper_name,        -- String transformation
       ABS(delta) AS abs_delta,          -- Math operation
       ROUND(price, 2) AS rounded_price  -- Rounding
FROM Trades
INSERT INTO Processed;
```

### Aggregate Functions (Stateful)

Aggregate functions accumulate values over a group of events (typically within a window) and return a summary result. They are **stateful** - they maintain internal state that tracks values across multiple events.

- Accumulate values across multiple events
- Require a window or GROUP BY context
- State resets when window expires (except "forever" variants)
- Examples: `SUM()`, `AVG()`, `COUNT()`, `FIRST()`, `LAST()`

```sql
-- Aggregate functions summarize groups of events
SELECT symbol,
       SUM(volume) AS total_volume,      -- Accumulates over window
       AVG(price) AS avg_price,          -- Running average
       FIRST(price) AS open_price,       -- First value in window
       LAST(price) AS close_price        -- Most recent value
FROM Trades
WINDOW TUMBLING(5 min)
GROUP BY symbol
INSERT INTO Summary;
```

See [Aggregations](/docs/sql-reference/aggregations) for detailed coverage of aggregate functions.

## Mathematical Functions

### Basic Math

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `ABS(x)` | Absolute value | `ABS(-5)` | `5` |
| `CEIL(x)` | Round up | `CEIL(4.2)` | `5` |
| `FLOOR(x)` | Round down | `FLOOR(4.8)` | `4` |
| `ROUND(x)` | Round to nearest | `ROUND(4.5)` | `5` |
| `ROUND(x, n)` | Round to n decimals | `ROUND(4.567, 2)` | `4.57` |
| `TRUNC(x)` | Truncate to integer | `TRUNC(4.9)` | `4` |
| `TRUNC(x, n)` | Truncate to n decimals | `TRUNC(4.567, 2)` | `4.56` |
| `SIGN(x)` | Returns -1, 0, or 1 | `SIGN(-5)` | `-1` |
| `MOD(x, y)` | Modulo (remainder) | `MOD(17, 5)` | `2` |
| `MAXIMUM(a, b, ...)` | Maximum of values | `MAXIMUM(1, 5, 3)` | `5` |
| `MINIMUM(a, b, ...)` | Minimum of values | `MINIMUM(1, 5, 3)` | `1` |

### Advanced Math

| Function | Description | Example |
|----------|-------------|---------|
| `SQRT(x)` | Square root | `SQRT(16)` → `4` |
| `POWER(x, y)` | x raised to y | `POWER(2, 3)` → `8` |
| `EXP(x)` | e raised to x | `EXP(1)` → `2.718...` |
| `LN(x)` | Natural logarithm | `LN(2.718)` → `1` |
| `LOG(x)` | Base-10 logarithm | `LOG(100)` → `2` |
| `LOG(base, x)` | Logarithm with base | `LOG(2, 8)` → `3` |

### Trigonometric Functions

| Function | Description |
|----------|-------------|
| `SIN(x)` | Sine (radians) |
| `COS(x)` | Cosine (radians) |
| `TAN(x)` | Tangent (radians) |
| `ASIN(x)` | Arc sine |
| `ACOS(x)` | Arc cosine |
| `ATAN(x)` | Arc tangent |

### Example

```sql
SELECT sensor_id,
       ABS(delta) AS abs_delta,
       SQRT(variance) AS std_dev,
       ROUND(value, 2) AS rounded_value,
       POWER(growth_rate, 2) AS squared_growth
FROM Measurements
INSERT INTO Processed;
```

## String Functions

### Basic String Operations

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `LENGTH(s)` | String length | `LENGTH('hello')` | `5` |
| `UPPER(s)` | Uppercase | `UPPER('hello')` | `'HELLO'` |
| `LOWER(s)` | Lowercase | `LOWER('HELLO')` | `'hello'` |
| `TRIM(s)` | Remove whitespace (both ends) | `TRIM('  hi  ')` | `'hi'` |
| `LTRIM(s)` | Remove leading whitespace | `LTRIM('  hi  ')` | `'hi  '` |
| `RTRIM(s)` | Remove trailing whitespace | `RTRIM('  hi  ')` | `'  hi'` |

### String Manipulation

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `CONCAT(a, b, ...)` | Concatenate | `CONCAT('a', 'b', 'c')` | `'abc'` |
| `SUBSTRING(s, start, len)` | Extract substring | `SUBSTRING('hello', 1, 3)` | `'hel'` |
| `LEFT(s, n)` | Get leftmost n characters | `LEFT('hello', 3)` | `'hel'` |
| `RIGHT(s, n)` | Get rightmost n characters | `RIGHT('hello', 3)` | `'llo'` |
| `LPAD(s, len, pad)` | Left-pad string to length | `LPAD('hi', 5, '*')` | `'***hi'` |
| `RPAD(s, len, pad)` | Right-pad string to length | `RPAD('hi', 5, '*')` | `'hi***'` |
| `REPLACE(s, from, to)` | Replace text | `REPLACE('hello', 'l', 'x')` | `'hexxo'` |
| `REVERSE(s)` | Reverse string | `REVERSE('hello')` | `'olleh'` |
| `REPEAT(s, n)` | Repeat string n times | `REPEAT('ab', 3)` | `'ababab'` |

### String Searching

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `POSITION(sub, s)` | Find position (1-based) | `POSITION('ll', 'hello')` | `3` |
| `LOCATE(sub, s)` | Find position (alias) | `LOCATE('ll', 'hello')` | `3` |
| `INSTR(sub, s)` | Find position (alias) | `INSTR('ll', 'hello')` | `3` |
| `STARTS_WITH(s, prefix)` | Check prefix | `STARTS_WITH('hello', 'he')` | `true` |
| `ENDS_WITH(s, suffix)` | Check suffix | `ENDS_WITH('hello', 'lo')` | `true` |
| `CONTAINS(s, sub)` | Contains check | `CONTAINS('hello', 'ell')` | `true` |

### Character Functions

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `ASCII(s)` | ASCII code of first character | `ASCII('A')` | `65` |
| `CHR(n)` | Character from ASCII code | `CHR(65)` | `'A'` |
| `CHAR(n)` | Character from ASCII code (alias) | `CHAR(65)` | `'A'` |

### Example

```sql
SELECT user_id,
       UPPER(first_name) AS first_name,
       LOWER(email) AS email,
       CONCAT(first_name, ' ', last_name) AS full_name,
       LENGTH(description) AS desc_length
FROM Users
INSERT INTO ProcessedUsers;
```

## Conditional Functions

### CASE Expression

CASE expressions provide SQL-standard conditional logic with full support for both **Searched CASE** and **Simple CASE** syntax.

#### Searched CASE (Boolean Conditions)

Evaluates boolean conditions in order, returning the result of the first matching condition:

```sql
SELECT symbol, price,
       CASE
           WHEN price > 1000 THEN 'EXPENSIVE'
           WHEN price > 100 THEN 'MODERATE'
           WHEN price > 10 THEN 'CHEAP'
           ELSE 'PENNY'
       END AS price_category
FROM Stocks
INSERT INTO Categorized;
```

#### Simple CASE (Value Matching)

Compares an expression against multiple values:

```sql
SELECT symbol, status,
       CASE status
           WHEN 'ACTIVE' THEN 1
           WHEN 'PENDING' THEN 2
           WHEN 'INACTIVE' THEN 3
           ELSE 0
       END AS status_code
FROM Orders
INSERT INTO StatusCodes;
```

#### Nested CASE Expressions

CASE expressions can be nested for complex decision trees:

```sql
SELECT symbol, price, volume,
       CASE
           WHEN price > 100 THEN
               CASE
                   WHEN volume > 1000 THEN 'PREMIUM_HIGH_VOL'
                   ELSE 'PREMIUM_LOW_VOL'
               END
           ELSE
               CASE
                   WHEN volume > 1000 THEN 'BUDGET_HIGH_VOL'
                   ELSE 'BUDGET_LOW_VOL'
               END
       END AS classification
FROM Trades
INSERT INTO Classified;
```

#### CASE in WHERE Clause

Use CASE for conditional filtering:

```sql
SELECT *
FROM Transactions
WHERE CASE
          WHEN amount > 10000 THEN true
          ELSE false
      END
INSERT INTO HighValueTransactions;
```

#### Multiple CASE Expressions

Combine multiple CASE expressions in a single query:

```sql
SELECT symbol, price, volume,
       CASE
           WHEN price > 100 THEN 'EXPENSIVE'
           WHEN price > 50 THEN 'MODERATE'
           ELSE 'CHEAP'
       END AS price_tier,
       CASE
           WHEN volume > 1000 THEN 'HIGH_VOLUME'
           WHEN volume > 500 THEN 'MEDIUM_VOLUME'
           ELSE 'LOW_VOLUME'
       END AS volume_tier
FROM MarketData
INSERT INTO Tiered;
```

#### CASE with Complex Expressions

Use arithmetic and logical expressions in CASE conditions:

```sql
SELECT symbol, price, volume,
       CASE
           WHEN price * volume > 100000 THEN 'MEGA_TRADE'
           WHEN price * volume > 10000 THEN 'LARGE_TRADE'
           ELSE 'SMALL_TRADE'
       END AS trade_size
FROM Trades
INSERT INTO SizedTrades;
```

:::tip CASE Expression Tips
- CASE evaluates conditions in order and returns the first match (short-circuit evaluation)
- Always include ELSE to handle unmatched cases (defaults to NULL if omitted)
- All result expressions must return compatible types
- Simple CASE uses equality comparison; use Searched CASE for complex conditions
:::

### COALESCE

Returns the first non-null value:

```sql
SELECT user_id,
       COALESCE(nickname, username, email, 'Anonymous') AS display_name
FROM Users
INSERT INTO DisplayNames;
```

### NULLIF

Returns null if values are equal:

```sql
SELECT order_id,
       NULLIF(status, 'UNKNOWN') AS valid_status,
       total / NULLIF(quantity, 0) AS unit_price  -- Avoid division by zero
FROM Orders
INSERT INTO Processed;
```

### IF / IIF

Conditional value selection:

```sql
SELECT symbol,
       price,
       IF(price > previous_price, 'UP', 'DOWN') AS direction,
       IIF(volume > avg_volume, 'HIGH', 'NORMAL') AS volume_status
FROM Trades
INSERT INTO Analysis;
```

## Type Conversion Functions

### CAST

Convert between types:

```sql
SELECT
    CAST(price AS INT) AS price_int,
    CAST(quantity AS DOUBLE) AS quantity_double,
    CAST(timestamp AS STRING) AS timestamp_str
FROM Orders
INSERT INTO Converted;
```

### Supported Conversions

| From | To | Example |
|------|-----|---------|
| INT | DOUBLE | `CAST(42 AS DOUBLE)` → `42.0` |
| DOUBLE | INT | `CAST(42.9 AS INT)` → `42` |
| INT | STRING | `CAST(42 AS STRING)` → `'42'` |
| STRING | INT | `CAST('42' AS INT)` → `42` |
| BOOL | INT | `CAST(true AS INT)` → `1` |

## Aggregate Functions

See [Aggregations](/docs/sql-reference/aggregations) for detailed coverage.

| Function | Description | Resets on Window? |
|----------|-------------|-------------------|
| `COUNT(*)` | Count all events | Yes |
| `COUNT(attr)` | Count non-null values | Yes |
| `COUNT(DISTINCT attr)` | Count unique values | Yes |
| `DISTINCTCOUNT(attr)` | Count unique values (alias) | Yes |
| `SUM(attr)` | Sum of values | Yes |
| `SUM(DISTINCT attr)` | Sum of unique values | Yes |
| `AVG(attr)` | Average of values | Yes |
| `MIN(attr)` | Minimum value in window | Yes |
| `MAX(attr)` | Maximum value in window | Yes |
| `FIRST(attr)` | First value in window | Yes |
| `LAST(attr)` | Last/most recent value in window | Yes |
| `STDDEV(attr)` | Standard deviation (Welford's algorithm) | Yes |
| `VARIANCE(attr)` | Variance of values | Yes |
| `AND(attr)` | Logical AND (true if all true) | Yes |
| `OR(attr)` | Logical OR (true if any true) | Yes |
| `MINFOREVER(attr)` | All-time minimum (never resets) | **No** |
| `MAXFOREVER(attr)` | All-time maximum (never resets) | **No** |

:::tip Forever Aggregates
`MINFOREVER` and `MAXFOREVER` track all-time values that persist across window boundaries. Useful for tracking session highs/lows or all-time records.
:::

## Date/Time Functions

### Current Time

| Function | Description |
|----------|-------------|
| `NOW()` | Current timestamp in milliseconds |

```sql
SELECT sensor_id,
       NOW() AS processing_time,
       value
FROM SensorData
INSERT INTO Timestamped;
```

### Time Extraction

```sql
SELECT event_id,
       EXTRACT(YEAR FROM timestamp) AS year,
       EXTRACT(MONTH FROM timestamp) AS month,
       EXTRACT(DAY FROM timestamp) AS day,
       EXTRACT(HOUR FROM timestamp) AS hour,
       EXTRACT(MINUTE FROM timestamp) AS minute
FROM Events
INSERT INTO TimeParts;
```

### Time Arithmetic

```sql
SELECT event_id,
       timestamp,
       timestamp + INTERVAL '1' HOUR AS plus_one_hour,
       timestamp - INTERVAL '30' MINUTE AS minus_thirty_min
FROM Events
INSERT INTO AdjustedTimes;
```

## Utility Functions

### NULL Handling

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `COALESCE(a, b, ...)` | First non-null value | `COALESCE(NULL, 'hi')` | `'hi'` |
| `DEFAULT(val, default)` | Return default if null | `DEFAULT(NULL, 0)` | `0` |
| `IFNULL(val, default)` | Return default if null (alias) | `IFNULL(NULL, 0)` | `0` |
| `NULLIF(a, b)` | Return null if equal | `NULLIF(5, 5)` | `NULL` |

```sql
SELECT
    COALESCE(a, b, c, 'default') AS first_non_null,
    DEFAULT(value, 0) AS value_or_zero,
    IFNULL(value, 0) AS also_value_or_zero,
    NULLIF(status, 'UNKNOWN') AS null_if_unknown
FROM Data
INSERT INTO Processed;
```

:::tip Numeric Type Widening
`DEFAULT()` and `IFNULL()` support automatic numeric type widening. You can use a LONG default with an INT value, or a DOUBLE default with a FLOAT value:
```sql
SELECT DEFAULT(int_column, 999999999999) AS widened_to_long,
       DEFAULT(float_column, 3.14159265358979) AS widened_to_double
FROM Data;
```
:::

### Type Checking

```sql
SELECT
    value,
    IS_NULL(value) AS is_null,
    IS_NOT_NULL(value) AS is_not_null
FROM Data
INSERT INTO Checks;
```

## Examples

### Financial Calculations

```sql
SELECT symbol,
       price,
       volume,
       price * volume AS notional,
       ROUND(price * volume / 1000000, 2) AS notional_millions,
       ABS(price - previous_close) / previous_close * 100 AS pct_change
FROM MarketData
INSERT INTO Calculations;
```

### Text Processing

```sql
SELECT order_id,
       UPPER(TRIM(customer_name)) AS customer_name,
       CONCAT(
           SUBSTRING(phone, 1, 3), '-',
           SUBSTRING(phone, 4, 3), '-',
           SUBSTRING(phone, 7, 4)
       ) AS formatted_phone
FROM Orders
INSERT INTO FormattedOrders;
```

### Data Cleansing

```sql
SELECT user_id,
       COALESCE(NULLIF(TRIM(email), ''), 'unknown@example.com') AS email,
       CASE
           WHEN age < 0 THEN NULL
           WHEN age > 150 THEN NULL
           ELSE age
       END AS valid_age,
       UPPER(COALESCE(country, 'UNKNOWN')) AS country
FROM RawUsers
INSERT INTO CleanedUsers;
```

## Best Practices

:::tip Function Usage

1. **Use COALESCE for defaults** - Handle nulls gracefully
2. **Validate before CAST** - Avoid runtime errors
3. **Filter early** - Apply functions after WHERE when possible
4. **Use appropriate precision** - ROUND for display, keep full precision for calculations

:::

## Next Steps

- **[SQL Reference](/docs/sql-reference/queries)** - Complete query syntax
- **[Aggregations](/docs/sql-reference/aggregations)** - Aggregate functions
- **[Patterns](/docs/sql-reference/patterns)** - Use functions in pattern filters
