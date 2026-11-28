# Migration Guide: Siddhi to EventFlux

This guide documents the syntax differences between Siddhi (Java) and EventFlux (Rust) query languages, helping
developers migrate queries from one to the other.

---

## Quick Reference Table

| Feature           | Siddhi Syntax                    | EventFlux Syntax                     |
|-------------------|----------------------------------|--------------------------------------|
| Stream Definition | `define stream Name (...)`       | `CREATE STREAM Name (...)`           |
| Table Definition  | `define table Name (...)`        | `CREATE TABLE Name (...)`            |
| Trigger (start)   | `define trigger T at start`      | `CREATE TRIGGER T AT START`          |
| Trigger (periodic)| `define trigger T at every 5 sec`| `CREATE TRIGGER T AT EVERY 5 SECONDS`|
| Trigger (cron)    | `define trigger T at '* * *'`    | `CREATE TRIGGER T AT CRON '* * *'`   |
| Window (length)   | `#window.length(4)`              | `WINDOW('length', 4)`                |
| Window (time)     | `#window.time(1 sec)`            | `WINDOW('time', 1 SECOND)`           |
| Insert Statement  | `insert into StreamName`         | `INSERT INTO StreamName`             |
| Filter            | `from Stream[price > 100]`       | `FROM Stream WHERE price > 100`      |
| Pattern           | `from e1=S1 -> e2=S2`            | `FROM PATTERN (e1=S1 -> e2=S2)`      |
| Query Annotation  | `@info(name = 'q1')`             | Not required                         |
| App Annotation    | `@app:name('AppName')`           | Not required                         |

---

## 1. Stream and Table Definitions

### 1.1 Stream Definition

**Siddhi:**

```sql
define
stream cseEventStream (symbol string, price float, volume int);
```

**EventFlux:**

```sql
CREATE
STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);
```

**Key Differences:**

- `define stream` → `CREATE STREAM`
- Type names are uppercase in EventFlux: `string` → `STRING`, `float` → `FLOAT`, `int` → `INT`
- `long` in Siddhi → `BIGINT` in EventFlux

### 1.2 Table Definition

**Siddhi:**

```sql
define
table StockTable (symbol string, price float, volume long);
```

**EventFlux:**

```sql
CREATE TABLE StockTable
(
    symbol STRING,
    price  FLOAT,
    volume BIGINT
);
```

### 1.3 Trigger Definition

Triggers generate events at specified times or intervals.

**Siddhi:**

```sql
-- Start trigger (fires once at app start)
define trigger StartTrigger at start;

-- Periodic trigger
define trigger FiveSecTrigger at every 5 sec;

-- Cron trigger
define trigger CronTrigger at '*/1 * * * * *';
```

**EventFlux:**

```sql
-- Start trigger (fires once at app start)
CREATE TRIGGER StartTrigger AT START;

-- Periodic trigger (readable time units)
CREATE TRIGGER FiveSecTrigger AT EVERY 5 SECONDS;
CREATE TRIGGER MillisTrigger AT EVERY 50 MILLISECONDS;
CREATE TRIGGER MinuteTrigger AT EVERY 1 MINUTE;
CREATE TRIGGER HourTrigger AT EVERY 2 HOURS;

-- Cron trigger (explicit CRON keyword)
CREATE TRIGGER CronTrigger AT CRON '*/1 * * * * *';
```

**Key Differences:**

- `define trigger` → `CREATE TRIGGER`
- Time units are explicit: `5 sec` → `5 SECONDS`, `50 ms` → `50 MILLISECONDS`
- Cron expressions require `CRON` keyword: `at '...'` → `AT CRON '...'`
- Supported time units: `MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`

**Triggers as Stream Sources:**

EventFlux allows triggers to be used as input sources in queries:

```sql
CREATE TRIGGER HeartbeatTrigger AT EVERY 1 SECOND;
CREATE STREAM TimestampStream (ts BIGINT);

INSERT INTO TimestampStream
SELECT currentTimeMillis() AS ts FROM HeartbeatTrigger;
```

---

## 2. Data Types

| Siddhi Type | EventFlux Type | Notes                           |
|-------------|----------------|---------------------------------|
| `string`    | `STRING`       |                                 |
| `int`       | `INT`          | 32-bit integer                  |
| `long`      | `BIGINT`       | 64-bit integer                  |
| `float`     | `FLOAT`        | 32-bit float                    |
| `double`    | `DOUBLE`       | 64-bit float                    |
| `bool`      | `BOOLEAN`      | Boolean (use BOOLEAN, not BOOL) |
| `object`    | `OBJECT`       | Generic object                  |

---

## 3. Query Structure

### 3.1 Basic Query

**Siddhi:**

```sql
@
info
(name = 'query1')
from cseEventStream[price > 100]
select symbol,
       price
    insert
into outputStream;
```

**EventFlux:**

```sql
INSERT INTO outputStream
SELECT symbol, price
FROM cseEventStream
WHERE price > 100;
```

**Key Differences:**

- EventFlux uses standard SQL order: `INSERT INTO ... SELECT ... FROM ... WHERE`
- Siddhi uses reverse order: `from ... select ... insert into`
- EventFlux filter uses `WHERE` clause, Siddhi uses `[condition]` syntax
- `@info` annotations are not needed in EventFlux

### 3.2 Query with Aggregation

**Siddhi:**

```sql
from stockStream#window.length(5)
select symbol,
       sum(price) as totalPrice group by symbol
insert
into outputStream;
```

**EventFlux:**

```sql
INSERT INTO outputStream
SELECT symbol, sum(price) AS totalPrice
FROM stockStream WINDOW('length', 5)
GROUP BY symbol;
```

---

## 4. Window Syntax

### 4.1 Length Window

**Siddhi:**

```sql
from stream#window.length(4)
```

**EventFlux:**

```sql
FROM stream WINDOW('length', 4)
```

### 4.2 Time Window

**Siddhi:**

```sql
from stream#window.time(1 sec)
from stream#window.time(500 milliseconds)
from stream#window.time(1 min)
```

**EventFlux:**

```sql
FROM stream WINDOW('time', 1 SECOND)
FROM stream WINDOW('time', 500 MILLISECONDS)
FROM stream WINDOW('time', 1 MINUTE)
```

**Note:** EventFlux uses readable time units (MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, WEEKS).

### 4.3 Length Batch Window

**Siddhi:**

```sql
from stream#window.lengthBatch(4)
```

**EventFlux:**

```sql
FROM stream WINDOW('lengthBatch', 4)
```

### 4.4 Time Batch Window

**Siddhi:**

```sql
from stream#window.timeBatch(1 sec)
```

**EventFlux:**

```sql
FROM stream WINDOW('timeBatch', 1 SECOND)
```

### 4.5 External Time Window

**Siddhi:**

```sql
from stream#window.externalTime(timestamp, 1 sec)
```

**EventFlux:**

```sql
FROM stream WINDOW('externalTime', timestamp, 1 SECOND)
```

### 4.6 Session Window

**Siddhi:**

```sql
from stream#window.session(5 sec, symbol)
```

**EventFlux:**

```sql
FROM stream WINDOW('session', 5 SECONDS, symbol)
```

### 4.7 Sort Window

**Siddhi:**

```sql
from stream#window.sort(5, price, 'desc')
```

**EventFlux:**

```sql
FROM stream WINDOW('sort', 5, price, 'desc')
```

---

## 5. Filter Conditions

### 5.1 Basic Filter

**Siddhi:**

```sql
from stream[price > 100]
select symbol,
       price
    insert
into output;
```

**EventFlux:**

```sql
INSERT INTO output
SELECT symbol, price
FROM stream
WHERE price > 100;
```

### 5.2 Combined Filters

**Siddhi:**

```sql
from stream[price > 100 and volume > 50]
```

**EventFlux:**

```sql
FROM stream WHERE price > 100 AND volume > 50
```

### 5.3 String Comparison

**Siddhi:**

```sql
from stream[symbol == 'IBM']
```

**EventFlux:**

```sql
FROM stream WHERE symbol = 'IBM'
```

**Note:** Siddhi uses `==` for equality, EventFlux uses `=`.

### 5.4 Not Equal

**Siddhi:**

```sql
from stream[symbol != 'IBM']
```

**EventFlux:**

```sql
FROM stream WHERE symbol != 'IBM'
-- or
FROM stream WHERE symbol <> 'IBM'
```

---

## 6. Pattern Matching

### 6.1 Simple Sequence (Followed-by)

**Siddhi:**

```sql
from e1=Stream1 -> e2=Stream2
select e1.symbol as symbol1,
       e2.symbol as symbol2
    insert
into output;
```

**EventFlux:**

```sql
INSERT INTO output
SELECT e1.symbol AS symbol1, e2.symbol AS symbol2
FROM PATTERN(e1 = Stream1 - > e2 = Stream2);
```

### 6.2 Pattern with Filter

**Siddhi:**

```sql
from e1=Stream1[price > 100] -> e2=Stream2
```

**EventFlux:**

```sql
FROM PATTERN (e1=Stream1 -> e2=Stream2)
WHERE e1.price > 100
```

**Note:** EventFlux does not apply inline element filters in sequence patterns yet, so use WHERE. WHERE filters the finished match. It does not choose which event the pattern takes, so add `EVERY` to keep matching after a match fails the WHERE.

### 6.3 Logical AND Pattern

**Siddhi:**

```sql
from e1=Stream1 and e2=Stream2
```

**EventFlux:**

```sql
FROM PATTERN (e1=Stream1 AND e2=Stream2)
```

### 6.4 Logical OR Pattern

**Siddhi:**

```sql
from e1=Stream1 or e2=Stream2
```

**EventFlux:**

```sql
FROM PATTERN (e1=Stream1 OR e2=Stream2)
```

### 6.5 Within Clause

**Siddhi:**

```sql
from e1=Stream1 -> e2=Stream2 within 1 sec
```

**EventFlux:**

```sql
FROM PATTERN (e1=Stream1 -> e2=Stream2) WITHIN 1 SECOND
```

### 6.6 EVERY Pattern

**Siddhi:**

```sql
from every e1=Stream1 -> e2=Stream2
```

**EventFlux:**

```sql
FROM PATTERN EVERY (e1=Stream1 -> e2=Stream2)
```

**Note:** In EventFlux, `EVERY` wraps the entire pattern rather than prefixing individual elements.

**Status:** EVERY pattern syntax is still under investigation - may require different placement.

---

## 7. Join Operations

### 7.1 Inner Join

**Siddhi:**

```sql
from stream1#window.length(5) as a
    join stream2#window.length(5) as b
    on a.symbol == b.symbol
select a.symbol,
       a.price,
       b.volume
    insert
into output;
```

**EventFlux:**

```sql
INSERT INTO output
SELECT stream1.symbol, stream1.price, stream2.volume
FROM stream1 WINDOW('length', 5)
         JOIN stream2 WINDOW('length', 5) ON stream1.symbol = stream2.symbol;
```

**Note:** EventFlux currently uses full stream names rather than aliases in join conditions.

### 7.2 Left Outer Join

**Siddhi:**

```sql
from stream1#window.length(5) as a
    left outer join stream2#window.length(5) as b
    on a.symbol == b.symbol
```

**EventFlux:**

```sql
FROM stream1 WINDOW('length', 5)
LEFT OUTER JOIN stream2 WINDOW('length', 5)
ON stream1.symbol = stream2.symbol
```

### 7.3 Right Outer Join

**Siddhi:**

```sql
right outer join
```

**EventFlux:**

```sql
RIGHT OUTER JOIN
```

### 7.4 Full Outer Join

**Siddhi:**

```sql
full outer join
```

**EventFlux:**

```sql
FULL OUTER JOIN
```

---

## 8. Aggregation Functions

Both implementations support the same core aggregation functions:

| Function       | Siddhi                  | EventFlux                    | Notes                   |
|----------------|-------------------------|------------------------------|-------------------------|
| Sum            | `sum(price)`            | `sum(price)`                 | Same                    |
| Average        | `avg(price)`            | `avg(price)`                 | Same                    |
| Count          | `count()`               | `count()`                    | Same                    |
| Min            | `min(price)`            | `min(price)`                 | Same                    |
| Max            | `max(price)`            | `max(price)`                 | Same                    |
| Distinct Count | `distinctCount(symbol)` | `distinctCount(symbol)`      | Same                    |
| Std Dev        | `stdDev(price)`         | Not yet as window aggregator | Only pattern collection |

---

## 9. Built-in Functions

### 9.1 String Functions

| Function    | Siddhi                | EventFlux            | Status                           |
|-------------|-----------------------|----------------------|----------------------------------|
| Concatenate | `str:concat(a, b)`    | `concat(a, ' ', b)`  | Working                          |
| Upper case  | `str:upper(s)`        | `upper(s)`           | Working                          |
| Lower case  | `str:lower(s)`        | `lower(s)`           | Working                          |
| Length      | `str:length(s)`       | `length(s)`          | Working                          |
| Substring   | `str:substr(s, i, j)` | `substring(s, i, j)` | Not supported (SQL parser issue) |

### 9.2 Math Functions

| Function    | Siddhi             | EventFlux     | Status                     |
|-------------|--------------------|---------------|----------------------------|
| Round       | `math:round(x)`    | `round(x)`    | Working (no precision arg) |
| Absolute    | `math:abs(x)`      | `abs(x)`      | Not registered             |
| Square root | `math:sqrt(x)`     | `sqrt(x)`     | Working                    |
| Log         | `math:log(x)`      | `log(x)`      | Not supported (SQL parser) |
| Sin         | `math:sin(x)`      | `sin(x)`      | Not supported (SQL parser) |
| Tan         | `math:tan(x)`      | `tan(x)`      | Not supported (SQL parser) |
| Power       | `math:power(x, y)` | `power(x, y)` | Not supported              |
| Floor       | `math:floor(x)`    | `floor(x)`    | Parsed as DateTimeField    |
| Ceil        | `math:ceil(x)`     | `ceil(x)`     | Parsed as DateTimeField    |

### 9.3 Utility Functions

| Function        | Siddhi                | EventFlux             | Status  |
|-----------------|-----------------------|-----------------------|---------|
| Coalesce        | `coalesce(a, b)`      | `coalesce(a, b)`      | Working |
| UUID            | `UUID()`              | `uuid()`              | Working |
| Event Timestamp | `eventTimestamp()`    | `eventTimestamp()`    | Working |
| Current Time    | `currentTimeMillis()` | `currentTimeMillis()` | Working |

---

## 10. Arithmetic Operations

| Operation      | Siddhi  | EventFlux | Status                   |
|----------------|---------|-----------|--------------------------|
| Addition       | `a + b` | `a + b`   | Working                  |
| Subtraction    | `a - b` | `a - b`   | Working                  |
| Multiplication | `a * b` | `a * b`   | Working                  |
| Division       | `a / b` | `a / b`   | Working (returns DOUBLE) |
| Modulo         | `a % b` | `a % b`   | Not yet supported        |

---

## 10.1 CASE WHEN Expressions

Both implementations support CASE WHEN expressions for conditional logic.

**Siddhi:**

```sql
from stream
select symbol,
       ifThenElse(price > 100, 'expensive', 'cheap') as category
    insert
into output;
```

**EventFlux:**

```sql
INSERT INTO output
SELECT symbol,
       CASE WHEN price > 100.0 THEN 'expensive' ELSE 'cheap' END AS category
FROM stream;
```

**Key Differences:**

- Siddhi uses `ifThenElse(condition, true_value, false_value)` function
- EventFlux uses standard SQL `CASE WHEN condition THEN value ELSE value END` syntax
- EventFlux supports multiple WHEN clauses: `CASE WHEN a THEN x WHEN b THEN y ELSE z END`

---

## 11. Output Event Types

**Siddhi:**

```sql
from stream#window.length(5)
select symbol,
       price insert all events
into output;
```

**EventFlux:**
Currently, EventFlux outputs both current and expired events by default. The `INSERT ALL EVENTS INTO` syntax is not
supported - use standard `INSERT INTO`.

---

## 12. Partition Syntax

**Siddhi:**

```sql
partition
with (symbol of stockStream)
begin
from stockStream#window.length(2)
select symbol,
       sum(price) as totalPrice
    insert
into output;
end;
```

**EventFlux:**

```sql
PARTITION
BY symbol OF stockStream
BEGIN
INSERT INTO output
SELECT symbol, sum(price) AS totalPrice
FROM stockStream WINDOW('length', 2);
END;
```

**Status:** PARTITION BY syntax not yet fully supported in EventFlux.

---

## 13. Annotations

### 13.1 App Annotation

**Siddhi:**

```sql
@
app
:
name
('StockApp')
@app:description('Stock processing app')
```

**EventFlux:**
Not required. App metadata is handled at runtime level.

### 13.2 Query Info Annotation

**Siddhi:**

```sql
@
info
(name = 'query1')
from stream
select * insert
into output;
```

**EventFlux:**
Not required. Queries are identified by their position or can be named via runtime API.

### 13.3 Source/Sink Annotations

**Siddhi:**

```sql
@
source
(type='inMemory', topic='stocks', @map(type='passThrough'))
define stream StockStream (symbol string, price float);

@sink
(type='inMemory', topic='output', @map(type='passThrough'))
define stream OutputStream (symbol string, price float);
```

**EventFlux:**
Source/Sink configuration is handled through WITH clause or runtime configuration.

---

## 14. Known Limitations in EventFlux

### 14.1 Operators Not Yet Supported

| Operator    | Siddhi Syntax           | Status        | Workaround                           | Test Reference                             |
|-------------|-------------------------|---------------|--------------------------------------|--------------------------------------------|
| IS NULL     | `field is null`         | Not supported | Use `coalesce()` with sentinel value | `is_null_operator`                         |
| IS NOT NULL | `field is not null`     | Not supported | Use comparison with sentinel         | `is_not_null_operator`                     |
| IN          | `field in (a, b, c)`    | Not supported | Use multiple OR conditions           | `in_operator`                              |
| NOT IN      | `field not in (a, b)`   | Not supported | Use multiple AND with !=             | `not_in_operator`                          |
| BETWEEN     | `field between a and b` | Not supported | Use `field >= a AND field <= b`      | `between_operator`                         |
| Modulo      | `a % b`                 | Not supported | -                                    | `modulo_*` tests in joins.rs, functions.rs |

### 14.2 Functions Not Yet Supported in SQL Converter

| Category      | Functions                                                                                               | Test References                                                                                                                                                                                        |
|---------------|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Math          | `log()`, `ln()`, `log10()`, `exp()`, `sin()`, `cos()`, `tan()`, `power()`, `floor()`, `ceil()`, `abs()` | `abs_function_*`, `sin_function_*`, `cos_function_*`, `tan_function_*`, `log_function_*`, `ln_function_*`, `floor_function_*`, `ceil_function_*`, `power_function_*`, `exp_function_*` in functions.rs |
| String        | `trim()`, `replace()`, `substring()`                                                                    | `trim_function_*`, `replace_function_*`, `substring_function_*`, `substr_function_*` in functions.rs                                                                                                   |
| Time          | `currentTimeMillis()`                                                                                   | `current_time_millis_function` in functions.rs                                                                                                                                                         |
| Conditional   | `ifThenElse()`                                                                                          | `if_then_else_function` in functions.rs                                                                                                                                                                |
| Null handling | `ifnull()`, `nullif()`                                                                                  | `ifnull_function`, `nullif_function` in functions.rs                                                                                                                                                   |

### 14.3 Other Limitations

1. **Join Aliases**: Siddhi supports `a.symbol` aliases; EventFlux requires full stream names
    - Tests: `join_alias_*` in joins.rs
2. **EVERY Pattern**: Syntax placement differs from Siddhi
    - Tests: `every_pattern_*` in patterns.rs
3. **stdDev Aggregator**: Only available for pattern collections, not window aggregation
    - Tests: `stddev_aggregator_not_supported` in aggregations.rs
4. **Inline Pattern Filters**: `e1=Stream[filter]` syntax - use WHERE clause instead, which filters the finished match
    - Tests: `pattern_filter_*` in patterns.rs
5. **PARTITION BY**: Syntax not yet fully supported
    - Tests: `partition_by_syntax_*` in tables.rs
6. **Session Window with Partition Key**: `WINDOW('session', timeout UNIT, key)` with partition key is supported
    - Tests: `session_window_with_partition_key` in windows.rs
7. **Output Rate Limiting**: `OUTPUT SNAPSHOT/ALL/FIRST/LAST EVERY ...` syntax not yet supported
    - Tests: `output_snapshot`, `output_all_every`, `output_first_every`, `output_last_every` in aggregations.rs
8. **UPDATE OR INSERT**: Upsert syntax not yet supported
    - Tests: `update_or_insert_*` in tables.rs
9. **CONTAINS IN**: Table containment check syntax not yet supported
    - Tests: `contains_in_*` in tables.rs
10. **Complex GROUP BY**: GROUP BY with table join expressions not yet supported
    - Tests: `complex_group_by_*` in tables.rs, aggregations.rs
11. **NOT Pattern**: `NOT e1=Stream` absent stream pattern not yet supported
    - Tests: `not_pattern_basic` in patterns.rs
12. **Count Pattern (Kleene)**: `<0:>` and `<1:>` Kleene star/plus patterns not yet supported
    - Tests: `count_pattern_*` in patterns.rs
13. **Unary Minus in WHERE**: `WHERE price > -10.0` - negative literals in comparisons not yet supported
    - Tests: `unary_minus_*` in filters.rs
14. **count(column)**: Counts all events including NULL (differs from SQL behavior)
    - Tests: N/A - documented behavioral difference
15. **Chained Logical Operators in Patterns**: `e1=A AND e2=B AND e3=C` - three-way AND/OR in patterns not supported
    - Tests: `chained_and_pattern`, `chained_or_pattern` in patterns.rs
16. **IS NOT NULL in Pattern WHERE**: `WHERE e1.value IS NOT NULL` - behavior differs in patterns
    - Tests: `is_not_null_in_pattern_where` in patterns.rs
17. **LIKE in Pattern WHERE**: `WHERE e1.name LIKE 'IBM%'` - LIKE operator not supported in pattern filters
    - Tests: `pattern_filter_with_like` in patterns.rs
18. **Nested Function Calls**: `round(sqrt(x))` - type inference for nested function calls not fully supported
    - Tests: `nested_function_calls_*` in functions.rs
19. **Qualified Column GROUP BY**: `GROUP BY Products.category` - complex GROUP BY with qualified column names in joins
    not supported
    - Tests: `qualified_column_group_by` in joins.rs
20. **Three-way Chained Joins**: `A JOIN B ON ... JOIN C ON ...` - chained joins not yet supported
    - Tests: `chained_joins_*` in joins.rs
21. **String Min/Max Aggregation**: `min(stringColumn)` - string comparison in min/max not yet supported
    - Tests: `string_min_max_*` in aggregations.rs
22. **Reserved Keyword `key`**: The word `key` is a reserved keyword in SQL parser - use alternatives like
    `partition_key`, `event_key`, etc.
    - Tests: N/A - documented parser limitation
23. **round() Precision Argument**: `round(value, 2)` - precision/decimal places argument not yet supported; only
    `round(value)` works
    - Tests: `round_with_precision` in functions.rs
24. **floor()/ceil() Parsing**: `floor(value)` and `ceil(value)` are parsed as DateTimeField expressions (for date
    truncation), not as math functions
    - Tests: `floor_as_datetime_field`, `ceil_as_datetime_field` in functions.rs
25. **Namespaced Functions in SQL**: `math:sin`, `math:tan`, `math:log` namespace syntax not recognized in SQL parser -
    functions must be registered without namespace
    - Tests: `math_sin_*`, `math_tan_*`, `math_log_*` in functions.rs
26. **Division Returns Double**: Integer division like `a / b` returns Double type, not integer. Use explicit cast if
    integer result needed
    - Tests: N/A - documented type behavior
27. **sum/count Return Long**: `sum()` and `count()` aggregations return Long type, while `avg()` returns Double
    - Tests: N/A - documented type behavior
28. **minForever/maxForever**: `minForever()` and `maxForever()` aggregations not yet supported in SQL parser
    - Tests: `partition_test47_minforever`, `partition_test48_maxforever` in partitions.rs, `minforever_aggregation`,
      `maxforever_aggregation` in aggregations.rs
29. **Aggregate Functions in SQL**: Only standard aggregations (sum, count, avg, min, max, distinctCount) work via SQL
    parser; forever variants require direct API
    - Tests: Various `*_forever_*` tests
30. **Partition State Isolation**: Per-partition aggregation state is not yet isolated. In Siddhi,
    `PARTITION WITH (key OF stream)` creates independent aggregation state per partition key value. Currently, EventFlux
    uses global state across all partitions, which means aggregations (SUM, COUNT, AVG, etc.) accumulate values from all
    partitions instead of maintaining separate counters per partition key. This is a critical behavioral difference from
    Siddhi.
    - Tests: `partition_test2_sum_aggregation`, `partition_test3_count_aggregation`, `partition_test4_avg_aggregation`,
      `partition_test9_int_key`, `partition_test23_cascading_partitions`, `partition_test24_multi_query`

### 14.4 Window Types Not Yet Supported

| Window Type | Siddhi Syntax              | Status        | Test Reference              |
|-------------|----------------------------|---------------|-----------------------------|
| Unique      | `#window.unique(key)`      | Not supported | `unique_window_basic`       |
| FirstUnique | `#window.firstUnique(key)` | Not supported | `first_unique_window_basic` |
| Delay       | `#window.delay(time)`      | Not supported | `delay_window_basic`        |
| Expression  | `#window.expression(expr)` | Not supported | `expression_window_basic`   |
| Cron        | `#window.cron(pattern)`    | Not supported | `cron_window_basic`         |
| Frequent    | `#window.frequent(k)`      | Not supported | `frequent_window_basic`     |
| Lossless    | `#window.lossless()`       | Not supported | `lossless_window_basic`     |

### 14.5 Aggregator Functions Not Yet Supported

| Aggregator   | Siddhi Syntax              | Status                   | Test Reference                                          |
|--------------|----------------------------|--------------------------|---------------------------------------------------------|
| first()      | `first(column)`            | Not supported            | `first_aggregator_basic`                                |
| last()       | `last(column)`             | Not supported            | `last_aggregator_basic`                                 |
| minForever() | `minForever(column)`       | Not supported            | `partition_test47_minforever`, `minforever_aggregation` |
| maxForever() | `maxForever(column)`       | Not supported            | `partition_test48_maxforever`, `maxforever_aggregation` |
| stdDev()     | `stdDev(column)` in window | Only pattern collections | `stddev_aggregator_not_supported`                       |

### 14.6 Pattern Limitations (Additional)

| Feature                        | Siddhi Syntax               | Status           | Test Reference                              |
|--------------------------------|-----------------------------|------------------|---------------------------------------------|
| Count Pattern (Kleene)         | `e1=A<0:>`, `e1=A<1:>`      | Not supported    | `count_pattern_*` tests                     |
| NOT Pattern                    | `NOT e1=Stream`             | Not supported    | `not_pattern_basic`                         |
| Pattern Collection Aggregation | `e1.price, e1.count()`      | Not supported    | `pattern_collection_aggregation`            |
| Complex (OR) -> followedby     | `(A OR B) -> C`             | Not supported    | `complex_or_followedby_pattern`             |
| Chained AND/OR                 | `A AND B AND C`             | Not supported    | `chained_and_pattern`, `chained_or_pattern` |
| LIKE in Pattern WHERE          | `WHERE e1.name LIKE 'IBM%'` | Not supported    | `pattern_filter_with_like`                  |
| Pattern String Filter          | `WHERE e1.name = 'value'`   | Behavior differs | `pattern_filter_string_equality_*`          |
| Pattern Numeric Filter         | `WHERE e1.price > 100`      | Behavior differs | `pattern_filter_numeric_*`                  |

### 14.7 Table Operation Limitations

| Operation                  | Siddhi Syntax                   | Status        | Test Reference              |
|----------------------------|---------------------------------|---------------|-----------------------------|
| UPDATE TABLE               | `UPDATE table SET ...`          | Not supported | `update_table_basic`        |
| DELETE FROM TABLE          | `DELETE table WHERE ...`        | Not supported | `delete_from_table_basic`   |
| Range Partition            | `PARTITION BY RANGE(col)`       | Not supported | `range_partition_basic`     |
| ORDER BY with Table Join   | `ORDER BY col` in table join    | Not supported | `order_by_table_join`       |
| LIMIT with Table Join      | `LIMIT n` in table join         | Not supported | `limit_table_join`          |
| RIGHT OUTER JOIN on Table  | `stream RIGHT OUTER JOIN table` | Rejected      | `right_outer_join_table`    |
| FULL OUTER JOIN on Table   | `FULL OUTER JOIN table`         | Rejected      | `full_outer_join_table`     |

### 14.8 Type System Limitations

| Type | Siddhi | EventFlux     | Notes        | Test Reference |
|------|--------|---------------|--------------|----------------|
| LONG | `long` | Use `BIGINT`  | Type mapping | `long_type_*`  |
| BOOL | `bool` | Use `BOOLEAN` | Type mapping | `bool_type_*`  |

### 14.9 Window Edge Case Limitations

| Issue                        | Description                            | Test Reference                   |
|------------------------------|----------------------------------------|----------------------------------|
| Multiple Sort Criteria       | Sort window with multiple columns      | `multiple_sort_criteria`         |
| Length Batch Count Semantics | Outputs per event instead of per batch | `length_batch_count_semantics`   |
| Time-Based Window Timing     | Environment-sensitive timing           | `time_window_aggregation_timing` |
| ExternalTimeBatch Output     | Output timing needs investigation      | `external_time_batch_timing`     |

### 14.10 Output Rate Limiting

| Syntax                        | Status        | Test Reference          |
|-------------------------------|---------------|-------------------------|
| `OUTPUT SNAPSHOT EVERY n sec` | Not supported | `output_snapshot`       |
| `OUTPUT ALL EVERY n sec`      | Not supported | `output_all_every`      |
| `OUTPUT FIRST EVERY n sec`    | Not supported | `output_first_every`    |
| `OUTPUT LAST EVERY n sec`     | Not supported | `output_last_every`     |
| `OUTPUT ALL EVERY n events`   | Not supported | `output_all_every_time` |

### 14.11 Summary of Ignored Tests by Category

| Category           | Ignored Count | Key Issues                                                                                                |
|--------------------|---------------|-----------------------------------------------------------------------------------------------------------|
| **Functions**      | ~45           | Math functions (abs, sin, cos, tan, log, floor, ceil, power), string functions (trim, replace, substring) |
| **Patterns**       | ~25           | Count patterns, NOT patterns, chained operators, filter behavior, LIKE operator                           |
| **Tables**         | ~25           | UPDATE/DELETE syntax, partitions, outer joins, WHERE filters, aliases                                     |
| **Partitions**     | ~15           | State isolation, multi-query blocks, multiple partition keys                                              |
| **Aggregations**   | ~15           | first/last, minForever/maxForever, output rate limiting, stddev, string min/max                           |
| **Windows**        | ~10           | Unique, FirstUnique, Delay, Cron, Frequent, Lossless, Expression windows                                  |
| **Joins**          | ~10           | Aliases, chained joins, non-equi joins, GROUP BY with joins                                               |
| **Filters**        | ~10           | IS NULL/NOT NULL, BETWEEN, IN/NOT IN, unary minus                                                         |
| **Infrastructure** | ~7            | RabbitMQ broker tests (require external service)                                                          |
| **Other**          | ~15           | Old EventFluxQL syntax, session window syntax                                                             |

**Total: ~177 ignored tests** (out of 1068+ compatibility tests)

> **Note:** Trigger tests were enabled on 2024-12-31 (10 tests now passing).

To re-run ignored tests and check progress:

```bash
cargo test --test compatibility -- --ignored
```

---

## 15. Migration Checklist

When migrating a Siddhi query to EventFlux:

- [ ] Change `define stream` to `CREATE STREAM`
- [ ] Change `define table` to `CREATE TABLE`
- [ ] Change `define trigger` to `CREATE TRIGGER`
- [ ] For triggers: add `CRON` keyword for cron expressions
- [ ] For triggers: use full time unit names (`sec` → `SECONDS`, `ms` → `MILLISECONDS`)
- [ ] Uppercase all type names (`string` → `STRING`, etc.)
- [ ] Change `long` to `BIGINT`
- [ ] Reorder query: `INSERT INTO ... SELECT ... FROM ... WHERE`
- [ ] Change `[filter]` syntax to `WHERE clause`
- [ ] Change `==` to `=` for equality
- [ ] Change `#window.type(params)` to `WINDOW('type', params)`
- [ ] Use time units for windows: `1 sec` → `1 SECOND`, `500 ms` → `500 MILLISECONDS`
- [ ] Wrap patterns with `PATTERN (...)`
- [ ] Remove `@info`, `@app` annotations
- [ ] Update join syntax to use full stream names

---

## Version History

| Date       | Changes                                                                                                                                                                                                                                                   |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2024-12-28 | Initial migration guide created based on 76 reference compatibility tests                                                                                                                                                                                 |
| 2024-12-28 | Updated with 127 tests, added operator/function limitations, CASE WHEN support                                                                                                                                                                            |
| 2024-12-28 | Updated with 175 tests, added output rate limiting, stream-table joins, NOT pattern limitations                                                                                                                                                           |
| 2024-12-28 | Updated with 196 tests (131 passing, 65 ignored), added LIKE operator, arithmetic in WHERE, complex patterns                                                                                                                                              |
| 2024-12-28 | Updated with 220 tests (151 passing, 69 ignored), added 15 window edge cases, 9 table lookup tests, 1 partition test                                                                                                                                      |
| 2024-12-28 | Updated with 249 tests (179 passing, 70 ignored), added 14 aggregation tests, 17 operator/filter tests                                                                                                                                                    |
| 2024-12-28 | Updated with 302 tests (224 passing, 78 ignored), added 22 function tests, 18 pattern edge cases, 16 join edge cases                                                                                                                                      |
| 2024-12-29 | Updated with 328 tests (249 passing, 79 ignored), added 12 table tests, 14 aggregation tests                                                                                                                                                              |
| 2024-12-29 | Updated with 386 tests (279 passing, 107 ignored), added 17 window edge cases, 20 filter edge cases, 28 function edge cases                                                                                                                               |
| 2024-12-29 | Updated with 421 tests (301 passing, 120 ignored), added 15 partition tests, 10 trigger tests, 10 table edge case tests                                                                                                                                   |
| 2024-12-29 | Updated with 457 tests (332 passing, 125 ignored), added 15 window edge cases, 12 aggregation edge cases, 10 pattern edge cases                                                                                                                           |
| 2024-12-29 | Updated with 489 tests (361 passing, 128 ignored), added 12 join edge cases, 12 filter edge cases, 10 table edge cases                                                                                                                                    |
| 2024-12-29 | Updated with 523 tests (393 passing, 130 ignored), added 12 aggregation edge cases, 11 pattern edge cases, 11 window edge cases                                                                                                                           |
| 2024-12-29 | Updated with 545 tests (414 passing, 131 ignored), added 12 function edge cases, 10 operator/filter edge cases                                                                                                                                            |
| 2024-12-29 | Updated with 583 tests (450 passing, 133 ignored), added 10 table edge cases, 10 window edge cases, 10 aggregation edge cases, 11 join edge cases                                                                                                         |
| 2024-12-29 | Updated with 613 tests (475 passing, 138 ignored), added 10 partition edge cases, 10 table edge cases, 11 pattern edge cases                                                                                                                              |
| 2024-12-29 | Updated with 643 tests (494 passing, 149 ignored), added 10 partition edge cases, 10 filter edge cases, 10 function edge cases                                                                                                                            |
| 2024-12-29 | Updated with 673 tests (522 passing, 151 ignored), added 10 partition edge cases, 10 table edge cases, 10 aggregation edge cases                                                                                                                          |
| 2024-12-29 | Updated with 693 tests (540 passing, 153 ignored), added 10 window edge cases, 10 join edge cases                                                                                                                                                         |
| 2024-12-30 | Updated with 720 tests (564 passing, 156 ignored), added 10 pattern edge cases, 10 filter edge cases, 10 table edge cases                                                                                                                                 |
| 2024-12-30 | Updated with 748 tests (589 passing, 159 ignored), added 10 function edge cases, 10 aggregation edge cases, 10 window edge cases                                                                                                                          |
| 2024-12-30 | Updated with 778 tests (611 passing, 167 ignored), added 10 partition edge cases, 10 join edge cases, 10 pattern edge cases                                                                                                                               |
| 2024-12-30 | Updated with 808 tests (641 passing, 167 ignored), added 10 partition edge cases, 10 table edge cases, 10 filter edge cases                                                                                                                               |
| 2024-12-30 | Updated with 838 tests (669 passing, 169 ignored), added 10 partition edge cases, 10 aggregation edge cases, 10 function edge cases                                                                                                                       |
| 2024-12-30 | Updated with 867 tests (697 passing, 170 ignored), added 10 join edge cases, 10 window edge cases, 10 pattern edge cases                                                                                                                                  |
| 2024-12-30 | Updated with 897 tests (725 passing, 172 ignored), added 10 table edge cases, 10 filter edge cases, 10 partition edge cases                                                                                                                               |
| 2024-12-30 | Updated with 935 tests (763 passing, 172 ignored), added 10 aggregation edge cases, 8 join edge cases, 19 window edge cases                                                                                                                               |
| 2024-12-30 | Updated with 962 tests (790 passing, 172 ignored), added 9 partition edge cases, 10 table edge cases, 10 pattern edge cases                                                                                                                               |
| 2024-12-30 | Updated with 991 tests (819 passing, 172 ignored), added 10 partition edge cases, 10 table edge cases, 9 filter edge cases                                                                                                                                |
| 2024-12-30 | Updated with 1028 tests (847 passing, 181 ignored), added 10 partition edge cases, 20 function edge cases, 10 aggregation edge cases                                                                                                                      |
| 2024-12-30 | **CONVERGED**: 1068 tests (885 passing, 183 ignored). All categories at 100%+ coverage. Added 28 partition tests, 13 table tests. Documented minForever/maxForever limitations.                                                                           |
| 2024-12-30 | **VALIDATED**: 1068 tests (881 passing, 187 ignored). Fixed 4 partition tests that were asserting incorrect global aggregation behavior. Added comprehensive limitation documentation (sections 14.4-14.11) with test references for all 30+ limitations. |
| 2024-12-31 | **TRIGGERS**: Added SQL trigger syntax support. 10 trigger tests now passing. Added section 1.3 for trigger migration. Updated Quick Reference Table with trigger syntax. |
| 2024-12-31 | **WINDOW TIME UNITS**: Updated window syntax to use readable time units (`1 SECOND`, `500 MILLISECONDS`) instead of raw milliseconds. Unified time parsing using standard SQL DateTimeField. |

---

## Test Coverage Status

See [COMMON_FEATURES.md](COMMON_FEATURES.md) for detailed test coverage of each feature.
