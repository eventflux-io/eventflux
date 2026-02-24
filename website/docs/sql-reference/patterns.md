---
sidebar_position: 5
title: Pattern Matching
description: Complex event pattern detection with sequences and logical operators
---

# Pattern Matching

Pattern matching detects complex event sequences across streams. EventFlux provides two matching modes with temporal constraints, logical operators, and count quantifiers.

## Pattern Syntax

```sql
INSERT INTO OutputStream
SELECT projection_list
FROM PATTERN (pattern_expression) [WITHIN time_constraint];
```

Or using SEQUENCE mode for strict consecutive matching:

```sql
INSERT INTO OutputStream
SELECT projection_list
FROM SEQUENCE (pattern_expression) [WITHIN time_constraint];
```

## PATTERN vs SEQUENCE Modes

EventFlux supports two distinct matching modes:

| Mode | Behavior | Use Case |
|------|----------|----------|
| PATTERN | Ignores non-matching events, keeps pending states | Event correlation with gaps allowed |
| SEQUENCE | Fails on non-matching events, clears pending states | Strict consecutive event matching |

### PATTERN Mode (Relaxed Matching)

Non-matching events are ignored. The pattern waits for the next matching event.

```sql
INSERT INTO Out
SELECT A.val AS aval, B.val AS bval, C.val AS cval
FROM PATTERN (e1=A -> e2=B -> e3=C);
```

Event stream: `[A, X, Y, B, Z, C]`

Result: Match. Events X, Y, Z are ignored.

### SEQUENCE Mode (Strict Matching)

Non-matching events cause the pattern to fail. All pending states are cleared.

```sql
INSERT INTO Out
SELECT A.val AS aval, B.val AS bval, C.val AS cval
FROM SEQUENCE (e1=A -> e2=B -> e3=C);
```

Event stream: `[A, X, B, C]`

Result: No match. Event X breaks the sequence.

Event stream: `[A, B, C]`

Result: Match. All events are consecutive.

## Sequence Operator

The `->` operator specifies temporal ordering. Events must occur in the specified order.

### Two-Element Sequence

```sql
INSERT INTO Out
SELECT A.val AS aval, B.val AS bval
FROM PATTERN (e1=A -> e2=B);
```

### N-Element Sequence

Sequences can have any number of elements:

```sql
-- Three-element pattern
INSERT INTO Out
SELECT A.val AS aval, B.val AS bval, C.val AS cval
FROM PATTERN (e1=A -> e2=B -> e3=C);

-- Four-element pattern
INSERT INTO Out
SELECT A.val, B.val, C.val, D.val
FROM PATTERN (e1=A -> e2=B -> e3=C -> e4=D);

-- Five-element pattern
INSERT INTO Out
SELECT A.val, B.val, C.val, D.val, E.val
FROM PATTERN (e1=A -> e2=B -> e3=C -> e4=D -> e5=E);
```

## Event Aliases

Event aliases assign names to pattern elements for reference in SELECT and filter expressions.

### Syntax

```sql
-- Assignment syntax
e1=StreamName

-- Full pattern with aliases
FROM PATTERN (e1=Trades -> e2=Trades)
SELECT e1.price AS first_price, e2.price AS second_price
```

### Referencing Aliases

Aliases are used in SELECT to access attributes from matched events:

```sql
INSERT INTO Out
SELECT e1.price AS first_price,
       e2.price AS second_price,
       e2.price - e1.price AS price_change
FROM PATTERN (e1=StreamA -> e2=StreamB);
```

### Same-Stream Patterns

When the same stream appears multiple times, aliases disambiguate the positions:

```sql
-- Two consecutive events from the same stream
INSERT INTO Out
SELECT e1.price AS first_price, e2.price AS second_price
FROM PATTERN (e1=Trades -> e2=Trades);

-- Three consecutive events from the same stream
INSERT INTO Out
SELECT e1.price AS p1, e2.price AS p2, e3.price AS p3
FROM PATTERN (e1=Trades -> e2=Trades -> e3=Trades);
```

For same-stream patterns, each event matches exactly one position. Event 1 matches e1, Event 2 matches e2, and so on.

## Logical Operators

### AND Operator

Both sides must match. Order does not matter.

```sql
INSERT INTO Out
SELECT e1.val AS aval, e2.val AS bval
FROM PATTERN (e1=A AND e2=B);
```

Event stream: `[B, A]` or `[A, B]`

Result: Match. Both A and B arrived.

### OR Operator

Either side can match.

```sql
INSERT INTO Out
SELECT e1.val AS aval
FROM PATTERN (e1=A OR e2=B);
```

Event stream: `[A]`

Result: Match. A arrived.

### Same-Stream Logical Patterns

Logical operators work with same-stream patterns:

```sql
-- Both events must arrive from same stream
INSERT INTO Out
SELECT e1.price AS p1, e2.price AS p2
FROM PATTERN (e1=Trades AND e2=Trades);
```

### Current Limitations

Logical groups within sequences are not yet supported:

```sql
-- NOT SUPPORTED: Logical group followed by sequence
FROM PATTERN ((e1=A AND e2=B) -> e3=C)

-- NOT SUPPORTED: Sequence followed by logical group
FROM PATTERN (e1=A -> (e2=B OR e3=C))
```

Use top-level logical patterns or pure sequence patterns instead.

## EVERY Modifier

The EVERY modifier enables continuous matching. After a pattern completes, it automatically restarts to look for the next match.

### Without EVERY (Default)

The pattern matches once and stops:

```sql
INSERT INTO Out
SELECT A.val, B.val, C.val
FROM PATTERN (e1=A -> e2=B -> e3=C);
```

Event stream: `[A, B, C, A, B, C]`

Result: One match (first A, B, C).

### With EVERY

The pattern restarts after each complete match:

```sql
INSERT INTO Out
SELECT A.val, B.val, C.val
FROM PATTERN (EVERY(e1=A -> e2=B -> e3=C));
```

Event stream: `[A, B, C, A, B, C]`

Result: Two matches. After the first complete match, the pattern restarts.

### EVERY Restrictions

1. EVERY is only allowed in PATTERN mode (not SEQUENCE)
2. EVERY must wrap the entire pattern at top level
3. Only one EVERY per pattern

```sql
-- Correct: EVERY wraps entire pattern
FROM PATTERN (EVERY(e1=A -> e2=B))

-- Incorrect: EVERY nested in sequence
FROM PATTERN (EVERY(e1=A) -> e2=B)  -- Error: EveryNotAtTopLevel

-- Incorrect: EVERY in SEQUENCE mode
FROM SEQUENCE (EVERY(e1=A -> e2=B))  -- Error: EveryInSequenceMode

-- Incorrect: Multiple EVERY
FROM PATTERN (EVERY(e1=A) -> EVERY(e2=B))  -- Error: MultipleEvery
```

## Count Quantifiers

Count quantifiers specify how many events must match at a position.

### Exact Count

```sql
-- Exactly 3 events from A
FROM PATTERN (e1=A{3} -> e2=B)
```

### Range Count

```sql
-- Between 2 and 5 events from A
FROM PATTERN (e1=A{2,5} -> e2=B)
```

### Restrictions

All count quantifiers must have explicit bounds:

| Pattern | Allowed | Reason |
|---------|---------|--------|
| `A{3}` | Yes | Exact count |
| `A{2,5}` | Yes | Bounded range |
| `A+` or `A{1,}` | No | Unbounded maximum |
| `A*` or `A{0,}` | No | Zero count and unbounded |
| `A?` or `A{0,1}` | No | Zero count |

## WITHIN Constraint

The WITHIN clause specifies a time limit for pattern completion.

```sql
INSERT INTO Out
SELECT A.val, B.val
FROM PATTERN (e1=A -> e2=B) WITHIN 5 SECONDS;
```

### Time Units

- MILLISECONDS, MILLISECOND, MS
- SECONDS, SECOND, SEC, S
- MINUTES, MINUTE, MIN, M
- HOURS, HOUR, H
- DAYS, DAY, D

### Examples

```sql
-- Pattern must complete within 10 minutes
FROM PATTERN (e1=A -> e2=B -> e3=C) WITHIN 10 MINUTES;

-- Pattern must complete within 1 hour
FROM PATTERN (e1=Login -> e2=DataAccess) WITHIN 1 HOUR;
```

Events outside the time window are discarded from pending pattern states.

## Filter Conditions

Filter conditions restrict which events match a pattern element.

### Syntax

Filters are specified in square brackets after the stream reference:

```sql
FROM PATTERN (e1=Trades[price > 100] -> e2=Trades[price > e1.price])
```

### Cross-Stream References

Filters can reference attributes from earlier pattern elements:

```sql
-- Second event's price must be greater than first
FROM PATTERN (
    e1=StockPrice[symbol = 'AAPL'] ->
    e2=StockPrice[symbol = 'AAPL' AND price > e1.price]
)
SELECT e1.price AS buy_price, e2.price AS sell_price
```

## Programmatic API

Patterns can be constructed programmatically using the Query API.

### Basic Sequence

```rust
use eventflux::query_api::execution::query::input::state::State;
use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;

// Create stream references
let a_si = SingleInputStream::new_basic("A".to_string(), false, false, None, Vec::new());
let b_si = SingleInputStream::new_basic("B".to_string(), false, false, None, Vec::new());

// Create pattern: A -> B
let pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));

// Create state input stream
let state_stream = StateInputStream::sequence_stream(pattern, None);
```

### Pattern with Aliases

```rust
let a_si = SingleInputStream::new_basic("StreamA".to_string(), false, false, None, Vec::new())
    .as_ref("e1".to_string());
let b_si = SingleInputStream::new_basic("StreamB".to_string(), false, false, None, Vec::new())
    .as_ref("e2".to_string());

let pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));
```

### Logical Pattern

```rust
// A AND B
let pattern = State::logical_and(
    State::stream(a_si),
    State::stream(b_si)
);

// A OR B
let pattern = State::logical_or(
    State::stream(a_si),
    State::stream(b_si)
);
```

### Count Quantifier

```rust
// A{2,5} - between 2 and 5 events
let pattern = State::count(State::stream(a_si), 2, 5);
```

### EVERY Pattern

```rust
let inner_pattern = State::next(State::stream_element(a_si), State::stream_element(b_si));
let every_pattern = State::every(inner_pattern);
```

### WITHIN Constraint

```rust
use eventflux::query_api::expression::constant::TimeUtil;

// Pattern with 10 second timeout
let state_stream = StateInputStream::sequence_stream(pattern, Some(TimeUtil::sec(10)));
```

## Examples

### Fraud Detection

Detect rapid transaction burst from the same card:

```sql
CREATE STREAM Transactions (card_id STRING, amount DOUBLE, location STRING);
CREATE STREAM FraudAlerts (card_id STRING, total_amount DOUBLE);

INSERT INTO FraudAlerts
SELECT e1.card_id AS card_id,
       e1.amount + e2.amount + e3.amount AS total_amount
FROM PATTERN (
    e1=Transactions -> e2=Transactions -> e3=Transactions
) WITHIN 10 MINUTES;
```

### Price Momentum

Detect three consecutive price increases:

```sql
CREATE STREAM StockTicks (symbol STRING, price DOUBLE);
CREATE STREAM MomentumSignals (symbol STRING, price1 DOUBLE, price2 DOUBLE, price3 DOUBLE);

INSERT INTO MomentumSignals
SELECT e1.symbol AS symbol,
       e1.price AS price1,
       e2.price AS price2,
       e3.price AS price3
FROM PATTERN (
    e1=StockTicks ->
    e2=StockTicks[symbol = e1.symbol AND price > e1.price] ->
    e3=StockTicks[symbol = e2.symbol AND price > e2.price]
) WITHIN 1 MINUTE;
```

### Login Session Tracking

Track login followed by activity:

```sql
CREATE STREAM Events (user_id STRING, event_type STRING, timestamp LONG);
CREATE STREAM SessionActivity (user_id STRING, login_time LONG, activity_time LONG);

INSERT INTO SessionActivity
SELECT e1.user_id AS user_id,
       e1.timestamp AS login_time,
       e2.timestamp AS activity_time
FROM PATTERN (
    e1=Events[event_type = 'LOGIN'] ->
    e2=Events[event_type = 'ACTIVITY' AND user_id = e1.user_id]
) WITHIN 30 MINUTES;
```

### Consecutive Temperature Readings

Detect three readings from the same sensor with increasing temperatures:

```sql
CREATE STREAM SensorReadings (sensor_id STRING, temperature DOUBLE);
CREATE STREAM TemperatureSpikes (sensor_id STRING, t1 DOUBLE, t2 DOUBLE, t3 DOUBLE);

INSERT INTO TemperatureSpikes
SELECT e1.sensor_id AS sensor_id,
       e1.temperature AS t1,
       e2.temperature AS t2,
       e3.temperature AS t3
FROM PATTERN (
    e1=SensorReadings ->
    e2=SensorReadings[sensor_id = e1.sensor_id AND temperature > e1.temperature] ->
    e3=SensorReadings[sensor_id = e2.sensor_id AND temperature > e2.temperature]
);
```

## Limitations

### Not Yet Supported

| Feature | Status | Notes |
|---------|--------|-------|
| Logical groups in sequences | Not supported | `(A AND B) -> C` |
| Event-count WITHIN | Not supported | `WITHIN 100 EVENTS` |
| PARTITION BY | Not supported | Multi-tenant pattern isolation |
| Absent patterns (NOT ... FOR) | Not supported | Requires timer infrastructure |
| Array access in SELECT | Limited | `e[0].attr`, `e[last].attr` |

### Workarounds

For logical groups in sequences, use either:
- Top-level logical patterns: `FROM PATTERN (e1=A AND e2=B)`
- Pure sequence patterns: `FROM PATTERN (e1=A -> e2=B -> e3=C)`

## Pattern Behavior

### Event Consumption

By default, each event can participate in multiple pattern matches when using EVERY. Without EVERY, patterns match once.

### State Management

Patterns maintain state for incomplete matches. State is cleaned up when:
- The WITHIN time window expires
- The pattern completes
- SEQUENCE mode encounters a non-matching event

### Matching Order

For same-stream patterns, events are processed in arrival order. Each event can only fill one position in a pattern instance.

## Best Practices

1. Use explicit aliases for all pattern elements
2. Use AS in SELECT for same-stream patterns to avoid duplicate column names
3. Set appropriate WITHIN constraints to limit state accumulation
4. Use PATTERN mode when events may be interleaved with unrelated events
5. Use SEQUENCE mode only when strict consecutive ordering is required
6. Test pattern behavior with edge cases and out-of-order events
