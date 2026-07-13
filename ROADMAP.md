# EventFlux Roadmap

Last verified: 2026-07-13 (after the connector-infrastructure series #115–#117 and the Kafka
connector #118 — sink/source status, test counts, and priorities below reflect that work)

---

## Project Identity

EventFlux is a lightweight Complex Event Processing (CEP) engine written in Rust.

- **Single binary**: 50-100MB (vs 4GB+ JVM heap)
- **Millisecond startup** (vs 30+ second JVM warmup)
- **Runs on a $50/month VPS** (vs Kubernetes cluster)
- **Zero external dependencies** for core operation
- **Target**: 100k+ events/sec on a single node, for teams that don't need Flink

---

## Current Status

**Test suite**: 3,085 tests passing, 189 ignored, 0 failures (`--features connectors-all`;
the minimal no-connector build passes 2,978)
**Architecture**: SQL-first CEP engine with vendored sqlparser-rs fork
**License**: Apache-2.0

---

## Completed Work

### M1: SQL Streaming Foundation (Complete - 2025-10-06)

Full SQL parser built on a vendored `datafusion-sqlparser-rs` fork with native streaming extensions (uses
`GenericDialect`).

| Feature                                                                          | Status |
|----------------------------------------------------------------------------------|--------|
| `CREATE STREAM` DDL                                                              | Done   |
| `SELECT`, `WHERE`, `GROUP BY`, `HAVING`                                          | Done   |
| `ORDER BY`, `LIMIT`, `OFFSET`                                                    | Done   |
| `INSERT INTO` (stream and table)                                                 | Done   |
| `JOIN` (INNER, LEFT, RIGHT, FULL OUTER)                                          | Done   |
| Window aggregations (COUNT, SUM, AVG, MIN, MAX)                                  | Done   |
| Native `WINDOW('type', params)` syntax (no regex)                                | Done   |
| SQL `WITH` clause for stream/table configuration                                 | Done   |
| TOML configuration (4-layer merge: SQL WITH > TOML stream > TOML app > defaults) | Done   |
| High-performance crossbeam event pipeline (>1M events/sec)                       | Done   |

### M2: Pattern Processing (Complete - 2025-12-06)

Full pattern/sequence runtime with SQL parser integration.

| Feature                                                                                     | Status |
|---------------------------------------------------------------------------------------------|--------|
| StateEvent with multi-stream tracking                                                       | Done   |
| Pre/PostStateProcessor architecture                                                         | Done   |
| StreamPreStateProcessor, StreamPostStateProcessor                                           | Done   |
| LogicalPreStateProcessor (AND/OR patterns)                                                  | Done   |
| LogicalPostStateProcessor                                                                   | Done   |
| PatternStreamReceiver, SequenceStreamReceiver                                               | Done   |
| Lock-free ProcessorSharedState                                                              | Done   |
| CountPreStateProcessor (`A{3}`, `A{2,5}`)                                                   | Done   |
| CountPostStateProcessor                                                                     | Done   |
| PatternChainBuilder (multi-processor wiring)                                                | Done   |
| WITHIN time constraints in chains                                                           | Done   |
| Pattern vs Sequence modes                                                                   | Done   |
| IndexedVariableExecutor (`e[0]`, `e[last]`, `e[n]`)                                         | Done   |
| Cross-stream references (condition receives StateEvent)                                     | Done   |
| EVERY multi-instance overlapping patterns                                                   | Done   |
| CollectionAggregationFunction trait + 6 implementations (count, sum, avg, min, max, stdDev) | Done   |
| FROM PATTERN / FROM SEQUENCE SQL parsing                                                    | Done   |
| Pattern validation (EVERY restrictions, count bounds)                                       | Done   |
| `e1[0].price`, `e1[last].symbol` in SELECT                                                  | Done   |
| TimerWheel for scheduler-based operations                                                   | Done   |

**Known limitations** (unchanged since M2):

- PATTERN/SEQUENCE in JOINs: not supported (explicit error)
- Event-count WITHIN: blocked (use time-based)
- A+, A* syntax: rejected by design (unbounded)
- Absent patterns (NOT ... FOR): not implemented (requires scheduler integration)
- PARTITION BY in patterns: not implemented

### M3: CASE Expression & Core SQL (Complete - 2026)

| Feature                                    | Status                          |
|--------------------------------------------|---------------------------------|
| CASE expression (searched and simple)      | Done — `CaseExpressionExecutor` |
| CAST expression for type conversion        | Done — `CastExecutor`           |
| Nested expression support in SQL converter | Done                            |
| CASE in compatibility tests (5 passing)    | Done                            |

### M4: Built-in Functions (Complete - 2026)

41 scalar functions registered, all verified in `register_builtin_scalar_functions()`:

**String**: concat, length, lower, upper, substring/substr, trim, ltrim, rtrim, replace, left, right, reverse, repeat,
lpad, rpad, position/locate/instr, ascii, chr/char, like

**Math**: round, sqrt, log/ln, log10, sin, cos, tan, asin, acos, atan, abs, floor, ceil, exp, power/pow, mod, sign,
trunc/truncate, maximum, minimum

**Utility**: cast, convert, coalesce, default/ifnull, nullif, uuid, now, formatDate, parseDate, dateAdd, eventTimestamp

**Compatibility tests**: 166 function tests passing, 1 ignored

### M5: Window Processors (Mostly Complete - 2026)

16 window types registered in `EventFluxContext`:

| Window                             | Registered Name     | Status |
|------------------------------------|---------------------|--------|
| Length (sliding count)             | `length`            | Done   |
| Length Batch                       | `lengthBatch`       | Done   |
| Time (sliding time)                | `time`              | Done   |
| Time Batch                         | `timeBatch`         | Done   |
| External Time                      | `externalTime`      | Done   |
| External Time Batch                | `externalTimeBatch` | Done   |
| Session                            | `session`           | Done   |
| Sort                               | `sort`              | Done   |
| Cron                               | `cron`              | Done   |
| Unique (latest per key)            | `unique`            | Done   |
| First Unique (first per key)       | `firstUnique`       | Done   |
| Delay                              | `delay`             | Done   |
| Expression (count-based condition) | `expression`        | Done   |
| Frequent (Misra-Gries)             | `frequent`          | Done   |
| Lossy Frequent (Lossy Counting)    | `lossyFrequent`     | Done   |
| Lossy Counting                     | `lossyCounting`     | Done   |

**SQL syntax**: `WINDOW('tumbling', 5 MINUTES)` maps to `timeBatch`. `WINDOW('sliding', ...)` is not yet implemented as
a converter mapping — use `time` for overlapping windows.

**Compatibility tests**: 136 window tests passing, 9 ignored

**State holders**: length, lengthBatch, time, timeBatch, externalTime, session, sort — all have state holder
implementations for checkpointing.

### M6: Aggregators (Complete - 2026)

13 attribute aggregators registered:

| Aggregator    | Status |
|---------------|--------|
| count         | Done   |
| sum           | Done   |
| avg           | Done   |
| min           | Done   |
| max           | Done   |
| distinctCount | Done   |
| stdDev        | Done   |
| first         | Done   |
| last          | Done   |
| minForever    | Done   |
| maxForever    | Done   |
| and (boolean) | Done   |
| or (boolean)  | Done   |

All have state holder implementations for checkpointing.

**Compatibility tests**: 124 passing, 6 ignored

### M7: Connectors (In Progress - 2026)

| Connector  | Source      | Sink        | Format           | Feature flag     |
|------------|-------------|-------------|------------------|------------------|
| Timer      | Done        | —           | —                | (always built)   |
| Log        | —           | Done        | —                | (always built)   |
| Kafka      | Done        | Done        | JSON, CSV, bytes | `kafka`          |
| RabbitMQ   | Done        | Done        | JSON, CSV, bytes | `rabbitmq`       |
| WebSocket  | Done        | Done        | JSON, CSV, bytes | `websocket`      |
| HTTP       | Not started | Not started | —                | —                |
| File       | Not started | Not started | —                | —                |
| TCP/Socket | Not started | Not started | —                | —                |

Connectors are cargo features named after the SQL extension name; the default
build is minimal and `connectors-all` enables everything (release/Docker
builds use it).

**Connector infrastructure (2026-07, #115–#117)**: one-event-per-message sink
semantics (`SinkMapper::map_event`), `SourceWorker` lifecycle (synchronous
join-on-stop, RAII, parallel bulk shutdown), shared source helpers
(`deliver_with_error_handling`, `SourceErrorContext::from_properties`), and
the feature-gating framework with helpful rebuild errors. Kafka (#118) uses
all of it; adoption in the older connectors is tracked in #130.

**Data mapping layer**: JSON, CSV, and bytes source/sink mappers registered.

**RabbitMQ tests**: 7 integration tests (require running broker, run with `--ignored`). CI has RabbitMQ service.

### M8: Table Operations (Complete - 2026)

| Feature                                  | Status |
|------------------------------------------|--------|
| Table trait (database-agnostic API)      | Done   |
| InMemoryTable with HashMap O(1) indexing | Done   |
| CacheTable extension                     | Done   |
| JdbcTable extension                      | Done   |
| InsertIntoTableProcessor                 | Done   |
| UpdateTableProcessor                     | Done   |
| DeleteTableProcessor                     | Done   |
| UpsertTableProcessor                     | Done   |
| Stream-table JOIN                        | Done   |
| JOIN alias support                       | Done   |

**Compatibility tests**: 141 table tests passing, 20 ignored (most relate to advanced JOIN features, PARTITION BY,
triggers with tables)

### M9: Triggers (Complete - 2026)

| Feature                                            | Status |
|----------------------------------------------------|--------|
| TriggerRuntime                                     | Done   |
| Periodic triggers (AT EVERY n MS)                  | Done   |
| Cron triggers (AT 'cron-expr')                     | Done   |
| SQL trigger syntax (`CREATE TRIGGER`)              | Done   |
| Trigger parser                                     | Done   |
| Unified time syntax across triggers/windows/WITHIN | Done   |

**Tests**: 5 app_runner tests + 10 compatibility tests, 0 ignored

### M10: Partitions (Mostly Complete - 2026)

| Feature                                  | Status                                          |
|------------------------------------------|-------------------------------------------------|
| Partition runtime                        | Done                                            |
| PARTITION WITH syntax                    | Done                                            |
| Basic partitioning (passthrough, filter) | Done                                            |
| Per-partition aggregation isolation      | Partial — 12 of 142 compatibility tests ignored |

**Remaining issues**: Per-partition aggregation state isolation, multiple partition keys, length batch window with
partition semantics.

### M11: Configuration & Error Handling (Complete - 2025)

| Feature                                                | Status |
|--------------------------------------------------------|--------|
| SQL WITH clause for stream configuration               | Done   |
| TOML [streams.*] and [application] configuration       | Done   |
| Environment variable substitution (`${VAR:default}`)   | Done   |
| Error handling system (drop/retry/dlq/fail strategies) | Done   |
| Dead Letter Queue (DLQ) with schema validation         | Done   |
| Exponential backoff retry logic                        | Done   |
| JSON/CSV source and sink mappers                       | Done   |
| Bytes source and sink mapper                           | Done   |

### M12: Type System (Complete - 2025-10-26)

| Feature                                           | Status |
|---------------------------------------------------|--------|
| Zero-allocation type inference (`&'a SqlCatalog`) | Done   |
| Expression validation at compile time             | Done   |
| Type propagation through operators and functions  | Done   |
| Data-driven function registry                     | Done   |
| Unified relation accessor for streams and tables  | Done   |
| Compile-time type validation for SQL queries      | Done   |

### M13: State Management & Persistence (Complete - 2025)

| Feature                                                         | Status |
|-----------------------------------------------------------------|--------|
| Enhanced StateHolder trait with compression (LZ4, Snappy, Zstd) | Done   |
| Incremental checkpointing with WAL                              | Done   |
| Checkpoint merger with delta compression                        | Done   |
| Point-in-time recovery with parallel engine                     | Done   |
| Pluggable persistence backends (File, Memory)                   | Done   |
| Schema versioning with compatibility checks                     | Done   |
| Redis state backend (RedisPersistenceStore)                     | Done   |
| ThreadBarrier synchronization                                   | Done   |
| Application restart lifecycle support                           | Done   |

### M14: Distributed Processing Foundation (Partial - 2025)

| Feature                                              | Status                                 |
|------------------------------------------------------|----------------------------------------|
| Runtime mode abstraction (Single/Distributed/Hybrid) | Done                                   |
| TCP transport with connection pooling                | Done                                   |
| gRPC transport (Tonic, Protocol Buffers, TLS)        | Done                                   |
| Redis state backend                                  | Done                                   |
| Raft coordination                                    | Foundation only — not production-ready |
| Distributed coordinator (leader election, barriers)  | Foundation only                        |
| Kafka/NATS message broker integration                | Not started                            |

### M15: Developer Experience & Tooling (2025-2026)

| Feature                                                              | Status |
|----------------------------------------------------------------------|--------|
| Docker image (`Dockerfile` + `docker-compose.yml`)                   | Done   |
| CI/CD (GitHub Actions: rust.yml, docker-publish.yml)                 | Done   |
| Docusaurus documentation website                                     | Done   |
| EventFlux Studio VS Code extension                                   | Done   |
| Output rate limiting                                                 | Done   |
| CLI config overrides and element config resolver                     | Done   |
| SEQUENCE auto-restart support (partial)                              | Done   |
| Code quality tooling (clippy, fmt, taplo)                            | Done   |
| Apache-2.0 license                                                   | Done   |
| Compatibility test suite (1,038 passing vs reference implementation) | Done   |

### M16: User-Defined Functions (Partial)

| Feature                              | Status                               |
|--------------------------------------|--------------------------------------|
| Scalar function factory registration | Done                                 |
| Stateful UDF support                 | Done (tests passing)                 |
| UDF invocation from SQL              | Done (1 test ignored for old syntax) |
| WASM UDF / Python UDF bridge         | Not started                          |

### Extension Registry (Complete)

Centralized registry in `EventFluxContext::register_default_extensions()` — all extensions (windows, aggregators,
functions, sources, sinks, mappers, collection aggregations, tables) registered via manual factory methods. Chose manual
registration over `inventory`/`linkme` crates for WASM compatibility.

---

## Ignored Tests Breakdown (189 total, including 51 doc tests)

| Category                            | Count | Notes                                                                  |
|-------------------------------------|-------|------------------------------------------------------------------------|
| Compatibility: patterns             | 24    | NOT patterns, count syntax, chained logical, filter issues             |
| Compatibility: tables               | 20    | PARTITION BY, triggers with tables, complex JOINs, WHERE in table JOIN |
| Compatibility: partitions           | 12    | Per-partition aggregation isolation, multiple keys                     |
| Compatibility: windows              | 9     | Timing-sensitive, cron syntax, sort criteria, batch semantics          |
| Compatibility: aggregations         | 6     | String min/max, complex GROUP BY, stddev, ORDER BY alias               |
| Compatibility: joins                | 4     | Chained joins, GROUP BY with joins                                     |
| Compatibility: functions            | 1     | Modulo operator                                                        |
| RabbitMQ integration                | 7     | Require running broker (CI has service)                                |
| Kafka integration                   | 7     | Require running broker (CI has service)                                |
| Old EventFluxQL syntax              | 10    | Legacy syntax, not applicable to SQL-first engine                      |
| DEFINE AGGREGATION                  | 7     | Incremental aggregation DDL not implemented                            |
| PATTERN/SEQUENCE (app_runner)       | 4     | Pattern syntax in old test format                                      |
| Collection aggregation E2E          | 6     | Double output wiring issue                                             |
| Session window                      | 3     | GROUP BY / aggregation syntax verification                             |
| Pattern phase 2b4                   | 1     | Proactive WITHIN expiry (Phase 3)                                      |
| Pattern EVERY overlapping           | 1     | Sliding window with count quantifiers                                  |
| CASE NULL in WHEN                   | 1     | SQL parser NULL in WHEN clause                                         |
| App runner tables                   | 2     | INSERT INTO TABLE, multiple table JOINs                                |
| App runner other                    | 7     | Functions (LOG/UPPER), joins, partitions, event serialization          |
| Type validation                     | 4     | Aggregation definitions                                                |
| Other (parser, mapper, config, ext) | 3     | Various legacy / minor                                                 |

---

## What's Next

### Priority 1: Kafka Connector — **DONE (2026-07)**

Implemented per `feat/kafka/README.md`: `kafka_source.rs` (BaseConsumer poll loop on `SourceWorker`,
consumer groups, at-least-once via commit-after-delivery, `error.*` strategies, SASL/SSL) and
`kafka_sink.rs` (ThreadedProducer, delivery reports, `kafka.delivery.sync`, backpressure on full
queues). Feature-gated behind `kafka` (in `connectors-all`), `kafka.rdkafka.*` passthrough for
librdkafka tuning, integration tests + CI broker service (apache/kafka:3.9.1), SQL example in
`examples/kafka.eventflux`. Deferred items tracked as issues #125–#131 (exactly-once
transactions, per-event keys/topics/headers, Avro, lag metrics, regex subscription,
shared-helper adoption, source supervision).

HTTP (Priority 2) is the next connector.

### Priority 2: HTTP Connector

- HTTP source: REST polling, webhook listener
- HTTP sink: webhooks, batch requests, retry with exponential backoff

### Priority 3: File Connector

- File source: CSV, JSON, tail mode
- File sink: rotation, compression

### Priority 4: Pattern Processing Gaps

Based on 24 ignored compatibility pattern tests:

| Gap                                                              | Impact                      |
|------------------------------------------------------------------|-----------------------------|
| NOT pattern syntax (`NOT A FOR duration`)                        | Absent event detection      |
| Count pattern SQL syntax (`A{3}`, `A{2,5}` from SQL)             | Pattern quantifiers via SQL |
| Chained logical operators (`A AND B AND C`)                      | Complex boolean patterns    |
| Pattern filter issues (string equality, numeric, function calls) | Filter reliability          |
| WITHIN timeout behavior                                          | Timing correctness          |

### Priority 5: Per-Partition Aggregation Isolation

12 ignored partition tests indicate that aggregation state is global instead of per-partition. Fix requires
partition-scoped state holders.

### Priority 6: Table JOIN Enhancements

20 ignored table compatibility tests cover:

- WHERE clause filtering during table JOIN
- RIGHT/FULL OUTER JOIN on tables
- ORDER BY / LIMIT with table JOINs
- Complex arithmetic with type coercion
- Table alias resolution in SELECT
- Primary key syntax
- CONTAINS IN syntax

### Priority 7: DEFINE AGGREGATION DDL

Incremental aggregation runtime exists but has no SQL DDL (`CREATE AGGREGATION`). 7 tests blocked.

### Priority 8: Sliding Window Converter

`WINDOW('sliding', size, slide)` is parsed but the converter returns an error. Needs mapping to an appropriate window
processor.

### Priority 9: Observability

- Prometheus metrics endpoint
- OpenTelemetry tracing integration
- Health check endpoints

### Priority 10: Production Hardening

- Graceful shutdown improvements
- Structured logging (JSON format)
- Security basics (input validation, rate limiting)
- Memory usage monitoring

---

## Deferred (No Current Timeline)

These features will be considered based on user demand:

| Feature                                               | Reason for Deferral                                 |
|-------------------------------------------------------|-----------------------------------------------------|
| Distributed multi-node processing                     | Foundation exists but no clear demand yet           |
| Query optimization engine                             | Direct AST execution is sufficient for target scale |
| WASM / Python UDFs                                    | Needs validated use case                            |
| AI/LLM integration                                    | Needs validated use case                            |
| Subqueries and CTEs                                   | Complex; no immediate demand                        |
| Window functions (OVER clause)                        | Complex; no immediate demand                        |
| Database backends (PostgreSQL, MySQL, MongoDB)        | Table trait API ready; implement when demanded      |
| Cloud-native state backends (S3, GCS)                 | Infrastructure exists; implement when demanded      |
| Streaming lakehouse integration (Delta Lake, Iceberg) | Future vision                                       |
| SIMD acceleration                                     | Future optimization                                 |

---

## Technical Debt

| Item                              | Document                                                 | Status                                                            |
|-----------------------------------|----------------------------------------------------------|-------------------------------------------------------------------|
| Unified Aggregation Logic         | `feat/unified_aggregation/UNIFIED_AGGREGATION_DESIGN.md` | Proposed — window and collection aggregators duplicate core logic |
| Collection aggregation E2E wiring | 6 ignored integration tests                              | Double output issue needs investigation                           |
| Compatibility pattern filters     | 24 ignored tests                                         | Various filter behaviors differ from reference                    |
| Sliding window converter          | SQL converter returns error                              | Needs window type mapping                                         |

---

## Architecture Reference

```
src/
  sql_compiler/          SQL parser (vendored sqlparser-rs fork)
    converter.rs         SQL AST to EventFlux runtime conversion
    type_inference.rs    Compile-time type validation
    pattern_validation.rs Pattern/Sequence validation
    catalog.rs           Schema catalog
    application.rs       Multi-statement SQL app compilation
  query_api/             AST and query language structures
  core/
    config/              Configuration (TOML, context, resolver)
    event/               Event types and processing
    executor/            Expression executors (condition, function, cast)
    query/
      processor/stream/window/  16 window processors
      selector/attribute/aggregator/  13 aggregators
      input/stream/state/  Pattern processing (pre/post state processors)
      output/            Insert/Update/Delete/Upsert processors
    stream/
      input/source/      Sources (timer, rabbitmq, websocket)
      output/sink/       Sinks (log, rabbitmq, websocket)
      mapper/            Data mappers (json, csv, bytes)
    table/               Table implementations (inMemory, cache, jdbc)
    trigger/             Trigger runtime
    partition/           Partition runtime
    persistence/         State management, checkpointing, WAL
    distributed/         Transport, state backends, coordination
    util/pipeline/       High-performance event pipeline
    extension/           Extension trait definitions and factories
    error/               Error handling system (DLQ, retry)
vendor/
  datafusion-sqlparser-rs/  Vendored SQL parser with streaming extensions
website/                 Docusaurus documentation site
studio/                  VS Code extension
```

---

## Release Philosophy

- **Ship > Users > Iterate**
- Focus on single-node excellence before distributed
- Build what users ask for, not what might be needed
- Keep it lightweight, keep it simple
