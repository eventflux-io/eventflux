# EventFlux Configuration System

**Target**: M2 (Part B - Essential Connectivity)
**Last Updated**: 2025-11-09

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Configuration Flow](#configuration-flow)
4. [Configuration Syntax](#configuration-syntax)
5. [Data Mapping](#data-mapping)
6. [TOML Configuration](#toml-configuration)
7. [Implementation Guide](#implementation-guide)
8. [Troubleshooting Configuration Issues](#troubleshooting-configuration-issues)
9. [Complete Example](#complete-example)

---

## Overview

EventFlux uses a **4-layer configuration system** combining SQL WITH clauses and TOML files. Configuration follows **progressive disclosure** - start simple, add complexity as needed.

### Key Principles

1. **SQL-First**: Standard SQL WITH clause for stream configuration
2. **Extension-Agnostic**: Parser validates syntax only, extensions validate semantics
3. **Progressive Complexity**: Defaults for development, full control for production
4. **Security-First**: Credentials only in TOML/environment variables
5. **Environment-Aware**: Separate TOML files per environment

### Configuration Layers

**4-Layer Merge Model** (highest to lowest priority):

```
Layer 1 (Highest):  SQL WITH clause           [Runtime overrides]
    ↓
Layer 2:            TOML [streams.StreamName] [Stream-specific config]
    ↓
Layer 3:            TOML [application]        [Application defaults]
    ↓
Layer 4 (Lowest):   Rust hardcoded defaults   [Framework defaults]
```

### Configuration Merge Strategy

When EventFlux loads a stream configuration:

1. Start with Rust defaults (framework defaults)
2. Merge TOML `[application]` (application-wide settings)
3. Merge TOML `[streams.StreamName]` (stream-specific config)
4. Merge SQL WITH (runtime overrides - highest priority)

**Merge Semantics**: **Per-Property Merge** (not per-namespace replacement)

**Example:**
```
Rust Default:          kafka.consumer.group = "eventflux-{stream_name}"
    ↓
TOML [application]:    kafka.consumer.group = "global-group"
    ↓
TOML [streams.Orders]: kafka.consumer.group = "orders-group"
    ↓
SQL WITH:              'kafka.consumer.group' = 'dev-group'  ← Final Value
```

### Per-Property Merge Behavior

**Key Rule**: Stream-specific properties override application properties **per-property**, NOT per-namespace.

**Why This Matters**: Allows defining common configuration (credentials, connection pools) once in `[application]`, with streams inheriting everything except what they explicitly override.

**Example - Authentication Inheritance**:

```toml
# config-prod.toml

# Common Kafka config for ALL streams
[application.kafka]
brokers = "prod1:9092,prod2:9092,prod3:9092"
security.protocol = "SASL_SSL"
security.username = "${KAFKA_USER}"
security.password = "${KAFKA_PASSWORD}"
timeout = "30s"
consumer.group = "default-group"

# Orders stream - only override topic and consumer group
[streams.Orders.kafka]
topic = "orders"
consumer.group = "orders-group"

# Alerts stream - only override topic
[streams.Alerts.kafka]
topic = "alerts"
```

**Effective Configuration for Orders**:
```
Orders inherits:
  brokers = "prod1:9092,prod2:9092,prod3:9092"    [from application.kafka]
  security.protocol = "SASL_SSL"                   [from application.kafka]
  security.username = "${KAFKA_USER}"              [from application.kafka]
  security.password = "${KAFKA_PASSWORD}"          [from application.kafka]
  timeout = "30s"                                  [from application.kafka]

Orders overrides:
  topic = "orders"                                 [from streams.Orders.kafka]
  consumer.group = "orders-group"                  [from streams.Orders.kafka]

Final Orders config:
  ✅ Has authentication (inherited)
  ✅ Has stream-specific topic and consumer group
```

**Effective Configuration for Alerts**:
```
Alerts inherits:
  brokers = "prod1:9092,prod2:9092,prod3:9092"    [from application.kafka]
  security.protocol = "SASL_SSL"                   [from application.kafka]
  security.username = "${KAFKA_USER}"              [from application.kafka]
  security.password = "${KAFKA_PASSWORD}"          [from application.kafka]
  timeout = "30s"                                  [from application.kafka]
  consumer.group = "default-group"                 [from application.kafka]

Alerts overrides:
  topic = "alerts"                                 [from streams.Alerts.kafka]

Final Alerts config:
  ✅ Has authentication (inherited)
  ✅ Uses default consumer group (inherited)
  ✅ Has stream-specific topic
```

**Benefits**:
- ✅ **DRY Configuration**: Define credentials once, all streams inherit
- ✅ **No Accidental Loss**: Streams don't lose authentication by specifying topic
- ✅ **Flexibility**: Override only what differs per stream
- ✅ **Security**: Credentials centralized in `[application]` section

**Array/List Representation**:

Lists are represented as **comma-separated strings** (consistent with industry standards like Kafka, Flink, Spark):
```toml
[application.kafka]
bootstrap.servers = "host1:9092,host2:9092"

[streams.Orders.kafka]
bootstrap.servers = "localhost:9092"

# Result: "localhost:9092"  ← Replaced (per-property merge)
```

**Merge Behavior**: Lists are **replaced**, not concatenated:
```toml
[application.kafka]
topics = "topic1,topic2,topic3"

[streams.Orders.kafka]
topics = "orders"

# Result: "orders"  ← Stream-specific replaces application-level
```

**Rationale**:
- ✅ **Consistency**: Same format in SQL WITH (`'servers' = 'a,b,c'`) and TOML (`servers = "a,b,c"`)
- ✅ **Industry Standard**: Matches Kafka, Flink, ksqlDB, Spark configuration patterns
- ✅ **Simplicity**: No conversion logic needed between formats
- ✅ **Extension Autonomy**: Extensions parse according to their own schema

---

## Architecture

### Stream Types: Internal with Optional External I/O

**Core Concept**: All streams in EventFlux are **inherently internal** (in-memory, query-able). The `type` property **extends** a stream with external I/O capabilities.

**Property**: `'type' = 'source' | 'sink'` (optional)

> **📘 Stream I/O Model**
>
> Every stream in EventFlux is **internally writable and readable**:
> - ✅ Can be written to via `INSERT INTO` queries
> - ✅ Can be read from via `SELECT` queries
> - ✅ Can be used in JOINs, windows, and aggregations
>
> Adding `type='source'` or `type='sink'` **extends** the stream with external I/O:
> - `type='source'` → Stream ALSO receives input from external system (Kafka, HTTP, etc.)
> - `type='sink'` → Stream ALSO writes output to external system (Kafka, HTTP, etc.)
> - Omit `type` → Pure internal stream (no external I/O)

**Design Rules**:
- `type` is OPTIONAL (omit for pure internal streams)
- When specified, `type` requires `extension` and `format` properties
- `type` CANNOT be defined in TOML (SQL-first, parser needs it at parse-time)
- Tables don't use `type` (always bidirectional)

```sql
-- Internal stream (pure in-memory, no external I/O)
CREATE STREAM FilteredOrders (orderId STRING, amount DOUBLE);

-- Populate via query
INSERT INTO FilteredOrders
SELECT orderId, amount FROM Orders WHERE amount > 100;

-- Query it
SELECT * FROM FilteredOrders;

-- External Source stream (receives FROM external + internally query-able)
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json'
);
-- ✅ Kafka → Orders → Can SELECT * FROM Orders

-- External Sink stream (writes TO external + still query-able!)
CREATE STREAM Alerts (...) WITH (
    'type' = 'sink',
    'extension' = 'http',
    'format' = 'json'
);
-- ✅ INSERT INTO Alerts ... → HTTP endpoint
-- ✅ Can also: SELECT * FROM Alerts (for monitoring, debugging)

-- Table (bidirectional, no type)
CREATE TABLE Users (...) WITH (
    'extension' = 'mysql'
);
```

> **🔥 Powerful Capability: Event Source Merging**
>
> Streams with `type='source'` can receive events from **BOTH** external systems **AND** internal queries simultaneously:
>
> ```sql
> -- Stream receives events from BOTH Kafka AND internal queries
> CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
>     'type' = 'source',
>     'extension' = 'kafka',
>     'topic' = 'orders-topic',
>     'format' = 'json'
> );
>
> -- ✅ Orders receives events FROM:
> --    1. Kafka topic 'orders-topic' (external source)
> --    2. Internal INSERT queries (below)
>
> -- Internal query writing to the SAME stream
> INSERT INTO Orders
> SELECT orderId, amount * 1.1 AS amount
> FROM HistoricalOrders
> WHERE timestamp > yesterday();
>
> -- Result: Orders stream processes events from BOTH sources
> -- This enables hybrid data ingestion patterns!
> ```
>
> **Why This Matters**:
> - ✅ Mix real-time external data with historical replays
> - ✅ Enrich external streams with computed events
> - ✅ Test production streams with synthetic data injection
> - ✅ Implement complex data merging topologies

**Stream Type Characteristics**:

| Type | External I/O | Extension Required | Format Required | Always Query-able | Use Case |
|------|--------------|-------------------|-----------------|-------------------|----------|
| `source` | ✅ Read IN | ✅ Yes | ✅ Yes | ✅ Yes | Read from Kafka/HTTP, also query |
| `sink` | ✅ Write OUT | ✅ Yes | ✅ Yes | ✅ Yes | Write to Kafka/HTTP, also query |
| *(omitted)* | ❌ None | ❌ No | ❌ No | ✅ Yes | Pure in-memory processing |

### Table Configuration

Tables are **bidirectional** data structures for persistent lookups and joins. Unlike streams, tables don't have a type property.

**Property Requirements**:

| Property | Required | Allowed Values | Notes |
|----------|----------|----------------|-------|
| `type` | ❌ No | N/A | Tables don't use `type` (always bidirectional) |
| `extension` | ✅ Yes | `mysql`, `postgres`, `redis`, etc. | Specifies backing store |
| `format` | ❌ No | N/A | Tables use relational schema only |

**Validation Rules**:
- `type` property is **FORBIDDEN** for tables (parse error if present)
- `format` property is **FORBIDDEN** for tables (parse error if present)
- `extension` is **REQUIRED** for tables (specifies backing store)
- Tables CANNOT be configured with `error.dlq.stream` (no DLQ for tables)

**Examples**:

```sql
-- ✅ Valid table configuration
CREATE TABLE Users (
    userId STRING,
    userName STRING,
    email STRING
) WITH (
    'extension' = 'mysql',
    'mysql.host' = 'localhost',
    'mysql.database' = 'users_db',
    'mysql.table' = 'users'
);

-- ❌ Invalid: type not allowed for tables
CREATE TABLE Users (...) WITH (
    'type' = 'source',  -- ❌ ERROR: Tables cannot have 'type' property
    'extension' = 'mysql'
);

-- ❌ Invalid: format not allowed for tables
CREATE TABLE Users (...) WITH (
    'extension' = 'mysql',
    'format' = 'json'  -- ❌ ERROR: Tables cannot have 'format' property
);
```

**TOML Configuration for Tables**:

```toml
# Application-level database defaults
[application.mysql]
host = "${MYSQL_HOST:localhost}"
port = 3306
username = "${MYSQL_USER}"
password = "${MYSQL_PASSWORD}"
database = "myapp"

# Table-specific configuration
[tables.Users.mysql]
# Inherits: host, port, username, password, database from [application.mysql]
table = "users"
cache.enabled = true
cache.ttl = "5m"
```

**Note**: Tables use `[tables.TableName.*]` syntax in TOML (not `[streams.*]`) for clarity. Tables follow the same per-property merge behavior as streams.

### Extension Property

**Property**: `'extension' = 'kafka' | 'http' | 'mysql' | 'file' | ...`

Specifies which connector to use. All extension-specific properties are prefixed with the extension name.

```sql
CREATE STREAM KafkaIn (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'kafka.brokers' = 'localhost:9092',
    'kafka.topic' = 'orders'
);
```

### Format Property

**Property**: `'format' = 'json' | 'avro' | 'csv' | 'protobuf' | 'xml' | 'bytes' | ...`

Specifies data serialization format. All format-specific properties are prefixed with the format name.

**Format Requirements**:

| Stream Configuration | Format | Rule |
|---------------------|--------|------|
| `type='source'` or `type='sink'` | **REQUIRED** | Must explicitly specify format for external I/O |
| No `type` (pure internal) | **OMIT** | No format (passthrough events) |
| `table` | **NOT ALLOWED** | Tables use relational schema only |

**Examples**:

```sql
-- External source - format REQUIRED
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json',  -- Must specify
    'json.ignore-parse-errors' = 'true'
);

-- Binary data - explicit 'bytes' format
CREATE STREAM BinaryEvents (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'bytes'  -- No deserialization
);

-- Internal stream - NO format (no external I/O)
CREATE STREAM FilteredOrders (...);
-- No WITH clause needed for pure internal streams

-- Table - NO format (relational only)
CREATE TABLE Users (...) WITH (
    'extension' = 'mysql',
    'mysql.database' = 'mydb'
);
```

**Rationale**: Explicit format requirements prevent implicit assumptions and ensure extensions can validate supported formats at initialization.

#### Format vs Passthrough Behavior

**CRITICAL DISTINCTION**: Format specifies *transport encoding*, NOT attribute data types.

**Passthrough (No Format)**:
- Used when NO format property specified (internal streams only)
- Event objects pass directly through junction in their internal representation
- **Zero serialization overhead** - Events remain as Rust `Event` structs
- Equivalent to Siddhi's passthrough mapper (default behavior)

**Binary Format (`format = 'bytes'`)**:
- Used for explicit binary serialization
- Events serialized to compact binary format (bincode/MessagePack)
- **Small wire format**, suitable for cross-process communication
- Equivalent to Siddhi's binary mapper

**Key Insight**:
```
Source Stream (Kafka) → JSON Format → Event (deserialized)
    ↓
Internal Stream → NO format → Event (passthrough, zero-cost)
    ↓
Internal Stream → NO format → Event (passthrough, zero-cost)
    ↓
Sink Stream (HTTP) → JSON Format → bytes (serialized)
```

**When to Use Each**:
- **Passthrough** (no format): Internal streams, query results, transformations
- **Binary** (format='bytes'): Cross-node communication, distributed processing
- **Text formats** (json/csv/xml): External systems, human-readable data
- **Schema formats** (avro/protobuf): Schema evolution, efficient serialization

**Type Definition Policy**:
- **Type REQUIRED in SQL WITH clause**: Stream type must be explicitly declared in CREATE STREAM
- **TOML cannot define `type`**: Type in TOML is **rejected with error** (SQL-first, parser needs type at parse-time)
- **Rationale**: Type is structural (affects parser validation at Phase 1), consistent with SQL-first configuration priority
- **Validation**: Parser validates type-specific rules (format required, extension required) at parse-time

---

## Configuration Flow

### ConfigManager and ApplicationConfig Relationship

EventFlux configuration uses a hierarchical structure where ApplicationConfig is nested within EventFluxConfig.

**Structure Hierarchy**:
```
EventFluxConfig (top-level loaded by ConfigManager)
├── apiVersion: String
├── kind: String
├── metadata: ConfigMetadata
├── eventflux: EventFluxGlobalConfig (global runtime settings)
└── applications: HashMap<String, ApplicationConfig>

ApplicationConfig (per-application configuration)
├── streams: HashMap<String, StreamConfig>
├── definitions: HashMap<String, DefinitionConfig>
├── queries: HashMap<String, QueryConfig>
├── persistence: Option<PersistenceConfig>
├── monitoring: Option<MonitoringConfig>
└── error_handling: Option<ErrorHandlingConfig>

StreamConfig
├── source: Option<SourceConfig>
└── sink: Option<SinkConfig>
```

**Configuration Path**:
There is a single configuration path in production:
```
YAML/TOML files
    ↓
ConfigManager::load_unified_config()
    ↓
EventFluxConfig { applications: HashMap<String, ApplicationConfig> }
    ↓
EventFluxManager::get_application_config(app_name)
    ↓
config.applications.get(app_name) → ApplicationConfig
    ↓
EventFluxAppRuntime::new_with_config(..., app_config)
    ↓
EventFluxAppContext { app_config: Some(ApplicationConfig) }
    ↓
EventFluxAppRuntime::start() → auto_attach_sources/sinks/tables
```

**YAML Example**:
```yaml
apiVersion: eventflux.io/v1
kind: EventFluxConfig

eventflux:
  application:
    name: "MyApp"
  runtime:
    mode: distributed

applications:
  MyApp:
    streams:
      OrdersOut:
        sink:
          type: kafka
          format: json
          connection:
            bootstrap_servers: "localhost:9092"
          security:
            tls:
              enabled: true
          delivery_guarantee: exactly-once
          retry:
            max_attempts: 5
```

ConfigManager loads this into EventFluxConfig, then EventFluxManager extracts the ApplicationConfig for "MyApp", which contains the sink configuration with all fields (connection, security, retry, etc.).

### ApplicationConfig Purpose

ApplicationConfig serves as the typed configuration container for application-specific settings. EventFluxConfig can contain multiple ApplicationConfigs keyed by application name, allowing a single configuration file to define settings for multiple EventFlux applications.

**Separation of Concerns**:
- ConfigManager: file loading, merging, validation, hot-reload
- EventFluxConfig: global infrastructure configuration (runtime mode, observability, coordination)
- ApplicationConfig: business logic configuration (streams, tables, queries, persistence)

---

## Configuration Syntax

### Property Namespacing

**Clear namespace separation prevents conflicts:**

| Pattern | Purpose | Example |
|---------|---------|---------|
| `{extension}.*` | Extension properties | `kafka.brokers`, `http.url` |
| `{format}.*` | Format options | `json.ignore-parse-errors` |
| `{format}.mapping.*` | Source field extraction | `json.mapping.orderId` |
| `{format}.template` | Sink output template | `json.template` |

### Validation Strategy

**Three-Phase Validation:**

#### Phase 1: Parse-Time (Syntax Only)

**When**: SQL parsing and TOML loading
**Purpose**: Validate configuration structure

Parser validates:
- Required properties present (`type`, `extension` for external streams)
- Property format valid (proper namespacing)
- Type consistency (quoted strings, numeric values)
- Stream type rules (format required for source/sink, omitted for internal)

Does NOT validate:
- Extension-specific property names or values
- Connectivity or resource availability
- Data format compatibility

**Example Errors**:
```
"Stream 'Orders' has type='source' but missing required 'format' property"
"Stream 'Internal' has type='internal' but specifies 'extension' (not allowed)"
```

#### Phase 2: Application Initialization (Fail-Fast)

**When**: Application startup, before processing events
**Purpose**: Validate all external connections and configurations

Extensions perform:
- **Config validation**: Check all extension-specific properties
- **Format validation**: Verify format is supported by extension
- **Connection establishment**: Connect to external systems
- **Credential validation**: Authenticate with external systems

**Behavior**: **Application FAILS to start** if any validation fails.

**Rationale**: "What's the point of deploying an application if the transports are not ready?" Fail fast at startup to catch configuration errors immediately.

**Example**:
```rust
// KafkaExtension validates at initialization
impl Extension for KafkaExtension {
    fn initialize(&self, config: &Config) -> Result<(), EventFluxError> {
        // 1. Validate config properties
        require_property(config, "kafka.brokers")?;
        require_property(config, "kafka.topic")?;

        // 2. Validate format support
        let format = config.get("format")?;
        if !["json", "avro", "bytes"].contains(&format) {
            return Err("Kafka only supports json, avro, bytes formats");
        }

        // 3. Establish connection (FAIL FAST if unreachable)
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;  // ← FAILS HERE if Kafka unreachable

        // 4. Validate topic exists
        consumer.fetch_metadata(Some(&topic), timeout)?;

        Ok(())
    }
}
```

**Operational Impact**:
- ✅ Kubernetes/systemd will restart until transports are available
- ✅ Configuration errors caught before production traffic
- ✅ No "zombie" applications running with broken connectivity

#### Phase 3: Runtime (Resilient Retry)

**When**: During event processing
**Purpose**: Handle transient failures gracefully

Extensions handle:
- **Connection drops**: Retry with exponential backoff
- **Transient errors**: Network blips, temporary unavailability
- **Data errors**: Malformed messages, parse failures (based on error strategy)

**Behavior**: Application CONTINUES running, retries failed operations.

**Rationale**: "If it starts and fails in the middle, then it makes sense to retry with backoff." Transient issues shouldn't crash a running application.

**Example**:
```rust
// KafkaSource handles runtime errors (push-based model)
impl Source for KafkaSource {
    fn start(&mut self, handler: Arc<Mutex<InputHandler>>) {
        let handler = Arc::clone(&handler);
        let consumer = Arc::clone(&self.consumer);
        let max_retries = self.max_retries;
        let error_strategy = self.error_strategy.clone();

        thread::spawn(move || {
            let mut consecutive_errors = 0;

            loop {
                match consumer.poll(timeout) {
                    Ok(msg) => {
                        consecutive_errors = 0; // Reset on success

                        // Push event to InputHandler
                        if let Err(e) = handler.lock().unwrap().send_event(msg) {
                            eprintln!("Failed to send event to pipeline: {}", e);
                        }
                    }
                    Err(e) if e.is_retriable() => {
                        consecutive_errors += 1;

                        if consecutive_errors >= max_retries {
                            // Max retries exceeded, apply error strategy
                            match error_strategy {
                                ErrorStrategy::Drop => {
                                    eprintln!("Dropping event after {} retries: {}", max_retries, e);
                                    consecutive_errors = 0; // Reset and continue
                                }
                                ErrorStrategy::Fail => {
                                    eprintln!("Failing source after {} retries: {}", max_retries, e);
                                    return; // Exit thread, stop source
                                }
                                ErrorStrategy::Dlq => {
                                    // Send to dead letter queue
                                    self.send_to_dlq(e);
                                    consecutive_errors = 0; // Reset and continue
                                }
                                _ => {}
                            }
                        } else {
                            // Retry with exponential backoff
                            let backoff = exponential_backoff(consecutive_errors);
                            eprintln!("Retriable error (attempt {}/{}): {}. Retrying in {:?}",
                                     consecutive_errors, max_retries, e, backoff);
                            sleep(backoff);
                        }
                    }
                    Err(e) => {
                        // Non-retriable error, apply error strategy immediately
                        match error_strategy {
                            ErrorStrategy::Drop => {
                                eprintln!("Non-retriable error, dropping: {}", e);
                            }
                            ErrorStrategy::Fail => {
                                eprintln!("Non-retriable error, failing source: {}", e);
                                return; // Exit thread, stop source
                            }
                            ErrorStrategy::Dlq => {
                                self.send_to_dlq(e);
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
    }
}
```

**Validation Summary**:

| Phase | Timing | Failure Behavior | Purpose |
|-------|--------|------------------|---------|
| **Parse-Time** | SQL/TOML load | Syntax error | Structure validation |
| **Initialization** | App startup | **Fail to start** | Config and connectivity |
| **Runtime** | Event processing | **Retry with backoff** | Transient failures |

**Circular Dependency Detection**:

EventFlux detects direct circular dependencies during query parsing:
```sql
INSERT INTO A SELECT * FROM B;
INSERT INTO B SELECT * FROM A;  -- ❌ ERROR: Circular dependency detected
```

**Full Cycle Detection Algorithm** (Phase 1 - Parse-Time):

```rust
fn detect_circular_dependencies(queries: &[InsertQuery]) -> Result<()> {
    // Build dependency graph: target_stream → [source_streams]
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for query in queries {
        let target = &query.target_stream;
        let sources = query.get_source_streams();  // Extract FROM, JOIN sources (no subqueries)

        dependencies.entry(target.clone())
            .or_insert_with(HashSet::new)
            .extend(sources);
    }

    // Check for circular dependencies using DFS
    let mut visited_global = HashSet::new();

    for start_node in dependencies.keys() {
        if !visited_global.contains(start_node) {
            let mut visited_path = HashSet::new();
            let mut path = Vec::new();
            dfs_check_cycle(start_node, &dependencies, &mut visited_global, &mut visited_path, &mut path)?;
        }
    }

    // ✅ No cycles detected
    Ok(())
}

fn dfs_check_cycle(
    node: &String,
    dependencies: &HashMap<String, HashSet<String>>,
    visited_global: &mut HashSet<String>,
    visited_path: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> Result<()> {
    // If node already in current path, cycle detected
    if visited_path.contains(node) {
        path.push(node.clone());
        let cycle_path = path.join(" → ");
        return Err(format!(
            "Circular dependency detected: {}\nStreams cannot form dependency cycles.",
            cycle_path
        ));
    }

    // If already visited in previous DFS, skip
    if visited_global.contains(node) {
        return Ok(());
    }

    // Mark as visited in current path
    visited_path.insert(node.clone());
    path.push(node.clone());

    // Visit all dependencies
    if let Some(deps) = dependencies.get(node) {
        for dep in deps {
            dfs_check_cycle(dep, dependencies, visited_global, visited_path, path)?;
        }
    }

    // Remove from current path (backtrack)
    visited_path.remove(node);
    path.pop();

    // Mark as globally visited
    visited_global.insert(node.clone());

    Ok(())
}
```

**Detection Scope**: **All cycles** (direct and multi-level)

**Validation Timing**: Called once after all INSERT queries parsed (Phase 1)

**Example Scenarios**:

✅ **Allowed** (no cycles):
```sql
INSERT INTO B SELECT * FROM A;
INSERT INTO C SELECT * FROM B;
INSERT INTO D SELECT * FROM C;
-- Linear: A → B → C → D (no cycles)
```

❌ **Blocked** (direct cycle):
```sql
INSERT INTO B SELECT * FROM A;
INSERT INTO A SELECT * FROM B;
-- ❌ ERROR: Circular dependency detected: A → B → A
```

❌ **Blocked** (multi-level cycle):
```sql
INSERT INTO B SELECT * FROM A;
INSERT INTO C SELECT * FROM B;
INSERT INTO A SELECT * FROM C;
-- ❌ ERROR: Circular dependency detected: A → B → C → A
```

**get_source_streams() Implementation Note**:

The `query.get_source_streams()` method extracts ALL source streams from a query:
- **FROM clause streams**: Primary data source
- **JOIN clause streams**: All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- **Subqueries**: NOT supported (EventFlux does not support subqueries)

**Example**:
```sql
INSERT INTO Result
SELECT * FROM StreamA
INNER JOIN StreamB ON StreamA.id = StreamB.id
LEFT JOIN StreamC ON StreamA.category = StreamC.category;

-- get_source_streams() returns: ["StreamA", "StreamB", "StreamC"]
```

**Edge Cases and Special Scenarios**:

**Tables in JOINs** - Tables are **INCLUDED** in dependency graph:
```sql
INSERT INTO EnrichedOrders
SELECT o.*, u.userName
FROM Orders o
JOIN Users u ON o.userId = u.userId;

-- Dependency: EnrichedOrders depends on [Orders, Users]
-- ✅ Allowed: Tables can be in dependency graph
-- ⚠️ Note: Circular dependencies involving tables are checked same as streams
```

**Self-Referencing Streams** - **NOT ALLOWED** (creates immediate cycle):
```sql
INSERT INTO A
SELECT * FROM A WINDOW TUMBLING(5 sec);

-- ❌ ERROR: Circular dependency detected: A → A
-- Rationale: Self-referencing creates immediate cycle
```

**Valid Pattern** - Use intermediate stream to avoid self-reference:
```sql
-- ✅ Correct approach: Use intermediate stream
CREATE STREAM A_Windowed (...);
-- Pure internal stream (no type needed, no external I/O)

INSERT INTO A_Windowed
SELECT * FROM A WINDOW TUMBLING(5 sec);

-- No cycle: A → A_Windowed (linear dependency)
```

**Table Self-Reference** - Same rules apply as streams:
```sql
-- ❌ NOT ALLOWED: Table cannot reference itself
INSERT INTO UsersTable
SELECT * FROM UsersTable WHERE active = true;

-- ❌ ERROR: Circular dependency detected: UsersTable → UsersTable
```

**Mixed Stream and Table Cycles** - Detected the same way:
```sql
INSERT INTO StreamA SELECT * FROM TableB;
INSERT INTO TableB SELECT * FROM StreamA;

-- ❌ ERROR: Circular dependency detected: StreamA → TableB → StreamA
-- Detection: DFS treats tables and streams uniformly in dependency graph
```

> **⚠️ Phase 1 Detection Limitations**
>
> **SQL-Level Only**: Phase 1 circular dependency detection only catches dependencies visible in SQL query structure (simple passthrough queries).
>
> **NOT Detected in Phase 1**:
> - **Runtime/Transport-Level Loops**: Example: Stream A → Stream B (Kafka sink to topic X) → Stream C (Kafka source from topic X) → Stream A
> - **Complex Processing Chains**: Queries with transformations, aggregations, or windows where runtime dependencies aren't obvious from SQL alone
> - **External System Loops**: Cycles created through external systems (databases, message brokers, APIs)
>
> **Rationale**: Detecting all possible runtime loops is extremely complex with many edge cases. Phase 1 focuses on obvious SQL-level cycles that can be caught at initialization time.
>
> **Future Work**: Enhanced runtime loop detection may be added in later phases, potentially with runtime monitoring and circuit breakers.

### Configuration System Integration Flow

**How ConfigManager connects to Stream Initialization:**

EventFlux uses a two-stage configuration resolution process:

**Stage 1: TOML Resolution (ConfigManager)**
```
ConfigManager.load_unified_config()
   └─> Merges: Rust defaults → YAML (50) → K8s (75) → Env (100)
   └─> Produces: EventFluxConfig
       ├─> eventflux: EventFluxGlobalConfig (global runtime settings)
       └─> applications: HashMap<String, ApplicationConfig>
           ├─> "StreamName1": ApplicationConfig
           ├─> "StreamName2": ApplicationConfig
           └─> ...
```

**Stage 2: Per-Stream Resolution (Stream Initialization)**
```
For each stream "StreamName":
   1. Extract TOML application config: EventFluxConfig.applications.get("*")
      └─> Contains [application.kafka], [application.json], etc.

   2. Extract TOML stream config: EventFluxConfig.applications.get("StreamName")
      └─> Contains [streams.StreamName.kafka], [streams.StreamName.json], etc.

   3. Build FlatConfig for this stream:
      Rust defaults (framework defaults)
        ↓ merge
      TOML [application.*]  (from step 1)
        ↓ merge
      TOML [streams.StreamName.*]  (from step 2)
        ↓ merge
      SQL WITH (parsed from CREATE STREAM)
        ↓
      Final FlatConfig for stream initialization
```

**Key Insight**: ConfigManager is NOT aware of SQL WITH clauses. It only merges TOML files and environment variables. SQL WITH merging happens later during stream initialization.

**Example Flow**:
```rust
// 1. Load unified TOML config (all loaders merged)
let eventflux_config = config_manager.load_unified_config().await?;

// 2. For each stream parsed from SQL:
for stream_def in parsed_streams {
    // 3. Get application-level TOML (if exists)
    let app_config = eventflux_config.applications.get("*");

    // 4. Get stream-specific TOML (if exists)
    let stream_config = eventflux_config.applications.get(&stream_def.name);

    // 5. Merge into FlatConfig
    let mut flat_config = FlatConfig::from_rust_defaults();
    if let Some(app) = app_config {
        flat_config.merge(app)?;  // Application-level TOML
    }
    if let Some(stream) = stream_config {
        flat_config.merge(stream)?;  // Stream-specific TOML
    }
    flat_config.merge(stream_def.with_clause)?;  // SQL WITH (highest priority)

    // 6. Initialize stream with final config
    initialize_stream(&stream_def, &flat_config)?;
}
```

### Error Handling Strategy

**Configuration Properties**: `error.*`

EventFlux provides configurable error handling for runtime failures (connection drops, malformed data, processing errors).

**Default Strategy**: **`drop`** with `warn` level logging

#### Error Strategies

**IMPORTANT**: Error strategies are **mutually exclusive**. You must choose exactly ONE primary strategy per stream.

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `drop` | Log error and discard event | **Default** - Lossy, high throughput |
| `retry` | Retry with exponential backoff, then drop (or send to the DLQ if `error.dlq.stream` is set) | Transient failures, eventually consistent |
| `dlq` | Send to dead-letter queue stream | Audit trail, manual review |
| `fail` | Fail application immediately | Critical data, zero tolerance |

**Note on Hybrid Behavior**: For retry with DLQ fallback, use `strategy = "retry"` with `dlq.stream` set: events whose retries are exhausted (or whose error is not retriable) go to the DLQ (see DLQ Pattern 1 below). `dlq.fallback-strategy` controls what happens when DLQ delivery itself fails (see DLQ Fallback Strategies below).

#### Configuration Examples

**Application-Level Defaults**:
```toml
[application.error]
strategy = "drop"           # Default: drop and log
log-level = "warn"          # Log level for dropped events
```

**Stream-Specific Overrides**:
```toml
# Critical orders - send to DLQ with retry fallback
[streams.Orders.error]
strategy = "dlq"
dlq.stream = "OrderErrors"
dlq.fallback-strategy = "retry"           # Retry DLQ delivery on failure
dlq.fallback-retry.max-attempts = 3
dlq.fallback-retry.initial-delay = "100ms"

# Transient failures - simple retry
[streams.Metrics.error]
strategy = "retry"
retry.max-attempts = 3
retry.backoff = "exponential"
retry.initial-delay = "100ms"
retry.max-delay = "30s"

# Non-critical logs - drop immediately
[streams.Logs.error]
strategy = "drop"
log-level = "debug"

# Payment processing - fail fast
[streams.Payments.error]
strategy = "fail"
fail.message = "Payment processing cannot tolerate errors"
```

**SQL WITH Clause**:
```sql
-- Example: DLQ strategy with simple fallback
CREATE STREAM CriticalOrders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json',
    'error.strategy' = 'dlq',
    'error.dlq.stream' = 'OrderErrors',
    'error.dlq.fallback-strategy' = 'log'
);

-- Example: Simple retry strategy
CREATE STREAM Metrics (...) WITH (
    'type' = 'source',
    'extension' = 'http',
    'format' = 'json',
    'error.strategy' = 'retry',
    'error.retry.max-attempts' = '5',
    'error.retry.backoff' = 'exponential'
);
```

#### Error Types and Handling

**Connection Errors** (Network, timeout):
- **retry**: Exponential backoff, reconnect
- **drop**: Log and skip
- **fail**: Terminate application

**Data Errors** (Parse failure, malformed):
- **retry**: May succeed after transient corruption
- **drop**: Skip invalid event
- **dlq**: Preserve for manual review
- **fail**: Critical data integrity requirement

**Processing Errors** (Query execution, transformation):
- **retry**: May succeed after resource availability
- **drop**: Best-effort processing
- **fail**: Strict correctness requirement

#### Exponential Backoff Formula

```rust
delay = min(initial_delay * 2^attempt, max_delay)

Example with initial=100ms, max=30s:
  Attempt 1: 100ms
  Attempt 2: 200ms
  Attempt 3: 400ms
  Attempt 4: 800ms
  Attempt 5: 1.6s
  Attempt 6: 3.2s
  Attempt 7: 6.4s
  Attempt 8: 12.8s
  Attempt 9: 25.6s
  Attempt 10+: 30s (capped)
```

#### Dead-Letter Queue (DLQ)

**DLQ Streams** receive failed events after retry attempts exhausted.

**Creation Rules**:
- **Manual Creation Required**: DLQ streams must be explicitly declared in SQL
- **Type Flexibility**: Can be pure internal (no type, in-memory review) OR `sink` (persistent storage)
- **Exact Schema Required**: DLQ stream schema must match error event structure exactly (no additional attributes)

**DLQ Pattern 1: Internal Stream (In-Memory)**:
```sql
-- Main processing stream
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json',
    'error.strategy' = 'retry',
    'error.retry.max-attempts' = '3',
    'error.dlq.stream' = 'OrderErrors'
);

-- DLQ stream for in-memory processing
CREATE STREAM OrderErrors (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
);
-- Pure internal stream (no type needed, no external I/O)

-- Process errors with CEP logic
INSERT INTO AlertsSink
SELECT * FROM OrderErrors
WHERE errorType = 'ParseError';
```

**DLQ Pattern 2: Direct Sink (Persistent)**:
```sql
-- DLQ stream directly to Kafka topic
CREATE STREAM OrderErrors (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
) WITH (
    'type' = 'sink',
    'extension' = 'kafka',
    'format' = 'json'
);
```

**DLQ Pattern 3: Multi-Stage (Internal → Sink)**:
```sql
-- DLQ internal stream
CREATE STREAM OrderErrors (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
);
-- Pure internal stream (no type needed, no external I/O)

-- Sink DLQ to persistent storage
CREATE STREAM OrderErrorsSink (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
) WITH (
    'type' = 'sink',
    'extension' = 'kafka',
    'format' = 'json'
);

INSERT INTO OrderErrorsSink
SELECT * FROM OrderErrors;
```

**DLQ Event Schema** (Exact Match Required):
```json
{
  "originalEvent": "<serialized original event>",
  "errorMessage": "Failed to parse JSON: unexpected token",
  "errorType": "ParseError",
  "timestamp": 1697123456789,
  "attemptCount": 3,
  "streamName": "Orders"
}
```

**Schema Enforcement**:
- EventFlux validates DLQ schema at initialization **against CREATE STREAM SQL definition** (not runtime data)
- DLQ stream must have exactly these attributes (order doesn't matter):
  - `originalEvent` (STRING): Serialized original event
  - `errorMessage` (STRING): Error description
  - `errorType` (STRING): Error category
  - `timestamp` (BIGINT): Failure timestamp
  - `attemptCount` (INT): Number of retry attempts
  - `streamName` (STRING): Name of the source stream that produced the error
- **No additional attributes allowed** (prevents schema drift)
- **No missing attributes allowed** (ensures complete error context)

**DLQ Schema Validation Algorithm** (Phase 2 - Initialization):

> **Note**: Phase 1 already validated DLQ stream name exists. This Phase 2 validation checks schema exactness.

```rust
fn validate_dlq_schema(stream_name: &str, dlq_stream_name: &str) -> Result<()> {
    // 1. Look up DLQ stream schema from parsed CREATE STREAM definition
    // (Phase 1 already verified this stream exists)
    let dlq_stream = parsed_streams.get(dlq_stream_name)
        .ok_or(format!("DLQ stream '{}' not found", dlq_stream_name))?;

    // 2. Define required DLQ schema (exact match required)
    let required_fields = HashMap::from([
        ("originalEvent", DataType::String),
        ("errorMessage", DataType::String),
        ("errorType", DataType::String),
        ("timestamp", DataType::BigInt),
        ("attemptCount", DataType::Int),
        ("streamName", DataType::String),
    ]);

    // 3. Extract actual schema from DLQ stream definition
    let actual_fields: HashMap<String, DataType> = dlq_stream.attributes
        .iter()
        .map(|attr| (attr.name.clone(), attr.data_type.clone()))
        .collect();

    // 4. Validate exact field count (no more, no less)
    if actual_fields.len() != required_fields.len() {
        return Err(format!(
            "DLQ stream '{}' has {} fields, but exactly 6 required (originalEvent, errorMessage, errorType, timestamp, attemptCount, streamName)",
            dlq_stream_name, actual_fields.len()
        ));
    }

    // 5. Validate each required field present with correct type
    for (required_name, required_type) in &required_fields {
        match actual_fields.get(*required_name) {
            Some(actual_type) if actual_type == required_type => {
                // ✅ Field present with correct type
            }
            Some(actual_type) => {
                return Err(format!(
                    "DLQ stream '{}' field '{}' has type {:?}, expected {:?}",
                    dlq_stream_name, required_name, actual_type, required_type
                ));
            }
            None => {
                return Err(format!(
                    "DLQ stream '{}' missing required field '{} {}'",
                    dlq_stream_name, required_name, type_to_sql(required_type)
                ));
            }
        }
    }

    // 6. Check for extra fields not in required schema
    for (actual_name, _) in &actual_fields {
        if !required_fields.contains_key(actual_name.as_str()) {
            return Err(format!(
                "DLQ stream '{}' has extra field '{}' not in required schema",
                dlq_stream_name, actual_name
            ));
        }
    }

    // ✅ Schema validation passed
    Ok(())
}
```

**Validation Timing**: Called during Phase 2 (Application Initialization) for every stream configured with `error.dlq.stream` property.

**Example Errors**:
```
❌ "DLQ stream 'OrderErrors' missing required field 'streamName STRING'"
❌ "DLQ stream 'OrderErrors' field 'timestamp' has type INT, expected BIGINT"
❌ "DLQ stream 'OrderErrors' has 7 fields, but exactly 6 required"
❌ "DLQ stream 'OrderErrors' has extra field 'severity' not in required schema"
```

#### DLQ Cascading Failure Behavior

**Initialization-Time** (Phase 2 - Application Startup):

If DLQ stream fails to initialize → **Source stream FAILS TO START**

- **Rationale**: "What's the point of deploying if the error handler is broken?"
- **Behavior**: Application fails to start, Kubernetes/systemd will restart until DLQ is available
- **Example**: DLQ stream 'OrderErrors' configured with invalid Kafka broker → Orders stream initialization fails

**Runtime** (Phase 3 - Event Processing):

If DLQ stream fails during event processing → **Fall back to configured fallback strategy**

- **Default Behavior**: Fall back to `log` (log at ERROR level with full error details and discard failed event)
- **Configurable**: Use `error.dlq.fallback-strategy` to control fallback behavior
- **Log Message**: `ERROR: DLQ stream '{name}' unavailable, applying fallback strategy: {strategy}`
- **Rationale**: Prevents cascading failures from taking down the entire application during transient DLQ issues

**DLQ Fallback Strategies**:

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `log` | Log detailed error and discard event | **Default** - Full error visibility without blocking |
| `fail` | Terminate application immediately | Critical data requiring guaranteed DLQ delivery |
| `retry` | Retry DLQ delivery with backoff | Transient DLQ failures, eventually consistent |

**Configuration Examples**:

```toml
# Default: Log detailed error if DLQ unavailable (prevents cascading failures)
[streams.Orders.error]
strategy = "retry"
dlq.stream = "OrderErrors"
dlq.fallback-strategy = "log"  # Default (log full error details and discard event)

# Critical data: Fail application if DLQ unavailable
[streams.Payments.error]
strategy = "retry"
dlq.stream = "PaymentErrors"
dlq.fallback-strategy = "fail"  # Ensures no data loss if DLQ down

# Retry DLQ delivery (for transient DLQ failures)
[streams.Metrics.error]
strategy = "retry"
dlq.stream = "MetricErrors"
dlq.fallback-strategy = "retry"
dlq.fallback-retry.max-attempts = 5
dlq.fallback-retry.initial-delay = "1s"
```

**Example Scenario** (DLQ Kafka sink temporarily unreachable):
- **`fallback-strategy = log`**: Orders stream continues processing, logs detailed error and discards failed events (DEFAULT)
- **`fallback-strategy = fail`**: Orders stream terminates, Kubernetes/systemd restarts application
- **`fallback-strategy = retry`**: Orders stream retries DLQ delivery with exponential backoff

**Recursive DLQ Restriction**:

DLQ streams **CANNOT** have their own DLQ (no recursive error handling)

- **Validation**: Initialization fails if DLQ stream has `error.dlq.stream` property
- **Error Message**: `"DLQ stream '{name}' cannot have its own DLQ (recursive error handling not allowed)"`
- **Rationale**: Recursive DLQ creates infinite error chains and operational complexity

**Example Configuration Error**:
```toml
[streams.OrderErrors.error]
dlq.stream = "OrderErrorsDLQ"  # ❌ NOT ALLOWED: DLQ cannot have DLQ
```

---

## Data Mapping

### Auto-Mapping (Default Behavior)

**When NO mapping specified, auto-map by field name:**

```sql
-- JSON with matching field names - no mapping needed
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
    'type' = 'source',
    'format' = 'json'
);
-- Automatically maps: JSON "orderId" → stream "orderId"
```

**Auto-Mapping Scope**: **Top-level fields only**

Works when:
- JSON **top-level** field names match stream attribute names
- CSV has header with matching column names
- XML **top-level** element names match stream attributes

**Limitations**:
- **NO nested field extraction**: Auto-mapping only works for top-level fields
- For nested JSON/XML, use explicit `mapping.*` properties
- **NO automatic flattening**: Nested structures require explicit paths

**Examples**:

✅ **Auto-mapping works** (top-level):
```json
{"orderId": "123", "amount": 100.0}
```
```sql
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH ('format' = 'json');
-- ✅ Automatically maps top-level fields
```

❌ **Auto-mapping fails** (nested):
```json
{"order": {"id": "123", "total": 100.0}}
```
```sql
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH ('format' = 'json');
-- ❌ Cannot auto-map nested "order.id" → "orderId"
```

✅ **Solution** (explicit mapping):
```sql
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
    'format' = 'json',
    'json.mapping.orderId' = '$.order.id',
    'json.mapping.amount' = '$.order.total'
);
-- ✅ Explicit mapping handles nested structure
```

**All-or-Nothing Auto-Mapping Policy**:

EventFlux enforces an **all-or-nothing** approach to auto-mapping:

- **If NO `mapping.*` properties specified** → Auto-map ALL top-level fields by name
- **If ANY `mapping.*` properties specified** → Explicitly map ALL fields (no auto-mapping)

**Why All-or-Nothing?**

Prevents ambiguity and configuration errors. Partial auto-mapping (some fields auto-mapped, others explicit) creates confusion about which fields are mapped how, leading to runtime errors when field names don't match expectations.

**Examples**:

✅ **Correct** (all auto-mapped):
```sql
-- All fields auto-mapped from top-level JSON
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
    'type' = 'source',
    'format' = 'json'
    -- No mappings → auto-map both orderId and amount
);
```

✅ **Correct** (all explicitly mapped):
```sql
-- All fields explicitly mapped
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
    'type' = 'source',
    'format' = 'json',
    'json.mapping.orderId' = '$.order.id',
    'json.mapping.amount' = '$.order.total'
    -- All fields have explicit mappings
);
```

❌ **Incorrect** (partial mapping NOT allowed):
```sql
-- ❌ ERROR: Cannot mix auto-mapping with explicit mappings
CREATE STREAM Orders (orderId STRING, amount DOUBLE) WITH (
    'type' = 'source',
    'format' = 'json',
    'json.mapping.amount' = '$.nested.amount'
    -- orderId would be auto-mapped, amount explicit → NOT ALLOWED
);
```

**Implementation Note**: The mapper checks if `mappings.is_empty()`. If true, auto-map all fields. If false, extract using explicit mappings for all fields. No partial behavior.

### Source: Field Extraction

**Use `{format}.mapping.*` for custom field mappings:**

**SQL:**
```sql
CREATE STREAM Orders (
    orderId STRING,
    customerName STRING,
    amount DOUBLE
) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json',
    -- Format options (no prefix)
    'json.ignore-parse-errors' = 'true',
    'json.date-format' = 'yyyy-MM-dd',
    -- Field mappings (mapping. prefix)
    'json.mapping.orderId' = '$.order.id',
    'json.mapping.customerName' = '$.order.customer.name',
    'json.mapping.amount' = '$.order.total'
);
```

**TOML:**
```toml
# Note: 'type', 'extension', 'format' must be in SQL WITH clause
# TOML provides only operational configuration (brokers, topics, credentials)

[streams.Orders.json]
# Format options (no prefix)
ignore-parse-errors = true
date-format = "yyyy-MM-dd"

# Field mappings (dotted properties with mapping. prefix)
mapping.orderId = "$.order.id"
mapping.customerName = "$.order.customer.name"
mapping.amount = "$.order.total"

[streams.Orders.kafka]
brokers = "localhost:9092"
topic = "orders"
```

> **📘 TOML Section Lookup Mechanism**
>
> EventFlux automatically maps SQL `format` and `extension` properties to TOML sections:
>
> **Mapping Rules**:
> - SQL `'format' = 'json'` → TOML `[streams.StreamName.json]`
> - SQL `'extension' = 'kafka'` → TOML `[streams.StreamName.kafka]`
> - SQL `'extension' = 'mysql'` → TOML `[streams.StreamName.mysql]`
>
> **Why This Works**:
> 1. Parser reads SQL: `CREATE STREAM Orders WITH ('format' = 'json', 'extension' = 'kafka')`
> 2. ConfigManager loads TOML sections: `[streams.Orders.json]` and `[streams.Orders.kafka]`
> 3. Stream initialization merges:
>    - Rust defaults
>    - TOML `[application.json]` and `[application.kafka]`
>    - TOML `[streams.Orders.json]` and `[streams.Orders.kafka]`
>    - SQL WITH properties
>
> **Example**:
> ```sql
> CREATE STREAM Metrics (...) WITH (
>     'type' = 'source',
>     'extension' = 'http',  -- Looks for [streams.Metrics.http]
>     'format' = 'csv'        -- Looks for [streams.Metrics.csv]
> );
> ```
>
> ```toml
> [streams.Metrics.http]
> url = "https://api.example.com/metrics"
>
> [streams.Metrics.csv]
> delimiter = ","
> header = true
> ```

**Why the `mapping.` prefix?**

Prevents namespace conflicts:
```toml
[streams.Orders.json]
ignore-parse-errors = true           # ✅ Clearly a format option
template = "$.data.template"         # ✅ Clearly sink template (format option)
mapping.template = "$.data.template" # ✅ Clearly a field mapping for "template" attribute
```

Without prefix, ambiguity:
```toml
template = "$.data.template"  # AMBIGUOUS: Format option or field mapping?
```

### Sink: Output Templates

**Use `{format}.template` for custom output formatting:**

**SQL:**
```sql
CREATE STREAM Alerts (
    alertId STRING,
    severity STRING,
    message STRING
) WITH (
    'type' = 'sink',
    'extension' = 'http',
    'format' = 'json',
    -- Format options
    'json.pretty-print' = 'true',
    -- Sink template
    'json.template' = '{
        "eventType": "ALERT",
        "timestamp": "{{_timestamp}}",
        "payload": {
            "id": "{{alertId}}",
            "severity": "{{severity}}",
            "message": "{{message}}"
        }
    }'
);
```

**TOML:**
```toml
# Note: 'type', 'extension', 'format' defined in SQL WITH clause

[streams.Alerts.json]
# Format options
pretty-print = true

# Sink template (simple property, no prefix)
template = '''
{
    "eventType": "ALERT",
    "timestamp": "{{_timestamp}}",
    "payload": {
        "id": "{{alertId}}",
        "severity": "{{severity}}",
        "message": "{{message}}"
    }
}
'''

[streams.Alerts.http]
url = "https://api.example.com/alerts"
method = "POST"
```

**Template Variables:**
- `{{fieldName}}` - Any stream attribute
- `{{_timestamp}}` - Event processing timestamp
- `{{_eventTime}}` - Original event time (if available)
- `{{_streamName}}` - Source stream name

**Template Implementation**: Simple regex/text replacement

EventFlux uses straightforward string substitution for templates. Each `{{variable}}` is replaced with its value. No complex templating language features (conditionals, loops, filters).

**Why Simple**:
- ✅ Predictable behavior
- ✅ Easy to debug
- ✅ High performance (no template engine overhead)
- ✅ Sufficient for data serialization
- ❌ No conditionals (`{{#if}}`)
- ❌ No loops (`{{#each}}`)
- ❌ No filters/functions (`{{upper(name)}}`)

**For Complex Transformations**: Use CEP query logic before sink:
```sql
-- Transform in query, not in template
INSERT INTO Alerts
SELECT
    alertId,
    CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END as priority,
    UPPER(message) as message
FROM RawAlerts;
```

### Validation Rules

**Parser enforces:**
- If `type = 'source'` → `mapping.*` allowed, `template` NOT allowed
- If `type = 'sink'` → `template` allowed, `mapping.*` NOT allowed
- If no `type` (pure internal) → Neither `mapping.*` nor `template` allowed

**Error Examples:**
```
"Stream 'Orders' has type='source' but specifies 'json.template' (sink-only property)"
"Stream 'Alerts' has type='sink' but specifies 'json.mapping.*' (source-only properties)"
"Stream 'Internal' is pure internal (no type) but specifies 'json.mapping.*' (not allowed)"
```

---

## Data Mapping Examples

### JSON Format

**Source with nested extraction:**
```sql
CREATE STREAM Orders (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    timestamp BIGINT
) WITH (
    'type' = 'source',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.date-format' = 'yyyy-MM-dd''T''HH:mm:ss',
    'json.mapping.orderId' = '$.order.id',
    'json.mapping.customerName' = '$.order.customer.name',
    'json.mapping.amount' = '$.order.total',
    'json.mapping.timestamp' = '$.order.createdAt'
);
```

**Sink with template:**
```sql
CREATE STREAM Alerts (alertId STRING, severity STRING) WITH (
    'type' = 'sink',
    'format' = 'json',
    'json.template' = '{"type":"ALERT","id":"{{alertId}}","level":"{{severity}}"}'
);
```

**Note**: Stream `type`, `extension`, and `format` MUST be defined in SQL WITH clause (as shown above). TOML configuration provides only operational properties (brokers, credentials, mappings). See complete working examples in the "Complete Example" section.

### CSV Format

**Source with column positions:**
```sql
CREATE STREAM CsvOrders (
    timestamp BIGINT,
    symbol STRING,
    price DOUBLE,
    volume BIGINT
) WITH (
    'type' = 'source',
    'format' = 'csv',
    'csv.delimiter' = ',',
    'csv.header' = 'false',
    'csv.mapping.timestamp' = '0',
    'csv.mapping.symbol' = '1',
    'csv.mapping.price' = '2',
    'csv.mapping.volume' = '3',
    'csv.timestamp-format' = 'yyyy-MM-dd''T''HH:mm:ss'
);
```

**Sink with template:**
```sql
CREATE STREAM CsvAlerts (...) WITH (
    'type' = 'sink',
    'format' = 'csv',
    'csv.delimiter' = '|',
    'csv.template' = '{{alertId}}|{{severity}}|{{message}}'
);
```

### XML Format

**Source with XPath:**
```sql
CREATE STREAM XmlOrders (...) WITH (
    'type' = 'source',
    'format' = 'xml',
    'xml.mapping.orderId' = '/order/id/text()',
    'xml.mapping.customerName' = '/order/customer/name/text()',
    'xml.mapping.amount' = '/order/total/text()'
);
```

**Sink with template:**
```sql
CREATE STREAM XmlAlerts (...) WITH (
    'type' = 'sink',
    'format' = 'xml',
    'xml.template' = '<alert><id>{{alertId}}</id><severity>{{severity}}</severity></alert>'
);
```

### Avro Format

**Source with inline schema:**
```sql
CREATE STREAM AvroOrders (...) WITH (
    'type' = 'source',
    'format' = 'avro',
    'avro.schema' = '{
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "double"}
        ]
    }'
);
```

**Source with schema registry:**
```sql
CREATE STREAM AvroOrders (...) WITH (
    'type' = 'source',
    'format' = 'avro',
    'avro.schema.registry' = 'http://localhost:8081',
    'avro.schema.subject' = 'orders-value',
    'avro.schema.version' = 'latest'
);
```

---

## TOML Configuration

### File Structure

**Simplified 2-section structure:**

```toml
[application]
name = "OrderProcessing"
buffer_size = 8192

# Note: type, extension, format MUST be in SQL WITH clause (not in TOML)
# TOML provides only operational configuration

[streams.StreamName.kafka]
# Extension properties (brokers, topic, credentials)

[streams.StreamName.json]
# Format options and mappings
```

### Environment-Specific Configuration

**Use separate TOML files per environment:**

```
project/
├── app.sql
├── config-dev.toml
├── config-staging.toml
└── config-prod.toml
```

**config-dev.toml:**
```toml
[application]
name = "OrderProcessing-Dev"

# Note: type, extension, format defined in app.sql

[streams.Orders.json]
ignore-parse-errors = true
mapping.orderId = "$.order.id"
mapping.amount = "$.order.total"

[streams.Orders.kafka]
brokers = "localhost:9092"
topic = "orders"
```

**config-prod.toml:**
```toml
[application]
name = "OrderProcessing-Prod"

# Note: type, extension, format defined in app.sql

[streams.Orders.json]
ignore-parse-errors = true
mapping.orderId = "$.order.id"
mapping.amount = "$.order.total"

[streams.Orders.kafka]
brokers = "prod1:9092,prod2:9092,prod3:9092"
topic = "orders"
security.protocol = "SASL_SSL"
security.username = "${KAFKA_USER}"
security.password = "${KAFKA_PASSWORD}"
```

**CLI Usage:**
```bash
# Development
eventflux run app.sql --config config-dev.toml

# Production
export KAFKA_USER="admin"
export KAFKA_PASSWORD="secret"
eventflux run app.sql --config config-prod.toml
```

### TOML Property Syntax

**Two equivalent styles:**

**Dot notation** (recommended):
```toml
[streams.Orders]
kafka.brokers = "localhost:9092"
kafka.topic = "orders"
```

**Nested sections**:
```toml
[streams.Orders.kafka]
brokers = "localhost:9092"
topic = "orders"
```

Both produce: `kafka.brokers = "localhost:9092"`

### Environment Variable Substitution

**Syntax**: `${VAR_NAME}` or `${VAR_NAME:default}`

```toml
[streams.Orders.kafka]
brokers = "${KAFKA_BROKERS:localhost:9092}"
security.username = "${KAFKA_USER}"
security.password = "${KAFKA_PASSWORD}"
```

**Substitution Timing**: **Eager loading at configuration resolution time**
- Environment variables substituted when TOML is loaded (Phase 1: Parse-Time)
- Variables resolved BEFORE Phase 2 (Application Initialization)
- **Cannot change during application lifetime** - changes to environment variables after startup are NOT reflected

**Failure Behavior**:
- **Missing variable WITHOUT default**: Application **fails to start**
  ```toml
  brokers = "${KAFKA_BROKERS}"  # ❌ Fails if KAFKA_BROKERS not set
  ```
- **Missing variable WITH default**: Uses default value
  ```toml
  brokers = "${KAFKA_BROKERS:localhost:9092}"  # ✅ Uses localhost:9092 if not set
  ```

**Best Practices**:
- **Production**: Always use environment variables for credentials (NO defaults)
- **Development**: Use defaults for local testing
- **CI/CD**: Validate all required environment variables before deployment

**Security requirement**: Credentials MUST use environment variables in TOML (never hardcoded).

---

## Implementation Guide

**Note**: The milestones below describe the **implementation roadmap** for developers building the configuration system. These are distinct from the **validation phases** (Parse-Time → Initialization → Runtime) described earlier in this document.

### Milestone 1: Core Data Structures

**Goal**: Define basic configuration types

```rust
pub struct FlatConfig {
    properties: HashMap<String, String>,
    sources: HashMap<String, PropertySource>,
}

pub enum PropertySource {
    RustDefault,
    TomlApplication,
    TomlStream,
    SqlWith,
}

pub struct StreamTypeConfig {
    pub stream_type: StreamType,
    pub extension: String,
    pub format: Option<String>,
    pub properties: HashMap<String, String>,
}

pub enum StreamType {
    Source,
    Sink,
    Internal,
}
```

### Milestone 2: SQL Parser Integration

**Goal**: Parse WITH clause into FlatConfig

```rust
fn extract_with_options(with: &WithClause) -> Result<FlatConfig, CompileError> {
    let mut config = FlatConfig::new();

    for (key, value) in &with.options {
        config.set(key, value, PropertySource::SqlWith);
    }

    Ok(config)
}

fn validate_with_clause(config: &FlatConfig) -> Result<(), CompileError> {
    if config.is_stream() {
        // Type is optional (omit for pure internal streams)
        if let Some(stream_type) = config.get("type") {
            match stream_type.as_str() {
                "source" | "sink" => {
                    // External streams require extension and format
                    require_property(config, "extension")?;
                    require_property(config, "format")?;
                }
                _ => return Err("Invalid stream type, must be 'source' or 'sink'"),
            }
        } else {
            // No type = pure internal stream
            // Internal streams must NOT have extension or format
            if config.get("extension").is_some() {
                return Err("Pure internal streams (no type) cannot specify 'extension'");
            }
            if config.get("format").is_some() {
                return Err("Pure internal streams (no type) cannot specify 'format'");
            }
        }
    }
    Ok(())
}
```

### Milestone 3: TOML Loading

**Goal**: Load and merge TOML configuration

```rust
#[derive(Deserialize)]
pub struct TomlConfig {
    pub application: Option<ApplicationSection>,
    pub streams: HashMap<String, TomlStreamConfig>,
    pub tables: HashMap<String, TomlTableConfig>,  // Separate section for tables
}

#[derive(Deserialize)]
pub struct TomlStreamConfig {
    /// NOTE: stream_type, extension, and format fields are NOT allowed in TOML
    /// These MUST be defined in SQL WITH clause (SQL-first principle)
    /// If present in TOML, they will be rejected during validation
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

impl TomlStreamConfig {
    pub fn to_flat_config(&self) -> FlatConfig {
        let mut config = FlatConfig::new();

        // Flatten nested properties
        for (key, value) in &self.properties {
            flatten_toml_value(key, value, &mut config);
        }

        config
    }

    /// Validate that type/extension/format are NOT present in TOML
    pub fn validate(&self) -> Result<(), String> {
        // Check if forbidden fields are present in properties
        let forbidden = ["type", "extension", "format"];
        for key in &forbidden {
            if self.properties.contains_key(*key) {
                return Err(format!(
                    "Stream defines '{}' in TOML, but '{}' MUST be in SQL WITH clause. \
                    Type, extension, and format are structural properties required by the parser \
                    at Phase 1 (parse-time), so they cannot be in TOML configuration.",
                    key, key
                ));
            }
        }
        Ok(())
    }
}

/// Helper function: Flatten nested TOML structures into dot-separated keys
///
/// **Behavior**:
/// - Converts TOML section hierarchy into flat dot-notation properties
/// - Creates keys like "kafka.brokers" from `[kafka] brokers = "..."`
/// - Validates top-level keys only (nested keys not checked against forbidden list)
///
/// **Examples**:
/// ```toml
/// [streams.Orders.kafka]
/// brokers = "localhost:9092"
/// topic = "orders"
/// ```
/// Produces:
/// - `kafka.brokers` = "localhost:9092"
/// - `kafka.topic` = "orders"
///
/// ```toml
/// [streams.Orders]
/// kafka.brokers = "localhost:9092"
/// kafka.topic = "orders"
/// ```
/// Produces (identical):
/// - `kafka.brokers` = "localhost:9092"
/// - `kafka.topic` = "orders"
///
/// **Validation Note**: The `validate()` method above checks for forbidden top-level keys
/// ("type", "extension", "format") in `self.properties`. Nested keys like "kafka.type"
/// are allowed because the check uses `properties.contains_key("type")`, which only
/// matches top-level "type", not "kafka.type".
///
/// **Example Allowed vs Rejected**:
/// ```toml
/// [streams.Orders]
/// type = "source"           # ❌ REJECTED (top-level "type")
/// kafka.type = "consumer"   # ✅ ALLOWED (nested "kafka.type", not top-level "type")
/// ```
```

### Milestone 4: Factory System & Mapper Selection

**Goal**: Extend existing Factory traits for validation and understand mapper selection flow

#### Factory Trait Enhancements

**Required Method Additions** to existing Factory traits in `src/core/extension/mod.rs`:

```rust
pub trait SourceFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn clone_box(&self) -> Box<dyn SourceFactory>;

    // 🔧 REQUIRED METHODS (no defaults - explicit declaration required):

    /// List supported formats for this source extension
    /// Example: &["json", "avro", "bytes"]
    fn supported_formats(&self) -> &[&str];

    /// List required configuration properties
    /// Example: &["kafka.brokers", "kafka.topic"]
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["kafka.consumer.group", "kafka.security.protocol"]
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use Source instance
    /// Validates configuration and returns error if invalid
    /// Source is guaranteed to be in valid state upon successful return
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError>;
}

pub trait SinkFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn clone_box(&self) -> Box<dyn SinkFactory>;

    // 🔧 REQUIRED METHODS (no defaults - explicit declaration required):

    /// List supported formats for this sink extension
    /// Example: &["json", "csv", "bytes"]
    fn supported_formats(&self) -> &[&str];

    /// List required configuration properties
    /// Example: &["http.url", "http.method"]
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["http.headers", "http.timeout"]
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use Sink instance
    /// Validates configuration and returns error if invalid
    /// Sink is guaranteed to be in valid state upon successful return
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Sink>, EventFluxError>;
}

pub trait SourceMapperFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn clone_box(&self) -> Box<dyn SourceMapperFactory>;

    // 🔧 REQUIRED METHODS (no defaults - explicit declaration required):

    /// List required configuration properties
    /// Example: &[] for JSON (no required config), &["avro.schema"] for Avro
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["json.ignore-parse-errors", "json.date-format"]
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use SourceMapper instance
    /// Validates configuration and returns error if invalid
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn SourceMapper>, EventFluxError>;
}

pub trait SinkMapperFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn clone_box(&self) -> Box<dyn SinkMapperFactory>;

    // 🔧 REQUIRED METHODS (no defaults - explicit declaration required):

    /// List required configuration properties
    /// Example: &[] for JSON, &["csv.delimiter"] for CSV
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["json.pretty-print", "json.template"]
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use SinkMapper instance
    /// Validates configuration and returns error if invalid
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn SinkMapper>, EventFluxError>;
}
```

**Design Rationale: Single-Phase Construction**

The `create_initialized()` pattern (Option 3) was chosen over two-phase initialization (separate `create()` and `initialize()`) based on software engineering principles:

**Why Single-Phase Construction?**

1. **Type Safety** - Source/Sink/Mapper instances cannot exist in uninitialized state
   - No `Option<>` wrappers needed for internal fields
   - No runtime panics from `unwrap()` on uninitialized state
   - Rust's type system enforces valid state at compile time

2. **Fail-Fast Principle** - Configuration errors discovered immediately during creation
   - Better user experience - errors at startup, not during first use
   - Simpler error handling - single point of failure

3. **Misuse Resistance** - Impossible to forget initialization step
   - Two-phase pattern: Can call `create()` and forget `initialize()`
   - Single-phase pattern: Instance creation guarantees initialization

4. **Rust Ecosystem Alignment** - Matches patterns in tower, tokio, actix
   - Builder pattern with `.build()?` that validates and constructs
   - Industry-standard approach in modern Rust

5. **Factory Pattern Alignment** - Classic GoF Factory pattern
   - "Factory returns objects ready to use"
   - Violates pattern if factory returns incomplete objects

6. **Cohesion** - Source/Sink focus purely on operational behavior
   - Configuration parsing/validation is construction concern (Factory's job)
   - Event processing is operational concern (Source/Sink's job)
   - Clean separation of concerns

**Trade-offs Accepted:**

- **Factory Responsibility** - Factory handles both validation and creation (but they're naturally paired)
- **Coupling** - Factory knows config structure (but Factory-Source already tightly coupled by definition)

**Rejected Alternative (Two-Phase):**

```rust
// REJECTED: Two-phase allows invalid states
pub struct KafkaSource {
    consumer: Option<Consumer>,  // Can be None - unsafe!
}

impl Source for KafkaSource {
    fn initialize(&mut self, config: &HashMap<String, String>) -> Result<()> { ... }
    fn start(&mut self) {
        self.consumer.as_ref().unwrap()  // Runtime panic risk!
    }
}
```

**Chosen Approach (Single-Phase):**

```rust
// CHOSEN: Always valid state
pub struct KafkaSource {
    consumer: Consumer,  // Always present - safe!
}

impl SourceFactory for KafkaSourceFactory {
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError> {
        // Validate and create in one atomic operation
    }
}
```

#### Extension and Mapper Registry

**Goal**: Understand factory registration and lookup mechanism

**Registry Structure** (built at application startup):

```rust
/// Central registry for all factories
pub struct ExtensionRegistry {
    source_factories: HashMap<String, Box<dyn SourceFactory>>,
    sink_factories: HashMap<String, Box<dyn SinkFactory>>,
    source_mapper_factories: HashMap<String, Box<dyn SourceMapperFactory>>,
    sink_mapper_factories: HashMap<String, Box<dyn SinkMapperFactory>>,
    store_factories: HashMap<String, Box<dyn StoreFactory>>,
    table_factories: HashMap<String, Box<dyn TableFactory>>,
}

impl ExtensionRegistry {
    pub fn new() -> Self {
        Self {
            source_factories: HashMap::new(),
            sink_factories: HashMap::new(),
            source_mapper_factories: HashMap::new(),
            sink_mapper_factories: HashMap::new(),
            store_factories: HashMap::new(),
            table_factories: HashMap::new(),
        }
    }

    /// Register a source factory (dynamic registration via name())
    pub fn register_source_factory(&mut self, factory: Box<dyn SourceFactory>) {
        let name = factory.name().to_string();  // Get name from factory itself
        self.source_factories.insert(name, factory);
    }

    /// Register a source mapper factory
    pub fn register_source_mapper_factory(&mut self, factory: Box<dyn SourceMapperFactory>) {
        let name = factory.name().to_string();  // e.g., "json", "avro", "csv"
        self.source_mapper_factories.insert(name, factory);
    }

    /// Lookup source factory by extension name
    pub fn get_source_factory(&self, name: &str) -> Option<&Box<dyn SourceFactory>> {
        self.source_factories.get(name)
    }

    /// Lookup source mapper factory by format name
    pub fn get_source_mapper_factory(&self, format: &str) -> Option<&Box<dyn SourceMapperFactory>> {
        self.source_mapper_factories.get(format)
    }

    // Similar methods for sink, store, table factories...
}
```

**Registration Pattern** (application startup):

```rust
// 1. Create registry
let mut registry = ExtensionRegistry::new();

// 2. Register built-in factories
registry.register_source_factory(Box::new(KafkaSourceFactory));
registry.register_source_factory(Box::new(HttpSourceFactory));
registry.register_source_factory(Box::new(FileSourceFactory));

registry.register_sink_factory(Box::new(KafkaSinkFactory));
registry.register_sink_factory(Box::new(HttpSinkFactory));

registry.register_source_mapper_factory(Box::new(JsonSourceMapperFactory));
registry.register_source_mapper_factory(Box::new(AvroSourceMapperFactory));
registry.register_source_mapper_factory(Box::new(CsvSourceMapperFactory));

registry.register_sink_mapper_factory(Box::new(JsonSinkMapperFactory));
registry.register_sink_mapper_factory(Box::new(CsvSinkMapperFactory));

// 3. Load dynamic extensions (if any)
// registry.load_dynamic_extension("path/to/extension.so")?;

// 4. Registry is now ready for stream initialization
```

**Lookup During Stream Initialization**:

```rust
fn initialize_source_stream(
    stream_def: &StreamDefinition,
    config: &HashMap<String, String>,
    registry: &ExtensionRegistry,
) -> Result<SourceStreamHandler, EventFluxError> {

    // 1. Lookup factories by name
    let extension_name = config.get("extension")
        .ok_or_else(|| EventFluxError::Configuration {
            message: "Missing 'extension' property".to_string(),
            config_key: Some("extension".to_string()),
        })?;
    let format_name = config.get("format")
        .ok_or_else(|| EventFluxError::Configuration {
            message: "Missing 'format' property".to_string(),
            config_key: Some("format".to_string()),
        })?;

    let source_factory = registry.get_source_factory(extension_name)
        .ok_or_else(|| EventFluxError::ExtensionNotFound {
            extension_type: "source".to_string(),
            name: extension_name.clone(),
        })?;

    let mapper_factory = registry.get_source_mapper_factory(format_name)
        .ok_or_else(|| EventFluxError::ExtensionNotFound {
            extension_type: "mapper".to_string(),
            name: format_name.clone(),
        })?;

    // 2. Validate format is supported by extension
    if !source_factory.supported_formats().contains(&format_name.as_str()) {
        return Err(EventFluxError::Configuration {
            message: format!("Extension '{}' does not support format '{}'", extension_name, format_name),
            config_key: Some("format".to_string()),
        });
    }

    // 3. Create fully initialized instances (validation happens inside factories)
    let source = source_factory.create_initialized(config)?;
    let mapper = mapper_factory.create_initialized(config)?;

    // 4. Wire together - instances are guaranteed to be in valid state
    Ok(SourceStreamHandler {
        source,
        mapper,
        junction: stream_def.junction.clone(),
    })
}
```

**Key Insights**:
- ✅ **Dynamic Discovery**: Registry uses `factory.name()` for automatic registration
- ✅ **Lazy Lookup**: Factories looked up during stream init (not at registration)
- ✅ **Format Validation**: Extensions declare supported formats, validated before creation
- ✅ **Extensibility**: Dynamic extensions can be loaded via shared libraries

#### Type Safety and Configuration Validation

**Simplified Approach: Extensions Own Their Types**

Each extension defines its own typed configuration struct INTERNAL to the factory. These types are not exposed in trait signatures - they're implementation details used inside `create_initialized()`.

**Extension-Specific Typed Config**:

Each extension's factory validates raw `HashMap<String, String>` and creates instances in valid state.

**Example: Kafka Extension**

```rust
/// Kafka-specific validated configuration (INTERNAL to KafkaSourceFactory)
struct KafkaSourceConfig {
    bootstrap_servers: Vec<String>,  // Parsed from comma-separated
    topic: String,
    consumer_group: String,
    timeout_ms: u64,
    security_protocol: Option<SecurityProtocol>,
    max_poll_records: Option<usize>,
}

impl KafkaSourceConfig {
    /// Parse and validate raw config into typed config (PRIVATE helper)
    fn parse(raw_config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        // 1. Validate required parameters present
        let brokers_str = raw_config.get("kafka.bootstrap.servers")
            .ok_or_else(|| EventFluxError::Configuration {
                message: "Missing required parameter 'kafka.bootstrap.servers'".to_string(),
                config_key: Some("kafka.bootstrap.servers".to_string()),
            })?;
        let topic = raw_config.get("kafka.topic")
            .ok_or_else(|| EventFluxError::Configuration {
                message: "Missing required parameter 'kafka.topic'".to_string(),
                config_key: Some("kafka.topic".to_string()),
            })?;

        // 2. Parse comma-separated brokers list
        let bootstrap_servers: Vec<String> = brokers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        if bootstrap_servers.is_empty() {
            return Err("kafka.bootstrap.servers cannot be empty".into());
        }

        // 3. Parse optional integer
        let timeout_ms = raw_config.get("kafka.timeout")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|_| "kafka.timeout must be a valid integer")?
            .unwrap_or(30000);  // Default 30s

        // 4. Parse consumer group (with default)
        let consumer_group = raw_config.get("kafka.consumer.group")
            .cloned()
            .unwrap_or_else(|| format!("eventflux-{}", topic));

        // 5. Parse security protocol enum
        let security_protocol = raw_config.get("kafka.security.protocol")
            .map(|s| match s.as_str() {
                "PLAINTEXT" => Ok(SecurityProtocol::PlainText),
                "SSL" => Ok(SecurityProtocol::Ssl),
                "SASL_SSL" => Ok(SecurityProtocol::SaslSsl),
                "SASL_PLAINTEXT" => Ok(SecurityProtocol::SaslPlainText),
                _ => Err(format!("Invalid security.protocol: {}", s)),
            })
            .transpose()?;

        // 6. Return typed config
        Ok(KafkaSourceConfig {
            bootstrap_servers,
            topic: topic.clone(),
            consumer_group,
            timeout_ms,
            security_protocol,
            max_poll_records: None,
        })
    }
}

impl SourceFactory for KafkaSourceFactory {
    fn create_initialized(&self, raw_config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError> {

        // 1. Parse and validate configuration
        let config = KafkaSourceConfig::parse(raw_config)?;

        // 2. Create Kafka consumer with validated config
        let consumer = rdkafka::consumer::StreamConsumer::from_config({
            let mut client_config = rdkafka::ClientConfig::new();
            client_config
                .set("bootstrap.servers", config.bootstrap_servers.join(","))
                .set("group.id", &config.consumer_group)
                .set("session.timeout.ms", config.timeout_ms.to_string());

            if let Some(protocol) = &config.security_protocol {
                client_config.set("security.protocol", protocol.to_string());
            }

            client_config
        })
        .map_err(|e| EventFluxError::ConnectionUnavailable {
            message: format!("Failed to create Kafka consumer: {}", e),
            source: Some(Box::new(e)),
        })?;

        // 3. Test connectivity (fail-fast)
        consumer.fetch_metadata(Some(&config.topic), Duration::from_secs(5))
            .map_err(|e| EventFluxError::ConnectionUnavailable {
                message: format!("Kafka connection test failed: {}", e),
                source: Some(Box::new(e)),
            })?;

        // 4. Return fully initialized Source
        Ok(Box::new(KafkaSource {
            consumer,
            topic: config.topic,
        }))
    }
}
```

**Example: HTTP Extension**

```rust
/// HTTP-specific validated configuration (INTERNAL to HttpSourceFactory)
struct HttpSourceConfig {
    url: String,
    method: HttpMethod,
    headers: HashMap<String, String>,
    poll_interval: Duration,
    timeout: Duration,
}

impl HttpSourceConfig {
    /// Parse and validate raw config into typed config (PRIVATE helper)
    fn parse(raw_config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        // Parse URL
        let url = raw_config.get("http.url")
            .ok_or_else(|| EventFluxError::Configuration {
                message: "Missing required parameter 'http.url'".to_string(),
                config_key: Some("http.url".to_string()),
            })?
            .clone();

        // Validate URL is valid
        url::Url::parse(&url)
            .map_err(|e| EventFluxError::InvalidParameter {
                message: format!("Invalid http.url: {}", e),
                parameter: Some("http.url".to_string()),
                expected: Some("valid URL".to_string()),
            })?;

        // Parse method (default GET)
        let method = raw_config.get("http.method")
            .map(|s| match s.to_uppercase().as_str() {
                "GET" => Ok(HttpMethod::Get),
                "POST" => Ok(HttpMethod::Post),
                "PUT" => Ok(HttpMethod::Put),
                _ => Err(format!("Invalid http.method: {}", s)),
            })
            .transpose()?
            .unwrap_or(HttpMethod::Get);

        // Parse comma-separated headers
        let headers = raw_config.get("http.headers")
            .map(|s| parse_header_list(s))
            .transpose()?
            .unwrap_or_default();

        // Parse durations
        let poll_interval = raw_config.get("http.poll.interval")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_secs(60));

        let timeout = raw_config.get("http.timeout")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_secs(30));

        Ok(HttpSourceConfig {
            url,
            method,
            headers,
            poll_interval,
            timeout,
        })
    }
}

impl SourceFactory for HttpSourceFactory {
    fn create_initialized(&self, raw_config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError> {

        // 1. Parse and validate configuration
        let config = HttpSourceConfig::parse(raw_config)?;

        // 2. Create HTTP client with validated config
        let client = reqwest::blocking::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| EventFluxError::Configuration {
                message: format!("Failed to create HTTP client: {}", e),
                config_key: None,
            })?;

        // 3. Test connectivity (fail-fast)
        let response = client
            .get(&config.url)
            .headers(build_header_map(&config.headers))
            .send()
            .map_err(|e| EventFluxError::ConnectionUnavailable {
                message: format!("HTTP connection test failed: {}", e),
                source: Some(Box::new(e)),
            })?;

        if !response.status().is_success() {
            return Err(EventFluxError::ConnectionUnavailable {
                message: format!("HTTP initial request failed with status {}", response.status()),
                source: None,
            });
        }

        // 4. Return fully initialized Source
        Ok(Box::new(HttpSource {
            client,
            url: config.url,
            method: config.method,
            headers: config.headers,
            poll_interval: config.poll_interval,
        }))
    }
}
```

**Key Benefits**:
- ✅ **Type Safety**: Each extension has strongly-typed config struct (internal)
- ✅ **Extension Autonomy**: Extensions parse comma-separated strings into vectors themselves
- ✅ **Clear Validation**: Each extension validates its own parameters and provides clear errors
- ✅ **No Generic Magic**: No central system needs to know "is this a list?" or "is this a number?"
- ✅ **Single-Phase Construction**: Factory returns ready-to-use instances, no two-phase initialization
- ✅ **Always Valid**: Source instances cannot exist in uninitialized state

**Validation Timing**: Phase 2 (Application Initialization) - during instance creation, fail-fast.

**Error Examples**:
```
ERROR: Invalid configuration for stream 'Orders'
  Missing required parameter 'kafka.bootstrap.servers'

ERROR: Invalid configuration for stream 'Orders'
  Property 'kafka.timeout' has value "not-a-number", expected integer

ERROR: Invalid configuration for stream 'Alerts'
  Invalid http.url: relative URL without a base
```

#### Mapper Configuration

**Mapper Trait Definitions** (no configuration method needed - mappers are created fully initialized):

```rust
pub trait SourceMapper: Debug + Send + Sync {
    /// Map raw bytes to EventFlux events
    /// Mapper is fully configured and ready to use
    /// Returns Result to handle malformed input gracefully
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError>;

    fn clone_box(&self) -> Box<dyn SourceMapper>;
}

pub trait SinkMapper: Debug + Send + Sync {
    /// Map EventFlux events to raw bytes
    /// Mapper is fully configured and ready to use
    /// Returns Result to handle serialization errors gracefully
    fn map(&self, events: &[Event]) -> Result<Vec<u8>, EventFluxError>;

    fn clone_box(&self) -> Box<dyn SinkMapper>;
}
```

**Mapper Factory Implementation Example**:

```rust
/// JSON mapper configuration (INTERNAL to JsonSourceMapperFactory)
struct JsonSourceMapperConfig {
    mappings: HashMap<String, String>,      // field → JSONPath
    ignore_parse_errors: bool,
    date_format: Option<String>,
}

impl JsonSourceMapperConfig {
    fn parse(config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        // Extract mapping configuration
        let mut mappings = HashMap::new();
        for (key, value) in config {
            if let Some(field_name) = key.strip_prefix("json.mapping.") {
                mappings.insert(field_name.to_string(), value.clone());
            }
        }

        // Extract format options
        let ignore_parse_errors = config.get("json.ignore-parse-errors")
            .map(|v| v == "true")
            .unwrap_or(false);

        let date_format = config.get("json.date-format").cloned();

        Ok(JsonSourceMapperConfig {
            mappings,
            ignore_parse_errors,
            date_format,
        })
    }
}

impl SourceMapperFactory for JsonSourceMapperFactory {
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn SourceMapper>, EventFluxError> {

        // Parse and validate configuration
        let mapper_config = JsonSourceMapperConfig::parse(config)?;

        // Create fully initialized mapper
        Ok(Box::new(JsonSourceMapper {
            mappings: mapper_config.mappings,
            ignore_parse_errors: mapper_config.ignore_parse_errors,
            date_format: mapper_config.date_format,
        }))
    }
}

impl SourceMapper for JsonSourceMapper {
    fn map(&self, input: &[u8]) -> Vec<Event> {
        // Use self.mappings (already configured during creation)
        // ...
    }
}
```

#### Stream Initialization Order

**Initialization Order Algorithm** (Phase 2 - Application Startup):

EventFlux initializes streams in **topological order** to ensure dependencies are ready before dependents:

```rust
fn initialize_streams(
    parsed_streams: Vec<StreamDefinition>,
    queries: Vec<InsertQuery>
) -> Result<(), EventFluxError> {

    // 1. Build dependency graph
    let mut dependencies = HashMap::new();

    for query in &queries {
        let target = &query.target_stream;
        let sources = query.get_source_streams();
        dependencies.entry(target.clone())
            .or_insert_with(HashSet::new)
            .extend(sources);
    }

    // 2. Add DLQ dependencies
    for stream_def in &parsed_streams {
        if let Some(dlq_stream) = stream_def.config.get("error.dlq.stream") {
            // Stream depends on its DLQ stream
            dependencies.entry(stream_def.name.clone())
                .or_insert_with(HashSet::new)
                .insert(dlq_stream.clone());

            // Phase 1: Validate DLQ stream name exists (fail-fast)
            // Note: Schema validation happens later in Phase 2 during initialization
            if !parsed_streams.iter().any(|s| s.name == *dlq_stream) {
                return Err(EventFluxError::Configuration {
                    message: format!("DLQ stream '{}' referenced by stream '{}' does not exist",
                                   dlq_stream, stream_def.name),
                    config_key: Some("error.dlq.stream".to_string()),
                });
            }
        }
    }

    // 3. Topological sort
    let init_order = topological_sort(&dependencies)?;

    // 4. Initialize in dependency order
    for stream_name in init_order {
        let stream_def = parsed_streams.iter()
            .find(|s| s.name == stream_name)
            .unwrap();

        initialize_single_stream(stream_def)?;
    }

    Ok(())
}

fn topological_sort(dependencies: &HashMap<String, HashSet<String>>)
    -> Result<Vec<String>, EventFluxError> {

    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut visiting = HashSet::new();

    for node in dependencies.keys() {
        if !visited.contains(node) {
            dfs_topo_sort(
                node,
                dependencies,
                &mut visited,
                &mut visiting,
                &mut result
            )?;
        }
    }

    result.reverse();  // DFS produces reverse order
    Ok(result)
}
```

**Initialization Rules**:
1. **Parse all streams first** (no initialization) - Phase 1
2. **Build dependency graph** including DLQ references
3. **DLQ Validation (Two-Phase Approach)**:
   - **3a. Validate DLQ stream names exist** - fail at Phase 1 if DLQ stream not found
   - **3b. Validate DLQ stream schemas** - fail at Phase 2 if schema mismatch
4. **Initialize in topological order** - dependencies first
5. **Fail fast if any initialization fails** - Phase 2 fail-fast behavior

**Example**:
```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'error.dlq.stream' = 'OrderErrors'
);

CREATE STREAM OrderErrors (...);  -- Must exist, will be initialized first

CREATE STREAM Processed (...);
INSERT INTO Processed SELECT * FROM Orders;

-- Initialization order:
-- 1. OrderErrors (no dependencies)
-- 2. Orders (depends on OrderErrors DLQ)
-- 3. Processed (depends on Orders)
```

#### Source-Mapper-Sink Wiring

**Wiring Pattern** (based on Siddhi architecture):

**Source Stream Initialization**:
```rust
pub struct SourceStreamHandler {
    source: Box<dyn Source>,
    mapper: Box<dyn SourceMapper>,
    junction: Arc<Junction>,
}

impl SourceStreamHandler {
    pub fn new(
        source_factory: &dyn SourceFactory,
        mapper_factory: &dyn SourceMapperFactory,
        config: &HashMap<String, String>,
        junction: Arc<Junction>,
    ) -> Result<Self, EventFluxError> {

        // Create fully initialized instances (fail-fast)
        let source = source_factory.create_initialized(config)?;
        let mapper = mapper_factory.create_initialized(config)?;

        Ok(Self {
            source,
            mapper,
            junction,
        })
    }

    pub fn run(&mut self) {
        loop {
            // Read raw bytes from source
            let raw_data = match self.source.read() {
                Ok(data) => data,
                Err(e) => {
                    // Handle error with configured strategy
                    continue;
                }
            };

            // Map bytes to events
            let events = self.mapper.map(&raw_data);

            // Send to junction for routing
            for event in events {
                self.junction.send_event(event);
            }
        }
    }
}
```

**Sink Stream Initialization**:
```rust
pub struct SinkStreamHandler {
    sink: Box<dyn Sink>,
    mapper: Box<dyn SinkMapper>,
    incoming_events: Receiver<Event>,
}

impl SinkStreamHandler {
    pub fn new(
        sink_factory: &dyn SinkFactory,
        mapper_factory: &dyn SinkMapperFactory,
        config: &HashMap<String, String>,
        incoming_events: Receiver<Event>,
    ) -> Result<Self, EventFluxError> {

        // Create fully initialized instances (fail-fast)
        let sink = sink_factory.create_initialized(config)?;
        let mapper = mapper_factory.create_initialized(config)?;

        Ok(Self {
            sink,
            mapper,
            incoming_events,
        })
    }

    pub fn run(&mut self) {
        loop {
            // Receive events from junction
            let event = match self.incoming_events.recv() {
                Ok(evt) => evt,
                Err(_) => break,  // Channel closed
            };

            // Map events to bytes
            let raw_data = self.mapper.map(&[event]);

            // Write to external sink
            match self.sink.write(&raw_data) {
                Ok(_) => {},
                Err(e) => {
                    // Handle error with configured strategy
                }
            }
        }
    }
}
```

**Key Points**:
- **SourceStreamHandler** owns both Source and SourceMapper
- **SinkStreamHandler** owns both Sink and SinkMapper
- Wiring happens in stream initialization (`initialize_single_stream()`)
- Junction connects all streams via pub/sub channels

#### Mapper Selection Flow

**How EventFlux resolves type → extension → format → Factory instances**:

```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source',        -- Step 1: Determines Factory type
    'extension' = 'kafka',    -- Step 2: Selects specific Factory
    'format' = 'json'         -- Step 3: Selects Mapper Factory
);
```

**Resolution Steps**:

1. **Type → Factory Type**:
   - `type='source'` → Use `SourceFactory` + `SourceMapperFactory`
   - `type='sink'` → Use `SinkFactory` + `SinkMapperFactory`
   - No `type` (pure internal) → No Factory needed (passthrough)

2. **Extension → Specific Factory**:
   - `extension='kafka'` → Look up `KafkaSourceFactory` from registry
   - Factory provides: `supported_formats()`, `required_parameters()`, `optional_parameters()`

3. **Format → Mapper Factory**:
   - `format='json'` → Look up `JsonSourceMapperFactory` from registry
   - Validate format is in `KafkaSourceFactory::supported_formats()`

4. **Validation**:
   - **Factory level**: Check format support, validate required parameters present
   - **Instance level** (Source/Sink/Mapper): Connection validation, credential auth, runtime config

5. **Instance Creation**:
   - `KafkaSourceFactory::create()` → Returns `Box<dyn Source>` (e.g., `KafkaSource`)
   - `JsonSourceMapperFactory::create()` → Returns `Box<dyn SourceMapper>` (e.g., `JsonSourceMapper`)

**Data Flow Pattern**:

**Source** (Receive → Map → Send):
```
External Transport (Kafka)
    ↓ (KafkaSource receives bytes)
Source Mapper (JsonSourceMapper)
    ↓ (Deserialize JSON → Event)
Internal Stream
```

**Sink** (Map → Send):
```
Internal Stream
    ↓ (Event)
Sink Mapper (JsonSinkMapper)
    ↓ (Serialize Event → JSON bytes)
External Transport (HTTP)
```

#### Factory Registry Pattern

**Goal**: Understand how factories are registered and retrieved at runtime

**Existing Implementation** (in `src/core/config/eventflux_context.rs`):

EventFlux uses a thread-safe registry pattern with separate `HashMap` collections for each factory type:

```rust
pub struct EventFluxContext {
    // Thread-safe factory registries
    source_factories: Arc<RwLock<HashMap<String, Box<dyn SourceFactory>>>>,
    sink_factories: Arc<RwLock<HashMap<String, Box<dyn SinkFactory>>>>,
    source_mapper_factories: Arc<RwLock<HashMap<String, Box<dyn SourceMapperFactory>>>>,
    sink_mapper_factories: Arc<RwLock<HashMap<String, Box<dyn SinkMapperFactory>>>>,
    // ... other registries
}

impl EventFluxContext {
    /// Register a source factory
    pub fn add_source_factory(&self, name: String, factory: Box<dyn SourceFactory>) {
        self.source_factories.write().unwrap().insert(name, factory);
    }

    /// Retrieve a source factory by extension name
    pub fn get_source_factory(&self, name: &str) -> Option<Box<dyn SourceFactory>> {
        self.source_factories.read().unwrap().get(name).map(|f| f.clone_box())
    }

    /// Register a sink factory
    pub fn add_sink_factory(&self, name: String, factory: Box<dyn SinkFactory>) {
        self.sink_factories.write().unwrap().insert(name, factory);
    }

    /// Retrieve a sink factory by extension name
    pub fn get_sink_factory(&self, name: &str) -> Option<Box<dyn SinkFactory>> {
        self.sink_factories.read().unwrap().get(name).map(|f| f.clone_box())
    }

    // Similar methods for mapper factories...
}
```

**Registration Flow** (at application startup):

```rust
// During EventFluxContext initialization
impl EventFluxContext {
    pub fn new() -> Self {
        let context = EventFluxContext {
            source_factories: Arc::new(RwLock::new(HashMap::new())),
            sink_factories: Arc::new(RwLock::new(HashMap::new())),
            source_mapper_factories: Arc::new(RwLock::new(HashMap::new())),
            sink_mapper_factories: Arc::new(RwLock::new(HashMap::new())),
            // ... initialize other registries
        };

        // Register built-in factories
        context.register_builtin_factories();

        context
    }

    fn register_builtin_factories(&self) {
        // Register source factories
        self.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));
        self.add_source_factory("http".to_string(), Box::new(HttpSourceFactory));
        self.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

        // Register sink factories
        self.add_sink_factory("kafka".to_string(), Box::new(KafkaSinkFactory));
        self.add_sink_factory("http".to_string(), Box::new(HttpSinkFactory));
        self.add_sink_factory("log".to_string(), Box::new(LogSinkFactory));

        // Register mapper factories
        self.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));
        self.add_source_mapper_factory("csv".to_string(), Box::new(CsvSourceMapperFactory));
        self.add_source_mapper_factory("avro".to_string(), Box::new(AvroSourceMapperFactory));

        self.add_sink_mapper_factory("json".to_string(), Box::new(JsonSinkMapperFactory));
        self.add_sink_mapper_factory("csv".to_string(), Box::new(CsvSinkMapperFactory));
        // ... register other built-in mappers
    }
}
```

**Lookup Pattern** (during stream initialization):

```rust
// When processing CREATE STREAM Orders WITH ('type' = 'source', 'extension' = 'kafka', 'format' = 'json')
fn initialize_stream(context: &EventFluxContext, config: &StreamConfig) -> Result<()> {
    match config.stream_type {
        StreamType::Source => {
            // 1. Look up source factory by extension
            let source_factory = context.get_source_factory(&config.extension)
                .ok_or(format!("Unknown source extension: {}", config.extension))?;

            // 2. Validate format support
            if !source_factory.supported_formats().contains(&config.format.as_str()) {
                return Err(format!("Extension '{}' does not support format '{}'",
                    config.extension, config.format));
            }

            // 3. Look up mapper factory by format
            let mapper_factory = context.get_source_mapper_factory(&config.format)
                .ok_or(format!("Unknown source format: {}", config.format))?;

            // 4. Create fully initialized instances (fail-fast)
            let source = source_factory.create_initialized(&config.properties)?;
            let mapper = mapper_factory.create_initialized(&config.properties)?;

            // 5. Wire them together - instances are guaranteed valid...
        }
        // Similar for Sink and Internal types...
    }
}
```

**Key Benefits**:
- **Thread-safe**: `Arc<RwLock<HashMap>>` allows concurrent access
- **Type-safe**: Separate registries prevent type confusion
- **Extensible**: Custom factories can be registered at runtime
- **Simple API**: Clean `add_*_factory()` and `get_*_factory()` methods

**Implementation Example** (based on existing LogSinkFactory pattern):

```rust
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "avro", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["kafka.brokers", "kafka.topic"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &["kafka.consumer.group", "kafka.security.protocol", ...]
    }

    fn create(&self) -> Box<dyn Source> {
        Box::new(KafkaSource::new())
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

// KafkaSource instance handles connection and runtime validation
pub struct KafkaSource {
    config: HashMap<String, String>,
    consumer: Option<Consumer>,
}

impl KafkaSource {
    pub fn new() -> Self { ... }

    // Instance-level initialization (called at Phase 2)
    pub fn initialize(&mut self, config: HashMap<String, String>) -> Result<()> {
        // 1. Validate required config present (fail-fast)
        self.validate_config(&config)?;

        // 2. Establish connection (fail-fast)
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", config.get("kafka.brokers"))
            .create()?;  // ← FAILS HERE if Kafka unreachable

        // 3. Test connectivity
        consumer.fetch_metadata(timeout)?;

        self.consumer = Some(consumer);
        Ok(())
    }
}
```

**Key Distinction**:
- **Factory**: Lightweight, validation logic (`supported_formats()`, `required_parameters()`)
- **Instance**: Stateful, connection management, runtime behavior

### Milestone 5: Mapper Implementation

**Goal**: Implement SourceMapper and SinkMapper for field extraction and templating

**Architecture**: Mappers follow the same Factory pattern as Source/Sink

**SourceMapper Responsibilities**:
- Deserialize raw bytes to Event objects
- Extract fields based on `mapping.*` configuration
- Handle format-specific parsing (JSON paths, XML XPath, CSV columns)
- Auto-mapping for top-level fields when mappings not specified

**SinkMapper Responsibilities**:
- Serialize Event objects to bytes
- Apply templates to event data
- Handle format-specific rendering (JSON stringify, XML generation, CSV formatting)

**Implementation Pattern** (follows existing SourceMapperFactory/SinkMapperFactory):

```rust
// Factory creates mapper instances
pub struct JsonSourceMapperFactory;

impl SourceMapperFactory for JsonSourceMapperFactory {
    fn name(&self) -> &'static str {
        "json"
    }

    fn required_parameters(&self) -> &[&str] {
        &[]  // No required params, has smart defaults
    }

    fn optional_parameters(&self) -> &[&str] {
        &["json.ignore-parse-errors", "json.date-format", "json.mapping.*"]
    }

    fn create(&self) -> Box<dyn SourceMapper> {
        Box::new(JsonSourceMapper::new())
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}

// Mapper instance handles actual transformation
pub struct JsonSourceMapper {
    config: HashMap<String, String>,
    mappings: HashMap<String, String>,  // Field → JSONPath
}

impl SourceMapper for JsonSourceMapper {
    fn map(&self, raw_data: &[u8]) -> Result<Event, MapperError> {
        let json: serde_json::Value = serde_json::from_slice(raw_data)?;

        // If mappings specified, use them; otherwise auto-map top-level
        if self.mappings.is_empty() {
            self.auto_map(&json)
        } else {
            self.extract_with_mappings(&json, &self.mappings)
        }
    }
}
```

**Key Points**:
- Factory: Lightweight, validation only
- Mapper instance: Stateful, configuration, transformation logic
- Follows same pattern as Source/Sink for consistency

#### Configuration Resolution Algorithm

**Goal**: Understand how EventFlux merges configurations from multiple sources (Rust defaults → TOML [application] → TOML [streams] → SQL WITH)

**Existing Implementation** (in `src/core/config/manager.rs`):

EventFlux uses priority-based configuration loaders that are sorted and merged sequentially:

```rust
pub struct ConfigManager {
    /// Registered configuration loaders in priority order
    loaders: Vec<Box<dyn ConfigLoader>>,

    /// Cached configuration
    cached_config: Arc<RwLock<Option<EventFluxConfig>>>,

    /// Configuration validation enabled
    validation_enabled: bool,
}

impl ConfigManager {
    /// Load configuration from all sources with proper precedence
    pub async fn load_unified_config(&self) -> ConfigResult<EventFluxConfig> {
        let mut final_config = EventFluxConfig::default();
        let mut loaded_any = false;
        let mut errors = Vec::new();

        // Load from each source in priority order (lowest to highest)
        for loader in &self.loaders {
            if !loader.is_available() {
                continue;
            }

            match loader.load().await {
                Ok(Some(config)) => {
                    // Merge this configuration into the final config
                    if let Err(e) = final_config.merge(config) {
                        errors.push(format!(
                            "Failed to merge config from {}: {}",
                            loader.description(),
                            e
                        ));
                        continue;
                    }
                    loaded_any = true;
                    println!("Loaded configuration from: {}", loader.description());
                }
                Ok(None) => {
                    // No configuration from this source, continue
                    continue;
                }
                Err(e) => {
                    errors.push(format!(
                        "Failed to load config from {}: {}",
                        loader.description(),
                        e
                    ));
                    continue;
                }
            }
        }

        // If no configuration was loaded and we have errors, report them
        if !loaded_any && !errors.is_empty() {
            return Err(ConfigError::internal_error(format!(
                "No configuration could be loaded. Errors: {}",
                errors.join("; ")
            )));
        }

        // Validate the final configuration if validation is enabled
        if self.validation_enabled {
            self.validate_config(&final_config)?;
        }

        // Update cache
        {
            let mut cache = self.cached_config.write().await;
            *cache = Some(final_config.clone());
        }

        Ok(final_config)
    }
}
```

**Resolution Flow**:

1. **Loader Registration and Sorting**:
   - ConfigManager maintains a list of configuration loaders
   - Each loader has a priority number (e.g., YAML=50, Kubernetes=75, Environment=100)
   - Loaders are sorted by priority (ascending), so highest priority is last
   - Default loaders: YamlConfigLoader (50) → KubernetesConfigMapLoader (75, if available) → EnvironmentConfigLoader (100)

2. **Sequential Merge**:
   - Start with `EventFluxConfig::default()` (Rust defaults)
   - Iterate through sorted loaders (lowest to highest priority)
   - For each available loader, load its config
   - Call `final_config.merge(config)` to merge loaded config into final config
   - **Merge is per-property**: Each property from the new config overwrites the corresponding property in final_config

3. **Per-Property Merge via EventFluxConfig::merge()**:
   ```rust
   impl EventFluxConfig {
       pub fn merge(&mut self, other: EventFluxConfig) -> Result<(), String> {
           // Merge metadata (other takes precedence for non-None values)
           if let Some(name) = other.metadata.name {
               self.metadata.name = Some(name);
           }
           if let Some(namespace) = other.metadata.namespace {
               self.metadata.namespace = Some(namespace);
           }

           // Merge labels and annotations (per-key merge)
           self.metadata.labels.extend(other.metadata.labels);
           self.metadata.annotations.extend(other.metadata.annotations);

           // Deep merge the eventflux global configuration
           self.eventflux.merge(other.eventflux);

           // Merge applications (per-application merge)
           self.applications.extend(other.applications);

           Ok(())
       }
   }
   ```

4. **Deep Merge for Nested Structures**:
   - `EventFluxGlobalConfig::merge()` performs similar per-property merge for runtime, observability, distributed config
   - Each nested level merges properties individually, not replacing entire sections

5. **Error Collection**:
   - ConfigManager collects errors from failed loaders but continues processing
   - Only fails if NO configuration loaded and errors exist
   - Warnings logged for non-fatal errors (loader unavailable, merge conflicts)

6. **Validation**:
   - After merge completes, validate final config if `validation_enabled`
   - Validation checks: required fields, circular dependencies, DLQ schemas, etc.

**Priority Order Example**:

```
Priority 50:  YamlConfigLoader
              - Loads from config-dev.toml or config-prod.toml
              - Provides [application] and [streams.*] sections

Priority 75:  KubernetesConfigMapLoader (if KUBERNETES_SERVICE_HOST set)
              - Loads from Kubernetes ConfigMap
              - Overrides YAML config with cluster-specific settings

Priority 100: EnvironmentConfigLoader
              - Loads from environment variables (EVENTFLUX_*)
              - Highest priority, overrides all other sources
```

**Merge Result**:

```
final_config = Rust defaults
  .merge(YAML config)          // Priority 50
  .merge(Kubernetes config)    // Priority 75 (if available)
  .merge(Environment variables) // Priority 100
```

**Key Characteristics**:
- **Per-Property Merge**: Properties override individually, not by namespace or section
- **Graceful Degradation**: Failed loaders don't stop the process (errors collected, processing continues)
- **Caching**: Final config cached for subsequent calls
- **Validation**: Post-merge validation ensures integrity
- **Transparency**: Logs which loaders succeeded and which failed

**Example Scenario**:

```rust
// YAML (priority 50):
{
  eventflux: {
    runtime: { mode: "single-node", performance: { thread_pool_size: 4 } }
  }
}

// Environment variables (priority 100):
{
  eventflux: {
    runtime: { performance: { thread_pool_size: 16 } }
  }
}

// Final merged config:
{
  eventflux: {
    runtime: {
      mode: "single-node",                    // from YAML (not overridden)
      performance: { thread_pool_size: 16 }   // from Environment (overridden)
    }
  }
}
```

**Benefits**:
- **Flexible**: Multiple configuration sources with clear precedence
- **Predictable**: Per-property merge is easy to reason about
- **Extensible**: New loaders can be added with custom priorities
- **Resilient**: Failed loaders don't prevent startup if other sources succeed

### Milestone 6: CLI Tools

**Goal**: Configuration introspection and validation

```bash
# List resolved configuration for specific stream (output: YAML)
eventflux config list app.sql --config config-prod.toml --stream Orders

# Validate configuration (check syntax, required properties, format support)
eventflux config validate app.sql --config config-prod.toml

# Show all stream configurations (output: YAML)
eventflux config show app.sql --config config-prod.toml

# Example output (YAML):
# streams:
#   Orders:
#     type: source
#     extension: kafka
#     format: json
#     kafka.brokers: prod1:9092,prod2:9092
#     kafka.topic: orders
#     kafka.security.protocol: SASL_SSL
#     # ... (shows fully resolved config with inheritance)
```

**Note**: All commands require both `app.sql` and `--config` since configuration resolution depends on merging SQL WITH and TOML.

---

## Runtime Architecture Cross-References

This section documents how the configuration system integrates with EventFlux's existing runtime architecture components. Understanding these relationships is critical for implementing the configuration system correctly.

### Component Lifecycle Model

**Phase 1: Creation & Initialization** (via `create_initialized()`)
```
SQL Parser → FlatConfig → Factory.create_initialized() → Initialized Component
```

**Phase 2: Runtime Execution** (via `start()`)
```
Initialized Component → start() → Active Processing → stop() → Cleanup
```

**Lifecycle Flowchart**:
```
┌─────────────────┐
│   SQL WITH      │
│   TOML Config   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  FlatConfig     │  Parse-Time Validation
│  Merge Layers   │  (syntax, required keys)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Factory         │
│ create_         │  Initialization Validation
│ initialized()   │  (fail-fast: connectivity,
└────────┬────────┘   type checking, resources)
         │
         ▼
┌─────────────────┐
│ Initialized     │  Ready-to-use instance
│ Component       │  (Source/Sink/Mapper/Junction)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ start()         │  Runtime Phase
│ Active          │  (retry on transient errors,
│ Processing      │   error handlers for DLQ/drop)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ stop()          │  Graceful Shutdown
│ Cleanup         │  (flush buffers, close connections)
└─────────────────┘
```

### Error Handling Architecture

**Existing Error Types** (defined in `src/core/exception/error.rs` and `src/core/config/error.rs`):

```rust
// DO NOT create InitError, RuntimeError, or MapperError
// Use existing EventFluxError variants:

#[derive(Error, Debug)]
pub enum EventFluxError {
    // Configuration errors
    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        config_key: Option<String>,
    },

    // Connection failures (initialization phase)
    #[error("Connection unavailable: {message}")]
    ConnectionUnavailable {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    // Mapping errors
    #[error("Mapping failed: {message}")]
    MappingFailed {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    // Extension not found
    #[error("{extension_type} extension '{name}' not found")]
    ExtensionNotFound {
        extension_type: String,
        name: String,
    },

    // Type errors
    #[error("Type error: {message}")]
    TypeError {
        message: String,
        expected: Option<String>,
        actual: Option<String>,
    },

    // Invalid parameters
    #[error("Invalid parameter: {message}")]
    InvalidParameter {
        message: String,
        parameter: Option<String>,
        expected: Option<String>,
    },

    // ... (19 more variants for various runtime scenarios)
}

// Configuration-specific errors
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration validation failed: {errors:?}")]
    ValidationFailed { errors: Vec<ValidationError> },

    #[error("Required field '{field}' is missing")]
    MissingRequiredField { field: String },

    // ... (22 more variants)
}
```

**Factory Error Handling Pattern**:
```rust
impl SourceFactory for KafkaSourceFactory {
    fn create_initialized(&self, config: &HashMap<String, String>)
        -> Result<Box<dyn Source>, EventFluxError> {  // ← Use EventFluxError, not InitError

        // Parse configuration
        let parsed = KafkaSourceConfig::parse(config)
            .map_err(|e| EventFluxError::Configuration {
                message: e.to_string(),
                config_key: Some("kafka".to_string()),
            })?;

        // Test connectivity (fail-fast)
        let consumer = create_kafka_consumer(&parsed)
            .map_err(|e| EventFluxError::ConnectionUnavailable {
                message: format!("Kafka connection failed: {}", e),
                source: Some(Box::new(e)),
            })?;

        Ok(Box::new(KafkaSource { consumer }))
    }
}
```

### Source Architecture (Push Model)

**Existing Trait** (defined in `src/core/stream/input/source/mod.rs`):

```rust
pub trait Source: Debug + Send + Sync {
    /// Start the source, providing an InputHandler to push events to
    /// Sources spawn internal threads and push events to the handler
    fn start(&mut self, handler: Arc<Mutex<InputHandler>>);

    /// Stop the source (graceful shutdown)
    fn stop(&mut self);

    /// Clone trait object
    fn clone_box(&self) -> Box<dyn Source>;
}
```

**Key Characteristics**:
- **Push-based**: Sources receive an `InputHandler` and actively push events to it
- **Thread-spawning**: Sources typically spawn background threads for I/O operations
- **No `read()` method**: Sources are not polled - they push events when data arrives

**Threading Model**:
- Sources run in dedicated threads
- Events flow **synchronously** through the pipeline in the **same thread** (till sink or terminating point)
- This preserves **event ordering** (FIFO guarantee)
- Exception: When **async junctions** (`OptimizedStreamJunction`) are used, events may be processed concurrently and reordering may occur

**InputHandler Integration** (`src/core/stream/input/input_handler.rs`):
```rust
impl InputHandler {
    /// Sources push raw data here
    pub fn send_data(&self, data: Vec<AttributeValue>) -> Result<(), EventFluxError>;

    /// Sources push events here
    pub fn send_single_event(&self, event: Event) -> Result<(), EventFluxError>;
    pub fn send_multiple_events(&self, events: Vec<Event>) -> Result<(), EventFluxError>;
}
```

**Configuration System Integration**:
- Factory creates Source with `create_initialized(config)`
- Source is fully configured and ready to accept InputHandler
- Runtime calls `source.start(input_handler)` to begin event flow

### Sink Architecture (Callback Model)

**Existing Trait** (defined in `src/core/stream/output/sink/sink_trait.rs`):

```rust
pub trait Sink: StreamCallback + Debug + Send + Sync {
    /// Start the sink (optional initialization)
    fn start(&self) {}

    /// Stop the sink (graceful shutdown)
    fn stop(&self) {}

    /// Clone trait object
    fn clone_box(&self) -> Box<dyn Sink>;
}

// Sinks implement StreamCallback to receive events
pub trait StreamCallback: Send + Sync {
    /// Receive an event (callback from pipeline)
    fn receive(&self, event: &Event) -> Result<(), EventFluxError>;
}
```

**Key Characteristics**:
- **Callback-based**: Sinks implement `StreamCallback::receive()` to handle events
- **No `write()` method**: Events are delivered via callback, not explicit write calls
- **Synchronous processing**: `receive()` is called in the event processing thread

**Configuration System Integration**:
- Factory creates Sink with `create_initialized(config)`
- Sink is fully configured and ready to receive events
- Runtime registers Sink as a callback handler
- Events are delivered via `sink.receive(event)` calls

### Junction Architecture (Event Routing)

**Existing Implementation** (defined in `src/core/stream/junction_factory.rs`):

```rust
pub enum JunctionType {
    /// Standard crossbeam_channel-based implementation
    Standard(Arc<Mutex<StreamJunction>>),

    /// Optimized crossbeam pipeline-based implementation
    Optimized(Arc<Mutex<OptimizedStreamJunction>>),
}

pub enum PerformanceLevel {
    Standard,           // Use crossbeam_channel (ordering preserved)
    HighPerformance,    // Use crossbeam pipeline (potential reordering)
    Auto,               // Auto-select based on workload hints
}

pub struct JunctionConfig {
    pub stream_id: String,
    pub buffer_size: usize,
    pub is_async: bool,              // false = synchronous (default)
    pub performance_level: PerformanceLevel,
    pub expected_throughput: Option<u64>,
    pub subscriber_count: Option<usize>,
}
```

**Junction Selection Logic**:
- **Standard** (default): Synchronous event flow, strict ordering, lower throughput
- **Optimized** (async): Concurrent processing, potential reordering, >100K events/sec capability
- **Auto**: Selects based on throughput hints and subscriber count

**Event Ordering Guarantees**:
- **Synchronous mode** (`is_async: false`): Events processed in arrival order
- **Async mode** (`is_async: true`): Events may be reordered due to concurrent processing

**Configuration System Integration**:
- Junctions are **not configured via WITH clause** (internal runtime component)
- Created by runtime based on stream characteristics
- May be exposed in future via runtime performance hints

### Mapper Architecture

Mapper traits are defined in `src/core/extension/mod.rs`:

```rust
pub trait SourceMapper: Debug + Send + Sync {
    /// Map raw bytes to EventFlux events
    /// Returns Result to handle malformed data gracefully
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError>;

    fn clone_box(&self) -> Box<dyn SourceMapper>;
}

pub trait SinkMapper: Debug + Send + Sync {
    /// Map EventFlux events to raw bytes
    /// Returns Result to handle serialization errors
    fn map(&self, events: &[Event]) -> Result<Vec<u8>, EventFluxError>;

    fn clone_box(&self) -> Box<dyn SinkMapper>;
}
```

**Error Handling**:
- Use `EventFluxError::MappingFailed` for parse/serialization errors
- Configuration determines error strategy: drop/retry/dlq/fail
- Mappers should never panic - always return Result

**Configuration System Integration**:
- Mappers created via factory `create_initialized(config)`
- Fully configured and ready to process data
- No separate configuration step needed

### Error Store Integration

**Existing Implementation** (`src/core/stream/output/error_store.rs`):

```rust
pub trait ErrorStore: Send + Sync + std::fmt::Debug {
    fn store(&self, stream_id: &str, error: EventFluxError);
}

pub struct InMemoryErrorStore {
    inner: Mutex<Vec<(String, String)>>,
}
```

**Configuration System Integration**:
- When `error.strategy = "dlq"` is configured:
  - Runtime creates ErrorStore instance
  - Failed events/errors are pushed to error store
  - Error store configured via `error.dlq.*` properties
- ErrorStore receives `EventFluxError` instances directly

### Summary: Runtime Integration Points

| Component | Configuration Entry | Runtime Interface | Threading Model |
|-----------|---------------------|-------------------|-----------------|
| **Source** | `type = "source"` + `extension` | `start(InputHandler)` → pushes events | Spawns thread, pushes to pipeline |
| **Sink** | `type = "sink"` + `extension` | `receive(Event)` callback | Called in pipeline thread |
| **Junction** | Not configurable (internal) | `send_event(Event)` | Sync (ordered) or Async (concurrent) |
| **Mapper** | `format = "json"` etc. | `map(input) -> Result<output>` | Called in pipeline thread |
| **ErrorStore** | `error.dlq.*` properties | `store(stream_id, error)` | Called on error in pipeline thread |

**Key Takeaways for Implementation**:
1. **Use existing error types**: EventFluxError, ConfigError (do not create new error enums)
2. **Push model for sources**: Sources push to InputHandler, not polled
3. **Callback model for sinks**: Sinks implement StreamCallback::receive()
4. **Event ordering**: Synchronous by default (same-thread processing)
5. **Lifecycle**: create_initialized() → start() → runtime → stop()

### Dead Letter Queue (DLQ) Event Construction

When `error.strategy = "dlq"` is configured, failed events must be wrapped with error context before being sent to the DLQ stream.

**DLQ Event Schema**:

DLQ events follow the schema defined in SQL (see lines 1008-1014 for authoritative schema):

```sql
CREATE STREAM OrderErrors (
    originalEvent STRING,   -- Serialized original event that failed
    errorMessage STRING,    -- Human-readable error description
    errorType STRING,       -- EventFluxError variant (e.g., "MappingFailed")
    timestamp BIGINT,       -- Error occurrence time (milliseconds since epoch)
    attemptCount INT,       -- Number of retry attempts made
    streamName STRING       -- Source stream where error occurred
);
```

**DLQ Event Creation Example**:

```rust
impl Source for KafkaSource {
    fn send_to_dlq(&self, original_event: Event, error: EventFluxError) {
        // Get DLQ stream junction from context
        let dlq_stream = self.config.get("error.dlq.stream")
            .unwrap_or("_dlq");

        if let Some(dlq_junction) = self.context.get_stream_junction(dlq_stream) {
            // Create DLQ event with metadata (order matches SQL schema)
            let error_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            let dlq_event = Event::new_with_data(
                error_timestamp,  // Event arrival timestamp
                vec![
                    AttributeValue::String(serde_json::to_string(&original_event).unwrap()),  // 1. originalEvent
                    AttributeValue::String(error.to_string()),                                // 2. errorMessage
                    AttributeValue::String(format!("{:?}", error)),                           // 3. errorType
                    AttributeValue::Long(error_timestamp),                                    // 4. timestamp (BIGINT)
                    AttributeValue::Int(self.retry_count as i32),                             // 5. attemptCount (INT)
                    AttributeValue::String(self.stream_id.clone()),                           // 6. streamName
                ],
            );

            // Send to DLQ stream
            dlq_junction.lock().unwrap().send_event(dlq_event);

            // Optionally log to ErrorStore as well
            if let Some(error_store) = &self.error_store {
                error_store.store(&self.stream_id, error);
            }
        } else {
            eprintln!("DLQ stream '{}' not found, dropping event", dlq_stream);
        }
    }
}
```

**DLQ Stream Definition**:

The DLQ stream must be defined in the EventFlux application:

```sql
-- Create DLQ stream with error metadata schema (matches lines 1008-1014)
CREATE STREAM _dlq (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
);

-- Optional: Route DLQ events to persistent storage
INSERT INTO DlqSinkStream
SELECT * FROM _dlq;
```

**Configuration Integration**:

```toml
[streams.Orders]
type = "source"
extension = "kafka"
format = "json"

[streams.Orders.error]
strategy = "dlq"
dlq.stream = "_dlq"          # Target DLQ stream name
dlq.include_stacktrace = true # Include debug info (development mode)
max_retries = 3
```

### Generic Configuration Validation Helper

To ensure consistent validation across all extensions, EventFlux provides a generic configuration validation helper:

**Validation Helper API**:

```rust
/// Generic configuration validator for extensions
/// Location: src/core/config/validator.rs
pub struct ConfigValidator<'a> {
    config: &'a HashMap<String, String>,
    errors: Vec<String>,
}

impl<'a> ConfigValidator<'a> {
    pub fn new(config: &'a HashMap<String, String>) -> Self {
        Self {
            config,
            errors: Vec::new(),
        }
    }

    /// Require a configuration key to exist
    pub fn require(&mut self, key: &str) -> &mut Self {
        if !self.config.contains_key(key) {
            self.errors.push(format!("Missing required configuration key '{}'", key));
        }
        self
    }

    /// Require one of several keys to exist
    pub fn require_one_of(&mut self, keys: &[&str]) -> &mut Self {
        if !keys.iter().any(|k| self.config.contains_key(*k)) {
            self.errors.push(format!(
                "Missing required configuration: one of [{}]",
                keys.join(", ")
            ));
        }
        self
    }

    /// Validate a key has an allowed value
    pub fn validate_enum(&mut self, key: &str, allowed_values: &[&str]) -> &mut Self {
        if let Some(value) = self.config.get(key) {
            if !allowed_values.contains(&value.as_str()) {
                self.errors.push(format!(
                    "Invalid value '{}' for key '{}'. Allowed values: [{}]",
                    value, key, allowed_values.join(", ")
                ));
            }
        }
        self
    }

    /// Validate a key is a valid integer
    pub fn validate_int(&mut self, key: &str, min: Option<i64>, max: Option<i64>) -> &mut Self {
        if let Some(value) = self.config.get(key) {
            match value.parse::<i64>() {
                Ok(n) => {
                    if let Some(min_val) = min {
                        if n < min_val {
                            self.errors.push(format!(
                                "Value {} for key '{}' is below minimum {}",
                                n, key, min_val
                            ));
                        }
                    }
                    if let Some(max_val) = max {
                        if n > max_val {
                            self.errors.push(format!(
                                "Value {} for key '{}' exceeds maximum {}",
                                n, key, max_val
                            ));
                        }
                    }
                }
                Err(_) => {
                    self.errors.push(format!("Invalid integer value '{}' for key '{}'", value, key));
                }
            }
        }
        self
    }

    /// Validate a key is a valid boolean
    pub fn validate_bool(&mut self, key: &str) -> &mut Self {
        if let Some(value) = self.config.get(key) {
            if !["true", "false"].contains(&value.to_lowercase().as_str()) {
                self.errors.push(format!(
                    "Invalid boolean value '{}' for key '{}'. Use 'true' or 'false'",
                    value, key
                ));
            }
        }
        self
    }

    /// Validate a key is a valid URL
    pub fn validate_url(&mut self, key: &str) -> &mut Self {
        if let Some(value) = self.config.get(key) {
            if !value.starts_with("http://") && !value.starts_with("https://") {
                self.errors.push(format!(
                    "Invalid URL '{}' for key '{}'. Must start with http:// or https://",
                    value, key
                ));
            }
        }
        self
    }

    /// Finalize validation and return result
    pub fn validate(self) -> Result<(), EventFluxError> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(EventFluxError::Configuration {
                message: format!("Configuration validation failed:\n  {}", self.errors.join("\n  ")),
                config_key: None,
            })
        }
    }
}
```

**Usage Example**:

```rust
impl KafkaSourceFactory {
    fn create_initialized(&self, config: HashMap<String, String>) -> Result<Box<dyn Source>, EventFluxError> {
        // Validate configuration using helper
        ConfigValidator::new(&config)
            .require("kafka.brokers")
            .require("kafka.topic")
            .require_one_of(&["kafka.group.id", "kafka.consumer.group.id"])
            .validate_enum("kafka.security.protocol", &["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
            .validate_int("kafka.session.timeout.ms", Some(1), Some(3600000))
            .validate_bool("kafka.enable.auto.commit")
            .validate()?;

        // Extract validated configuration
        let brokers = config.get("kafka.brokers").unwrap();  // Safe: validated above
        let topic = config.get("kafka.topic").unwrap();      // Safe: validated above

        // Create and initialize Kafka source
        let source = KafkaSource::new(brokers, topic, config)?;

        Ok(Box::new(source))
    }
}
```

**Benefits**:
- **Consistent validation**: All extensions use the same validation patterns
- **Clear error messages**: Users get precise information about configuration issues
- **Fail-fast**: Invalid configuration detected during initialization, not runtime
- **Type safety**: Validation ensures correct types before parsing
- **Extensible**: Easy to add new validation rules

### Source/Sink/Table Handler Registry

The EventFlux runtime provides handler registries for managing the lifecycle of sources, sinks, and tables. Handlers encapsulate instances and provide thread-safe access for lifecycle operations.

#### Handler Implementations

Handler structs are implemented in `src/core/stream/handler/mod.rs`:

```rust
/// Manages source lifecycle and integrates with InputHandler
pub struct SourceStreamHandler {
    source: Arc<Mutex<Box<dyn Source>>>,
    mapper: Option<Arc<Mutex<Box<dyn SourceMapper>>>>,
    input_handler: Arc<Mutex<InputHandler>>,
    stream_id: String,
    is_running: AtomicBool,
}

impl SourceStreamHandler {
    pub fn new(
        source: Box<dyn Source>,
        mapper: Option<Box<dyn SourceMapper>>,
        input_handler: Arc<Mutex<InputHandler>>,
        stream_id: String,
    ) -> Self {
        Self {
            source: Arc::new(Mutex::new(source)),
            mapper: mapper.map(|m| Arc::new(Mutex::new(m))),
            input_handler,
            stream_id,
            is_running: AtomicBool::new(false),
        }
    }

    /// Start the source (spawns thread, begins pushing events)
    pub fn start(&self) -> Result<(), EventFluxError> {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err(EventFluxError::app_runtime(
                format!("Source '{}' is already running", self.stream_id)
            ));
        }

        self.source.lock().unwrap().start(Arc::clone(&self.input_handler));
        Ok(())
    }

    /// Stop the source (signals thread to exit)
    pub fn stop(&self) {
        if self.is_running.swap(false, Ordering::SeqCst) {
            self.source.lock().unwrap().stop();
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

/// Manages sink lifecycle and integrates with StreamJunction
pub struct SinkStreamHandler {
    sink: Arc<Mutex<Box<dyn Sink>>>,
    mapper: Option<Arc<Mutex<Box<dyn SinkMapper>>>>,
    stream_id: String,
}

impl SinkStreamHandler {
    pub fn new(
        sink: Box<dyn Sink>,
        mapper: Option<Box<dyn SinkMapper>>,
        stream_id: String,
    ) -> Self {
        Self {
            sink: Arc::new(Mutex::new(sink)),
            mapper: mapper.map(|m| Arc::new(Mutex::new(m))),
            stream_id,
        }
    }

    pub fn start(&self) {
        self.sink.lock().unwrap().start();
    }

    pub fn stop(&self) {
        self.sink.lock().unwrap().stop();
    }
}
```

#### Runtime Registries

Handler registries are implemented in `EventFluxAppRuntime` (`src/core/eventflux_app_runtime.rs`):

```rust
pub struct EventFluxAppRuntime {
    source_handlers: Arc<RwLock<HashMap<String, Arc<SourceStreamHandler>>>>,
    sink_handlers: Arc<RwLock<HashMap<String, Arc<SinkStreamHandler>>>>,
    table_handlers: Arc<RwLock<HashMap<String, Arc<dyn Table>>>>,
    // ... other fields ...
}

impl EventFluxAppRuntime {
    pub fn register_source_handler(&self, stream_id: String, handler: Arc<SourceStreamHandler>) {
        self.source_handlers.write().unwrap().insert(stream_id, handler);
    }

    pub fn get_source_handler(&self, stream_id: &str) -> Option<Arc<SourceStreamHandler>> {
        self.source_handlers.read().unwrap().get(stream_id).cloned()
    }

    pub fn register_sink_handler(&self, stream_id: String, handler: Arc<SinkStreamHandler>) {
        self.sink_handlers.write().unwrap().insert(stream_id, handler);
    }

    pub fn get_sink_handler(&self, stream_id: &str) -> Option<Arc<SinkStreamHandler>> {
        self.sink_handlers.read().unwrap().get(stream_id).cloned()
    }

    pub fn register_table_handler(&self, table_name: String, table: Arc<dyn Table>) {
        self.table_handlers.write().unwrap().insert(table_name, table);
    }

    pub fn get_table_handler(&self, table_name: &str) -> Option<Arc<dyn Table>> {
        self.table_handlers.read().unwrap().get(table_name).cloned()
    }
}
```

#### Auto-Attach Implementation

Auto-attach is implemented in `src/core/eventflux_app_runtime.rs`:

- `auto_attach_sources_from_config()` - Creates source handlers from configuration with error accumulation
- `auto_attach_sinks_from_config()` - Creates sink handlers from configuration with error accumulation
- `auto_attach_tables_from_config()` - Creates table handlers from configuration with error accumulation

Key features:
- Idempotent operation (skips already-registered handlers)
- Error accumulation (partial success handling)
- Automatic lifecycle management (start/stop)
- Factory-based instantiation with mapper support

The runtime's `start()` method calls all auto-attach functions during initialization.

#### Configuration Usage

```toml
# config.toml
[streams.Orders]
type = "source"
extension = "kafka"
format = "json"
kafka.brokers = "localhost:9092"
kafka.topic = "orders"
```

```rust
// Application code
let runtime = EventFluxAppRuntime::new_with_config(
    app,
    eventflux_context,
    None,
    Some(config)
)?;

runtime.start();
// ... processing happens ...
runtime.shutdown();
```

## Security Considerations

**Current Status (M2)**: Development-friendly defaults with TODOs for production hardening.

### Credential Management

**M2 Behavior** (Current):
```toml
# ✅ Currently ALLOWED (for development convenience)
[application.kafka]
password = "literal-secret"  # Not recommended, but allowed
```

**Future Enhancement** (TODO for M3+):
```toml
# ✅ RECOMMENDED: Environment variables only
[application.kafka]
password = "${KAFKA_PASSWORD}"  # Fail if not set (production mode)

# ❌ REJECTED in production mode
password = "literal-secret"  # Validation error in production
```

**Implementation TODO**:
- [ ] Add `--production` flag to CLI
- [ ] Reject literal secrets in production mode
- [ ] Add credential validator that checks for hardcoded passwords

### CLI Output Redaction

**M2 Behavior** (Current):
```bash
eventflux config list app.sql --config config-prod.toml
# Output shows FULL config including secrets:
#   kafka.password: "my-secret-password"  # Visible!
```

**Future Enhancement** (TODO for M3+):
```bash
eventflux config list app.sql --config config-prod.toml
# Output redacts sensitive keys:
#   kafka.password: "****"  # Redacted

eventflux config list app.sql --config config-prod.toml --show-secrets
# Explicit flag required to show secrets (with warning)
```

**Implementation TODO**:
- [ ] Define list of sensitive keys (`password`, `secret`, `token`, `key`, `credentials`)
- [ ] Add redaction filter to CLI output
- [ ] Add `--show-secrets` flag with security warning

### Logging Redaction

**M2 Behavior** (Current):
```rust
// Logs show resolved values including secrets
println!("Loaded config: kafka.password = {}", config.get("kafka.password"));
// Output: Loaded config: kafka.password = my-secret-password
```

**Future Enhancement** (TODO for M3+):
```rust
// Logs redact sensitive values
println!("Loaded config: kafka.password = {}", redact(config.get("kafka.password")));
// Output: Loaded config: kafka.password = ****
```

**Implementation TODO**:
- [ ] Add `redact()` utility function
- [ ] Update all logging statements to use redaction
- [ ] Add config option `log.redact-secrets = true`

### Encryption at Rest

**M2 Behavior** (Current):
- Config files stored in plain text
- No encryption for TOML files

**Future Enhancement** (TODO for M4+):
```bash
# Encrypt sensitive sections
eventflux config encrypt config-prod.toml --key-file encryption.key
# Creates: config-prod.encrypted.toml

# Decrypt at runtime
eventflux run app.sql --config config-prod.encrypted.toml --key-file encryption.key
```

**Implementation TODO**:
- [ ] Add encryption/decryption utilities
- [ ] Support encrypted TOML sections
- [ ] Key management integration (vault, KMS)

### Production Security Checklist

**For M2 (Current)**:
- ✅ Use environment variables for ALL credentials
- ✅ Never commit `.toml` files with secrets to git
- ✅ Use `.gitignore` for `config-*prod*.toml`
- ⚠️ Be aware CLI output shows secrets

**For M3+ (Future)**:
- [ ] Enable production mode validation
- [ ] Use encrypted config files
- [ ] Enable log redaction
- [ ] Regular secret rotation
- [ ] Audit CLI config access

---

## Troubleshooting Configuration Issues

This section covers common configuration errors and how to diagnose them.

### Environment Variable Resolution Failures

**Symptom**: Application fails to start with "Environment variable not found" error

**Scenario**:
```toml
[streams.Orders.kafka]
brokers = "${KAFKA_BROKERS}"  # Variable not set
```

**Error Message**:
```
ERROR: Environment variable KAFKA_BROKERS not found
Failed to load configuration from config-prod.toml
```

**Solution**:
- **Production**: Set the missing environment variable before startup
  ```bash
  export KAFKA_BROKERS="prod1:9092,prod2:9092"
  eventflux run app.sql --config config-prod.toml
  ```
- **Development**: Use default values in TOML
  ```toml
  brokers = "${KAFKA_BROKERS:localhost:9092}"  # Fallback to localhost
  ```

**Debug with CLI**:
```bash
# This will show the same error during config validation
eventflux config validate app.sql --config config-prod.toml
```

---

### Configuration Precedence Confusion

**Symptom**: Configuration value not what you expected

**Scenario**: You set `kafka.consumer.group` in TOML, but SQL WITH overrides it unexpectedly

**TOML**:
```toml
[streams.Orders.kafka]
consumer.group = "orders-group"
```

**SQL**:
```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'kafka.consumer.group' = 'dev-group'  -- Overrides TOML
);
```

**Debug with CLI**:
```bash
# Show fully resolved configuration to see final values
eventflux config list app.sql --config config-prod.toml --stream Orders
```

**Output (YAML)**:
```yaml
streams:
  Orders:
    kafka.consumer.group: dev-group  # ← SQL WITH wins (highest priority)
    kafka.brokers: prod1:9092  # ← From TOML
```

**Resolution**: Remember the priority order:
1. SQL WITH (highest)
2. TOML `[streams.StreamName.*]`
3. TOML `[application.*]`
4. Rust defaults (lowest)

---

### Missing Extension or Format in SQL

**Symptom**: Parse error "Stream requires 'extension' property"

**Invalid SQL**:
```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source'
    -- ❌ Missing 'extension' and 'format'
);
```

**Error Message**:
```
ERROR: Stream 'Orders' has type='source' but missing required 'extension' property
```

**Solution**: Always specify `extension` and `format` for source/sink streams in SQL
```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json'
);
```

**Why**: `type`, `extension`, and `format` are **structural properties** required by the parser at Phase 1 (parse-time), so they MUST be in SQL WITH clause, not TOML.

---

### Type/Extension/Format in TOML

**Symptom**: "Stream defines 'type' in TOML, but 'type' MUST be in SQL WITH clause"

**Invalid TOML**:
```toml
[streams.Orders]
type = "source"  # ❌ NOT ALLOWED
extension = "kafka"  # ❌ NOT ALLOWED
format = "json"  # ❌ NOT ALLOWED
```

**Error Message**:
```
ERROR: Stream defines 'type' in TOML, but 'type' MUST be in SQL WITH clause.
Type, extension, and format are structural properties required by the parser
at Phase 1 (parse-time), so they cannot be in TOML configuration.
```

**Solution**: Move these properties to SQL WITH clause
```sql
CREATE STREAM Orders (...) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json'
);
```

**TOML** (only operational config):
```toml
[streams.Orders.kafka]
brokers = "localhost:9092"
topic = "orders"
```

---

### DLQ Schema Mismatch

**Symptom**: "DLQ stream 'OrderErrors' missing required field 'streamName STRING'"

**Invalid DLQ Schema**:
```sql
CREATE STREAM OrderErrors (
    originalEvent STRING,
    errorMessage STRING
    -- ❌ Missing required fields
) WITH (
    'type' = 'internal'
);
```

**Error Message**:
```
ERROR: DLQ stream 'OrderErrors' missing required field 'errorType STRING'
ERROR: DLQ stream 'OrderErrors' missing required field 'timestamp BIGINT'
ERROR: DLQ stream 'OrderErrors' missing required field 'attemptCount INT'
ERROR: DLQ stream 'OrderErrors' missing required field 'streamName STRING'
```

**Solution**: DLQ streams require **exact schema**
```sql
CREATE STREAM OrderErrors (
    originalEvent STRING,
    errorMessage STRING,
    errorType STRING,
    timestamp BIGINT,
    attemptCount INT,
    streamName STRING
);
-- Pure internal stream (no type needed)
```

**Validation**: Happens at Phase 2 (Application Initialization)

---

### Circular Dependency Detection

**Symptom**: "Circular dependency detected: A → B → A"

**Invalid Query**:
```sql
INSERT INTO StreamA SELECT * FROM StreamB;
INSERT INTO StreamB SELECT * FROM StreamA;
```

**Error Message**:
```
ERROR: Circular dependency detected: StreamA → StreamB → StreamA
Streams cannot form dependency cycles.
```

**Solution**: Break the cycle using an intermediate stream
```sql
-- ✅ Valid: Linear dependency
INSERT INTO StreamB SELECT * FROM StreamA;
INSERT INTO StreamC SELECT * FROM StreamB;
```

**Validation**: Happens at Phase 1 (Parse-Time)

---

### Format Not Supported by Extension

**Symptom**: "Extension 'http' does not support format 'avro'"

**Invalid Configuration**:
```sql
CREATE STREAM WebhookSink (...) WITH (
    'type' = 'sink',
    'extension' = 'http',
    'format' = 'avro'  -- ❌ HTTP doesn't support Avro
);
```

**Error Message**:
```
ERROR: Extension 'http' does not support format 'avro'
Supported formats: json, csv, xml, bytes
```

**Solution**: Use a supported format
```sql
CREATE STREAM WebhookSink (...) WITH (
    'type' = 'sink',
    'extension' = 'http',
    'format' = 'json'  -- ✅ Supported
);
```

**Validation**: Happens at Phase 2 (Application Initialization)

---

### Using CLI Tools for Debugging

**Validate Configuration**:
```bash
# Check all configuration for syntax and semantic errors
eventflux config validate app.sql --config config-prod.toml
```

**Inspect Resolved Config**:
```bash
# Show fully resolved configuration for a specific stream
eventflux config list app.sql --config config-prod.toml --stream Orders

# Output shows final merged config with inheritance
```

**Show All Streams**:
```bash
# Show all stream configurations (useful for comparing)
eventflux config show app.sql --config config-prod.toml
```

---

## Complete Example

**Scenario**: Order processing with filtering, enrichment, and multi-sink fan-out

**app.sql:**
```sql
-- Source: Read orders from Kafka
CREATE STREAM Orders (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    timestamp BIGINT
) WITH (
    'type' = 'source',
    'extension' = 'kafka',
    'format' = 'json'
);

-- Internal: Filter high-value orders
CREATE STREAM HighValueOrders (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    timestamp BIGINT
);
-- Pure internal stream (no external I/O)

INSERT INTO HighValueOrders
SELECT * FROM Orders WHERE amount > 1000;

-- Internal: Enrich orders
CREATE STREAM EnrichedOrders (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    priority STRING,
    enrichedAt BIGINT
);
-- Pure internal stream (no external I/O)

INSERT INTO EnrichedOrders
SELECT
    orderId,
    customerName,
    amount,
    CASE WHEN amount > 5000 THEN 'HIGH' ELSE 'MEDIUM' END as priority,
    timestamp as enrichedAt
FROM HighValueOrders;

-- Sink 1: Send to HTTP webhook
CREATE STREAM HttpAlerts (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    priority STRING,
    enrichedAt BIGINT
) WITH (
    'type' = 'sink',
    'extension' = 'http',
    'format' = 'json'
);

INSERT INTO HttpAlerts
SELECT * FROM EnrichedOrders;

-- Sink 2: Send to Kafka analytics topic
CREATE STREAM KafkaAnalytics (
    orderId STRING,
    customerName STRING,
    amount DOUBLE,
    priority STRING,
    enrichedAt BIGINT
) WITH (
    'type' = 'sink',
    'extension' = 'kafka',
    'format' = 'json'
);

INSERT INTO KafkaAnalytics
SELECT * FROM EnrichedOrders;
```

**config-prod.toml:**
```toml
[application]
name = "OrderProcessing"
buffer_size = 8192

# Global error handling defaults
[application.error]
strategy = "drop"
log-level = "warn"

# Global Kafka configuration (inherited by all Kafka streams)
[application.kafka]
brokers = "${KAFKA_BROKERS}"
security.protocol = "SASL_SSL"
security.username = "${KAFKA_USER}"
security.password = "${KAFKA_PASSWORD}"
timeout = "30s"
consumer.group = "order-processing"

# Note: All stream types, extensions, and formats defined in app.sql
# TOML provides only operational configuration

# Source: Orders from Kafka
[streams.Orders.json]
ignore-parse-errors = true
date-format = "yyyy-MM-dd'T'HH:mm:ss"
mapping.orderId = "$.order.id"
mapping.customerName = "$.order.customer.name"
mapping.amount = "$.order.total"
mapping.timestamp = "$.order.createdAt"

[streams.Orders.kafka]
# Inherits: brokers, security.*, timeout, consumer.group from [application.kafka]
# Override only what's specific to this stream
topic = "orders"
consumer.group = "orders-consumer"  # Override global consumer group

[streams.Orders.error]
# Critical data - retry with DLQ
strategy = "retry"
retry.max-attempts = 3
retry.backoff = "exponential"
retry.initial-delay = "100ms"
retry.max-delay = "30s"
dlq.stream = "OrderErrors"

# Internal streams have NO configuration (passthrough)
# [streams.HighValueOrders] - not needed
# [streams.EnrichedOrders] - not needed

# Sink 1: HTTP webhook
[streams.HttpAlerts.json]
pretty-print = false
template = '''
{
  "eventType": "HIGH_VALUE_ORDER",
  "processedAt": "{{_timestamp}}",
  "priority": "{{priority}}",
  "data": {
    "orderId": "{{orderId}}",
    "customerName": "{{customerName}}",
    "amount": {{amount}},
    "enrichedAt": {{enrichedAt}}
  }
}
'''

[streams.HttpAlerts.http]
url = "${API_URL}/alerts"
method = "POST"
headers.Authorization = "Bearer ${API_TOKEN}"
headers.Content-Type = "application/json"
timeout = "30s"
retry.max = "3"

[streams.HttpAlerts.error]
# HTTP failures - retry then fail (don't lose alerts)
strategy = "retry"
retry.max-attempts = 5
retry.backoff = "exponential"

# Sink 2: Kafka analytics topic
[streams.KafkaAnalytics.kafka]
# Inherits: brokers, security.*, timeout from [application.kafka]
# Override only topic
topic = "analytics"
producer.acks = "all"
producer.compression = "snappy"

[streams.KafkaAnalytics.error]
# Analytics - best effort, drop on failure
strategy = "drop"
log-level = "info"

# DLQ stream for Orders (if needed for manual review)
# This would be configured only if DLQ is persisted
# [streams.OrderErrorsSink]
# type = "sink"
# extension = "kafka"
# format = "json"
# ...
```

**Environment Variables**:
```bash
# Kafka
export KAFKA_BROKERS="prod1:9092,prod2:9092,prod3:9092"
export KAFKA_USER="eventflux-prod"
export KAFKA_PASSWORD="<secret>"

# HTTP
export API_URL="https://api.production.com"
export API_TOKEN="<secret>"
```

**CLI Usage**:
```bash
eventflux run app.sql --config config-prod.toml
```

**Configuration Flow**:
```
Orders stream effective config:
  ✅ brokers: "prod1:9092,..." [from application.kafka]
  ✅ security.*: SASL_SSL       [from application.kafka]
  ✅ timeout: "30s"             [from application.kafka]
  ✅ topic: "orders"            [from streams.Orders.kafka]
  ✅ consumer.group: "orders-consumer" [from streams.Orders.kafka - overrides application]

KafkaAnalytics stream effective config:
  ✅ brokers: "prod1:9092,..." [from application.kafka]
  ✅ security.*: SASL_SSL       [from application.kafka]
  ✅ timeout: "30s"             [from application.kafka]
  ✅ consumer.group: "order-processing" [from application.kafka - inherited]
  ✅ topic: "analytics"         [from streams.KafkaAnalytics.kafka]
  ✅ producer.*: ...            [from streams.KafkaAnalytics.kafka]
```

---

## Configuration System Design Summary

**Core Design Decisions**:

1. **Stream I/O Model**: All streams are inherently internal (query-able); `type='source'/'sink'` extends with external I/O
2. **Type Definition**: Type is OPTIONAL (omit for pure internal); required in SQL WITH when specified
3. **Format Rules**: Required when type specified, omitted for pure internal streams
4. **Passthrough vs Binary**: Passthrough (no format) passes Event objects through junction with zero serialization overhead
5. **Merge Semantics**: Per-property merge with inheritance; arrays replaced (not concatenated)
6. **Error Handling**: Default `drop` strategy with retry/DLQ/fail options
7. **DLQ Creation**: Manual creation required, can be internal OR sink, exact schema enforced against SQL definition
8. **DLQ Fallback**: Default `log` strategy with fail/retry options for runtime DLQ failures
9. **Validation Timing**: Parse (syntax) → Init (fail-fast) → Runtime (retry)
10. **Template System**: Simple `{{field}}` replacement (no complex logic)
11. **Factory Pattern**: Single-phase construction via `create_initialized()`; factories return ready-to-use instances
12. **Mapper Initialization**: Mappers created fully initialized via factory `create_initialized()`; no separate configuration step
13. **Initialization Order**: Topological sort with DLQ dependencies; fail if DLQ stream not found
14. **Type Safety**: Extension-specific typed configs (internal to factories); validation during instance creation
15. **Handler Pattern**: SourceStreamHandler, SinkStreamHandler, and table handlers manage lifecycle
16. **Auto-Attach**: Sources, sinks, and tables automatically configured from TOML at runtime start
17. **Auto-Mapping**: Top-level fields only; all-or-nothing policy (either all auto-mapped OR all explicit, no partial)
18. **Environment Variables**: Eager loading at config resolution (TOML load time); cannot change during runtime
19. **Circular Dependencies**: SQL-level detection at parse-time
20. **CLI Output**: YAML format; commands require both app.sql and --config for resolution
21. **Table Configuration**: Separate `[tables.*]` TOML syntax for clarity; same merge behavior as streams
