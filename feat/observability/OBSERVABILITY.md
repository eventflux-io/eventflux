# EventFlux Observability & Monitoring System

**Feature Status**: 🟠 **PARTIALLY IMPLEMENTED** - Foundation exists, enterprise features needed
**Priority**: 🟠 **HIGH** - Critical for production deployments
**Target Milestone**: M6 (Production Hardening)
**Estimated Effort**: 4-6 weeks
**Last Updated**: 2025-10-11

---

## Executive Summary

EventFlux Rust currently has **basic monitoring** with crossbeam pipeline metrics but lacks the **enterprise-grade observability** required for production deployments. This document outlines the comprehensive monitoring, metrics, tracing, and logging infrastructure needed to operate EventFlux at scale.

**Current Status**: Basic pipeline metrics (✅) + Global counters (⚠️)
**Production Gap**: No Prometheus integration, no distributed tracing, no operational dashboards
**Business Impact**: **Cannot operate in production** without visibility into system health and performance

---

## Table of Contents

1. [Current Status](#current-status)
2. [Critical Gaps](#critical-gaps)
3. [What's Implemented](#whats-implemented)
4. [Architecture & Design](#architecture--design)
5. [Observability Stack](#observability-stack)
6. [Implementation Plan](#implementation-plan)
7. [Metrics Specification](#metrics-specification)
8. [Tracing Strategy](#tracing-strategy)
9. [Logging Standards](#logging-standards)
10. [Operational Dashboards](#operational-dashboards)
11. [Testing Strategy](#testing-strategy)
12. [Future Enhancements](#future-enhancements)

---

## Current Status

### Implemented Components ✅

| Component | Status | Coverage | Notes |
|-----------|--------|----------|-------|
| **Pipeline Metrics** | ✅ Complete | 100% | Crossbeam pipeline performance tracking |
| **Basic Counters** | ⚠️ Partial | ~30% | Global event counters in some components |
| **Logging** | ⚠️ Basic | ~20% | println! and basic log crate usage |
| **Health Checks** | ❌ Missing | 0% | No health check endpoints |
| **Prometheus** | ❌ Missing | 0% | No metrics exporter |
| **OpenTelemetry** | ❌ Missing | 0% | No distributed tracing |
| **Dashboards** | ❌ Missing | 0% | No pre-built monitoring dashboards |

### Enterprise Observability Gaps

**🔴 CRITICAL GAPS**:
1. **No Prometheus Integration** - Cannot export metrics to monitoring systems
2. **No Distributed Tracing** - Cannot trace events across distributed deployments
3. **No Health Check API** - Cannot integrate with load balancers and orchestrators
4. **No Operational Dashboards** - No visibility into system behavior

**🟠 HIGH PRIORITY GAPS**:
5. **Limited Logging** - Inconsistent log levels and structured logging
6. **No Query-Level Metrics** - Cannot track individual query performance
7. **No Stream-Level Metrics** - Limited visibility into stream throughput
8. **No Alerting Rules** - Cannot proactively detect issues

---

## Critical Gaps

### Gap 1: Prometheus Metrics Integration 🔴

**Problem**: No way to export metrics to industry-standard monitoring systems

**Impact**:
- Cannot integrate with Prometheus, Grafana, Datadog, or other monitoring platforms
- No historical metrics or trend analysis
- Cannot set up alerts or operational thresholds
- Blind to production system behavior

**Example Missing Capability**:
```rust
// MISSING: Prometheus exporter endpoint
// Should expose: http://localhost:9090/metrics

// MISSING: Query-level metrics
query_throughput_total{query_id="stock_aggregation"} 1234567
query_latency_seconds{query_id="stock_aggregation",quantile="0.99"} 0.0015
query_errors_total{query_id="stock_aggregation"} 0

// MISSING: Stream-level metrics
stream_events_total{stream="StockStream"} 9876543
stream_backpressure_events_total{stream="StockStream"} 123
```

**Current Workaround**: Manual log parsing and custom scripts (not production viable)

---

### Gap 2: OpenTelemetry Distributed Tracing 🔴

**Problem**: No distributed tracing for event flow and query execution

**Impact**:
- Cannot trace events through complex processing pipelines
- No visibility into distributed query execution across nodes
- Cannot identify performance bottlenecks in multi-stage queries
- Difficult to debug production issues

**Example Missing Capability**:
```rust
// MISSING: Distributed tracing spans
#[tracing::instrument(skip(event))]
async fn process_event(event: StreamEvent) {
    // Trace event through:
    // 1. Stream ingestion
    // 2. Filter processing
    // 3. Window aggregation
    // 4. Join operation
    // 5. Output emission
}

// MISSING: Cross-service trace propagation
// Should propagate trace context across gRPC/TCP boundaries
```

**Current Workaround**: None - completely blind to distributed execution

---

### Gap 3: Health Check & Readiness API 🔴

**Problem**: No standard health check endpoints for orchestration systems

**Impact**:
- Cannot integrate with Kubernetes liveness/readiness probes
- Cannot use with load balancers (HAProxy, NGINX)
- Cannot implement graceful shutdown in container environments
- Manual health monitoring only

**Example Missing Capability**:
```rust
// MISSING: Health check endpoints
GET /health       -> 200 OK if healthy, 503 if unhealthy
GET /ready        -> 200 OK if ready to serve traffic
GET /metrics      -> Prometheus metrics format

// MISSING: Structured health response
{
  "status": "healthy",
  "uptime_seconds": 3600,
  "queries_running": 5,
  "streams_active": 10,
  "memory_usage_mb": 256,
  "checks": {
    "event_pipeline": "ok",
    "state_backend": "ok",
    "query_runtime": "ok"
  }
}
```

---

## What's Implemented

### 1. Crossbeam Pipeline Metrics ✅

**Location**: `src/core/util/pipeline/metrics.rs`

**Features**:
- Real-time performance metrics (throughput, latency, utilization)
- Producer/consumer coordination tracking
- Queue efficiency and health scoring
- Historical trend analysis (1 min, 5 min, 15 min)

**Example Usage**:
```rust
let metrics = pipeline.get_metrics();
println!("Throughput: {} events/sec", metrics.throughput_per_sec);
println!("Latency p99: {} ms", metrics.latency_ms_p99);
println!("Health Score: {}", metrics.health_score);
```

**Limitation**: Metrics are not exported to external monitoring systems

### 2. Basic Global Counters ⚠️

**Scattered Implementation**: Various components have basic counters

**Examples**:
```rust
// Window processors track event counts
let events_processed = window.get_event_count();

// Aggregators track computation counts
let aggregations_computed = aggregator.get_computation_count();
```

**Limitation**: Inconsistent implementation, no unified metrics framework

---

## Architecture & Design

### Target Architecture: Three-Tier Observability

```
┌─────────────────────────────────────────────────────────┐
│                    EventFlux Runtime                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Queries   │  │   Streams   │  │  Processors │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
│         │                 │                 │            │
│         └─────────────────┴─────────────────┘            │
│                           │                              │
│                 ┌─────────▼─────────┐                   │
│                 │  MetricsCollector  │                   │
│                 │  (internal layer)  │                   │
│                 └─────────┬─────────┘                   │
└───────────────────────────┼──────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
    ┌─────▼──────┐   ┌─────▼──────┐   ┌─────▼──────┐
    │ Prometheus │   │OpenTelemetry│   │  Logging  │
    │  Exporter  │   │   Tracing   │   │  Framework │
    └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
          │                 │                 │
    ┌─────▼──────┐   ┌─────▼──────┐   ┌─────▼──────┐
    │  Grafana   │   │   Jaeger    │   │    ELK     │
    │ Dashboards │   │     UI      │   │   Stack    │
    └────────────┘   └─────────────┘   └────────────┘
```

### Design Principles

1. **Low Overhead** - <1% CPU overhead from metrics collection
2. **Non-Blocking** - Metrics collection never blocks event processing
3. **Composable** - Multiple exporters can be enabled simultaneously
4. **Standard Formats** - Prometheus, OpenTelemetry, JSON formats
5. **Hot-Path Optimization** - Atomic counters for critical metrics
6. **Cardinality Control** - Limit high-cardinality labels to prevent explosion

---

## Observability Stack

### Tier 1: Metrics Collection (Prometheus)

**Purpose**: Time-series metrics for performance monitoring and alerting

**Key Metrics Categories**:
1. **System Metrics** - CPU, memory, threads, file descriptors
2. **Query Metrics** - Throughput, latency, errors per query
3. **Stream Metrics** - Event rates, backpressure, buffer utilization
4. **Processor Metrics** - Window state, aggregation efficiency
5. **Network Metrics** - gRPC/TCP throughput, connection pool usage

**Implementation**:
```rust
use prometheus::{Encoder, Registry, Counter, Histogram, Gauge};

pub struct EventFluxMetrics {
    // System metrics
    events_processed_total: Counter,
    query_latency_seconds: Histogram,
    active_queries: Gauge,

    // Stream metrics
    stream_throughput: CounterVec,  // label: stream_name
    stream_backpressure: CounterVec,

    // Error metrics
    errors_total: CounterVec,  // label: error_type, query_id
}

// HTTP endpoint for Prometheus scraping
GET /metrics -> text/plain (Prometheus format)
```

### Tier 2: Distributed Tracing (OpenTelemetry)

**Purpose**: End-to-end event flow tracing across components and nodes

**Trace Spans**:
1. **Event Ingestion** - Source to stream junction
2. **Query Execution** - Per-query processing pipeline
3. **Window Processing** - Event accumulation and trigger
4. **Aggregation** - State updates and computation
5. **Network Communication** - Cross-node RPC calls

**Implementation**:
```rust
use opentelemetry::{trace::Tracer, global};
use tracing_subscriber::layer::SubscriberExt;

// Initialize tracer
let tracer = opentelemetry_jaeger::new_agent_pipeline()
    .with_service_name("eventflux")
    .install_simple()?;

// Instrument query execution
#[tracing::instrument(skip(query))]
async fn execute_query(query: &Query) {
    let span = tracer.start("query_execution");
    span.set_attribute("query.id", query.id.clone());

    // Trace event through pipeline
    process_events(query).await;

    span.end();
}
```

### Tier 3: Structured Logging (tracing + slog)

**Purpose**: Detailed diagnostic logs with context propagation

**Log Levels**:
- **ERROR**: System errors requiring immediate attention
- **WARN**: Degraded performance or approaching limits
- **INFO**: Important lifecycle events (query start/stop)
- **DEBUG**: Detailed execution flow for debugging
- **TRACE**: Fine-grained event-level logging

**Implementation**:
```rust
use tracing::{error, warn, info, debug, trace};

// Structured logging with context
info!(
    query_id = %query.id,
    stream_name = %stream.name,
    events_processed = events.len(),
    "Query executed successfully"
);

// Error logging with context
error!(
    query_id = %query.id,
    error = %err,
    "Query execution failed"
);
```

---

## Implementation Plan

### Phase 1: Prometheus Metrics Foundation (Week 1-2)

**Goals**: Export basic metrics to Prometheus

**Tasks**:
1. **Metrics Registry Setup**
   ```rust
   // src/core/metrics/registry.rs
   pub struct MetricsRegistry {
       registry: prometheus::Registry,
       // Core counters
       events_processed: Counter,
       queries_active: Gauge,
       errors_total: CounterVec,
   }
   ```

2. **HTTP Metrics Endpoint**
   ```rust
   // src/core/metrics/exporter.rs
   pub async fn metrics_handler() -> impl warp::Reply {
       let encoder = prometheus::TextEncoder::new();
       let metrics = gather_metrics();
       encoder.encode_to_string(&metrics).unwrap()
   }

   // Expose: GET /metrics
   ```

3. **Query-Level Metrics**
   ```rust
   // Instrument QueryRuntime
   struct QueryMetrics {
       events_in: Counter,
       events_out: Counter,
       latency: Histogram,
   }
   ```

4. **Stream-Level Metrics**
   ```rust
   // Instrument StreamJunction
   struct StreamMetrics {
       throughput: Counter,
       backpressure_events: Counter,
       subscribers_active: Gauge,
   }
   ```

**Deliverable**: Prometheus metrics at http://localhost:9090/metrics

### Phase 2: OpenTelemetry Tracing (Week 2-3)

**Goals**: Distributed tracing for event flow

**Tasks**:
1. **Tracer Initialization**
   ```rust
   // src/core/telemetry/tracer.rs
   pub fn init_tracer(service_name: &str) -> Result<Tracer> {
       opentelemetry_jaeger::new_agent_pipeline()
           .with_service_name(service_name)
           .install_simple()
   }
   ```

2. **Query Execution Spans**
   ```rust
   #[tracing::instrument(skip(query, event))]
   async fn process_event(query: &Query, event: StreamEvent) {
       // Automatic span creation with context
   }
   ```

3. **Cross-Node Trace Propagation**
   ```rust
   // Inject trace context into gRPC metadata
   fn inject_trace_context(metadata: &mut MetadataMap) {
       let span_context = current_span().context();
       // Serialize to W3C Trace Context format
   }
   ```

4. **Span Attributes**
   - Query ID, stream name, event count
   - Processor type, window duration
   - Error details, retry counts

**Deliverable**: Jaeger UI showing end-to-end traces

### Phase 3: Health Checks & Readiness (Week 3-4)

**Goals**: Standard health check endpoints

**Tasks**:
1. **Health Check Framework**
   ```rust
   // src/core/health/mod.rs
   pub trait HealthCheck: Send + Sync {
       async fn check(&self) -> HealthStatus;
   }

   pub struct HealthMonitor {
       checks: Vec<Box<dyn HealthCheck>>,
   }
   ```

2. **Component Health Checks**
   ```rust
   // Check event pipeline health
   impl HealthCheck for EventPipeline {
       async fn check(&self) -> HealthStatus {
           if self.get_metrics().health_score < 0.5 {
               HealthStatus::Degraded
           } else {
               HealthStatus::Healthy
           }
       }
   }
   ```

3. **HTTP Endpoints**
   ```rust
   GET /health  -> {"status": "healthy", ...}
   GET /ready   -> {"status": "ready", ...}
   GET /live    -> 200 OK (minimal liveness probe)
   ```

4. **Graceful Shutdown**
   ```rust
   // Transition to NOT READY before shutdown
   async fn shutdown(&mut self) {
       self.health_status.set(HealthStatus::ShuttingDown);
       sleep(Duration::from_secs(5)).await;  // Drain period
       self.stop_all_queries().await;
   }
   ```

**Deliverable**: Kubernetes-ready health check endpoints

### Phase 4: Operational Dashboards (Week 4-5)

**Goals**: Pre-built Grafana dashboards

**Tasks**:
1. **Dashboard Templates**
   - EventFlux Overview Dashboard
   - Query Performance Dashboard
   - Stream Monitoring Dashboard
   - System Resources Dashboard

2. **Alert Rules**
   ```yaml
   # alerts/eventflux_alerts.yml
   - alert: HighQueryLatency
     expr: query_latency_seconds{quantile="0.99"} > 0.1
     for: 5m
     labels:
       severity: warning
     annotations:
       summary: "Query {{ $labels.query_id }} has high latency"

   - alert: StreamBackpressure
     expr: rate(stream_backpressure_events_total[5m]) > 100
     for: 2m
     labels:
       severity: critical
   ```

3. **Dashboard JSON Export**
   - Import into Grafana with one click
   - Pre-configured panels and thresholds

**Deliverable**: Production-ready monitoring dashboards

### Phase 5: Structured Logging Enhancement (Week 5-6)

**Goals**: Enterprise logging standards

**Tasks**:
1. **Logging Framework**
   ```rust
   // Replace println! with tracing
   use tracing::{info, error, debug};

   // Structured fields
   info!(
       target: "eventflux::query",
       query.id = %query_id,
       duration_ms = duration.as_millis(),
       "Query completed"
   );
   ```

2. **Log Levels Configuration**
   ```toml
   # eventflux.toml
   [logging]
   level = "info"
   format = "json"  # or "pretty" for dev
   output = "stdout"  # or file path
   ```

3. **Context Propagation**
   ```rust
   // Attach query context to all logs within span
   let _guard = tracing::info_span!("query", id = %query.id).entered();
   // All logs within this scope include query.id
   ```

**Deliverable**: Production-grade structured logging

---

## Metrics Specification

### Query Metrics

```
# Query execution metrics
query_events_processed_total{query_id, stream_name}       - Counter
query_events_emitted_total{query_id, output_stream}       - Counter
query_latency_seconds{query_id, quantile}                 - Histogram
query_execution_errors_total{query_id, error_type}        - Counter
query_active_count                                        - Gauge

# Query state
query_window_size_bytes{query_id, window_type}            - Gauge
query_buffer_utilization{query_id}                        - Gauge
```

### Stream Metrics

```
# Stream throughput
stream_events_received_total{stream_name}                 - Counter
stream_events_sent_total{stream_name}                     - Counter
stream_subscribers_active{stream_name}                    - Gauge

# Stream health
stream_backpressure_events_total{stream_name}             - Counter
stream_buffer_size_bytes{stream_name}                     - Gauge
stream_oldest_event_age_seconds{stream_name}              - Gauge
```

### System Metrics

```
# Runtime metrics
eventflux_uptime_seconds                                  - Counter
eventflux_memory_usage_bytes{component}                   - Gauge
eventflux_cpu_usage_percent                               - Gauge
eventflux_threads_active                                  - Gauge

# Connection metrics
connections_active{protocol}                              - Gauge
connections_total{protocol, result}                       - Counter
connection_errors_total{protocol, error_type}             - Counter
```

---

## Tracing Strategy

### Span Hierarchy

```
Query Execution (root span)
├── Event Ingestion
│   ├── Source Read
│   └── Stream Junction Send
├── Filter Processing
│   └── Expression Evaluation
├── Window Processing
│   ├── Event Accumulation
│   └── Window Trigger
├── Aggregation Processing
│   ├── State Update
│   └── Computation
└── Output Emission
    ├── Sink Formatting
    └── Network Send
```

### Span Attributes

**Standard Attributes**:
- `service.name`: "eventflux"
- `service.version`: "0.2.0"
- `deployment.environment`: "production" | "staging" | "dev"

**EventFlux Attributes**:
- `query.id`: Unique query identifier
- `stream.name`: Source stream name
- `event.count`: Number of events processed
- `window.type`: Window type (tumbling, sliding, etc.)
- `processor.type`: Processor type (filter, aggregator, etc.)

---

## Logging Standards

### Log Format (JSON)

```json
{
  "timestamp": "2025-10-11T10:30:45.123Z",
  "level": "INFO",
  "target": "eventflux::query",
  "message": "Query executed successfully",
  "fields": {
    "query.id": "stock_aggregation",
    "stream.name": "StockStream",
    "events.processed": 1234,
    "duration.ms": 15
  },
  "span": {
    "trace_id": "abc123",
    "span_id": "def456"
  }
}
```

### Logging Guidelines

1. **Use Structured Logging** - Always use key-value fields
2. **Include Context** - Query ID, stream name in every log
3. **Appropriate Levels** - ERROR for failures, INFO for lifecycle
4. **No Secrets** - Never log credentials or sensitive data
5. **Performance** - Use DEBUG/TRACE sparingly in hot paths

---

## Operational Dashboards

### Dashboard 1: EventFlux Overview

**Panels**:
- System uptime and version
- Active queries count
- Total events processed/sec
- Memory and CPU usage
- Error rate (events/sec)

### Dashboard 2: Query Performance

**Panels**:
- Query throughput (per query)
- Query latency p50, p95, p99 (per query)
- Query error rate (per query)
- Query buffer utilization
- Query execution timeline

### Dashboard 3: Stream Monitoring

**Panels**:
- Stream throughput (per stream)
- Stream backpressure events
- Subscriber count (per stream)
- Buffer size trend
- Oldest event age

### Dashboard 4: System Resources

**Panels**:
- CPU usage over time
- Memory usage over time
- Thread pool utilization
- Disk I/O (if applicable)
- Network throughput

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        let registry = MetricsRegistry::new();
        assert!(registry.get_metric("events_processed_total").is_some());
    }

    #[test]
    fn test_health_check() {
        let pipeline = EventPipeline::new(1024);
        let health = pipeline.check_health();
        assert_eq!(health, HealthStatus::Healthy);
    }
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_prometheus_endpoint() {
    let app = start_test_app().await;

    let response = reqwest::get("http://localhost:9090/metrics").await.unwrap();
    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert!(body.contains("events_processed_total"));
}

#[tokio::test]
async fn test_distributed_tracing() {
    let tracer = init_test_tracer().await;

    // Create root span
    let span = tracer.start("test_query");

    // Process event (should create child spans)
    process_test_event().await;

    // Verify span hierarchy
    let spans = collect_spans();
    assert_eq!(spans.len(), 5);  // Root + 4 child spans
}
```

---

## Future Enhancements

### Phase 6: Advanced Analytics (M7+)

1. **Custom Metrics** - User-defined metrics from queries
   ```sql
   SELECT
       COUNT(*) as total_trades,
       AVG(price) as avg_price
   FROM StockStream
   EMIT METRICS;  -- Emit as custom metrics
   ```

2. **Adaptive Alerting** - ML-based anomaly detection
3. **Cost Attribution** - Track compute cost per query/tenant
4. **Capacity Planning** - Predictive resource recommendations

### Phase 7: Observability Extensions (M8+)

1. **APM Integration** - Datadog, New Relic native support
2. **Log Aggregation** - ELK/Loki integration
3. **Profiling** - Continuous profiling integration (pprof)
4. **Service Mesh** - Istio/Linkerd integration for K8s deployments

---

## Success Criteria

**Metrics**:
- ✅ Prometheus endpoint exposed at /metrics
- ✅ <1% CPU overhead from metrics collection
- ✅ 100+ metrics covering all components
- ✅ Health check endpoints working with K8s

**Tracing**:
- ✅ End-to-end traces for all queries
- ✅ <0.1ms overhead per span
- ✅ Cross-node trace propagation working
- ✅ Jaeger UI shows complete event flow

**Logging**:
- ✅ All components use structured logging
- ✅ JSON log format for production
- ✅ Context propagation across all logs
- ✅ Zero secrets in logs (validated)

**Dashboards**:
- ✅ 4 pre-built Grafana dashboards
- ✅ 10+ alert rules for common issues
- ✅ One-click import into Grafana
- ✅ Real-time system visibility

---

## Related Documentation

- **[ROADMAP.md](../../ROADMAP.md)** - Implementation priorities and timeline
- **[MILESTONES.md](../../MILESTONES.md)** - M6 Production Hardening details
- **[DISTRIBUTED_ARCHITECTURE_DESIGN.md](../../DISTRIBUTED_ARCHITECTURE_DESIGN.md)** - Distributed monitoring requirements
- **[IMPLEMENTATION_GUIDE.md](../../IMPLEMENTATION_GUIDE.md)** - Developer patterns for metrics

---

**Last Updated**: 2025-10-11
**Target Release**: M6 (Q3 2026)
**Status**: 🟠 Foundation exists, enterprise features in progress
