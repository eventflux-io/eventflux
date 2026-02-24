# Distributed Processing

**Last Updated**: 2025-10-02
**Implementation Status**: Foundation Complete, Extensions In Progress
**Related Code**: `src/core/distributed/`, `src/core/persistence/`

---

## Overview

EventFlux Rust's distributed processing enables horizontal scaling from single-node deployments to 10+ node clusters with:
- **Zero-overhead single-node mode** (default, 1.46M events/sec)
- **Progressive enhancement** to distributed via configuration
- **Linear scaling** targeting 85-90% efficiency
- **Automatic failover** with <5 second recovery

**Design Philosophy**: Single-node first, distributed when needed, same binary for both modes.

---

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────┐
│                 User Application                     │
├─────────────────────────────────────────────────────┤
│            EventFluxAppRuntime (Unified API)            │
├──────────────────┬──────────────────────────────────┤
│  Single-Node     │       Distributed Mode           │
│     Mode         │                                  │
│                  │  ┌────────────────────────────┐  │
│  Local Event     │  │ Distributed Coordinator    │  │
│  Processing      │  │ • Query Distribution       │  │
│                  │  │ • State Partitioning       │  │
│  In-Memory       │  │ • Load Balancing           │  │
│  State           │  │ • Health Monitoring        │  │
├──────────────────┴──┴────────────────────────────────┤
│              Extension Points Layer                  │
│  ┌──────────┐ ┌──────────┐ ┌────────────┐          │
│  │Transport │ │  State   │ │Coordination│          │
│  │ TCP/gRPC │ │  Redis   │ │    Raft    │          │
│  └──────────┘ └──────────┘ └────────────┘          │
└─────────────────────────────────────────────────────┘
```

### Runtime Modes

**SingleNode** (Default):
```yaml
# No configuration needed - zero overhead
```

**Distributed**:
```yaml
eventflux:
  distributed:
    node_id: "node-1"
    cluster:
      seed_nodes: ["node-0:8080", "node-1:8080"]
    transport: grpc
    state_backend: redis
```

**Hybrid**: Distributed coordination, local processing (future).

---

## Implementation Status

### ✅ Completed

#### Core Framework
- **Runtime Mode Abstraction**: `SingleNode`, `Distributed`, `Hybrid`
- **Processing Engine**: Unified execution abstraction
- **Distributed Runtime**: Wrapper maintaining API compatibility
- **Extension Points**: Trait-based abstractions ready

#### Transport Layer (Production Ready)
- **TCP Transport**: Async Tokio-based, connection pooling
  - Native Rust implementation
  - Configurable timeouts and buffers
  - Binary serialization with bincode
  - **Tests**: 4/4 passing
- **gRPC Transport**: Tonic with Protocol Buffers
  - HTTP/2 multiplexing
  - TLS/mTLS support
  - Built-in compression (LZ4, Snappy, Zstd)
  - **Tests**: 7/7 passing

#### State Backend (Production Ready)
- **Redis Backend**: Enterprise-grade implementation
  - Connection pooling with deadpool-redis
  - Automatic failover and error recovery
  - Complete RedisPersistenceStore integration
  - **Tests**: 15/15 passing
  - **Docker Setup**: Development environment ready
  - **Status**: Production-ready for window filtering without aggregations

### 🔄 In Progress

#### Coordination Service
- **Raft Framework**: Leader election structure ready
- **Consensus Protocol**: Implementation in progress
- **Cluster Membership**: Dynamic join/leave planned

#### Message Broker
- **Kafka Integration**: Trait defined, implementation pending
- **Event Distribution**: Partitioning strategies designed

#### Query Distribution
- **Load Balancing**: Algorithms designed
- **State Sharding**: Horizontal distribution planned

### ❌ Known Limitations

#### Redis State Backend
- ✅ **Works**: Basic window filtering, simple projections
- ❌ **Doesn't Work**: Aggregations with Group By
- **Root Cause**: Aggregator executors within groups not restored
- **Infrastructure Complete**: ThreadBarrier coordination, SelectProcessor StateHolder
- **Future**: Requires per-group aggregator executor state restoration

---

## Configuration

### YAML Structure

```yaml
apiVersion: eventflux.io/v1
kind: EventFluxConfig

eventflux:
  distributed:
    # Node Configuration
    node:
      node_id: "node-${HOSTNAME}"
      endpoints: ["${NODE_IP}:8080"]
      capabilities:
        can_coordinate: true
        can_process: true
        can_store_state: true

    # Cluster Configuration
    cluster:
      cluster_name: "eventflux-cluster"
      seed_nodes:
        - "node-0.eventflux.svc.cluster.local:8080"
        - "node-1.eventflux.svc.cluster.local:8080"
      min_nodes: 2
      heartbeat_interval: "1s"
      failure_timeout: "10s"

    # Transport Configuration
    transport:
      implementation: grpc  # or tcp
      pool_size: 10
      request_timeout: "30s"
      compression: true
      tls:
        enabled: true
        cert_path: "/etc/eventflux/certs/server.crt"
        key_path: "/etc/eventflux/certs/server.key"

    # State Backend Configuration
    state_backend:
      implementation: redis
      endpoints:
        - "redis-cluster-0:6379"
        - "redis-cluster-1:6379"
        - "redis-cluster-2:6379"
      pool_size: 20
      checkpoint_interval: "60s"
      state_ttl: "24h"
      compression: zstd

    # Coordination Configuration
    coordination:
      implementation: raft
      election_timeout: "5s"
      consensus_level: majority
```

### Configuration Sources

**Precedence** (highest to lowest):
1. CLI arguments
2. Environment variables
3. Kubernetes Secrets
4. Kubernetes ConfigMaps
5. YAML configuration file
6. Defaults

**Example - Kubernetes ConfigMap**:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: eventflux-config
  namespace: production
data:
  eventflux.yaml: |
    eventflux:
      distributed:
        cluster:
          cluster_name: "prod-cluster"
```

**Example - Environment Variables**:
```bash
export EVENTFLUX_NODE_ID="node-1"
export EVENTFLUX_REDIS_ENDPOINTS="redis:6379"
export EVENTFLUX_TRANSPORT="grpc"
```

### Secrets Management

**External Secret Stores** (recommended for production):

```yaml
# Reference secrets from Vault
eventflux:
  distributed:
    state_backend:
      endpoints: "vault://secret/redis/endpoints"
      password: "vault://secret/redis/password"
```

**Kubernetes Secrets**:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: eventflux-redis-credentials
type: Opaque
stringData:
  password: "super-secret-password"
```

Reference in pod:
```yaml
env:
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef:
        name: eventflux-redis-credentials
        key: password
```

---

## Docker Setup

### Quick Start

```bash
# Start Redis cluster for testing
docker compose up -d

# Run example
cargo run --example redis_state_example

# Run tests
cargo test distributed_redis_state
```

### Services

**Redis** (`eventflux-redis`):
- Port: 6379
- Persistent storage with AOF
- Memory limit: 256MB (LRU eviction)
- Health checks enabled

**Redis Commander** (`eventflux-redis-commander`):
- Port: 8081
- Web UI: http://localhost:8081
- Redis management and debugging

### Docker Compose

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    container_name: eventflux-redis
    ports:
      - "6379:6379"
    command: >
      redis-server
      --appendonly yes
      --maxmemory 256mb
      --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    volumes:
      - redis-data:/data
    networks:
      - eventflux-network

  redis-commander:
    image: rediscommander/redis-commander:latest
    container_name: eventflux-redis-commander
    ports:
      - "8081:8081"
    environment:
      - REDIS_HOSTS=local:redis:6379
    networks:
      - eventflux-network
    depends_on:
      - redis

volumes:
  redis-data:

networks:
  eventflux-network:
    driver: bridge
```

### Useful Commands

```bash
# Docker management
docker compose up -d          # Start services
docker compose down           # Stop services
docker compose down -v        # Remove everything including data
docker logs eventflux-redis      # View Redis logs

# Redis operations
docker exec -it eventflux-redis redis-cli           # Connect to CLI
docker exec eventflux-redis redis-cli keys '*'      # List all keys
docker exec eventflux-redis redis-cli monitor       # Monitor activity
docker exec eventflux-redis redis-cli info          # Get Redis info

# Cleanup
docker exec eventflux-redis redis-cli FLUSHALL     # Clear all data
```

---

## Kubernetes Deployment

### StatefulSet (Recommended)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: eventflux
  namespace: production
spec:
  serviceName: "eventflux"
  replicas: 3
  selector:
    matchLabels:
      app: eventflux
  template:
    metadata:
      labels:
        app: eventflux
    spec:
      containers:
      - name: eventflux
        image: eventflux-io/eventflux:latest
        ports:
        - containerPort: 8080
          name: transport
        env:
        - name: NODE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: config
          mountPath: /etc/eventflux
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
          limits:
            memory: "4Gi"
            cpu: "4"
      volumes:
      - name: config
        configMap:
          name: eventflux-config
---
apiVersion: v1
kind: Service
metadata:
  name: eventflux
  namespace: production
spec:
  clusterIP: None  # Headless service for StatefulSet
  selector:
    app: eventflux
  ports:
  - port: 8080
    name: transport
```

### Service Discovery

StatefulSet provides predictable DNS names:
```
eventflux-0.eventflux.production.svc.cluster.local
eventflux-1.eventflux.production.svc.cluster.local
eventflux-2.eventflux.production.svc.cluster.local
```

Use these in `seed_nodes` configuration.

---

## Usage Examples

### Single-Node (Default)

```rust
use eventflux::EventFluxAppRuntimeBuilder;

let app = "
    CREATE STREAM InputStream (id STRING, value DOUBLE);

    INSERT INTO OutputStream
    SELECT *
    FROM InputStream
    WHERE value > 100;";

let runtime = EventFluxAppRuntimeBuilder::new(app)
    .build()?;

runtime.start();
// Automatic single-node mode - zero overhead
```

### Distributed Mode

```rust
use eventflux::distributed::*;

let config = DistributedConfig {
    mode: RuntimeMode::Distributed,
    cluster: ClusterConfig {
        cluster_name: "prod-cluster".to_string(),
        seed_nodes: vec![
            "node-0:8080".to_string(),
            "node-1:8080".to_string(),
        ],
        min_nodes: 2,
    },
    transport: TransportConfig {
        implementation: TransportType::GRPC,
        pool_size: 10,
        compression: true,
        tls: Some(TlsConfig { /* ... */ }),
    },
    state_backend: StateBackendConfig {
        implementation: StateBackendType::Redis(RedisConfig {
            endpoints: vec!["redis-cluster:6379".to_string()],
            pool_size: 20,
            compression: CompressionType::Zstd,
        }),
    },
};

let runtime = DistributedRuntimeBuilder::new(app)
    .with_config(config)
    .build()?;

runtime.start();
// Distributed mode with automatic state management
```

### Redis State Persistence

```rust
use eventflux::persistence::RedisPersistenceStore;

// Create Redis backend
let redis_store = RedisPersistenceStore::new(RedisConfig {
    url: "redis://localhost:6379".to_string(),
    pool_size: 10,
    ..Default::default()
})?;

// Build runtime with persistence
let runtime = EventFluxAppRuntimeBuilder::new(app)
    .with_persistence_store(Box::new(redis_store))
    .build()?;

runtime.start();

// Periodically save state
runtime.persist_state()?;

// Restore after restart
runtime.restore_last_revision()?;
```

---

## Performance Targets

| Metric | Single-Node | 3-Node Cluster | 10-Node Cluster |
|--------|-------------|----------------|-----------------|
| **Throughput** | 1.46M events/sec | 4M events/sec | 12M events/sec |
| **Latency (p99)** | <1ms | <5ms | <10ms |
| **Failover Time** | N/A | <5 seconds | <5 seconds |
| **State Recovery** | <30s | <60s | <120s |
| **Scaling Efficiency** | 100% | 90% | 85% |

---

## Troubleshooting

### Common Issues

**Issue**: Connection refused to seed nodes
```bash
# Check network connectivity
kubectl exec -it eventflux-0 -- nc -zv eventflux-1.eventflux.svc.cluster.local 8080

# Verify DNS resolution
kubectl exec -it eventflux-0 -- nslookup eventflux-1.eventflux.svc.cluster.local
```

**Issue**: Redis connection failures
```bash
# Test Redis connectivity
docker exec eventflux-redis redis-cli ping

# Check Redis logs
docker logs eventflux-redis

# Verify network
docker network inspect eventflux-network
```

**Issue**: State not persisting
```rust
// Enable debug logging
env_logger::Builder::from_default_env()
    .filter_level(log::LevelFilter::Debug)
    .init();

// Check StateHolder implementations
runtime.persist_state()
    .map_err(|e| eprintln!("Persistence failed: {}", e))?;
```

**Issue**: Aggregation state restoration fails
- **Known Limitation**: Group By aggregations not supported yet
- **Workaround**: Use simple windows without aggregations
- **Tracking**: Per-group aggregator executor state restoration (future enhancement)

### Debug Checklist

1. **Configuration**:
   ```bash
   # Verify config loaded correctly
   kubectl describe configmap eventflux-config
   kubectl get secret eventflux-redis-credentials -o yaml
   ```

2. **Network**:
   ```bash
   # Check pod IPs
   kubectl get pods -o wide

   # Test connectivity
   kubectl exec -it eventflux-0 -- ping eventflux-1
   ```

3. **State Backend**:
   ```bash
   # Verify Redis keys
   docker exec eventflux-redis redis-cli keys '*'

   # Check key content
   docker exec eventflux-redis redis-cli get "eventflux:state:my_window"
   ```

4. **Logs**:
   ```bash
   # Application logs
   kubectl logs eventflux-0 --tail=100 -f

   # Previous crash logs
   kubectl logs eventflux-0 --previous
   ```

---

## Testing

### Unit Tests

```bash
# All distributed tests
cargo test distributed

# Transport tests
cargo test transport

# State backend tests
cargo test redis_state

# Specific test
cargo test test_redis_persistence_basic -- --nocapture
```

### Integration Tests

```bash
# Start dependencies
docker compose up -d

# Run integration tests
cargo test --test distributed_integration

# Cleanup
docker compose down -v
```

### Load Testing

```rust
// Benchmark distributed throughput
use criterion::Criterion;

fn bench_distributed_throughput(c: &mut Criterion) {
    let runtime = create_distributed_runtime();

    c.bench_function("distributed_1M_events", |b| {
        b.iter(|| {
            send_million_events(&runtime);
        });
    });
}
```

---

## Next Steps

See [MILESTONES.md](../../MILESTONES.md) **Milestone 7: Distributed Processing (v0.7)** for:
- Raft coordinator completion
- Kafka message broker integration
- Query distribution algorithms
- Integration testing
- Production deployment guides

---

## Contributing

When working on distributed features:

1. **Maintain Zero Overhead**: Single-node mode must not regress
2. **Test Both Modes**: Ensure single-node and distributed work
3. **Document Configuration**: Add to YAML structure
4. **Integration Tests**: Write end-to-end distributed scenarios
5. **Performance Benchmarks**: Validate scaling efficiency

**Code Structure**:
```
src/core/distributed/
├── mod.rs                    # Core module
├── runtime_mode.rs           # Mode selection
├── processing_engine.rs      # Engine abstraction
├── distributed_runtime.rs    # Distributed wrapper
├── transport.rs              # TCP transport
├── grpc/                     # gRPC transport
├── state_backend.rs          # Redis backend
├── coordinator.rs            # Raft coordination (WIP)
└── message_broker.rs         # Message broker (WIP)
```

---

**Status**: Foundation complete, extensions in active development
