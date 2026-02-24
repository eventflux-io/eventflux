# Redis State Backend

EventFlux provides enterprise-grade Redis-based state persistence that integrates with EventFlux's native persistence system.

## Features

- Enterprise Connection Management: Connection pooling with deadpool-redis
- Automatic Failover: Graceful error recovery and connection retry
- PersistenceStore Integration: Implements EventFlux's `PersistenceStore` trait
- ThreadBarrier Coordination: Race-condition-free state restoration

## Setup

### Start Redis

```bash
# Docker Compose
cd eventflux
docker compose up -d

# Or locally
brew install redis  # macOS
redis-server
```

### Configure Backend

```rust
use eventflux::core::persistence::RedisPersistenceStore;
use eventflux::core::distributed::RedisConfig;

let config = RedisConfig {
    url: "redis://localhost:6379".to_string(),
    max_connections: 10,
    connection_timeout_ms: 5000,
    key_prefix: "eventflux:".to_string(),
    ttl_seconds: Some(3600),
};

let store = RedisPersistenceStore::new_with_config(config)?;
manager.set_persistence_store(Arc::new(store));
```

### Use with Persistence

```rust
let runtime = manager.create_eventflux_app_runtime(app)?;

// Persist state
let revision = runtime.persist()?;

// Restore from checkpoint
runtime.restore_revision(&revision)?;
```

## Configuration Options

| Parameter               | Description               | Default                  |
|-------------------------|---------------------------|--------------------------|
| `url`                   | Redis connection URL      | `redis://localhost:6379` |
| `max_connections`       | Connection pool size      | `10`                     |
| `connection_timeout_ms` | Connection timeout        | `5000`                   |
| `key_prefix`            | Redis key namespace       | `eventflux:`             |
| `ttl_seconds`           | Key expiration (optional) | `None`                   |

## Features

- Connection Pooling: Efficient resource management with deadpool-redis
- Health Monitoring: Built-in connection health checks
- Error Recovery: Automatic retry with exponential backoff
- Memory Efficiency: Optimized serialization with optional compression
- Cluster Support: Compatible with Redis Cluster

## Integration

The Redis backend integrates with:
- SnapshotService: Automatic state persistence and restoration
- StateHolders: All window and aggregation state persisted
- ThreadBarrier: Coordinated state restoration
- Incremental Checkpointing: Compatible with advanced checkpointing

## Testing

Tests skip if Redis is not available:

```bash
# Run Redis tests
cargo test redis_persistence
cargo test redis_backend
cargo test test_redis_eventflux_persistence

# With Docker
docker compose up -d
cargo test redis
```

GitHub Actions includes Redis service container for CI.

## ThreadBarrier Coordination

EventFlux implements Java EventFlux's ThreadBarrier pattern for coordinating state restoration with concurrent event processing.

### How It Works

1. Event Processing: All event processing threads enter the ThreadBarrier before processing
2. State Restoration: During restoration, the barrier locks to prevent new events
3. Coordination: Active threads complete current processing before restoration begins
4. Synchronization: State is restored while event processing is blocked
5. Resume: After restoration, the barrier unlocks and processing resumes

### Implementation

```rust
// Automatic ThreadBarrier initialization
let thread_barrier = Arc::new(ThreadBarrier::new());
ctx.set_thread_barrier(thread_barrier);

// Event processing coordination
if let Some(barrier) = self.eventflux_app_context.get_thread_barrier() {
    barrier.enter();
    // Process events...
    barrier.exit();
}

// State restoration coordination
if let Some(barrier) = self.eventflux_app_context.get_thread_barrier() {
    barrier.lock();
    // Wait for active threads...
    service.restore_revision(revision)?;
    barrier.unlock();
}
```

This ensures aggregation state restoration is atomic and thread-safe.

## State Backend Options

### In-Memory Backend (Default)

For single-node or testing:

```rust
use eventflux::core::distributed::state_backend::InMemoryBackend;

let backend = InMemoryBackend::new();
```

Features:
- Zero external dependencies
- High performance
- Automatic cleanup on shutdown

### Redis Backend (Production)

For distributed deployments:

```rust
use eventflux::core::distributed::state_backend::{RedisBackend, RedisConfig};

let mut backend = RedisBackend::new();
backend.initialize().await?;

// Operations
backend.set("key1", b"value1".to_vec()).await?;
let value = backend.get("key1").await?;

// Checkpoint
backend.checkpoint("checkpoint_1").await?;
backend.restore("checkpoint_1").await?;

backend.shutdown().await?;
```

### Distributed Configuration

```rust
use eventflux::core::distributed::{
    DistributedConfig, StateBackendConfig, StateBackendImplementation
};

let config = DistributedConfig {
    state_backend: StateBackendConfig {
        implementation: StateBackendImplementation::Redis {
            endpoints: vec!["redis://node1:6379".to_string()]
        },
        checkpoint_interval: Duration::from_secs(60),
        state_ttl: Some(Duration::from_secs(7200)),
        incremental_checkpoints: true,
        compression: CompressionType::Zstd,
    },
    ..Default::default()
};
```

## Performance

- Latency: 1-5ms for local Redis, 10-50ms for network Redis
- Throughput: 10K-100K operations/second
- Memory: Efficient binary serialization
- Scaling: Linear with Redis cluster size

## Future Backends

The architecture supports additional backends:
- Apache Ignite: In-memory data grid
- Hazelcast: Distributed caching
- RocksDB: Embedded high-performance storage
- Cloud Storage: AWS DynamoDB, Google Cloud Datastore

## Status

Production Ready:
- Basic window filtering with persistence and restoration
- Enterprise connection management and error handling
- Complete PersistenceStore trait implementation
- ThreadBarrier coordination

In Development:
- Group By aggregation state persistence
- Complex window combinations with aggregations

See [REDIS_PERSISTENCE_STATUS.md](../../REDIS_PERSISTENCE_STATUS.md) for detailed status.
