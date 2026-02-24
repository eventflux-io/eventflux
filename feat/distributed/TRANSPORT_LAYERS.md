# Transport Layers Architecture

EventFlux provides enterprise-grade distributed processing capabilities with multiple transport layer implementations.

## Available Transports

### TCP Transport

Simple, efficient binary protocol for low-latency communication.

Features:
- Connection pooling for efficient resource usage
- Configurable timeouts and buffer sizes
- TCP keepalive support
- Binary message serialization with bincode
- Support for 6 message types (Event, Query, State, Control, Heartbeat, Checkpoint)

Configuration:

```rust
use eventflux::core::distributed::transport::{TcpTransport, TcpTransportConfig};

let config = TcpTransportConfig {
    connection_timeout_ms: 5000,
    read_timeout_ms: 30000,
    write_timeout_ms: 30000,
    keepalive_enabled: true,
    keepalive_interval_secs: 30,
    nodelay: true,
    send_buffer_size: Some(65536),
    recv_buffer_size: Some(65536),
    max_message_size: 10 * 1024 * 1024,
};

let transport = TcpTransport::with_config(config);
```

### gRPC Transport

HTTP/2-based transport with Protocol Buffers for enterprise deployments.

Features:
- HTTP/2 multiplexing - multiple streams per connection
- Protocol Buffers for efficient, schema-evolution-friendly serialization
- Built-in compression (LZ4, Snappy, Zstd)
- TLS/mTLS support for secure communication
- Client-side load balancing
- Streaming support (unary and bidirectional)
- Health checks and heartbeat monitoring

Setup Requirements:

Install Protocol Buffer Compiler:

```bash
# macOS
brew install protobuf

# Ubuntu/Debian
apt-get install protobuf-compiler

# Verify
protoc --version
```

Configuration:

```rust
use eventflux::core::distributed::grpc::simple_transport::{
    SimpleGrpcTransport, SimpleGrpcConfig
};

let config = SimpleGrpcConfig {
    connection_timeout_ms: 10000,
    enable_compression: true,
    server_address: "127.0.0.1:50051".to_string(),
};

let transport = SimpleGrpcTransport::with_config(config);

// Connect
transport.connect("127.0.0.1:50051").await?;

// Send message
let message = Message::event(b"event data".to_vec())
    .with_header("source_node".to_string(), "node-1".to_string());
let response = transport.send_message("127.0.0.1:50051", message).await?;

// Heartbeat
let heartbeat_response = transport.heartbeat(
    "127.0.0.1:50051",
    "node-1".to_string()
).await?;
```

## gRPC Module Architecture

### eventflux.transport.rs (Generated)

Auto-generated Protocol Buffer definitions from `proto/transport.proto` by `tonic-build`. Contains Rust structs for all protobuf messages and gRPC service traits.

### transport.rs (Full Implementation)

Complete gRPC transport with full feature set:
- Implements unified `Transport` trait
- Connection pooling, TLS support, streaming, compression
- Server implementation for accepting connections

Use for production deployments requiring all gRPC features.

### simple_transport.rs (Simplified Client)

Simplified, immediately usable gRPC client:
- Focused on client-side operations
- Simpler API without unified transport interface complexity
- Direct methods for common operations

Use for applications needing gRPC without full distributed framework integration.

## Transport Comparison

| Feature                   | TCP Transport         | gRPC Transport                    |
|---------------------------|-----------------------|-----------------------------------|
| Latency                   | Lower (direct binary) | Slightly higher (HTTP/2 overhead) |
| Throughput                | High                  | Very High (multiplexing)          |
| Connection Efficiency     | Good (pooling)        | Excellent (multiplexing)          |
| Protocol Evolution        | Manual versioning     | Automatic (protobuf)              |
| Security                  | Basic                 | Built-in TLS/mTLS                 |
| Load Balancing            | External              | Built-in client-side              |
| Monitoring                | Custom                | Rich ecosystem                    |
| Complexity                | Simple                | More complex                      |
| Dependencies              | Minimal               | Requires protoc                   |

Recommendations:
- Use TCP for: Simple deployments, lowest latency, minimal dependencies
- Use gRPC for: Enterprise deployments, microservices, streaming, strong typing

## Message Types

Both transports support:

```rust
pub enum MessageType {
    Event,      // Stream events
    Query,      // Query requests/responses
    State,      // State synchronization
    Control,    // Control plane messages
    Heartbeat,  // Health monitoring
    Checkpoint, // State checkpointing
}
```

## Testing

```bash
# TCP transport
cargo test distributed_tcp_integration

# gRPC transport
cargo test distributed_grpc_integration

# All distributed
cargo test distributed
```

## Future Transports

The architecture supports additional transport layers:
- RDMA: Ultra-low latency for HPC environments
- QUIC: Improved performance over unreliable networks
- WebSocket: Browser-based clients
- Unix Domain Sockets: Local inter-process communication
