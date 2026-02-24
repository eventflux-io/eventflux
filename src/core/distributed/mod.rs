// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/mod.rs

//! Distributed Processing Framework
//!
//! This module implements the distributed processing capabilities for EventFlux Rust,
//! following the Single-Node First philosophy from the architecture design.
//! The system provides zero-overhead single-node mode by default with progressive
//! enhancement to distributed mode through configuration.

pub mod distributed_runtime;
pub mod grpc;
pub mod processing_engine;
pub mod runtime_mode;
pub mod state_backend;
pub mod transport;
pub use state_backend::{InMemoryBackend, RedisBackend, RedisConfig, StateBackend};
pub mod coordinator;
pub mod message_broker;

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for distributed processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    /// Runtime mode selection
    pub mode: RuntimeMode,

    /// Node configuration (for distributed mode)
    pub node: Option<NodeConfig>,

    /// Cluster configuration (for distributed mode)
    pub cluster: Option<ClusterConfig>,

    /// Transport configuration
    pub transport: TransportConfig,

    /// State backend configuration
    pub state_backend: StateBackendConfig,

    /// Coordination configuration
    pub coordination: CoordinationConfig,

    /// Message broker configuration
    pub message_broker: Option<MessageBrokerConfig>,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::SingleNode,
            node: None,
            cluster: None,
            transport: TransportConfig::default(),
            state_backend: StateBackendConfig::default(),
            coordination: CoordinationConfig::default(),
            message_broker: None,
        }
    }
}

/// Runtime mode selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMode {
    /// Single-node mode (default, zero overhead)
    SingleNode,

    /// Distributed mode with cluster coordination
    Distributed,

    /// Hybrid mode (local processing with distributed state)
    Hybrid,
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// Node configuration for distributed mode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Node endpoints for communication
    pub endpoints: Vec<String>,

    /// Node capabilities
    pub capabilities: NodeCapabilities,

    /// Resource limits
    pub resources: ResourceLimits,
}

/// Node capabilities declaration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Can this node act as coordinator
    pub can_coordinate: bool,

    /// Can this node process queries
    pub can_process: bool,

    /// Can this node store state
    pub can_store_state: bool,

    /// Supported transport protocols
    pub transport_protocols: Vec<String>,

    /// Custom capabilities (extensible)
    pub custom: std::collections::HashMap<String, String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            can_coordinate: true,
            can_process: true,
            can_store_state: true,
            transport_protocols: vec!["tcp".to_string()],
            custom: std::collections::HashMap::new(),
        }
    }
}

/// Resource limits for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes
    pub max_memory: usize,

    /// Maximum CPU cores
    pub max_cpu_cores: usize,

    /// Maximum network bandwidth in bytes/sec
    pub max_network_bandwidth: usize,

    /// Maximum storage capacity in bytes
    pub max_storage: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 8 * 1024 * 1024 * 1024, // 8GB
            max_cpu_cores: 8,
            max_network_bandwidth: 1024 * 1024 * 1024, // 1Gbps
            max_storage: 100 * 1024 * 1024 * 1024,     // 100GB
        }
    }
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub cluster_name: String,

    /// Seed nodes for discovery
    pub seed_nodes: Vec<String>,

    /// Minimum nodes required for operation
    pub min_nodes: usize,

    /// Expected cluster size
    pub expected_size: usize,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Failure detection timeout
    pub failure_timeout: Duration,

    /// Partition handling strategy
    pub partition_handling: PartitionHandling,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "eventflux-cluster".to_string(),
            seed_nodes: vec![],
            min_nodes: 1,
            expected_size: 3,
            heartbeat_interval: Duration::from_secs(1),
            failure_timeout: Duration::from_secs(10),
            partition_handling: PartitionHandling::default(),
        }
    }
}

/// Partition handling strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionHandling {
    /// Allow minority partition to continue (AP mode)
    AllowMinority,

    /// Require majority for operations (CP mode)
    RequireMajority,

    /// Custom quorum size
    CustomQuorum { size: usize },

    /// Shut down on partition
    ShutdownOnPartition,
}

impl Default for PartitionHandling {
    fn default() -> Self {
        Self::RequireMajority
    }
}

/// Transport layer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Transport implementation
    pub implementation: TransportImplementation,

    /// Connection pool size
    pub pool_size: usize,

    /// Request timeout
    pub request_timeout: Duration,

    /// Enable compression
    pub compression: bool,

    /// Enable encryption
    pub encryption: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            implementation: TransportImplementation::Tcp,
            pool_size: 10,
            request_timeout: Duration::from_secs(30),
            compression: true,
            encryption: false,
        }
    }
}

/// Transport implementation choices
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportImplementation {
    /// TCP sockets (default)
    Tcp,

    /// gRPC
    Grpc,

    /// RDMA (for high-performance networks)
    Rdma,

    /// InfiniBand
    InfiniBand,

    /// Custom implementation
    Custom { name: String },
}

/// State backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateBackendConfig {
    /// Backend implementation
    pub implementation: StateBackendImplementation,

    /// Checkpoint interval
    pub checkpoint_interval: Duration,

    /// State TTL
    pub state_ttl: Option<Duration>,

    /// Enable incremental checkpoints
    pub incremental_checkpoints: bool,

    /// Compression for state
    pub compression: CompressionType,
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        Self {
            implementation: StateBackendImplementation::InMemory,
            checkpoint_interval: Duration::from_secs(60),
            state_ttl: None,
            incremental_checkpoints: true,
            compression: CompressionType::Lz4,
        }
    }
}

/// State backend implementation choices
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateBackendImplementation {
    /// In-memory state (single-node)
    InMemory,

    /// Redis
    Redis { endpoints: Vec<String> },

    /// Apache Ignite
    Ignite { endpoints: Vec<String> },

    /// Hazelcast
    Hazelcast { endpoints: Vec<String> },

    /// RocksDB (embedded)
    RocksDB { path: String },

    /// Custom backend
    Custom { name: String, config: String },
}

/// Compression type for state
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Snappy,
    Zstd,
}

/// Coordination service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationConfig {
    /// Coordination implementation
    pub implementation: CoordinationImplementation,

    /// Leader election timeout
    pub election_timeout: Duration,

    /// Session timeout
    pub session_timeout: Duration,

    /// Consensus level
    pub consensus_level: ConsensusLevel,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            implementation: CoordinationImplementation::Raft,
            election_timeout: Duration::from_secs(5),
            session_timeout: Duration::from_secs(30),
            consensus_level: ConsensusLevel::Majority,
        }
    }
}

/// Coordination implementation choices
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinationImplementation {
    /// Built-in Raft implementation
    Raft,

    /// Etcd
    Etcd { endpoints: Vec<String> },

    /// Zookeeper
    Zookeeper { endpoints: Vec<String> },

    /// Consul
    Consul { endpoints: Vec<String> },

    /// Custom coordination
    Custom { name: String },
}

/// Consensus level for distributed operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsensusLevel {
    /// Single node acknowledgment
    One,

    /// Majority of nodes
    Majority,

    /// All nodes must acknowledge
    All,

    /// Custom quorum
    Quorum { size: usize },
}

/// Message broker configuration for event distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBrokerConfig {
    /// Broker implementation
    pub implementation: MessageBrokerImplementation,

    /// Producer configuration
    pub producer: ProducerConfig,

    /// Consumer configuration
    pub consumer: ConsumerConfig,
}

/// Message broker implementation choices
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageBrokerImplementation {
    /// Apache Kafka
    Kafka { brokers: Vec<String> },

    /// Apache Pulsar
    Pulsar { service_url: String },

    /// NATS
    Nats { servers: Vec<String> },

    /// RabbitMQ
    RabbitMQ { endpoints: Vec<String> },

    /// Custom broker
    Custom { name: String },
}

/// Producer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerConfig {
    /// Batch size for sending
    pub batch_size: usize,

    /// Linger time before sending
    pub linger_ms: u64,

    /// Compression type
    pub compression: CompressionType,

    /// Acknowledgment level
    pub acks: AcknowledgmentLevel,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            linger_ms: 10,
            compression: CompressionType::Lz4,
            acks: AcknowledgmentLevel::Leader,
        }
    }
}

/// Consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Consumer group ID
    pub group_id: String,

    /// Auto-commit interval
    pub auto_commit_interval: Duration,

    /// Max poll records
    pub max_poll_records: usize,

    /// Session timeout
    pub session_timeout: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "eventflux-consumer".to_string(),
            auto_commit_interval: Duration::from_secs(5),
            max_poll_records: 500,
            session_timeout: Duration::from_secs(30),
        }
    }
}

/// Acknowledgment level for producers
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AcknowledgmentLevel {
    /// No acknowledgment
    None,

    /// Leader acknowledgment only
    Leader,

    /// All in-sync replicas
    All,
}

/// Distributed processing statistics
#[derive(Debug, Default)]
pub struct DistributedStats {
    /// Total events processed across cluster
    pub total_events: u64,

    /// Events processed by this node
    pub node_events: u64,

    /// Number of active nodes
    pub active_nodes: usize,

    /// Number of running queries
    pub running_queries: usize,

    /// Network messages sent
    pub messages_sent: u64,

    /// Network messages received
    pub messages_received: u64,

    /// State synchronization operations
    pub state_syncs: u64,

    /// Checkpoint operations
    pub checkpoints: u64,

    /// Leader elections
    pub leader_elections: u64,

    /// Network partitions detected
    pub partitions_detected: u64,
}

/// Result type for distributed operations
pub type DistributedResult<T> = Result<T, DistributedError>;

/// Error types for distributed processing
#[derive(thiserror::Error, Debug)]
pub enum DistributedError {
    #[error("Network error: {message}")]
    NetworkError { message: String },

    #[error("Coordination error: {message}")]
    CoordinationError { message: String },

    #[error("State synchronization error: {message}")]
    StateSyncError { message: String },

    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: String },

    #[error("Cluster not ready: {reason}")]
    ClusterNotReady { reason: String },

    #[error("Partition detected: {details}")]
    PartitionDetected { details: String },

    #[error("Configuration error: {message}")]
    ConfigurationError { message: String },

    #[error("Transport error: {message}")]
    TransportError { message: String },

    #[error("Broker error: {message}")]
    BrokerError { message: String },

    #[error("State error: {message}")]
    StateError { message: String },
}
