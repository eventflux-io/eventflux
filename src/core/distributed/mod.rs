/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// src/core/distributed/mod.rs

//! Distributed processing.
//!
//! Holds the settings, runtime, transport, state backend, coordinator and broker
//! pieces for running EventFlux across nodes. Single node mode is the default and
//! adds no distributed overhead. Distributed mode is turned on through settings.

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

/// Settings for distributed processing.
///
/// The default is single node mode with no node, cluster or broker settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedConfig {
    pub mode: RuntimeMode,

    /// Node settings. Used in distributed mode.
    pub node: Option<NodeConfig>,

    /// Cluster settings. Used in distributed mode.
    pub cluster: Option<ClusterConfig>,

    pub transport: TransportConfig,

    pub state_backend: StateBackendConfig,

    pub coordination: CoordinationConfig,

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

/// How the runtime runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeMode {
    /// One node, no distributed overhead. The default.
    SingleNode,

    /// Many nodes working as a coordinated cluster.
    Distributed,

    /// Local processing with state kept in a shared backend.
    Hybrid,
}

impl Default for RuntimeMode {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// Settings for one node in distributed mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node id.
    pub node_id: String,

    /// Addresses other nodes use to reach this node.
    pub endpoints: Vec<String>,

    pub capabilities: NodeCapabilities,

    pub resources: ResourceLimits,
}

/// What a node can do. By default a node can do everything and speaks `tcp`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Can act as coordinator.
    pub can_coordinate: bool,

    /// Can run queries.
    pub can_process: bool,

    /// Can store state.
    pub can_store_state: bool,

    /// Transport protocols the node supports.
    pub transport_protocols: Vec<String>,

    /// Free form extra capabilities.
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

/// Resource limits for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Most memory to use, in bytes. Default 8 GiB.
    pub max_memory: usize,

    /// Most CPU cores to use. Default 8.
    pub max_cpu_cores: usize,

    /// Most network bandwidth, in bytes per second. Default 1 GiB per second.
    pub max_network_bandwidth: usize,

    /// Most storage, in bytes. Default 100 GiB.
    pub max_storage: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 8 * 1024 * 1024 * 1024, // 8 GiB
            max_cpu_cores: 8,
            max_network_bandwidth: 1024 * 1024 * 1024, // 1 GiB/s
            max_storage: 100 * 1024 * 1024 * 1024,     // 100 GiB
        }
    }
}

/// Cluster settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Default `eventflux-cluster`.
    pub cluster_name: String,

    /// Known nodes to contact first when joining. Default empty.
    pub seed_nodes: Vec<String>,

    /// Fewest nodes needed to operate. Default 1.
    pub min_nodes: usize,

    /// Expected cluster size. Default 3.
    pub expected_size: usize,

    /// Default 1 second.
    pub heartbeat_interval: Duration,

    /// How long a node can stay silent before it counts as failed. Default 10
    /// seconds.
    pub failure_timeout: Duration,

    /// What to do when the network splits the cluster. Default `RequireMajority`.
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

/// What to do when the network splits the cluster into groups that cannot reach
/// each other.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionHandling {
    /// Let the smaller group keep running. Favors staying up over consistency (AP
    /// mode).
    AllowMinority,

    /// Only a group with a majority of nodes may operate. Favors consistency over
    /// staying up (CP mode).
    RequireMajority,

    /// Only a group with at least `size` nodes may operate.
    CustomQuorum { size: usize },

    /// Shut down when a split is detected.
    ShutdownOnPartition,
}

impl Default for PartitionHandling {
    fn default() -> Self {
        Self::RequireMajority
    }
}

/// Transport settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Default `Tcp`.
    pub implementation: TransportImplementation,

    /// Connection pool size. Default 10.
    pub pool_size: usize,

    /// Default 30 seconds.
    pub request_timeout: Duration,

    /// Default true.
    pub compression: bool,

    /// Default false.
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

/// Transport choices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportImplementation {
    /// TCP sockets. The default.
    Tcp,

    Grpc,

    /// RDMA, for fast networks that let nodes read each other's memory directly.
    Rdma,

    InfiniBand,

    /// A transport registered under `name`.
    Custom {
        name: String,
    },
}

/// State backend settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateBackendConfig {
    /// Default `InMemory`.
    pub implementation: StateBackendImplementation,

    /// Default 60 seconds.
    pub checkpoint_interval: Duration,

    /// How long state is kept. `None`, the default, keeps it forever.
    pub state_ttl: Option<Duration>,

    /// Save only what changed since the last checkpoint. Default true.
    pub incremental_checkpoints: bool,

    /// Default `Lz4`.
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

/// State backend choices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateBackendImplementation {
    /// State kept in local memory. For a single node.
    InMemory,

    Redis {
        endpoints: Vec<String>,
    },

    /// Apache Ignite.
    Ignite {
        endpoints: Vec<String>,
    },

    Hazelcast {
        endpoints: Vec<String>,
    },

    /// RocksDB, embedded in the process and stored at `path`.
    RocksDB {
        path: String,
    },

    /// A backend registered under `name`, with its own settings string.
    Custom {
        name: String,
        config: String,
    },
}

/// Compression type for state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Snappy,
    Zstd,
}

/// Coordination service settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinationConfig {
    /// Default `Raft`.
    pub implementation: CoordinationImplementation,

    /// How long to wait for a leader to be chosen. Default 5 seconds.
    pub election_timeout: Duration,

    /// Default 30 seconds.
    pub session_timeout: Duration,

    /// How many nodes must agree. Default `Majority`.
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

/// Coordination service choices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoordinationImplementation {
    /// Built in Raft.
    Raft,

    Etcd {
        endpoints: Vec<String>,
    },

    Zookeeper {
        endpoints: Vec<String>,
    },

    Consul {
        endpoints: Vec<String>,
    },

    /// A coordinator registered under `name`.
    Custom {
        name: String,
    },
}

/// How many nodes must agree before a distributed operation counts as done.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConsensusLevel {
    /// One node.
    One,

    /// More than half of the nodes.
    Majority,

    /// Every node.
    All,

    /// At least `size` nodes.
    Quorum { size: usize },
}

/// Message broker settings for passing events between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBrokerConfig {
    pub implementation: MessageBrokerImplementation,

    pub producer: ProducerConfig,

    pub consumer: ConsumerConfig,
}

/// Message broker choices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageBrokerImplementation {
    /// Apache Kafka.
    Kafka {
        brokers: Vec<String>,
    },

    /// Apache Pulsar.
    Pulsar {
        service_url: String,
    },

    Nats {
        servers: Vec<String>,
    },

    RabbitMQ {
        endpoints: Vec<String>,
    },

    /// A broker registered under `name`.
    Custom {
        name: String,
    },
}

/// Producer settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerConfig {
    /// Messages per send batch. Default 1000.
    pub batch_size: usize,

    /// How long to wait for a batch to fill before sending, in milliseconds.
    /// Default 10.
    pub linger_ms: u64,

    /// Default `Lz4`.
    pub compression: CompressionType,

    /// Default `Leader`.
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

/// Consumer settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// Consumer group id. Default `eventflux-consumer`.
    pub group_id: String,

    /// How often read positions are committed automatically. Default 5 seconds.
    pub auto_commit_interval: Duration,

    /// Most records returned by one poll. Default 500.
    pub max_poll_records: usize,

    /// Default 30 seconds.
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

/// Which brokers must confirm a sent message.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AcknowledgmentLevel {
    /// No confirmation.
    None,

    /// Only the leader confirms.
    Leader,

    /// Every replica that is caught up confirms.
    All,
}

/// Counters for distributed processing.
#[derive(Debug, Default)]
pub struct DistributedStats {
    /// Events processed across the cluster.
    pub total_events: u64,

    /// Events processed by this node.
    pub node_events: u64,

    pub active_nodes: usize,

    pub running_queries: usize,

    /// Network messages sent.
    pub messages_sent: u64,

    /// Network messages received.
    pub messages_received: u64,

    /// State sync operations.
    pub state_syncs: u64,

    pub checkpoints: u64,

    pub leader_elections: u64,

    /// Network splits detected.
    pub partitions_detected: u64,
}

/// Result type for distributed operations.
pub type DistributedResult<T> = Result<T, DistributedError>;

/// Errors from distributed processing.
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
