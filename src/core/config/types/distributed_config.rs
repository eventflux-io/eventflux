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

//! Distributed Processing Configuration Types
//!
//! Configuration for EventFlux deployments that run across several nodes.

use super::{duration_serde, optional_duration_serde};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Distributed processing configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DistributedConfig {
    /// Node configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeConfig>,

    /// Cluster configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterConfig>,

    /// Transport configuration
    pub transport: TransportConfig,

    /// State backend configuration
    pub state_backend: StateBackendConfig,

    /// Coordination configuration
    pub coordination: CoordinationConfig,

    /// Message broker configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_broker: Option<MessageBrokerConfig>,
}

impl DistributedConfig {
    /// Merge `other` into this config.
    ///
    /// Node, cluster and message broker are replaced only when `other` has them.
    /// Transport, state backend and coordination are always replaced as a whole, with
    /// no field by field merge.
    pub fn merge(&mut self, other: DistributedConfig) {
        if other.node.is_some() {
            self.node = other.node;
        }

        if other.cluster.is_some() {
            self.cluster = other.cluster;
        }

        // Required sections, so `other` always has them and simply wins
        self.transport = other.transport;
        self.state_backend = other.state_backend;
        self.coordination = other.coordination;

        if other.message_broker.is_some() {
            self.message_broker = other.message_broker;
        }
    }
}

/// Node configuration for distributed mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Addresses other nodes use to reach this node
    pub endpoints: Vec<String>,

    /// Node capabilities
    pub capabilities: NodeCapabilities,

    /// Resource limits
    pub resources: ResourceLimits,
}

/// Node capabilities declaration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeCapabilities {
    /// Whether this node can act as coordinator. Defaults to true.
    #[serde(default = "default_true")]
    pub can_coordinate: bool,

    /// Whether this node can process queries. Defaults to true.
    #[serde(default = "default_true")]
    pub can_process: bool,

    /// Whether this node can store state. Defaults to true.
    #[serde(default = "default_true")]
    pub can_store_state: bool,

    /// Supported transport protocols. Defaults to `["tcp"]`.
    #[serde(default = "default_transport_protocols")]
    pub transport_protocols: Vec<String>,

    /// Any extra capabilities, as free form key and value pairs
    #[serde(default)]
    pub custom: HashMap<String, String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            can_coordinate: true,
            can_process: true,
            can_store_state: true,
            transport_protocols: default_transport_protocols(),
            custom: HashMap::new(),
        }
    }
}

/// Resource limits for a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes
    pub max_memory: usize,

    /// Maximum CPU cores
    pub max_cpu_cores: usize,

    /// Maximum network bandwidth in bytes per second
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterConfig {
    /// Cluster name
    pub cluster_name: String,

    /// Seed nodes for discovery
    pub seed_nodes: Vec<String>,

    /// Fewest nodes the cluster needs to run. Defaults to 1.
    #[serde(default = "default_min_nodes")]
    pub min_nodes: usize,

    /// Expected cluster size. Defaults to 3.
    #[serde(default = "default_expected_size")]
    pub expected_size: usize,

    /// Time between heartbeats. Defaults to 1 second.
    #[serde(with = "duration_serde", default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// Silence after which a node is treated as failed. Defaults to 10 seconds.
    #[serde(with = "duration_serde", default = "default_failure_timeout")]
    pub failure_timeout: Duration,

    /// What to do when the network splits the cluster. Defaults to `RequireMajority`.
    #[serde(default)]
    pub partition_handling: PartitionHandling,

    /// Service discovery configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery: Option<ServiceDiscoveryConfig>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "eventflux-cluster".to_string(),
            seed_nodes: vec![],
            min_nodes: default_min_nodes(),
            expected_size: default_expected_size(),
            heartbeat_interval: default_heartbeat_interval(),
            failure_timeout: default_failure_timeout(),
            partition_handling: PartitionHandling::default(),
            discovery: None,
        }
    }
}

/// What to do when the network splits the cluster into groups that cannot see each other
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PartitionHandling {
    /// Let a smaller group keep running. Stays available but may diverge (AP mode).
    AllowMinority,

    /// Only a group with a majority of nodes keeps running. Stays consistent (CP mode).
    RequireMajority,

    /// Only a group with at least `size` nodes keeps running
    CustomQuorum { size: usize },

    /// Shut down when the cluster splits
    ShutdownOnPartition,
}

impl Default for PartitionHandling {
    fn default() -> Self {
        Self::RequireMajority
    }
}

/// Service discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceDiscoveryConfig {
    /// Discovery method
    pub method: ServiceDiscoveryMethod,

    /// Settings for the chosen method. Holds every other key at the same YAML level.
    #[serde(flatten)]
    pub config: serde_yaml::Value,
}

/// Service discovery methods
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceDiscoveryMethod {
    /// Kubernetes service discovery
    Kubernetes,

    /// Consul service discovery
    Consul,

    /// Etcd service discovery
    Etcd,

    /// Service discovery through DNS
    Dns,

    /// Static configuration
    Static,
}

/// Transport layer configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransportConfig {
    /// Transport implementation
    pub implementation: TransportImplementation,

    /// Connection pool size. Defaults to 10.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Request timeout. Defaults to 30 seconds.
    #[serde(with = "duration_serde", default = "default_request_timeout")]
    pub request_timeout: Duration,

    /// Compress traffic. Defaults to true.
    #[serde(default = "default_true")]
    pub compression: bool,

    /// Encrypt traffic. Defaults to false.
    #[serde(default)]
    pub encryption: bool,

    /// Extra settings for the chosen transport
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_yaml::Value>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            implementation: TransportImplementation::Tcp,
            pool_size: default_pool_size(),
            request_timeout: default_request_timeout(),
            compression: true,
            encryption: false,
            config: None,
        }
    }
}

/// Transport implementation choices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransportImplementation {
    /// TCP sockets (default)
    Tcp,

    /// gRPC
    Grpc,

    /// RDMA, for very fast networks
    Rdma,

    /// InfiniBand
    #[serde(rename = "infiniband")]
    InfiniBand,

    /// Custom implementation
    Custom { name: String },
}

/// State backend configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateBackendConfig {
    /// Backend implementation
    pub implementation: StateBackendImplementation,

    /// Time between checkpoints. Defaults to 60 seconds.
    #[serde(with = "duration_serde", default = "default_checkpoint_interval")]
    pub checkpoint_interval: Duration,

    /// How long state is kept before it expires. None keeps it forever.
    #[serde(
        with = "optional_duration_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub state_ttl: Option<Duration>,

    /// Save only what changed since the last checkpoint. Defaults to true.
    #[serde(default = "default_true")]
    pub incremental_checkpoints: bool,

    /// Compression for state. Defaults to `Lz4`.
    #[serde(default)]
    pub compression: CompressionType,

    /// Extra settings for the chosen backend
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_yaml::Value>,
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        Self {
            implementation: StateBackendImplementation::InMemory,
            checkpoint_interval: default_checkpoint_interval(),
            state_ttl: None,
            incremental_checkpoints: true,
            compression: CompressionType::Lz4,
            config: None,
        }
    }
}

/// State backend implementation choices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum StateBackendImplementation {
    /// State held in memory on one node
    InMemory,

    /// Redis
    Redis {
        endpoints: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cluster_mode: Option<bool>,
    },

    /// Apache Ignite
    Ignite { endpoints: Vec<String> },

    /// Hazelcast
    Hazelcast { endpoints: Vec<String> },

    /// RocksDB (embedded)
    RocksDB { path: String },

    /// Custom backend
    Custom { name: String, config: String },
}

/// Compression type for state. Defaults to `Lz4`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Lz4,
    Snappy,
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4
    }
}

/// Coordination service configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CoordinationConfig {
    /// Coordination implementation
    pub implementation: CoordinationImplementation,

    /// Leader election timeout. Defaults to 5 seconds.
    #[serde(with = "duration_serde", default = "default_election_timeout")]
    pub election_timeout: Duration,

    /// Session timeout. Defaults to 30 seconds.
    #[serde(with = "duration_serde", default = "default_session_timeout")]
    pub session_timeout: Duration,

    /// How many nodes must agree. Defaults to `Majority`.
    #[serde(default)]
    pub consensus_level: ConsensusLevel,

    /// Extra settings for the chosen coordination service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_yaml::Value>,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            implementation: CoordinationImplementation::Raft,
            election_timeout: default_election_timeout(),
            session_timeout: default_session_timeout(),
            consensus_level: ConsensusLevel::Majority,
            config: None,
        }
    }
}

/// Coordination implementation choices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CoordinationImplementation {
    /// Built in Raft implementation
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ConsensusLevel {
    /// One node must acknowledge
    One,

    /// A majority of nodes must acknowledge
    Majority,

    /// All nodes must acknowledge
    All,

    /// At least `size` nodes must acknowledge
    Quorum { size: usize },
}

impl Default for ConsensusLevel {
    fn default() -> Self {
        Self::Majority
    }
}

/// Message broker configuration for event distribution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageBrokerConfig {
    /// Broker implementation
    pub implementation: MessageBrokerImplementation,

    /// Producer configuration
    pub producer: ProducerConfig,

    /// Consumer configuration
    pub consumer: ConsumerConfig,
}

/// Message broker implementation choices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MessageBrokerImplementation {
    /// Apache Kafka
    Kafka { brokers: Vec<String> },

    /// Apache Pulsar
    Pulsar { service_url: String },

    /// NATS
    Nats { servers: Vec<String> },

    /// RabbitMQ
    RabbitMq { endpoints: Vec<String> },

    /// Custom broker
    Custom { name: String },
}

/// Producer configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProducerConfig {
    /// Batch size for sending. Defaults to 1000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Time in milliseconds to wait for more events before sending. Defaults to 10.
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,

    /// Compression type. Defaults to `Lz4`.
    #[serde(default)]
    pub compression: CompressionType,

    /// Acknowledgment level. Defaults to `Leader`.
    #[serde(default)]
    pub acks: AcknowledgmentLevel,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            linger_ms: default_linger_ms(),
            compression: CompressionType::Lz4,
            acks: AcknowledgmentLevel::Leader,
        }
    }
}

/// Consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsumerConfig {
    /// Consumer group ID
    pub group_id: String,

    /// Time between automatic offset commits. Defaults to 5 seconds.
    #[serde(with = "duration_serde", default = "default_auto_commit_interval")]
    pub auto_commit_interval: Duration,

    /// Most records returned by one poll. Defaults to 500.
    #[serde(default = "default_max_poll_records")]
    pub max_poll_records: usize,

    /// Session timeout. Defaults to 30 seconds.
    #[serde(with = "duration_serde", default = "default_consumer_session_timeout")]
    pub session_timeout: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "eventflux-consumer".to_string(),
            auto_commit_interval: default_auto_commit_interval(),
            max_poll_records: default_max_poll_records(),
            session_timeout: default_consumer_session_timeout(),
        }
    }
}

/// Acknowledgment level for producers
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AcknowledgmentLevel {
    /// No acknowledgment
    None,

    /// Leader acknowledgment only
    Leader,

    /// All replicas that are in sync must acknowledge
    All,
}

impl Default for AcknowledgmentLevel {
    fn default() -> Self {
        Self::Leader
    }
}

// Default values used by the serde attributes above
fn default_true() -> bool {
    true
}

fn default_transport_protocols() -> Vec<String> {
    vec!["tcp".to_string()]
}

fn default_min_nodes() -> usize {
    1
}

fn default_expected_size() -> usize {
    3
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_failure_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_pool_size() -> usize {
    10
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_election_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_session_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_batch_size() -> usize {
    1000
}

fn default_linger_ms() -> u64 {
    10
}

fn default_auto_commit_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_max_poll_records() -> usize {
    500
}

fn default_consumer_session_timeout() -> Duration {
    Duration::from_secs(30)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distributed_config_default() {
        let config = DistributedConfig::default();
        assert_eq!(
            config.transport.implementation,
            TransportImplementation::Tcp
        );
        assert_eq!(
            config.state_backend.implementation,
            StateBackendImplementation::InMemory
        );
        assert_eq!(
            config.coordination.implementation,
            CoordinationImplementation::Raft
        );
    }

    #[test]
    fn test_partition_handling_serde() {
        let handling = PartitionHandling::CustomQuorum { size: 3 };
        let yaml = serde_yaml::to_string(&handling).unwrap();
        let deserialized: PartitionHandling = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, handling);
    }

    #[test]
    fn test_state_backend_implementation_serde() {
        let backend = StateBackendImplementation::Redis {
            endpoints: vec!["redis1:6379".to_string(), "redis2:6379".to_string()],
            cluster_mode: Some(true),
        };

        let yaml = serde_yaml::to_string(&backend).unwrap();
        let deserialized: StateBackendImplementation = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, backend);
    }

    #[test]
    fn test_compression_type_serde() {
        let compression = CompressionType::Zstd;
        let yaml = serde_yaml::to_string(&compression).unwrap();
        assert_eq!(yaml.trim(), "zstd");

        let deserialized: CompressionType = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, compression);
    }

    #[test]
    fn test_consensus_level_serde() {
        let level = ConsensusLevel::Quorum { size: 5 };
        let yaml = serde_yaml::to_string(&level).unwrap();
        let deserialized: ConsensusLevel = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized, level);
    }

    #[test]
    fn test_node_capabilities_default() {
        let capabilities = NodeCapabilities::default();
        assert!(capabilities.can_coordinate);
        assert!(capabilities.can_process);
        assert!(capabilities.can_store_state);
        assert_eq!(capabilities.transport_protocols, vec!["tcp"]);
    }

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory, 8 * 1024 * 1024 * 1024); // 8GB
        assert_eq!(limits.max_cpu_cores, 8);
    }

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.cluster_name, "eventflux-cluster");
        assert_eq!(config.min_nodes, 1);
        assert_eq!(config.expected_size, 3);
        assert_eq!(
            config.partition_handling,
            PartitionHandling::RequireMajority
        );
    }
}
