// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/persistence/incremental/mod.rs

//! Incremental Checkpointing System
//!
//! This module implements enterprise-grade incremental checkpointing with Write-Ahead Logging (WAL),
//! checkpoint merging, and distributed coordination. The design follows industry best practices
//! from Apache Flink, Kafka, and other stream processing systems.
//!
//! ## Architecture Overview
//!
//! ```text
//! State Updates → WAL → Checkpoint Merger → Persistence Backend
//!      ↓              ↓                         ↓
//!  Change Events → Log Entries → Incremental → Distributed Storage
//!      ↓              ↓         Checkpoints          ↓
//!  StateHolder → Batch Flush → Delta Merge → Recovery Engine
//! ```
//!
//! ## Key Components
//!
//! - **WriteAheadLog**: Durable event log for state changes
//! - **CheckpointMerger**: Combines incremental updates with base checkpoints
//! - **PersistenceBackend**: Pluggable storage (File, Distributed, Cloud)
//! - **RecoveryEngine**: Reconstructs state from incremental checkpoints
//! - **DistributedCoordinator**: Cluster-wide checkpoint coordination

pub mod checkpoint_merger;
pub mod distributed_coordinator;
pub mod persistence_backend;
pub mod recovery_engine;
pub mod write_ahead_log;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::persistence::state_holder::{
    ChangeLog, CheckpointId, ComponentId, StateError, StateSnapshot,
};

/// Configuration for incremental checkpointing system
#[derive(Debug, Clone)]
pub struct IncrementalCheckpointConfig {
    /// Minimum interval between checkpoints
    pub checkpoint_interval: Duration,

    /// Maximum number of incremental checkpoints before full checkpoint
    pub max_incremental_count: usize,

    /// WAL segment size in bytes
    pub wal_segment_size: usize,

    /// WAL retention period
    pub wal_retention: Duration,

    /// Batch size for state updates
    pub batch_size: usize,

    /// Enable compression for incremental checkpoints
    pub enable_compression: bool,

    /// Background merger thread count
    pub merger_threads: usize,

    /// Persistence backend configuration
    pub backend_config: PersistenceBackendConfig,

    /// Distributed coordination settings
    pub distributed_config: Option<DistributedConfig>,
}

impl Default for IncrementalCheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(30),
            max_incremental_count: 10,
            wal_segment_size: 64 * 1024 * 1024,       // 64MB
            wal_retention: Duration::from_secs(3600), // 1 hour
            batch_size: 1000,
            enable_compression: true,
            merger_threads: 2,
            backend_config: PersistenceBackendConfig::default(),
            distributed_config: None,
        }
    }
}

/// Configuration for persistence backends
#[derive(Debug, Clone)]
pub enum PersistenceBackendConfig {
    /// Local file system storage
    LocalFile {
        base_path: String,
        sync_writes: bool,
    },

    /// Distributed storage (e.g., etcd, Consul)
    Distributed {
        endpoints: Vec<String>,
        prefix: String,
        replication_factor: usize,
    },

    /// Cloud storage (S3, GCS, Azure)
    Cloud {
        provider: CloudProvider,
        bucket: String,
        region: Option<String>,
        credentials: CloudCredentials,
    },

    /// In-memory storage (for testing)
    Memory,
}

impl Default for PersistenceBackendConfig {
    fn default() -> Self {
        Self::LocalFile {
            base_path: "./checkpoints".to_string(),
            sync_writes: true,
        }
    }
}

/// Cloud storage providers
#[derive(Debug, Clone)]
pub enum CloudProvider {
    AWS,
    GoogleCloud,
    Azure,
}

/// Cloud storage credentials
#[derive(Debug, Clone)]
pub enum CloudCredentials {
    Default,
    AccessKey {
        access_key: String,
        secret_key: String,
    },
    ServiceAccount {
        key_file: String,
    },
}

/// Configuration for distributed coordination
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    /// Node identifier
    pub node_id: String,

    /// Cluster endpoints
    pub cluster_endpoints: Vec<String>,

    /// Leader election timeout
    pub election_timeout: Duration,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Maximum network partition tolerance
    pub partition_tolerance: Duration,
}

/// Incremental checkpoint metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IncrementalCheckpoint {
    /// Unique checkpoint identifier
    pub checkpoint_id: CheckpointId,

    /// Base checkpoint this incremental is built upon
    pub base_checkpoint_id: CheckpointId,

    /// Sequence number in incremental chain
    pub sequence_number: u64,

    /// Timestamp when checkpoint was created (microseconds since UNIX epoch)
    pub created_at: u64,

    /// Components included in this checkpoint
    pub component_changes: HashMap<ComponentId, ChangeLog>,

    /// Total size of checkpoint data
    pub total_size: usize,

    /// Compression ratio achieved
    pub compression_ratio: f64,

    /// WAL offset range covered by this checkpoint
    pub wal_offset_range: (u64, u64),
}

/// Result of incremental checkpoint operation
#[derive(Debug)]
pub struct CheckpointResult {
    /// Checkpoint metadata
    pub checkpoint: IncrementalCheckpoint,

    /// Duration taken to create checkpoint
    pub duration: Duration,

    /// Number of state operations included
    pub operation_count: usize,

    /// Storage location identifier
    pub storage_location: String,
}

/// Incremental checkpointing system coordinator
pub struct IncrementalCheckpointSystem {
    /// System configuration
    config: IncrementalCheckpointConfig,

    /// Write-ahead log for state changes
    wal: Arc<dyn WriteAheadLog>,

    /// Checkpoint merger for combining incrementals
    merger: Arc<dyn CheckpointMerger>,

    /// Persistence backend for storing checkpoints
    backend: Arc<dyn PersistenceBackend>,

    /// Recovery engine for restoring state
    recovery: Arc<dyn RecoveryEngine>,

    /// Distributed coordinator (optional)
    coordinator: Option<Arc<dyn DistributedCoordinator>>,

    /// Current incremental checkpoint chain
    current_chain: Vec<IncrementalCheckpoint>,

    /// Last full checkpoint ID
    last_full_checkpoint: Option<CheckpointId>,

    /// System metrics
    metrics: IncrementalCheckpointMetrics,
}

/// Metrics for incremental checkpointing system
#[derive(Debug, Default)]
pub struct IncrementalCheckpointMetrics {
    /// Total incremental checkpoints created
    pub total_incrementals: u64,

    /// Total full checkpoints created
    pub total_full_checkpoints: u64,

    /// Average incremental checkpoint size
    pub avg_incremental_size: usize,

    /// Average incremental checkpoint duration
    pub avg_incremental_duration: Duration,

    /// Average compression ratio
    pub avg_compression_ratio: f64,

    /// WAL throughput (operations/second)
    pub wal_throughput: f64,

    /// Recovery time statistics
    pub avg_recovery_time: Duration,

    /// Failed checkpoint count
    pub failed_checkpoints: u64,
}

// Trait definitions for pluggable components

/// Write-Ahead Log trait for durable state change logging
pub trait WriteAheadLog: Send + Sync {
    /// Append a state change to the log
    fn append(&self, entry: LogEntry) -> Result<LogOffset, StateError>;

    /// Append multiple entries atomically
    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Vec<LogOffset>, StateError>;

    /// Read entries from a given offset
    fn read_from(&self, offset: LogOffset, limit: usize) -> Result<Vec<LogEntry>, StateError>;

    /// Get the current log tail offset
    fn tail_offset(&self) -> Result<LogOffset, StateError>;

    /// Trim log up to the given offset (for cleanup)
    fn trim_to(&self, offset: LogOffset) -> Result<(), StateError>;

    /// Sync log to durable storage
    fn sync(&self) -> Result<(), StateError>;
}

/// Checkpoint merger for combining incremental updates
pub trait CheckpointMerger: Send + Sync {
    /// Merge incremental checkpoints into a single checkpoint
    fn merge_incrementals(
        &self,
        base: &StateSnapshot,
        incrementals: &[IncrementalCheckpoint],
    ) -> Result<StateSnapshot, StateError>;

    /// Create a new incremental checkpoint from changes
    fn create_incremental(
        &self,
        base_checkpoint_id: CheckpointId,
        changes: HashMap<ComponentId, ChangeLog>,
        wal_range: (u64, u64),
    ) -> Result<IncrementalCheckpoint, StateError>;

    /// Optimize incremental chain by merging adjacent checkpoints
    fn optimize_chain(
        &self,
        chain: &[IncrementalCheckpoint],
    ) -> Result<Vec<IncrementalCheckpoint>, StateError>;
}

/// Persistence backend for storing checkpoints
pub trait PersistenceBackend: Send + Sync {
    /// Store a full checkpoint
    fn store_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        snapshot: &StateSnapshot,
    ) -> Result<String, StateError>;

    /// Store an incremental checkpoint
    fn store_incremental_checkpoint(
        &self,
        checkpoint: &IncrementalCheckpoint,
    ) -> Result<String, StateError>;

    /// Load a full checkpoint
    fn load_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError>;

    /// Load an incremental checkpoint
    fn load_incremental_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<IncrementalCheckpoint, StateError>;

    /// List available checkpoints
    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, StateError>;

    /// Delete old checkpoints (for cleanup)
    fn cleanup_checkpoints(&self, before: Instant) -> Result<usize, StateError>;
}

/// Recovery engine for reconstructing state from checkpoints
pub trait RecoveryEngine: Send + Sync {
    /// Recover state from a full checkpoint
    fn recover_from_full(
        &self,
        checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError>;

    /// Recover state from incremental checkpoints
    fn recover_from_incrementals(
        &self,
        base_checkpoint_id: CheckpointId,
        target_checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError>;

    /// Find the best recovery path for given timestamp
    fn find_recovery_path(
        &self,
        target_time: Instant,
        components: &[ComponentId],
    ) -> Result<RecoveryPath, StateError>;
}

/// Distributed coordinator for cluster-wide checkpointing
pub trait DistributedCoordinator: Send + Sync {
    /// Initiate a distributed checkpoint
    fn initiate_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<(), StateError>;

    /// Report checkpoint completion
    fn report_completion(
        &self,
        checkpoint_id: CheckpointId,
        node_id: &str,
        success: bool,
    ) -> Result<(), StateError>;

    /// Wait for all nodes to complete checkpoint
    fn wait_for_completion(
        &self,
        checkpoint_id: CheckpointId,
        timeout: Duration,
    ) -> Result<bool, StateError>;

    /// Get cluster health status
    fn cluster_health(&self) -> Result<ClusterHealth, StateError>;
}

/// WAL log entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    /// Component that generated this entry
    pub component_id: ComponentId,

    /// Sequence number within component
    pub sequence: u64,

    /// Timestamp when entry was created
    pub timestamp: u64,

    /// The actual state change
    pub change: ChangeLog,

    /// Optional metadata
    pub metadata: HashMap<String, String>,
}

/// WAL offset type
pub type LogOffset = u64;

/// Recovery path information
#[derive(Debug)]
pub struct RecoveryPath {
    /// Base full checkpoint to start from
    pub base_checkpoint: CheckpointId,

    /// Incremental checkpoints to apply in order
    pub incremental_chain: Vec<CheckpointId>,

    /// Estimated recovery time
    pub estimated_duration: Duration,

    /// Total data size to process
    pub total_size: usize,
}

/// Cluster health information
#[derive(Debug)]
pub struct ClusterHealth {
    /// Total number of nodes
    pub total_nodes: usize,

    /// Number of healthy nodes
    pub healthy_nodes: usize,

    /// Current leader node
    pub leader_node: Option<String>,

    /// Network partition status
    pub partition_status: PartitionStatus,
}

/// Network partition status
#[derive(Debug)]
pub enum PartitionStatus {
    Healthy,
    MinorPartition { affected_nodes: Vec<String> },
    MajorPartition { majority_nodes: Vec<String> },
    SplitBrain,
}

// Error types specific to incremental checkpointing
#[derive(thiserror::Error, Debug)]
pub enum IncrementalCheckpointError {
    #[error("WAL operation failed: {message}")]
    WalError { message: String },

    #[error("Checkpoint merge failed: {message}")]
    MergeError { message: String },

    #[error("Persistence backend error: {message}")]
    PersistenceError { message: String },

    #[error("Recovery failed: {message}")]
    RecoveryError { message: String },

    #[error("Distributed coordination failed: {message}")]
    CoordinationError { message: String },

    #[error("Invalid checkpoint chain: {message}")]
    InvalidChain { message: String },

    #[error("Checkpoint not found: {checkpoint_id}")]
    CheckpointNotFound { checkpoint_id: CheckpointId },
}
