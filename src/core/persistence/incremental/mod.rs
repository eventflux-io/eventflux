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

// src/core/persistence/incremental/mod.rs

//! Incremental checkpoints.
//!
//! Saves only what changed since the last checkpoint. Changes go to a write ahead
//! log (WAL) first, are packed into incremental checkpoints, merged with a full
//! base checkpoint, stored by a persistence backend, and read back by the
//! recovery engine. The design borrows from Apache Flink and Kafka. Several
//! parts are still stand ins, see each module.
//!
//! ## Architecture overview
//!
//! ```text
//! State Updates → WAL → Checkpoint Merger → Persistence Backend
//!      ↓              ↓                         ↓
//!  Change Events → Log Entries → Incremental → Distributed Storage
//!      ↓              ↓         Checkpoints          ↓
//!  StateHolder → Batch Flush → Delta Merge → Recovery Engine
//! ```
//!
//! ## Key components
//!
//! - **WriteAheadLog**: Append only log of state changes on disk.
//! - **CheckpointMerger**: Combines incremental changes with a base checkpoint.
//! - **PersistenceBackend**: Where checkpoints are stored (file, distributed or
//!   memory, with cloud not built yet).
//! - **RecoveryEngine**: Rebuilds state from checkpoints.
//! - **DistributedCoordinator**: Coordinates checkpoints across a cluster.

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

/// Incremental checkpoint settings.
#[derive(Debug, Clone)]
pub struct IncrementalCheckpointConfig {
    /// Shortest time between checkpoints. Default 30 seconds.
    pub checkpoint_interval: Duration,

    /// Incremental checkpoints allowed before a full one. Default 10.
    pub max_incremental_count: usize,

    /// WAL segment size in bytes. Default 64 MB.
    pub wal_segment_size: usize,

    /// How long WAL data is kept. Default 1 hour.
    pub wal_retention: Duration,

    /// State updates per batch. Default 1000.
    pub batch_size: usize,

    /// Default true.
    pub enable_compression: bool,

    /// Background merger threads. Default 2.
    pub merger_threads: usize,

    /// Default is local files under `./checkpoints` with synced writes.
    pub backend_config: PersistenceBackendConfig,

    /// Cluster settings. `None`, the default, means a single node.
    pub distributed_config: Option<DistributedConfig>,
}

impl Default for IncrementalCheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(30),
            max_incremental_count: 10,
            wal_segment_size: 64 * 1024 * 1024,       // 64 MB
            wal_retention: Duration::from_secs(3600), // 1 hour
            batch_size: 1000,
            enable_compression: true,
            merger_threads: 2,
            backend_config: PersistenceBackendConfig::default(),
            distributed_config: None,
        }
    }
}

/// Where checkpoints are stored.
#[derive(Debug, Clone)]
pub enum PersistenceBackendConfig {
    /// Files on the local disk. The default, with `./checkpoints` and synced
    /// writes.
    LocalFile {
        base_path: String,
        sync_writes: bool,
    },

    /// A distributed store such as etcd or Consul. The backend for this is a
    /// stand in that stores nothing.
    Distributed {
        endpoints: Vec<String>,
        prefix: String,
        replication_factor: usize,
    },

    /// Cloud storage such as S3, GCS or Azure. Not built yet, so creating this
    /// backend fails.
    Cloud {
        provider: CloudProvider,
        bucket: String,
        region: Option<String>,
        credentials: CloudCredentials,
    },

    /// In memory, for tests.
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

/// Cloud storage providers.
#[derive(Debug, Clone)]
pub enum CloudProvider {
    AWS,
    GoogleCloud,
    Azure,
}

/// Cloud storage credentials.
#[derive(Debug, Clone)]
pub enum CloudCredentials {
    /// The provider's default credential lookup.
    Default,
    AccessKey {
        access_key: String,
        secret_key: String,
    },
    ServiceAccount {
        key_file: String,
    },
}

/// Cluster settings for checkpoint coordination.
#[derive(Debug, Clone)]
pub struct DistributedConfig {
    pub node_id: String,

    pub cluster_endpoints: Vec<String>,

    /// How long to wait before starting a leader election.
    pub election_timeout: Duration,

    pub heartbeat_interval: Duration,

    /// Longest network split to tolerate. Not read yet.
    pub partition_tolerance: Duration,
}

/// An incremental checkpoint, holding the changes made since its base
/// checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IncrementalCheckpoint {
    pub checkpoint_id: CheckpointId,

    /// Checkpoint this one builds on.
    pub base_checkpoint_id: CheckpointId,

    /// Position in the incremental chain.
    pub sequence_number: u64,

    /// Creation time in microseconds since the Unix epoch.
    pub created_at: u64,

    /// Changes for each component.
    pub component_changes: HashMap<ComponentId, ChangeLog>,

    /// Size in bytes, after any compression.
    pub total_size: usize,

    /// Compressed size over original size. 1.0 means no compression.
    pub compression_ratio: f64,

    /// First and last WAL offset covered.
    pub wal_offset_range: (u64, u64),
}

/// Result of creating an incremental checkpoint.
#[derive(Debug)]
pub struct CheckpointResult {
    pub checkpoint: IncrementalCheckpoint,

    /// Time taken to create it.
    pub duration: Duration,

    /// State operations included.
    pub operation_count: usize,

    /// Where the backend stored it.
    pub storage_location: String,
}

/// Ties the WAL, merger, backend, recovery engine and optional coordinator
/// together. No methods are built on it yet.
#[allow(dead_code)]
pub struct IncrementalCheckpointSystem {
    config: IncrementalCheckpointConfig,

    wal: Arc<dyn WriteAheadLog>,

    merger: Arc<dyn CheckpointMerger>,

    backend: Arc<dyn PersistenceBackend>,

    recovery: Arc<dyn RecoveryEngine>,

    /// Set only in cluster mode.
    coordinator: Option<Arc<dyn DistributedCoordinator>>,

    /// Incremental checkpoints since the last full one.
    current_chain: Vec<IncrementalCheckpoint>,

    last_full_checkpoint: Option<CheckpointId>,

    metrics: IncrementalCheckpointMetrics,
}

/// Counters for incremental checkpoints.
#[derive(Debug, Default)]
pub struct IncrementalCheckpointMetrics {
    pub total_incrementals: u64,

    pub total_full_checkpoints: u64,

    /// Bytes.
    pub avg_incremental_size: usize,

    pub avg_incremental_duration: Duration,

    pub avg_compression_ratio: f64,

    /// WAL operations per second.
    pub wal_throughput: f64,

    pub avg_recovery_time: Duration,

    pub failed_checkpoints: u64,
}

// Traits for the swappable parts.

/// Append only log of state changes.
pub trait WriteAheadLog: Send + Sync {
    /// Append one entry and return its offset.
    fn append(&self, entry: LogEntry) -> Result<LogOffset, StateError>;

    /// Append several entries and return their offsets. Meant to be all or
    /// nothing, though [`write_ahead_log::SegmentedWAL`] does not guarantee it.
    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Vec<LogOffset>, StateError>;

    /// Read up to `limit` entries starting at `offset`.
    fn read_from(&self, offset: LogOffset, limit: usize) -> Result<Vec<LogEntry>, StateError>;

    /// Returns the offset the next entry will get.
    fn tail_offset(&self) -> Result<LogOffset, StateError>;

    /// Drop entries before `offset` to free space.
    fn trim_to(&self, offset: LogOffset) -> Result<(), StateError>;

    /// Make written entries durable on disk.
    fn sync(&self) -> Result<(), StateError>;
}

/// Combines incremental checkpoints.
pub trait CheckpointMerger: Send + Sync {
    /// Apply a chain of incremental checkpoints to a base snapshot.
    fn merge_incrementals(
        &self,
        base: &StateSnapshot,
        incrementals: &[IncrementalCheckpoint],
    ) -> Result<StateSnapshot, StateError>;

    /// Build an incremental checkpoint from per component changes.
    fn create_incremental(
        &self,
        base_checkpoint_id: CheckpointId,
        changes: HashMap<ComponentId, ChangeLog>,
        wal_range: (u64, u64),
    ) -> Result<IncrementalCheckpoint, StateError>;

    /// Shorten a chain by merging neighboring checkpoints.
    fn optimize_chain(
        &self,
        chain: &[IncrementalCheckpoint],
    ) -> Result<Vec<IncrementalCheckpoint>, StateError>;
}

/// Stores and loads checkpoints.
pub trait PersistenceBackend: Send + Sync {
    /// Store a full checkpoint and return where it was stored.
    fn store_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        snapshot: &StateSnapshot,
    ) -> Result<String, StateError>;

    /// Store an incremental checkpoint and return where it was stored.
    fn store_incremental_checkpoint(
        &self,
        checkpoint: &IncrementalCheckpoint,
    ) -> Result<String, StateError>;

    fn load_full_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError>;

    fn load_incremental_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<IncrementalCheckpoint, StateError>;

    /// Returns the stored checkpoint ids.
    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>, StateError>;

    /// Delete checkpoints older than `before` and return how many were deleted.
    fn cleanup_checkpoints(&self, before: Instant) -> Result<usize, StateError>;
}

/// Rebuilds state from checkpoints.
pub trait RecoveryEngine: Send + Sync {
    /// Returns each component's state from a full checkpoint.
    fn recover_from_full(
        &self,
        checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError>;

    /// Returns each component's state after applying incremental checkpoints
    /// from the base up to the target.
    fn recover_from_incrementals(
        &self,
        base_checkpoint_id: CheckpointId,
        target_checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError>;

    /// Returns the checkpoints to load to reach the state at `target_time`.
    fn find_recovery_path(
        &self,
        target_time: Instant,
        components: &[ComponentId],
    ) -> Result<RecoveryPath, StateError>;
}

/// Coordinates checkpoints across a cluster.
pub trait DistributedCoordinator: Send + Sync {
    /// Start a checkpoint across the cluster.
    fn initiate_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<(), StateError>;

    /// Record whether a node finished the checkpoint.
    fn report_completion(
        &self,
        checkpoint_id: CheckpointId,
        node_id: &str,
        success: bool,
    ) -> Result<(), StateError>;

    /// Wait until enough nodes finish the checkpoint. Returns false on timeout
    /// or failure.
    fn wait_for_completion(
        &self,
        checkpoint_id: CheckpointId,
        timeout: Duration,
    ) -> Result<bool, StateError>;

    fn cluster_health(&self) -> Result<ClusterHealth, StateError>;
}

/// One WAL entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    /// Component that made the change.
    pub component_id: ComponentId,

    /// Sequence number within the component.
    pub sequence: u64,

    /// Set by the caller. Tests use microseconds since the Unix epoch.
    pub timestamp: u64,

    /// The state change.
    pub change: ChangeLog,

    pub metadata: HashMap<String, String>,
}

/// Position of an entry in the WAL, counted in entries.
pub type LogOffset = u64;

/// Checkpoints to load for a recovery.
#[derive(Debug)]
pub struct RecoveryPath {
    /// Full checkpoint to start from.
    pub base_checkpoint: CheckpointId,

    /// Incremental checkpoints to apply, in order.
    pub incremental_chain: Vec<CheckpointId>,

    pub estimated_duration: Duration,

    /// Bytes to process.
    pub total_size: usize,
}

/// Health of the cluster.
#[derive(Debug)]
pub struct ClusterHealth {
    pub total_nodes: usize,

    pub healthy_nodes: usize,

    /// Current leader, if known.
    pub leader_node: Option<String>,

    pub partition_status: PartitionStatus,
}

/// Whether the network has split the cluster into groups that cannot reach
/// each other.
#[derive(Debug)]
pub enum PartitionStatus {
    Healthy,
    MinorPartition {
        affected_nodes: Vec<String>,
    },
    MajorPartition {
        majority_nodes: Vec<String>,
    },
    /// Groups on both sides think they lead.
    SplitBrain,
}

// Errors specific to incremental checkpoints. Not used yet. The traits above
// return StateError instead.
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
