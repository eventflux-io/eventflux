// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/persistence/mod.rs

pub mod data_source;
pub mod persistence_store; // For PersistenceStore traits
pub mod snapshot_service;

// Enhanced state management system (Phase 1)
pub mod state_holder;
pub mod state_manager;
pub mod state_registry;

// Incremental checkpointing system (Phase 2)
pub mod incremental;

pub use self::data_source::{DataSource, DataSourceConfig, SqliteDataSource};
pub use self::persistence_store::{
    FilePersistenceStore, InMemoryPersistenceStore, IncrementalPersistenceStore, PersistenceStore,
    RedisPersistenceStore, SqlitePersistenceStore,
};
pub use self::snapshot_service::{PersistReport, SnapshotService};

// Enhanced state management exports
pub use self::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, ComponentId, CompressionType, SchemaVersion,
    SerializationHints, StateError, StateHolder, StateMetadata, StateSize, StateSnapshot,
};
pub use self::state_manager::{
    CheckpointHandle, CheckpointMode, RecoveryStats, SchemaMigration, StateConfig, StateMetrics,
    UnifiedStateManager,
};
pub use self::state_registry::{
    ComponentMetadata, ComponentPriority, ResourceRequirements, StateDependencyGraph,
    StateRegistry, StateTopology,
};

// Incremental checkpointing exports
pub use self::incremental::checkpoint_merger::MergerConfig;
pub use self::incremental::recovery_engine::RecoveryConfig;
pub use self::incremental::write_ahead_log::WALConfig;
pub use self::incremental::{
    CheckpointMerger, ClusterHealth, DistributedConfig, DistributedCoordinator,
    IncrementalCheckpoint, IncrementalCheckpointConfig, LogEntry, LogOffset, PartitionStatus,
    PersistenceBackend, PersistenceBackendConfig, RecoveryEngine, RecoveryPath, WriteAheadLog,
};
