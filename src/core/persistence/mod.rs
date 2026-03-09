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
