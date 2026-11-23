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

// src/core/persistence/state_manager.rs

//! Unified state manager.
//!
//! The main entry point for state checkpoints, recovery and schema migration
//! across every component in a [`StateRegistry`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};

use super::state_holder::{
    CheckpointId, ComponentId, CompressionType, SerializationHints, StateError, StateHolder,
    StateSnapshot,
};
use super::state_registry::StateRegistry;

#[cfg(test)]
use super::state_holder::{AccessPattern, ChangeLog, StateSize};

/// Tracks one checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointHandle {
    pub checkpoint_id: CheckpointId,
    pub started_at: Instant,
    pub mode: CheckpointMode,
    /// Always `None` for now.
    pub estimated_completion: Option<Instant>,
}

/// Result of a recovery.
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    pub checkpoint_id: CheckpointId,
    pub recovery_time: Duration,
    pub components_recovered: usize,
    /// Bytes in the checkpoint.
    pub total_state_size: usize,
    /// The configured `recovery_parallelism`. Recovery itself runs one component
    /// at a time.
    pub parallel_workers: usize,
}

/// A request to move a component's state to a new schema version.
#[derive(Debug, Clone)]
pub struct SchemaMigration {
    pub component_id: ComponentId,
    pub from_version: super::state_holder::SchemaVersion,
    pub to_version: super::state_holder::SchemaVersion,
    pub migration_strategy: MigrationStrategy,
}

/// How a schema migration is rolled out. None of these are carried out yet.
#[derive(Debug, Clone)]
pub enum MigrationStrategy {
    /// Migrate the live state directly, with no downtime.
    InPlace,
    /// Build the new version next to the old one, then switch over.
    BlueGreen,
    /// Migrate a share of traffic at a time, writing to both versions.
    /// `percentage` is a fraction, so 0.1 means 10 percent.
    CanaryRollout { percentage: f64 },
}

/// Kinds of checkpoint.
///
/// [`UnifiedStateManager::checkpoint`] records the mode but always takes a full
/// snapshot of every component.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointMode {
    /// Full snapshot of every component.
    Full,

    /// Only the changes since checkpoint `base`.
    Incremental { base: CheckpointId },

    /// Mix of incremental and full checkpoints. Thresholds are in bytes.
    Hybrid {
        incremental_threshold: usize,
        differential_threshold: usize,
    },

    /// Waits for barriers to line up across inputs, for consistency.
    /// `timeout` caps the wait.
    Aligned { timeout: Duration },

    /// Does not wait for barriers, for lower latency.
    Unaligned,
}

/// State management settings.
#[derive(Debug, Clone)]
pub struct StateConfig {
    pub checkpoint_mode: CheckpointMode,
    pub checkpoint_interval: Duration,
    pub max_concurrent_checkpoints: usize,
    pub compression_type: CompressionType,
    pub enable_async_checkpointing: bool,
    pub recovery_parallelism: usize,
    pub state_retention_time: Duration,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            checkpoint_mode: CheckpointMode::Hybrid {
                incremental_threshold: 1024 * 1024,       // 1 MB
                differential_threshold: 10 * 1024 * 1024, // 10 MB
            },
            checkpoint_interval: Duration::from_secs(30),
            max_concurrent_checkpoints: 2,
            compression_type: CompressionType::LZ4,
            enable_async_checkpointing: true,
            recovery_parallelism: 4,
            state_retention_time: Duration::from_secs(3600 * 24), // 24 hours
        }
    }
}

/// Counters for checkpoints and recoveries.
#[derive(Debug, Clone, Default)]
pub struct StateMetrics {
    pub total_checkpoints: u64,
    pub successful_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub total_checkpoint_time: Duration,
    pub average_checkpoint_size: usize,
    pub last_checkpoint_id: Option<CheckpointId>,
    pub recovery_count: u64,
    pub total_recovery_time: Duration,
}

impl StateMetrics {
    /// Count a checkpoint. Time and size only count toward the averages when
    /// `success` is true.
    pub fn record_checkpoint(&mut self, duration: Duration, size: usize, success: bool) {
        self.total_checkpoints += 1;
        if success {
            self.successful_checkpoints += 1;
            self.total_checkpoint_time += duration;

            // Running average over successful checkpoints, in whole bytes.
            let total_successful = self.successful_checkpoints;
            self.average_checkpoint_size =
                (self.average_checkpoint_size * (total_successful - 1) as usize + size)
                    / total_successful as usize;
        } else {
            self.failed_checkpoints += 1;
        }
    }

    pub fn record_recovery(&mut self, duration: Duration) {
        self.recovery_count += 1;
        self.total_recovery_time += duration;
    }

    /// Returns successful over total checkpoints, from 0.0 to 1.0. Returns 0.0
    /// when there are none.
    pub fn checkpoint_success_rate(&self) -> f64 {
        if self.total_checkpoints == 0 {
            0.0
        } else {
            self.successful_checkpoints as f64 / self.total_checkpoints as f64
        }
    }

    /// Returns the average time of successful checkpoints, or zero when there
    /// are none.
    pub fn average_checkpoint_time(&self) -> Duration {
        if self.successful_checkpoints == 0 {
            Duration::default()
        } else {
            self.total_checkpoint_time / self.successful_checkpoints as u32
        }
    }
}

/// Takes checkpoints of every registered component and recovers from them.
///
/// Checkpoints are kept in memory only, so they are lost when the process ends.
pub struct UnifiedStateManager {
    registry: Arc<StateRegistry>,
    config: StateConfig,
    metrics: RwLock<StateMetrics>,
    /// Checkpoints in progress.
    active_checkpoints: Mutex<HashMap<CheckpointId, CheckpointHandle>>,
    /// Next checkpoint id. Starts at 1.
    checkpoint_counter: Mutex<CheckpointId>,
    /// Completed checkpoints.
    checkpoints: RwLock<HashMap<CheckpointId, CheckpointMetadata>>,
}

/// A completed checkpoint and its component snapshots.
#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
    pub checkpoint_id: CheckpointId,
    pub created_at: Instant,
    pub mode: CheckpointMode,
    pub component_snapshots: HashMap<ComponentId, StateSnapshot>,
    /// Sum of snapshot data sizes in bytes.
    pub total_size: usize,
    /// Always 1.0 for now, since sizes are not measured before compression.
    pub compression_ratio: f64,
}

impl UnifiedStateManager {
    pub fn new(registry: Arc<StateRegistry>, config: StateConfig) -> Self {
        Self {
            registry,
            config,
            metrics: RwLock::new(StateMetrics::default()),
            active_checkpoints: Mutex::new(HashMap::new()),
            checkpoint_counter: Mutex::new(1),
            checkpoints: RwLock::new(HashMap::new()),
        }
    }

    /// Check the registry before use.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the component dependencies form a cycle.
    pub fn initialize(&self) -> Result<(), StateError> {
        let analysis = self.registry.analyze_dependencies();
        if !analysis.circular_dependencies.is_empty() {
            return Err(StateError::InvalidStateData {
                message: format!(
                    "Circular dependencies detected: {:?}",
                    analysis.circular_dependencies
                ),
            });
        }

        debug!(
            "State management initialized with {} components",
            analysis.total_components
        );
        debug!("Recovery parallelism: {}", analysis.parallel_recovery_width);

        Ok(())
    }

    /// Take a checkpoint and wait for it to finish.
    ///
    /// Always takes a full snapshot of every component, whatever `mode` says.
    /// The mode is only recorded. Updates the metrics either way.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if `max_concurrent_checkpoints` are already
    /// running, in which case the checkpoint id is still used up. Also returns the
    /// first error from serializing a component.
    pub fn checkpoint(&self, mode: CheckpointMode) -> Result<CheckpointHandle, StateError> {
        let start_time = Instant::now();

        let checkpoint_id = {
            let mut counter = self.checkpoint_counter.lock().unwrap();
            let id = *counter;
            *counter += 1;
            id
        };

        {
            let active = self.active_checkpoints.lock().unwrap();
            if active.len() >= self.config.max_concurrent_checkpoints {
                return Err(StateError::InvalidStateData {
                    message: "Maximum concurrent checkpoints exceeded".to_string(),
                });
            }
        }

        let handle = CheckpointHandle {
            checkpoint_id,
            started_at: start_time,
            mode: mode.clone(),
            estimated_completion: None,
        };

        {
            let mut active = self.active_checkpoints.lock().unwrap();
            active.insert(checkpoint_id, handle.clone());
        }

        // Runs on the calling thread. Async checkpoints are not supported yet.
        let result = self.execute_checkpoint(checkpoint_id, mode);

        {
            let mut active = self.active_checkpoints.lock().unwrap();
            active.remove(&checkpoint_id);
        }

        let duration = start_time.elapsed();
        let mut metrics = self.metrics.write().unwrap();

        match result {
            Ok(metadata) => {
                metrics.record_checkpoint(duration, metadata.total_size, true);
                metrics.last_checkpoint_id = Some(checkpoint_id);
                info!(
                    "Checkpoint {} completed in {:?}, size: {} bytes",
                    checkpoint_id, duration, metadata.total_size
                );
            }
            Err(e) => {
                metrics.record_checkpoint(duration, 0, false);
                error!("Checkpoint {checkpoint_id} failed: {e}");
                return Err(e);
            }
        }

        Ok(handle)
    }

    /// Serialize every component in dependency order and store the result.
    ///
    /// Stops at the first component that fails, and stores nothing in that case.
    fn execute_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        mode: CheckpointMode,
    ) -> Result<CheckpointMetadata, StateError> {
        let start_time = Instant::now();
        let _component_ids = self.registry.get_all_component_ids();
        let mut component_snapshots = HashMap::new();
        let mut total_size = 0;
        let mut uncompressed_size = 0;

        let hints = SerializationHints {
            prefer_compression: Some(self.config.compression_type.clone()),
            target_chunk_size: Some(64 * 1024), // 64 KB chunks
            priority: super::state_holder::SerializationPriority::Balanced,
        };

        // Go stage by stage in dependency order so the snapshot is consistent.
        let topology = self.registry.get_topology();
        for stage in &topology.recovery_stages {
            // Components in one stage could run in parallel, but run one by one.
            for component_id in stage {
                if let Some(component) = self.registry.get_component(component_id) {
                    match Self::checkpoint_component(component, &hints, checkpoint_id) {
                        Ok(snapshot) => {
                            uncompressed_size += snapshot.data.len();
                            total_size += snapshot.data.len();
                            component_snapshots.insert(component_id.clone(), snapshot);
                        }
                        Err(e) => {
                            error!("Failed to checkpoint component {component_id}: {e}");
                            return Err(e);
                        }
                    }
                }
            }
        }

        let compression_ratio = if uncompressed_size > 0 {
            total_size as f64 / uncompressed_size as f64
        } else {
            1.0
        };

        let metadata = CheckpointMetadata {
            checkpoint_id,
            created_at: start_time,
            mode,
            component_snapshots,
            total_size,
            compression_ratio,
        };

        {
            let mut checkpoints = self.checkpoints.write().unwrap();
            checkpoints.insert(checkpoint_id, metadata.clone());
        }

        Ok(metadata)
    }

    /// Serialize one component and stamp the snapshot with `checkpoint_id`.
    fn checkpoint_component(
        component: Arc<dyn StateHolder>,
        hints: &SerializationHints,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError> {
        let mut snapshot = component.serialize_state(hints)?;
        snapshot.checkpoint_id = checkpoint_id;
        Ok(snapshot)
    }

    /// Restore every component from a checkpoint, in dependency order.
    ///
    /// Components with no snapshot in the checkpoint are skipped. Stops at the
    /// first failure, leaving components restored before it in their new state.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointNotFound` if the id is unknown, `IncompatibleVersion` if
    /// a snapshot's schema version cannot be read by its component, or any error
    /// from restoring a component.
    pub fn recover(&self, checkpoint_id: CheckpointId) -> Result<RecoveryStats, StateError> {
        let start_time = Instant::now();

        let metadata = {
            let checkpoints = self.checkpoints.read().unwrap();
            checkpoints
                .get(&checkpoint_id)
                .cloned()
                .ok_or(StateError::CheckpointNotFound { checkpoint_id })?
        };

        let topology = self.registry.get_topology();
        let mut recovered_components = 0;

        // One component at a time, in dependency order.
        for stage in &topology.recovery_stages {
            for component_id in stage {
                if let Some(component) = self.registry.get_component(component_id) {
                    if let Some(snapshot) = metadata.component_snapshots.get(component_id) {
                        match Self::recover_component(component, snapshot.clone()) {
                            Ok(()) => {
                                recovered_components += 1;
                                info!("Recovered component: {component_id}");
                            }
                            Err(e) => {
                                error!("Failed to recover component {component_id}: {e}");
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        let recovery_time = start_time.elapsed();

        {
            let mut metrics = self.metrics.write().unwrap();
            metrics.record_recovery(recovery_time);
        }

        let stats = RecoveryStats {
            checkpoint_id,
            recovery_time,
            components_recovered: recovered_components,
            total_state_size: metadata.total_size,
            parallel_workers: self.config.recovery_parallelism,
        };

        info!("Recovery completed: {recovered_components} components in {recovery_time:?}");

        Ok(stats)
    }

    /// Restore one component from a snapshot after checking the schema version.
    fn recover_component(
        component: Arc<dyn StateHolder>,
        snapshot: StateSnapshot,
    ) -> Result<(), StateError> {
        let current_version = component.schema_version();
        if !current_version.is_compatible_with(&snapshot.version) {
            return Err(StateError::IncompatibleVersion {
                current: current_version,
                required: snapshot.version,
            });
        }

        // State holders use interior mutability, such as Arc<Mutex<T>>, so
        // deserialize_state works through a shared reference.
        component.deserialize_state(&snapshot)?;

        info!(
            "Successfully recovered component: {}",
            snapshot.metadata.component_id
        );
        Ok(())
    }

    /// Migrate a component's state to a new schema version while running.
    ///
    /// Not carried out yet. Every strategy only logs a warning and returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the component is not registered.
    pub fn migrate_schema(&self, migration: SchemaMigration) -> Result<(), StateError> {
        let _component = self
            .registry
            .get_component(&migration.component_id)
            .ok_or_else(|| StateError::InvalidStateData {
                message: format!("Component '{}' not found", migration.component_id),
            })?;

        match migration.migration_strategy {
            MigrationStrategy::InPlace => {
                info!(
                    "Starting in-place migration for component '{}' from {} to {}",
                    migration.component_id, migration.from_version, migration.to_version
                );

                // TODO: in place migration. Needs careful coordination.
                warn!("In-place migration not yet implemented");
                Ok(())
            }
            MigrationStrategy::BlueGreen => {
                info!(
                    "Starting blue-green migration for component '{}'",
                    migration.component_id
                );

                // TODO: blue green migration. Needs a new component version
                // built next to the old one.
                warn!("Blue-green migration not yet implemented");
                Ok(())
            }
            MigrationStrategy::CanaryRollout { percentage } => {
                info!(
                    "Starting canary rollout migration for component '{}' at {}%",
                    migration.component_id,
                    percentage * 100.0
                );

                // TODO: canary migration. Needs traffic moved over a share at a
                // time.
                warn!("Canary rollout migration not yet implemented");
                Ok(())
            }
        }
    }

    /// Returns a copy of the metrics.
    pub fn get_metrics(&self) -> StateMetrics {
        let metrics = self.metrics.read().unwrap();
        metrics.clone()
    }

    /// Returns every stored checkpoint, oldest id first.
    pub fn get_checkpoint_history(&self) -> Vec<CheckpointMetadata> {
        let checkpoints = self.checkpoints.read().unwrap();
        let mut history: Vec<_> = checkpoints.values().cloned().collect();
        history.sort_by_key(|m| m.checkpoint_id);
        history
    }

    /// Remove checkpoints older than `state_retention_time` and return how many
    /// were removed. Never returns an error.
    pub fn cleanup_old_checkpoints(&self) -> Result<usize, StateError> {
        let mut removed_count = 0;
        let retention_cutoff = Instant::now() - self.config.state_retention_time;

        {
            let mut checkpoints = self.checkpoints.write().unwrap();
            let ids_to_remove: Vec<_> = checkpoints
                .iter()
                .filter(|(_, metadata)| metadata.created_at < retention_cutoff)
                .map(|(id, _)| *id)
                .collect();

            for id in ids_to_remove {
                checkpoints.remove(&id);
                removed_count += 1;
            }
        }

        if removed_count > 0 {
            info!("Cleaned up {removed_count} old checkpoints");
        }

        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Holds one number as its state.
    struct MockStateHolder {
        id: String,
        data: Arc<AtomicUsize>,
    }

    impl StateHolder for MockStateHolder {
        fn schema_version(&self) -> super::super::state_holder::SchemaVersion {
            super::super::state_holder::SchemaVersion::new(1, 0, 0)
        }

        fn serialize_state(
            &self,
            _hints: &SerializationHints,
        ) -> Result<StateSnapshot, StateError> {
            let data = self.data.load(Ordering::Relaxed).to_le_bytes().to_vec();
            let checksum = StateSnapshot::calculate_checksum(&data);

            Ok(StateSnapshot {
                version: self.schema_version(),
                checkpoint_id: 0,
                data,
                compression: CompressionType::None,
                checksum,
                metadata: self.component_metadata(),
            })
        }

        fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
            if snapshot.data.len() != 8 {
                return Err(StateError::InvalidStateData {
                    message: "Invalid data size".to_string(),
                });
            }

            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&snapshot.data);
            let value = usize::from_le_bytes(bytes);
            self.data.store(value, Ordering::Relaxed);

            Ok(())
        }

        fn get_changelog(&self, _since: CheckpointId) -> Result<ChangeLog, StateError> {
            Ok(ChangeLog::new(0, 1))
        }

        fn apply_changelog(&self, _changes: &ChangeLog) -> Result<(), StateError> {
            Ok(())
        }

        fn estimate_size(&self) -> StateSize {
            StateSize {
                bytes: 8,
                entries: 1,
                estimated_growth_rate: 0.0,
            }
        }

        fn access_pattern(&self) -> AccessPattern {
            AccessPattern::Hot
        }

        fn component_metadata(&self) -> super::super::state_holder::StateMetadata {
            super::super::state_holder::StateMetadata::new(
                self.id.clone(),
                "MockComponent".to_string(),
            )
        }
    }

    #[test]
    fn test_unified_state_manager_initialization() {
        let registry = Arc::new(StateRegistry::new());
        let config = StateConfig::default();
        let manager = UnifiedStateManager::new(registry, config);

        assert!(manager.initialize().is_ok());
    }

    #[test]
    fn test_checkpoint_and_recovery() {
        let registry = Arc::new(StateRegistry::new());

        let component = Arc::new(MockStateHolder {
            id: "test1".to_string(),
            data: Arc::new(AtomicUsize::new(42)),
        });

        let metadata = super::super::state_registry::ComponentMetadata {
            component_id: "test1".to_string(),
            component_type: "MockComponent".to_string(),
            dependencies: Vec::new(),
            dependents: Vec::new(),
            priority: super::super::state_registry::ComponentPriority::Medium,
            resource_requirements: super::super::state_registry::ResourceRequirements {
                memory_estimate: 1024,
                cpu_weight: 1.0,
                io_intensity: super::super::state_registry::IoIntensity::Light,
                network_requirements: super::super::state_registry::NetworkRequirements {
                    requires_replication: false,
                    bandwidth_estimate: None,
                },
            },
        };

        registry
            .register("test1".to_string(), component, metadata)
            .unwrap();

        let config = StateConfig::default();
        let manager = UnifiedStateManager::new(registry, config);

        assert!(manager.initialize().is_ok());

        let _handle = manager.checkpoint(CheckpointMode::Full).unwrap();

        let history = manager.get_checkpoint_history();
        assert!(!history.is_empty());
    }
}
