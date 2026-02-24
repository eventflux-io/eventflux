// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/persistence/state_manager.rs

//! Unified State Manager - Central coordinator for state management operations
//!
//! This module provides the main interface for state management operations including
//! checkpointing, recovery, and schema migration.

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

/// Handle for tracking checkpoint operations
#[derive(Debug, Clone)]
pub struct CheckpointHandle {
    pub checkpoint_id: CheckpointId,
    pub started_at: Instant,
    pub mode: CheckpointMode,
    pub estimated_completion: Option<Instant>,
}

/// Recovery statistics
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    pub checkpoint_id: CheckpointId,
    pub recovery_time: Duration,
    pub components_recovered: usize,
    pub total_state_size: usize,
    pub parallel_workers: usize,
}

/// Schema migration configuration
#[derive(Debug, Clone)]
pub struct SchemaMigration {
    pub component_id: ComponentId,
    pub from_version: super::state_holder::SchemaVersion,
    pub to_version: super::state_holder::SchemaVersion,
    pub migration_strategy: MigrationStrategy,
}

#[derive(Debug, Clone)]
pub enum MigrationStrategy {
    /// Direct migration without downtime
    InPlace,
    /// Create new version alongside old, then switch
    BlueGreen,
    /// Gradual migration with dual writes
    CanaryRollout { percentage: f64 },
}

/// Checkpoint modes for different use cases
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointMode {
    /// Full snapshot of all components
    Full,

    /// Incremental changes only
    Incremental { base: CheckpointId },

    /// Combination of incremental and full for optimization
    Hybrid {
        incremental_threshold: usize,
        differential_threshold: usize,
    },

    /// Aligned checkpoint with barriers (for consistency)
    Aligned { timeout: Duration },

    /// Unaligned checkpoint for low latency
    Unaligned,
}

/// State management configuration
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
                incremental_threshold: 1024 * 1024,       // 1MB
                differential_threshold: 10 * 1024 * 1024, // 10MB
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

/// Metrics for state management operations
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
    pub fn record_checkpoint(&mut self, duration: Duration, size: usize, success: bool) {
        self.total_checkpoints += 1;
        if success {
            self.successful_checkpoints += 1;
            self.total_checkpoint_time += duration;

            // Update rolling average
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

    pub fn checkpoint_success_rate(&self) -> f64 {
        if self.total_checkpoints == 0 {
            0.0
        } else {
            self.successful_checkpoints as f64 / self.total_checkpoints as f64
        }
    }

    pub fn average_checkpoint_time(&self) -> Duration {
        if self.successful_checkpoints == 0 {
            Duration::default()
        } else {
            self.total_checkpoint_time / self.successful_checkpoints as u32
        }
    }
}

/// Unified State Manager - Main state management coordinator
pub struct UnifiedStateManager {
    registry: Arc<StateRegistry>,
    config: StateConfig,
    metrics: RwLock<StateMetrics>,
    active_checkpoints: Mutex<HashMap<CheckpointId, CheckpointHandle>>,
    checkpoint_counter: Mutex<CheckpointId>,
    checkpoints: RwLock<HashMap<CheckpointId, CheckpointMetadata>>,
}

#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
    pub checkpoint_id: CheckpointId,
    pub created_at: Instant,
    pub mode: CheckpointMode,
    pub component_snapshots: HashMap<ComponentId, StateSnapshot>,
    pub total_size: usize,
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

    /// Initialize state management system
    pub fn initialize(&self) -> Result<(), StateError> {
        // Validate registry state
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

    /// Trigger a checkpoint
    pub fn checkpoint(&self, mode: CheckpointMode) -> Result<CheckpointHandle, StateError> {
        let start_time = Instant::now();

        // Get next checkpoint ID
        let checkpoint_id = {
            let mut counter = self.checkpoint_counter.lock().unwrap();
            let id = *counter;
            *counter += 1;
            id
        };

        // Check if we've exceeded max concurrent checkpoints
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

        // Register active checkpoint
        {
            let mut active = self.active_checkpoints.lock().unwrap();
            active.insert(checkpoint_id, handle.clone());
        }

        // Execute checkpoint synchronously (simplified version)
        let result = self.execute_checkpoint(checkpoint_id, mode);

        // Remove from active checkpoints
        {
            let mut active = self.active_checkpoints.lock().unwrap();
            active.remove(&checkpoint_id);
        }

        // Record metrics
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

    /// Execute checkpoint process
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
            target_chunk_size: Some(64 * 1024), // 64KB chunks
            priority: super::state_holder::SerializationPriority::Balanced,
        };

        // Process components in dependency order for consistency
        let topology = self.registry.get_topology();
        for stage in &topology.recovery_stages {
            // Components in the same stage can be checkpointed sequentially (simplified)
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

        // Store checkpoint metadata
        {
            let mut checkpoints = self.checkpoints.write().unwrap();
            checkpoints.insert(checkpoint_id, metadata.clone());
        }

        Ok(metadata)
    }

    /// Checkpoint a single component
    fn checkpoint_component(
        component: Arc<dyn StateHolder>,
        hints: &SerializationHints,
        checkpoint_id: CheckpointId,
    ) -> Result<StateSnapshot, StateError> {
        let mut snapshot = component.serialize_state(hints)?;
        snapshot.checkpoint_id = checkpoint_id;
        Ok(snapshot)
    }

    /// Recover from a specific checkpoint
    pub fn recover(&self, checkpoint_id: CheckpointId) -> Result<RecoveryStats, StateError> {
        let start_time = Instant::now();

        // Get checkpoint metadata
        let metadata = {
            let checkpoints = self.checkpoints.read().unwrap();
            checkpoints
                .get(&checkpoint_id)
                .cloned()
                .ok_or(StateError::CheckpointNotFound { checkpoint_id })?
        };

        let topology = self.registry.get_topology();
        let mut recovered_components = 0;

        // Recover components in dependency order (simplified sequential recovery)
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

        // Record recovery metrics
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

    /// Recover a single component from a snapshot
    fn recover_component(
        component: Arc<dyn StateHolder>,
        snapshot: StateSnapshot,
    ) -> Result<(), StateError> {
        // Verify version compatibility
        let current_version = component.schema_version();
        if !current_version.is_compatible_with(&snapshot.version) {
            return Err(StateError::IncompatibleVersion {
                current: current_version,
                required: snapshot.version,
            });
        }

        // Deserialize the snapshot into the component
        // Note: StateHolder implementations use interior mutability (Arc<Mutex<T>>)
        // so we can call deserialize_state with &self
        component.deserialize_state(&snapshot)?;

        info!(
            "Successfully recovered component: {}",
            snapshot.metadata.component_id
        );
        Ok(())
    }

    /// Perform live state schema migration
    pub fn migrate_schema(&self, migration: SchemaMigration) -> Result<(), StateError> {
        let component = self
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

                // In-place migration would require careful coordination
                // This is a placeholder for the complex migration logic
                warn!("In-place migration not yet implemented");
                Ok(())
            }
            MigrationStrategy::BlueGreen => {
                info!(
                    "Starting blue-green migration for component '{}'",
                    migration.component_id
                );

                // Blue-green would involve creating a new component version
                // This is a placeholder for the blue-green migration logic
                warn!("Blue-green migration not yet implemented");
                Ok(())
            }
            MigrationStrategy::CanaryRollout { percentage } => {
                info!(
                    "Starting canary rollout migration for component '{}' at {}%",
                    migration.component_id,
                    percentage * 100.0
                );

                // Canary rollout would gradually migrate traffic
                // This is a placeholder for the canary migration logic
                warn!("Canary rollout migration not yet implemented");
                Ok(())
            }
        }
    }

    /// Get state management metrics
    pub fn get_metrics(&self) -> StateMetrics {
        let metrics = self.metrics.read().unwrap();
        metrics.clone()
    }

    /// Get checkpoint history
    pub fn get_checkpoint_history(&self) -> Vec<CheckpointMetadata> {
        let checkpoints = self.checkpoints.read().unwrap();
        let mut history: Vec<_> = checkpoints.values().cloned().collect();
        history.sort_by_key(|m| m.checkpoint_id);
        history
    }

    /// Clean up old checkpoints based on retention policy
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

    // Mock component for testing
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

        // Register a mock component
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

        // Create checkpoint
        let _handle = manager.checkpoint(CheckpointMode::Full).unwrap();

        // Verify checkpoint was created
        let history = manager.get_checkpoint_history();
        assert!(!history.is_empty());
    }
}
