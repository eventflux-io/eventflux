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

// src/core/persistence/incremental/recovery_engine.rs

//! Recovery engine.
//!
//! Rebuilds component state from a full checkpoint, or from a full checkpoint
//! plus a chain of incremental ones, and picks a checkpoint close to a point in
//! time.
//!
//! Recovery runs one step at a time, not in parallel. The checkpoint info cache
//! that incremental recovery and point in time lookup rely on is never filled
//! outside tests, and applying incremental changes is still a stand in.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{
    CheckpointMerger, IncrementalCheckpoint, PersistenceBackend, RecoveryEngine, RecoveryPath,
};
use crate::core::persistence::state_holder::{
    CheckpointId, ComponentId, StateError, StateSnapshot,
};

/// Recovery engine that loads checkpoints from a backend.
#[allow(dead_code)]
pub struct AdvancedRecoveryEngine {
    backend: Arc<dyn PersistenceBackend>,

    /// Not used yet.
    merger: Arc<dyn CheckpointMerger>,

    config: RecoveryConfig,

    /// Info about known checkpoints, keyed by id. Starts empty and is only
    /// filled by the unused `build_checkpoint_cache` or by tests.
    checkpoint_cache: HashMap<CheckpointId, CheckpointInfo>,

    /// Never updated yet.
    stats: RecoveryStatistics,
}

/// Recovery engine settings.
///
/// Only `verification_level` is passed along, and it does not change the checks
/// yet. The other fields are not read.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Most recovery threads. Default 4.
    pub max_parallel_threads: usize,

    /// Default 5 minutes.
    pub recovery_timeout: Duration,

    /// Keep going when one component fails. Default true.
    pub optimistic_recovery: bool,

    /// Default `Adaptive`.
    pub prefetch_strategy: PrefetchStrategy,

    /// Default `Standard`.
    pub verification_level: VerificationLevel,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_parallel_threads: 4,
            recovery_timeout: Duration::from_secs(300),
            optimistic_recovery: true,
            prefetch_strategy: PrefetchStrategy::Adaptive,
            verification_level: VerificationLevel::Standard,
        }
    }
}

/// When to load checkpoints ahead of time. Not used yet.
#[derive(Debug, Clone)]
pub enum PrefetchStrategy {
    None,

    /// Load the next checkpoint in the chain.
    Sequential,

    /// Load based on past access.
    Adaptive,

    /// Load every checkpoint on the recovery path.
    Aggressive,
}

/// How much to check during recovery.
///
/// Every level currently runs the same checksum check on each result.
#[derive(Debug, Clone)]
pub enum VerificationLevel {
    /// Checksums only.
    Basic,

    /// Checksums plus integrity checks.
    Standard,

    /// Everything, including dependency checks.
    Full,
}

/// What the engine knows about one checkpoint, for planning.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CheckpointInfo {
    pub checkpoint_id: CheckpointId,

    pub checkpoint_type: CheckpointType,

    /// Creation time in microseconds since the Unix epoch.
    pub created_at: u64,

    pub size_bytes: usize,

    /// Checkpoints this one builds on.
    pub dependencies: Vec<CheckpointId>,

    /// Components included.
    pub components: HashSet<ComponentId>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum CheckpointType {
    Full,
    Incremental { base: CheckpointId },
}

/// Recovery counters. Never updated yet.
#[derive(Debug, Default)]
pub struct RecoveryStatistics {
    pub total_recoveries: u64,

    pub avg_recovery_time: Duration,

    /// Bytes recovered.
    pub total_data_recovered: u64,

    pub failed_recoveries: u64,

    pub point_in_time_recoveries: u64,

    pub parallel_efficiency: f64,
}

/// Steps to recover a set of components.
#[derive(Debug)]
#[allow(dead_code)]
struct RecoveryPlan {
    /// One stage per checkpoint in the chain, oldest first. Run one at a time.
    pub stages: Vec<RecoveryStage>,

    /// Rough estimate.
    pub estimated_duration: Duration,

    /// Bytes to process.
    pub total_size: usize,

    /// The checkpoint chain, oldest first.
    pub checkpoints_needed: Vec<CheckpointId>,
}

/// One recovery step, which applies one checkpoint.
#[derive(Debug)]
#[allow(dead_code)]
struct RecoveryStage {
    pub components: Vec<ComponentId>,

    pub checkpoint_id: CheckpointId,

    /// Checkpoints this stage builds on.
    pub dependencies: Vec<CheckpointId>,

    /// Rough estimate.
    pub estimated_duration: Duration,
}

/// Progress of one recovery run.
#[derive(Debug)]
#[allow(dead_code)]
struct RecoveryContext {
    pub target_components: HashSet<ComponentId>,

    /// Components already restored from a full checkpoint.
    pub recovered_components: HashSet<ComponentId>,

    /// Checkpoints loaded so far, so each is loaded once.
    pub loaded_checkpoints: HashMap<CheckpointId, CachedCheckpoint>,

    pub start_time: Instant,

    /// State rebuilt so far for each component.
    pub intermediate_results: HashMap<ComponentId, StateSnapshot>,
}

/// A loaded checkpoint.
#[derive(Debug, Clone)]
enum CachedCheckpoint {
    Full(StateSnapshot),
    Incremental(IncrementalCheckpoint),
}

impl AdvancedRecoveryEngine {
    /// Create an engine with an empty checkpoint info cache.
    pub fn new(
        backend: Arc<dyn PersistenceBackend>,
        merger: Arc<dyn CheckpointMerger>,
        config: RecoveryConfig,
    ) -> Self {
        Self {
            backend,
            merger,
            config,
            checkpoint_cache: HashMap::new(),
            stats: RecoveryStatistics::default(),
        }
    }

    /// Fill the checkpoint info cache from the backend's checkpoint list.
    ///
    /// Not called anywhere yet. Every entry is guessed. It is marked full,
    /// stamped with the current time, and given a size of 1000 bytes.
    #[allow(dead_code)]
    fn build_checkpoint_cache(&mut self) -> Result<(), StateError> {
        let checkpoint_ids = self.backend.list_checkpoints()?;

        for checkpoint_id in checkpoint_ids {
            // TODO: read the real type and metadata from the backend.
            let info = CheckpointInfo {
                checkpoint_id,
                checkpoint_type: CheckpointType::Full, // guessed
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                size_bytes: 1000, // guessed
                dependencies: Vec::new(),
                components: HashSet::new(),
            };

            self.checkpoint_cache.insert(checkpoint_id, info);
        }

        Ok(())
    }

    /// Plan one stage for each checkpoint in the chain that ends at the target.
    ///
    /// Each stage is estimated at 1 ms per 1000 bytes.
    fn create_recovery_plan(
        &self,
        target_checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<RecoveryPlan, StateError> {
        let checkpoint_chain = self.find_checkpoint_chain(target_checkpoint_id)?;

        let mut stages = Vec::new();
        let mut total_size = 0;
        let mut estimated_duration = Duration::default();

        for &checkpoint_id in &checkpoint_chain {
            if let Some(checkpoint_info) = self.checkpoint_cache.get(&checkpoint_id) {
                let stage = RecoveryStage {
                    components: components.to_vec(),
                    checkpoint_id,
                    dependencies: checkpoint_info.dependencies.clone(),
                    estimated_duration: Duration::from_millis(
                        checkpoint_info.size_bytes as u64 / 1000,
                    ), // rough estimate
                };

                total_size += checkpoint_info.size_bytes;
                estimated_duration += stage.estimated_duration;
                stages.push(stage);
            }
        }

        Ok(RecoveryPlan {
            stages,
            estimated_duration,
            total_size,
            checkpoints_needed: checkpoint_chain,
        })
    }

    /// Returns the checkpoints needed to reach the target, oldest first.
    ///
    /// Walks back through incremental bases from the target until it reaches a
    /// full checkpoint or an id missing from the cache. A missing id ends the
    /// chain early without an error, and a target missing from the cache gives an
    /// empty chain. Loops forever if the bases form a cycle.
    fn find_checkpoint_chain(
        &self,
        target_checkpoint_id: CheckpointId,
    ) -> Result<Vec<CheckpointId>, StateError> {
        let mut chain = Vec::new();
        let mut current_id = target_checkpoint_id;

        while let Some(checkpoint_info) = self.checkpoint_cache.get(&current_id) {
            chain.push(current_id);

            match &checkpoint_info.checkpoint_type {
                CheckpointType::Full => {
                    break;
                }
                CheckpointType::Incremental { base } => {
                    current_id = *base;
                }
            }
        }

        chain.reverse();
        Ok(chain)
    }

    /// Run the plan stages one after another and return the rebuilt states.
    fn execute_recovery_plan(
        &self,
        plan: &RecoveryPlan,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError> {
        let mut context = RecoveryContext {
            target_components: components.iter().cloned().collect(),
            recovered_components: HashSet::new(),
            loaded_checkpoints: HashMap::new(),
            start_time: Instant::now(),
            intermediate_results: HashMap::new(),
        };

        // One stage at a time. Stages are not run in parallel yet.
        for stage in &plan.stages {
            self.execute_recovery_stage(stage, &mut context)?;
        }

        Ok(context.intermediate_results)
    }

    /// Load the stage's checkpoint, if not loaded yet, and apply it to each
    /// target component.
    ///
    /// A full checkpoint sets a component's state once. An incremental one
    /// applies its changes on top of the state built so far.
    fn execute_recovery_stage(
        &self,
        stage: &RecoveryStage,
        context: &mut RecoveryContext,
    ) -> Result<(), StateError> {
        if let std::collections::hash_map::Entry::Vacant(e) =
            context.loaded_checkpoints.entry(stage.checkpoint_id)
        {
            let cached_checkpoint = self.load_checkpoint(stage.checkpoint_id)?;
            e.insert(cached_checkpoint);
        }

        // Cloned so `context` can be borrowed mutably below.
        let cached_checkpoint = context
            .loaded_checkpoints
            .get(&stage.checkpoint_id)
            .unwrap()
            .clone();

        match cached_checkpoint {
            CachedCheckpoint::Full(snapshot) => {
                for component_id in &stage.components {
                    if context.target_components.contains(component_id)
                        && !context.recovered_components.contains(component_id)
                    {
                        let component_snapshot =
                            self.extract_component_state(&snapshot, component_id)?;
                        context
                            .intermediate_results
                            .insert(component_id.clone(), component_snapshot);
                        context.recovered_components.insert(component_id.clone());
                    }
                }
            }
            CachedCheckpoint::Incremental(incremental) => {
                for component_id in &stage.components {
                    if context.target_components.contains(component_id) {
                        self.apply_incremental_changes(&incremental, component_id, context)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Load a checkpoint, trying it as a full checkpoint first and then as an
    /// incremental one.
    ///
    /// # Errors
    ///
    /// Returns the incremental load error when both fail. The full load error is
    /// dropped.
    fn load_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<CachedCheckpoint, StateError> {
        match self.backend.load_full_checkpoint(checkpoint_id) {
            Ok(snapshot) => Ok(CachedCheckpoint::Full(snapshot)),
            Err(_) => {
                let incremental = self.backend.load_incremental_checkpoint(checkpoint_id)?;
                Ok(CachedCheckpoint::Incremental(incremental))
            }
        }
    }

    /// Returns one component's state from a full snapshot.
    ///
    /// A stand in that returns the whole snapshot for every component.
    fn extract_component_state(
        &self,
        snapshot: &StateSnapshot,
        _component_id: &ComponentId,
    ) -> Result<StateSnapshot, StateError> {
        // TODO: pull out only this component's data.
        Ok(snapshot.clone())
    }

    /// Apply an incremental checkpoint's changes for one component.
    ///
    /// Does nothing when the checkpoint has no changes for the component, or the
    /// component has no state built yet.
    fn apply_incremental_changes(
        &self,
        incremental: &IncrementalCheckpoint,
        component_id: &ComponentId,
        context: &mut RecoveryContext,
    ) -> Result<(), StateError> {
        if let Some(changes) = incremental.component_changes.get(component_id) {
            let base_state = context.intermediate_results.get(component_id);

            if let Some(base_state) = base_state {
                let mut updated_state = base_state.clone();

                // FIXME: stand in. Each operation in the change log must be
                // decoded and applied to the state. This appends fixed dummy
                // bytes instead, so incremental recovery gives wrong state.
                for _operation in &changes.operations {
                    updated_state.data.extend_from_slice(&[1, 2, 3]); // dummy bytes
                }

                updated_state.checksum =
                    crate::core::persistence::state_holder::StateSnapshot::calculate_checksum(
                        &updated_state.data,
                    );

                context
                    .intermediate_results
                    .insert(component_id.clone(), updated_state);
            }
        }

        Ok(())
    }

    /// Check every result's checksum. The verification level is ignored for now.
    ///
    /// # Errors
    ///
    /// Returns `ChecksumMismatch` for the first result whose data does not match
    /// its checksum.
    fn verify_recovery_results(
        &self,
        results: &HashMap<ComponentId, StateSnapshot>,
        _verification_level: &VerificationLevel,
    ) -> Result<(), StateError> {
        for snapshot in results.values() {
            let calculated_checksum =
                crate::core::persistence::state_holder::StateSnapshot::calculate_checksum(
                    &snapshot.data,
                );
            if calculated_checksum != snapshot.checksum {
                return Err(StateError::ChecksumMismatch);
            }
        }

        // TODO: extra checks for the Standard and Full levels.

        Ok(())
    }
}

impl RecoveryEngine for AdvancedRecoveryEngine {
    /// Load a full checkpoint and return its state for each component.
    ///
    /// Every component gets a copy of the whole snapshot for now.
    fn recover_from_full(
        &self,
        checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError> {
        let start_time = Instant::now();

        let snapshot = self.backend.load_full_checkpoint(checkpoint_id)?;

        let mut results = HashMap::new();
        for component_id in components {
            let component_snapshot = self.extract_component_state(&snapshot, component_id)?;
            results.insert(component_id.clone(), component_snapshot);
        }

        self.verify_recovery_results(&results, &self.config.verification_level)?;

        // TODO: update self.stats.

        println!(
            "Recovered {} components from full checkpoint {} in {:?}",
            components.len(),
            checkpoint_id,
            start_time.elapsed()
        );

        Ok(results)
    }

    /// Rebuild state by walking the checkpoint chain that ends at the target.
    ///
    /// The chain comes from the checkpoint info cache, which is empty unless
    /// filled, so the result is empty in that case. `base_checkpoint_id` is only
    /// printed. The chain's own full checkpoint is used as the base.
    fn recover_from_incrementals(
        &self,
        base_checkpoint_id: CheckpointId,
        target_checkpoint_id: CheckpointId,
        components: &[ComponentId],
    ) -> Result<HashMap<ComponentId, StateSnapshot>, StateError> {
        let start_time = Instant::now();

        let plan = self.create_recovery_plan(target_checkpoint_id, components)?;

        let results = self.execute_recovery_plan(&plan, components)?;

        self.verify_recovery_results(&results, &self.config.verification_level)?;

        // TODO: update self.stats.

        println!(
            "Recovered {} components from incremental chain {} to {} in {:?}",
            components.len(),
            base_checkpoint_id,
            target_checkpoint_id,
            start_time.elapsed()
        );

        Ok(results)
    }

    /// Pick the cached checkpoint created closest to `target_time`, before or
    /// after it.
    ///
    /// The incremental chain is always empty, and the duration and size are
    /// rough guesses of 100 ms and 1000 bytes per component.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointNotFound` with id 0 if the cache is empty, which it is
    /// unless filled.
    fn find_recovery_path(
        &self,
        target_time: Instant,
        components: &[ComponentId],
    ) -> Result<RecoveryPath, StateError> {
        let mut best_checkpoint = None;
        let mut best_distance = Duration::from_secs(u64::MAX);

        // An Instant has no fixed epoch, so map it to wall clock time through the
        // current time.
        let now = Instant::now();
        let now_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let target_timestamp = if target_time <= now {
            let elapsed = now.duration_since(target_time);
            now_timestamp.saturating_sub(elapsed.as_micros() as u64)
        } else {
            let future = target_time.duration_since(now);
            now_timestamp + future.as_micros() as u64
        };

        for (checkpoint_id, checkpoint_info) in &self.checkpoint_cache {
            let distance = if checkpoint_info.created_at <= target_timestamp {
                Duration::from_micros(target_timestamp - checkpoint_info.created_at)
            } else {
                Duration::from_micros(checkpoint_info.created_at - target_timestamp)
            };

            if distance < best_distance {
                best_distance = distance;
                best_checkpoint = Some(*checkpoint_id);
            }
        }

        let base_checkpoint =
            best_checkpoint.ok_or(StateError::CheckpointNotFound { checkpoint_id: 0 })?;

        let incremental_chain = if best_distance == Duration::ZERO {
            Vec::new()
        } else {
            // TODO: find the incremental chain from the base to the target time.
            Vec::new()
        };

        let estimated_duration = Duration::from_millis(components.len() as u64 * 100);
        let total_size = components.len() * 1000; // rough estimate

        Ok(RecoveryPath {
            base_checkpoint,
            incremental_chain,
            estimated_duration,
            total_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::persistence::incremental::checkpoint_merger::AdvancedCheckpointMerger;
    use crate::core::persistence::incremental::persistence_backend::MemoryPersistenceBackend;
    use crate::core::persistence::state_holder::{CompressionType, SchemaVersion, StateMetadata};

    fn create_test_snapshot(checkpoint_id: CheckpointId) -> StateSnapshot {
        let data = vec![1, 2, 3, 4, 5];
        let checksum = StateSnapshot::calculate_checksum(&data);
        StateSnapshot {
            version: SchemaVersion::new(1, 0, 0),
            checkpoint_id,
            data,
            compression: CompressionType::None,
            checksum,
            metadata: StateMetadata::new("test".to_string(), "TestComponent".to_string()),
        }
    }

    #[test]
    fn test_recovery_engine_creation() {
        let backend = Arc::new(MemoryPersistenceBackend::new());
        let merger = Arc::new(AdvancedCheckpointMerger::new(Default::default()));
        let config = RecoveryConfig::default();

        let engine = AdvancedRecoveryEngine::new(backend, merger, config);

        assert_eq!(engine.config.max_parallel_threads, 4);
    }

    #[test]
    fn test_recover_from_full() {
        let backend = Arc::new(MemoryPersistenceBackend::new());
        let merger = Arc::new(AdvancedCheckpointMerger::new(Default::default()));
        let engine =
            AdvancedRecoveryEngine::new(backend.clone(), merger, RecoveryConfig::default());

        let snapshot = create_test_snapshot(1);
        backend.store_full_checkpoint(1, &snapshot).unwrap();

        let components = vec!["component1".to_string(), "component2".to_string()];
        let results = engine.recover_from_full(1, &components).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains_key("component1"));
        assert!(results.contains_key("component2"));
    }

    #[test]
    fn test_find_recovery_path() {
        let backend = Arc::new(MemoryPersistenceBackend::new());
        let merger = Arc::new(AdvancedCheckpointMerger::new(Default::default()));
        let mut engine =
            AdvancedRecoveryEngine::new(backend.clone(), merger, RecoveryConfig::default());

        // The cache is not filled automatically, so add an entry by hand.
        let checkpoint_info = CheckpointInfo {
            checkpoint_id: 1,
            checkpoint_type: CheckpointType::Full,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            size_bytes: 1000,
            dependencies: Vec::new(),
            components: HashSet::new(),
        };
        engine.checkpoint_cache.insert(1, checkpoint_info);

        let components = vec!["component1".to_string()];
        let path = engine
            .find_recovery_path(Instant::now(), &components)
            .unwrap();

        assert_eq!(path.base_checkpoint, 1);
        assert!(path.incremental_chain.is_empty());
    }

    #[test]
    fn test_checkpoint_chain_discovery() {
        let backend = Arc::new(MemoryPersistenceBackend::new());
        let merger = Arc::new(AdvancedCheckpointMerger::new(Default::default()));
        let mut engine = AdvancedRecoveryEngine::new(backend, merger, RecoveryConfig::default());

        // Chain: 1 is full, 2 builds on 1, and 3 builds on 2.
        engine.checkpoint_cache.insert(
            1,
            CheckpointInfo {
                checkpoint_id: 1,
                checkpoint_type: CheckpointType::Full,
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                size_bytes: 1000,
                dependencies: Vec::new(),
                components: HashSet::new(),
            },
        );

        engine.checkpoint_cache.insert(
            2,
            CheckpointInfo {
                checkpoint_id: 2,
                checkpoint_type: CheckpointType::Incremental { base: 1 },
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                size_bytes: 500,
                dependencies: vec![1],
                components: HashSet::new(),
            },
        );

        engine.checkpoint_cache.insert(
            3,
            CheckpointInfo {
                checkpoint_id: 3,
                checkpoint_type: CheckpointType::Incremental { base: 2 },
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                size_bytes: 300,
                dependencies: vec![2],
                components: HashSet::new(),
            },
        );

        let chain = engine.find_checkpoint_chain(3).unwrap();

        assert_eq!(chain, vec![1, 2, 3]);
    }

    #[test]
    fn test_recovery_plan_creation() {
        let backend = Arc::new(MemoryPersistenceBackend::new());
        let merger = Arc::new(AdvancedCheckpointMerger::new(Default::default()));
        let mut engine = AdvancedRecoveryEngine::new(backend, merger, RecoveryConfig::default());

        engine.checkpoint_cache.insert(
            1,
            CheckpointInfo {
                checkpoint_id: 1,
                checkpoint_type: CheckpointType::Full,
                created_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                size_bytes: 1000,
                dependencies: Vec::new(),
                components: HashSet::new(),
            },
        );

        let components = vec!["component1".to_string()];
        let plan = engine.create_recovery_plan(1, &components).unwrap();

        assert_eq!(plan.stages.len(), 1);
        assert_eq!(plan.checkpoints_needed, vec![1]);
        assert_eq!(plan.total_size, 1000);
    }
}
