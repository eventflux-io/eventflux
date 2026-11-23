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

// src/core/persistence/incremental/checkpoint_merger.rs

//! Checkpoint merger.
//!
//! Combines incremental checkpoints with a base snapshot, builds new incremental
//! checkpoints, and shortens long checkpoint chains.
//!
//! Much of this is still a stand in. The merge appends new values to the raw
//! snapshot bytes instead of rebuilding the state, compression only tags the
//! snapshot, deduplication only reports a made up saving, and conflict detection
//! never finds a conflict.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{CheckpointMerger, IncrementalCheckpoint};
use crate::core::persistence::state_holder::{
    ChangeLog, CheckpointId, ComponentId, CompressionType, SchemaVersion, StateError,
    StateOperation, StateSnapshot,
};
use crate::core::util::to_bytes;

/// Checkpoint merger with chain shortening.
pub struct AdvancedCheckpointMerger {
    config: MergerConfig,

    compressor: Arc<CompressionEngine>,

    stats: Arc<Mutex<MergeStatistics>>,
}

/// Checkpoint merger settings.
#[derive(Debug, Clone)]
pub struct MergerConfig {
    /// Longest incremental chain before it is shortened. Default 10.
    pub max_chain_length: usize,

    /// Compress merged snapshots. Default true.
    pub enable_delta_compression: bool,

    /// Size in bytes above which compression is applied, and below which a
    /// checkpoint counts as small. Default 1 KB.
    pub compression_threshold: usize,

    /// Default `Optimal`. Not read by the merger yet.
    pub merge_strategy: MergeStrategy,

    /// Default true.
    pub enable_deduplication: bool,

    /// Default `LastWriteWins`.
    pub conflict_resolution: ConflictResolution,

    /// Default 2. Not read by the merger yet.
    pub parallel_threads: usize,
}

impl Default for MergerConfig {
    fn default() -> Self {
        Self {
            max_chain_length: 10,
            enable_delta_compression: true,
            compression_threshold: 1024, // 1 KB
            merge_strategy: MergeStrategy::Optimal,
            enable_deduplication: true,
            conflict_resolution: ConflictResolution::LastWriteWins,
            parallel_threads: 2,
        }
    }
}

/// How to merge. Not read by the merger yet.
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// One operation after another.
    Sequential,

    /// Merge with compression.
    Optimal,

    /// Merge on several threads, for large data.
    Parallel,

    Custom {
        batch_size: usize,
        enable_streaming: bool,
    },
}

/// Which write wins when two changes hit the same key.
#[derive(Debug, Clone)]
pub enum ConflictResolution {
    /// The later write wins. The default.
    LastWriteWins,

    /// The earlier write wins.
    FirstWriteWins,

    /// The write with the newer timestamp wins. Acts like `LastWriteWins` for
    /// now.
    TimestampPriority,

    /// A custom rule. Acts like `LastWriteWins` for now.
    Custom,
}

/// Compresses snapshots. Only tags them for now, see `compress_snapshot`.
#[allow(dead_code)]
pub struct CompressionEngine {
    compression_type: CompressionType,

    /// Not used yet.
    compression_level: u8,
}

/// Merge counters.
///
/// `avg_compression_ratio` is never updated, and `deduplication_savings` adds
/// up made up numbers.
#[derive(Debug, Default)]
pub struct MergeStatistics {
    pub total_merges: u64,

    /// Kept in whole milliseconds.
    pub avg_merge_duration: Duration,

    /// Bytes of base snapshot data merged.
    pub total_data_processed: u64,

    pub avg_compression_ratio: f64,

    pub conflicts_resolved: u64,

    /// Bytes saved by deduplication.
    pub deduplication_savings: u64,
}

/// Intermediate merge result. Not used yet.
#[derive(Debug)]
#[allow(dead_code)]
struct MergeResult {
    component_states: HashMap<ComponentId, MergedComponentState>,

    metadata: MergeMetadata,

    /// Size after the merge, in bytes.
    total_size: usize,

    compression_ratio: f64,
}

/// Merged state of one component. Not used yet.
#[derive(Debug)]
#[allow(dead_code)]
struct MergedComponentState {
    snapshot: StateSnapshot,

    operations_applied: usize,

    conflicts_resolved: usize,

    /// Schema versions seen across the merged checkpoints.
    schema_versions: Vec<SchemaVersion>,
}

/// Details of one merge. Not used yet.
#[derive(Debug)]
#[allow(dead_code)]
struct MergeMetadata {
    start_time: Instant,

    end_time: Instant,

    components_merged: usize,

    total_operations: usize,

    strategy_used: MergeStrategy,
}

impl AdvancedCheckpointMerger {
    /// Create a merger. Uses LZ4 when `enable_delta_compression` is set.
    pub fn new(config: MergerConfig) -> Self {
        let compression_type = if config.enable_delta_compression {
            CompressionType::LZ4
        } else {
            CompressionType::None
        };

        Self {
            config,
            compressor: Arc::new(CompressionEngine::new(compression_type, 6)),
            stats: Arc::new(Mutex::new(MergeStatistics::default())),
        }
    }

    /// Check that each checkpoint builds on the one before it.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if a checkpoint's base is not the previous
    /// checkpoint, or its sequence number is not the previous one plus 1.
    fn validate_chain(&self, incrementals: &[IncrementalCheckpoint]) -> Result<(), StateError> {
        if incrementals.is_empty() {
            return Ok(());
        }

        for window in incrementals.windows(2) {
            let current = &window[0];
            let next = &window[1];

            if next.base_checkpoint_id != current.checkpoint_id {
                return Err(StateError::InvalidStateData {
                    message: format!(
                        "Broken incremental chain: checkpoint {} does not follow {}",
                        next.checkpoint_id, current.checkpoint_id
                    ),
                });
            }

            if next.sequence_number != current.sequence_number + 1 {
                return Err(StateError::InvalidStateData {
                    message: format!(
                        "Invalid sequence: expected {}, got {}",
                        current.sequence_number + 1,
                        next.sequence_number
                    ),
                });
            }
        }

        Ok(())
    }

    /// Sum sizes and per component counts across the chain, and list merge
    /// opportunities.
    fn analyze_chain(&self, incrementals: &[IncrementalCheckpoint]) -> ChainAnalysis {
        let mut total_size = 0;
        let mut component_frequencies = HashMap::new();
        let mut operation_counts = HashMap::new();

        for checkpoint in incrementals {
            total_size += checkpoint.total_size;

            for (component_id, changes) in &checkpoint.component_changes {
                *component_frequencies
                    .entry(component_id.clone())
                    .or_insert(0) += 1;
                *operation_counts.entry(component_id.clone()).or_insert(0) +=
                    changes.operations.len();
            }
        }

        ChainAnalysis {
            total_checkpoints: incrementals.len(),
            total_size,
            component_frequencies,
            operation_counts,
            merge_opportunities: self.identify_merge_opportunities(incrementals),
        }
    }

    /// List places where merging could save space.
    ///
    /// Flags neighbors that touch the same components, and checkpoints smaller
    /// than `compression_threshold`. The savings figures are rough guesses.
    fn identify_merge_opportunities(
        &self,
        incrementals: &[IncrementalCheckpoint],
    ) -> Vec<MergeOpportunity> {
        let mut opportunities = Vec::new();

        for window in incrementals.windows(2) {
            let current = &window[0];
            let next = &window[1];

            let current_components: std::collections::HashSet<_> =
                current.component_changes.keys().collect();
            let next_components: std::collections::HashSet<_> =
                next.component_changes.keys().collect();

            let overlap = current_components.intersection(&next_components).count();

            if overlap > 0 {
                opportunities.push(MergeOpportunity {
                    checkpoint_range: (current.checkpoint_id, next.checkpoint_id),
                    overlapping_components: overlap,
                    potential_savings: current.total_size.min(next.total_size) / 2,
                    merge_type: MergeOpportunityType::ComponentOverlap,
                });
            }
        }

        for checkpoint in incrementals {
            if checkpoint.total_size < self.config.compression_threshold {
                opportunities.push(MergeOpportunity {
                    checkpoint_range: (checkpoint.checkpoint_id, checkpoint.checkpoint_id),
                    overlapping_components: checkpoint.component_changes.len(),
                    potential_savings: checkpoint.total_size / 4,
                    merge_type: MergeOpportunityType::SmallCheckpoint,
                });
            }
        }

        opportunities
    }

    /// Apply operations to a copy of the base snapshot and return it with a new
    /// checksum.
    ///
    /// A stand in, not a real state rebuild. Operations run grouped as deletes
    /// and clears, then inserts, then updates. Inserts and updates append the new
    /// value to the raw bytes. Deletes change nothing. A clear empties the data
    /// unless the resolution is `FirstWriteWins`.
    fn apply_operations(
        &self,
        base_snapshot: &StateSnapshot,
        operations: &[StateOperation],
        resolution: &ConflictResolution,
    ) -> Result<StateSnapshot, StateError> {
        let mut conflicts_count = 0;

        let mut insertions = Vec::new();
        let mut updates = Vec::new();
        let mut deletions = Vec::new();

        for operation in operations {
            match operation {
                StateOperation::Insert { .. } => insertions.push(operation),
                StateOperation::Update { .. } => updates.push(operation),
                StateOperation::Delete { .. } => deletions.push(operation),
                StateOperation::Clear => {
                    // A clear affects the whole state and should get its own
                    // handling. For now it runs with the deletes.
                    deletions.push(operation);
                }
            }
        }

        // Order is deletes, then inserts, then updates. This loses the original
        // order between operation kinds.
        let mut new_data = base_snapshot.data.clone();

        // Stand in merge that appends values to the raw bytes. A real merge would
        // rebuild the state from the operation meanings.
        for operation_group in [&deletions, &insertions, &updates] {
            for operation in operation_group {
                match operation {
                    StateOperation::Insert { key, value } => {
                        if self.detect_conflict(&new_data, key) {
                            conflicts_count += 1;
                            self.resolve_conflict(&mut new_data, key, value, resolution)?;
                        } else {
                            new_data.extend_from_slice(value);
                        }
                    }
                    StateOperation::Update { key, new_value, .. } => {
                        if self.detect_conflict(&new_data, key) {
                            conflicts_count += 1;
                            self.resolve_conflict(&mut new_data, key, new_value, resolution)?;
                        } else {
                            new_data.extend_from_slice(new_value);
                        }
                    }
                    StateOperation::Delete { key, .. } => {
                        // Deletes do not change the data yet.
                        if self.detect_conflict(&new_data, key) {
                            conflicts_count += 1;
                            // Could ignore or apply the delete based on the
                            // resolution.
                        }
                    }
                    StateOperation::Clear => {
                        // A clear on non empty data counts as a conflict.
                        if !new_data.is_empty() {
                            conflicts_count += 1;
                            match resolution {
                                ConflictResolution::LastWriteWins => {
                                    new_data.clear();
                                }
                                ConflictResolution::FirstWriteWins => {
                                    // Keep the existing data and skip the clear.
                                }
                                _ => {
                                    // Every other resolution clears.
                                    new_data.clear();
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut merged_snapshot = base_snapshot.clone();
        merged_snapshot.data = new_data;
        merged_snapshot.checksum = StateSnapshot::calculate_checksum(&merged_snapshot.data);

        {
            let mut stats = self.stats.lock().unwrap();
            stats.conflicts_resolved += conflicts_count;
        }

        Ok(merged_snapshot)
    }

    /// Returns whether a write to `key` conflicts. Always false for now. A real
    /// check would look for overlapping key ranges or concurrent changes.
    fn detect_conflict(&self, _data: &[u8], _key: &[u8]) -> bool {
        false
    }

    /// Apply the conflict resolution for one write. Every resolution except
    /// `FirstWriteWins` appends the value. `FirstWriteWins` keeps the data as is.
    fn resolve_conflict(
        &self,
        data: &mut Vec<u8>,
        _key: &[u8],
        value: &[u8],
        resolution: &ConflictResolution,
    ) -> Result<(), StateError> {
        match resolution {
            ConflictResolution::LastWriteWins => {
                data.extend_from_slice(value);
            }
            ConflictResolution::FirstWriteWins => {
                // Keep the existing value.
            }
            ConflictResolution::TimestampPriority => {
                // Should compare timestamps. Acts like last write wins for now.
                data.extend_from_slice(value);
            }
            ConflictResolution::Custom => {
                // No custom rule hook yet. Acts like last write wins.
                data.extend_from_slice(value);
            }
        }

        Ok(())
    }

    /// Returns the data and the bytes saved by deduplication.
    ///
    /// A stand in. The data comes back unchanged, and when deduplication is on
    /// the saving is reported as 10 percent of the size without removing
    /// anything. A real version would use rolling hashes or content defined
    /// chunks.
    fn deduplicate_data(&self, data: &[u8]) -> (Vec<u8>, usize) {
        if !self.config.enable_deduplication {
            return (data.to_vec(), 0);
        }

        let original_size = data.len();

        let deduplicated = data.to_vec();
        let savings = original_size / 10; // made up 10 percent saving

        (deduplicated, savings)
    }
}

impl CheckpointMerger for AdvancedCheckpointMerger {
    fn merge_incrementals(
        &self,
        base: &StateSnapshot,
        incrementals: &[IncrementalCheckpoint],
    ) -> Result<StateSnapshot, StateError> {
        let start_time = Instant::now();

        self.validate_chain(incrementals)?;

        if incrementals.is_empty() {
            return Ok(base.clone());
        }

        // Operations in chain order. Within one checkpoint, components come in
        // map order, which is not fixed. The component id is dropped, so every
        // component's operations apply to the one base snapshot.
        let mut all_operations = Vec::new();
        for checkpoint in incrementals {
            for (component_id, changes) in &checkpoint.component_changes {
                for operation in &changes.operations {
                    all_operations.push((component_id.clone(), operation.clone()));
                }
            }
        }

        // Not sorted by time. Operations carry no timestamp yet.
        // all_operations.sort_by_key(|(_, op)| op.timestamp());

        let operations: Vec<_> = all_operations.iter().map(|(_, op)| op.clone()).collect();
        let merged_snapshot =
            self.apply_operations(base, &operations, &self.config.conflict_resolution)?;

        let final_snapshot = if self.config.enable_delta_compression {
            self.compressor.compress_snapshot(merged_snapshot)?
        } else {
            merged_snapshot
        };

        let (deduplicated_data, savings) = self.deduplicate_data(&final_snapshot.data);
        let mut deduped_snapshot = final_snapshot;
        deduped_snapshot.data = deduplicated_data;
        deduped_snapshot.checksum = StateSnapshot::calculate_checksum(&deduped_snapshot.data);

        // Running average of merge time, in whole milliseconds.
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_merges += 1;
            stats.avg_merge_duration = Duration::from_millis(
                (stats.avg_merge_duration.as_millis() as u64 * (stats.total_merges - 1)
                    + start_time.elapsed().as_millis() as u64)
                    / stats.total_merges,
            );
            stats.total_data_processed += base.data.len() as u64;
            stats.deduplication_savings += savings as u64;
        }

        Ok(deduped_snapshot)
    }

    fn create_incremental(
        &self,
        base_checkpoint_id: CheckpointId,
        changes: HashMap<ComponentId, ChangeLog>,
        wal_range: (u64, u64),
    ) -> Result<IncrementalCheckpoint, StateError> {
        let _start_time = Instant::now();

        // Serialized size of all change logs, in bytes.
        let mut total_size = 0;
        for changelog in changes.values() {
            total_size += to_bytes(changelog)
                .map_err(|e| StateError::SerializationError {
                    message: format!("Failed to serialize changelog: {e}"),
                })?
                .len();
        }

        // Nothing is compressed. Above the threshold the size is simply assumed
        // to shrink by 30 percent.
        let compression_ratio = if self.config.enable_delta_compression
            && total_size > self.config.compression_threshold
        {
            0.7 // assumed, not measured
        } else {
            1.0
        };

        let compressed_size = (total_size as f64 * compression_ratio) as usize;

        Ok(IncrementalCheckpoint {
            checkpoint_id: base_checkpoint_id + 1, // not unique across chains yet
            base_checkpoint_id,
            sequence_number: 1, // always 1, not tracked across the chain yet
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: changes,
            total_size: compressed_size,
            compression_ratio,
            wal_offset_range: wal_range,
        })
    }

    fn optimize_chain(
        &self,
        chain: &[IncrementalCheckpoint],
    ) -> Result<Vec<IncrementalCheckpoint>, StateError> {
        if chain.len() <= 1 {
            return Ok(chain.to_vec());
        }

        let analysis = self.analyze_chain(chain);

        if chain.len() > self.config.max_chain_length {
            return self.compress_chain(chain);
        }

        // Currently returns the chain unchanged.
        if !analysis.merge_opportunities.is_empty() {
            return self.apply_merge_opportunities(chain, &analysis.merge_opportunities);
        }

        Ok(chain.to_vec())
    }
}

impl AdvancedCheckpointMerger {
    /// Shorten the chain by merging each run of 3 checkpoints into one. Any 1
    /// or 2 left over at the end stay as they are.
    fn compress_chain(
        &self,
        chain: &[IncrementalCheckpoint],
    ) -> Result<Vec<IncrementalCheckpoint>, StateError> {
        let mut compressed = Vec::new();
        let mut current_group = Vec::new();

        for checkpoint in chain {
            current_group.push(checkpoint.clone());

            if current_group.len() >= 3 {
                let merged = self.merge_checkpoint_group(&current_group)?;
                compressed.push(merged);
                current_group.clear();
            }
        }

        compressed.extend(current_group);

        Ok(compressed)
    }

    /// Merge a group of checkpoints into one.
    ///
    /// The result takes the last checkpoint id, the first base, sequence number
    /// and creation time, and the full WAL range. Change logs per component are
    /// joined in order, but their from and to checkpoint ids are set to 0. The
    /// compression ratio is a fixed 0.8.
    ///
    /// # Errors
    ///
    /// Returns `InvalidStateData` if the group is empty.
    fn merge_checkpoint_group(
        &self,
        group: &[IncrementalCheckpoint],
    ) -> Result<IncrementalCheckpoint, StateError> {
        if group.is_empty() {
            return Err(StateError::InvalidStateData {
                message: "Cannot merge empty checkpoint group".to_string(),
            });
        }

        if group.len() == 1 {
            return Ok(group[0].clone());
        }

        let mut merged_changes = HashMap::new();
        let mut total_size = 0;

        for checkpoint in group {
            for (component_id, changes) in &checkpoint.component_changes {
                let entry = merged_changes
                    .entry(component_id.clone())
                    .or_insert_with(|| ChangeLog::new(0, 0));

                for operation in &changes.operations {
                    entry.add_operation(operation.clone());
                }
            }
            total_size += checkpoint.total_size;
        }

        let first = &group[0];
        let last = &group[group.len() - 1];

        Ok(IncrementalCheckpoint {
            checkpoint_id: last.checkpoint_id,
            base_checkpoint_id: first.base_checkpoint_id,
            sequence_number: first.sequence_number,
            created_at: first.created_at,
            component_changes: merged_changes,
            total_size,
            compression_ratio: 0.8, // assumed, not measured
            wal_offset_range: (first.wal_offset_range.0, last.wal_offset_range.1),
        })
    }

    /// Apply merge opportunities. Does nothing yet and returns the chain
    /// unchanged.
    fn apply_merge_opportunities(
        &self,
        chain: &[IncrementalCheckpoint],
        _opportunities: &[MergeOpportunity],
    ) -> Result<Vec<IncrementalCheckpoint>, StateError> {
        // TODO: apply a change for each kind of opportunity found.
        Ok(chain.to_vec())
    }
}

impl CompressionEngine {
    fn new(compression_type: CompressionType, level: u8) -> Self {
        Self {
            compression_type,
            compression_level: level,
        }
    }

    /// Mark the snapshot with the compression type and print a message. The data
    /// is not compressed yet.
    fn compress_snapshot(&self, mut snapshot: StateSnapshot) -> Result<StateSnapshot, StateError> {
        match self.compression_type {
            CompressionType::None => Ok(snapshot),
            CompressionType::LZ4 => {
                // TODO: real LZ4 compression. Only the tag is set for now.
                println!("LZ4 compression simulated");
                snapshot.compression = CompressionType::LZ4;
                Ok(snapshot)
            }
            CompressionType::Snappy => {
                println!("Snappy compression simulated");
                snapshot.compression = CompressionType::Snappy;
                Ok(snapshot)
            }
            CompressionType::Zstd => {
                println!("Zstd compression simulated");
                snapshot.compression = CompressionType::Zstd;
                Ok(snapshot)
            }
        }
    }
}

/// Summary of a checkpoint chain.
#[derive(Debug)]
#[allow(dead_code)]
struct ChainAnalysis {
    total_checkpoints: usize,
    total_size: usize,
    /// Checkpoints touching each component.
    component_frequencies: HashMap<ComponentId, usize>,
    /// Operations for each component.
    operation_counts: HashMap<ComponentId, usize>,
    merge_opportunities: Vec<MergeOpportunity>,
}

/// A place where merging could save space.
#[derive(Debug)]
#[allow(dead_code)]
struct MergeOpportunity {
    /// First and last checkpoint id. The same id twice for a single checkpoint.
    checkpoint_range: (CheckpointId, CheckpointId),
    overlapping_components: usize,
    /// Rough guess in bytes.
    potential_savings: usize,
    merge_type: MergeOpportunityType,
}

/// Kind of merge opportunity. Only the first two are detected.
#[derive(Debug)]
#[allow(dead_code)]
enum MergeOpportunityType {
    ComponentOverlap,
    SmallCheckpoint,
    ConsecutiveUpdates,
    RedundantOperations,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::persistence::state_holder::{CompressionType, SchemaVersion, StateMetadata};

    fn create_test_snapshot() -> StateSnapshot {
        StateSnapshot {
            version: SchemaVersion::new(1, 0, 0),
            checkpoint_id: 1,
            data: vec![1, 2, 3, 4, 5],
            compression: CompressionType::None,
            checksum: 12345,
            metadata: StateMetadata::new("test".to_string(), "TestComponent".to_string()),
        }
    }

    fn create_test_changelog() -> ChangeLog {
        let mut changelog = ChangeLog::new(1, 2);
        changelog.add_operation(StateOperation::Insert {
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
        });
        changelog
    }

    #[test]
    fn test_merger_creation() {
        let config = MergerConfig::default();
        let merger = AdvancedCheckpointMerger::new(config);

        assert_eq!(merger.config.max_chain_length, 10);
    }

    #[test]
    fn test_create_incremental() {
        let merger = AdvancedCheckpointMerger::new(MergerConfig::default());

        let mut changes = HashMap::new();
        changes.insert("component1".to_string(), create_test_changelog());

        let incremental = merger.create_incremental(1, changes, (0, 10)).unwrap();

        assert_eq!(incremental.base_checkpoint_id, 1);
        assert_eq!(incremental.checkpoint_id, 2);
        assert!(!incremental.component_changes.is_empty());
    }

    #[test]
    fn test_merge_incrementals_empty() {
        let merger = AdvancedCheckpointMerger::new(MergerConfig::default());
        let base = create_test_snapshot();

        let result = merger.merge_incrementals(&base, &[]).unwrap();

        assert_eq!(result.data, base.data);
    }

    #[test]
    fn test_merge_incrementals_single() {
        let merger = AdvancedCheckpointMerger::new(MergerConfig::default());
        let base = create_test_snapshot();

        let mut changes = HashMap::new();
        changes.insert("component1".to_string(), create_test_changelog());

        let incremental = IncrementalCheckpoint {
            checkpoint_id: 2,
            base_checkpoint_id: 1,
            sequence_number: 1,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: changes,
            total_size: 100,
            compression_ratio: 1.0,
            wal_offset_range: (0, 10),
        };

        let result = merger.merge_incrementals(&base, &[incremental]).unwrap();

        // The stand in merge appends values, so the data cannot shrink.
        assert!(result.data.len() >= base.data.len());
    }

    #[test]
    fn test_chain_validation() {
        let merger = AdvancedCheckpointMerger::new(MergerConfig::default());

        let incremental1 = IncrementalCheckpoint {
            checkpoint_id: 2,
            base_checkpoint_id: 1,
            sequence_number: 1,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: HashMap::new(),
            total_size: 100,
            compression_ratio: 1.0,
            wal_offset_range: (0, 10),
        };

        let incremental2 = IncrementalCheckpoint {
            checkpoint_id: 3,
            base_checkpoint_id: 2,
            sequence_number: 2,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: HashMap::new(),
            total_size: 100,
            compression_ratio: 1.0,
            wal_offset_range: (10, 20),
        };

        assert!(merger
            .validate_chain(&[incremental1.clone(), incremental2.clone()])
            .is_ok());

        // Break the link. The second checkpoint no longer builds on the first.
        let mut broken_incremental2 = incremental2;
        broken_incremental2.base_checkpoint_id = 99;

        assert!(merger
            .validate_chain(&[incremental1, broken_incremental2])
            .is_err());
    }

    #[test]
    fn test_optimize_chain_short() {
        let merger = AdvancedCheckpointMerger::new(MergerConfig::default());

        let incremental = IncrementalCheckpoint {
            checkpoint_id: 2,
            base_checkpoint_id: 1,
            sequence_number: 1,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            component_changes: HashMap::new(),
            total_size: 100,
            compression_ratio: 1.0,
            wal_offset_range: (0, 10),
        };

        let result = merger
            .optimize_chain(std::slice::from_ref(&incremental))
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].checkpoint_id, incremental.checkpoint_id);
    }
}
