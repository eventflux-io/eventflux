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

// src/core/persistence/state_holder.rs

//! State holder trait and snapshot types.
//!
//! [`StateHolder`] is what every stateful component implements so its state can
//! be saved, restored, migrated between schema versions and tracked as a log of
//! changes. This module also holds the snapshot, change log and error types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Checkpoint id.
pub type CheckpointId = u64;

/// State component id.
pub type ComponentId = String;

/// Version of a state schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SchemaVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Returns true if this version can read state written by `other`.
    ///
    /// The major versions must match and this minor version must be the same or
    /// higher. The patch version is ignored.
    pub fn is_compatible_with(&self, other: &SchemaVersion) -> bool {
        self.major == other.major && self.minor >= other.minor
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Estimated state size, for resource planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSize {
    pub bytes: usize,
    pub entries: usize,
    pub estimated_growth_rate: f64, // bytes per second
}

/// How a component's state is read, as a hint for where to keep it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccessPattern {
    /// Read often. Keep in memory.
    Hot,
    /// Read now and then. Can be evicted.
    Warm,
    /// Read rarely. Can be moved to disk.
    Cold,
    /// Read in order.
    Sequential,
    /// Read in no set order.
    Random,
}

/// Compression type for state snapshots.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    LZ4,
    Snappy,
    Zstd,
}

/// Hints a state holder may use when serializing. Holders are free to ignore
/// them.
#[derive(Debug, Clone, Default)]
pub struct SerializationHints {
    pub prefer_compression: Option<CompressionType>,
    pub target_chunk_size: Option<usize>,
    pub priority: SerializationPriority,
}

/// What to favor when serializing. Defaults to `Balanced`.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SerializationPriority {
    Speed,
    Size,
    #[default]
    Balanced,
}

/// Metadata stored with a state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMetadata {
    pub component_id: ComponentId,
    pub component_type: String,
    pub created_at: u64, // Unix time in seconds
    pub access_pattern: AccessPattern,
    pub size_estimation: StateSize,
    pub custom_metadata: HashMap<String, String>,
}

impl StateMetadata {
    /// Create metadata stamped with the current time, a `Warm` access pattern
    /// and a zero size estimate.
    pub fn new(component_id: ComponentId, component_type: String) -> Self {
        Self {
            component_id,
            component_type,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            access_pattern: AccessPattern::Warm,
            size_estimation: StateSize {
                bytes: 0,
                entries: 0,
                estimated_growth_rate: 0.0,
            },
            custom_metadata: HashMap::new(),
        }
    }
}

/// Saved state of one component, with its schema version, checksum and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub version: SchemaVersion,
    pub checkpoint_id: CheckpointId,
    pub data: Vec<u8>,
    pub compression: CompressionType,
    /// Checksum of `data` from [`calculate_checksum`](Self::calculate_checksum).
    pub checksum: u64,
    pub metadata: StateMetadata,
}

impl StateSnapshot {
    /// Returns a checksum of `data`, used to detect corruption.
    ///
    /// Uses the standard library `DefaultHasher`, whose output is not
    /// guaranteed to stay the same across Rust releases.
    pub fn calculate_checksum(data: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns true if `data` still matches `checksum`.
    pub fn verify_integrity(&self) -> bool {
        Self::calculate_checksum(&self.data) == self.checksum
    }
}

/// One change to state, recorded for incremental checkpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateOperation {
    Insert {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Update {
        key: Vec<u8>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
        old_value: Vec<u8>,
    },
    Clear,
}

/// Changes made between two checkpoints, so only the changes need saving.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeLog {
    pub from_checkpoint: CheckpointId,
    pub to_checkpoint: CheckpointId,
    pub operations: Vec<StateOperation>,
    /// Total bytes of keys and values in `operations`.
    pub size_bytes: usize,
    pub compression: CompressionType,
}

impl ChangeLog {
    /// Create an empty, uncompressed change log.
    pub fn new(from: CheckpointId, to: CheckpointId) -> Self {
        Self {
            from_checkpoint: from,
            to_checkpoint: to,
            operations: Vec::new(),
            size_bytes: 0,
            compression: CompressionType::None,
        }
    }

    /// Add an operation and add its key and value bytes to `size_bytes`.
    pub fn add_operation(&mut self, operation: StateOperation) {
        let op_size = match &operation {
            StateOperation::Insert { key, value } => key.len() + value.len(),
            StateOperation::Update {
                key,
                old_value,
                new_value,
            } => key.len() + old_value.len() + new_value.len(),
            StateOperation::Delete { key, old_value } => key.len() + old_value.len(),
            StateOperation::Clear => 0,
        };

        self.size_bytes += op_size;
        self.operations.push(operation);
    }
}

/// A component whose state can be saved, restored, migrated and tracked as
/// changes.
pub trait StateHolder: Send + Sync {
    /// Version of this component's state schema.
    fn schema_version(&self) -> SchemaVersion;

    /// Serialize the state. `hints` may guide compression and chunking.
    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError>;

    /// Restore the state from a snapshot. Implementations should check the
    /// snapshot version is compatible.
    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError>;

    /// Returns the changes made since checkpoint `since`.
    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError>;

    /// Apply a log of changes to the state.
    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError>;

    /// Estimate the state size, for resource planning.
    fn estimate_size(&self) -> StateSize;

    /// How the state is read, as a hint for where to keep it.
    fn access_pattern(&self) -> AccessPattern;

    fn component_metadata(&self) -> StateMetadata;

    /// Returns true if state written with `version` can be loaded. By default
    /// this follows [`SchemaVersion::is_compatible_with`].
    fn can_migrate_from(&self, version: &SchemaVersion) -> bool {
        self.schema_version().is_compatible_with(version)
    }

    /// Load state written with an older schema version.
    ///
    /// The default wraps `data` in an uncompressed snapshot tagged with
    /// `from_version` and passes it to [`deserialize_state`](Self::deserialize_state).
    ///
    /// # Errors
    ///
    /// Returns `IncompatibleVersion` if [`can_migrate_from`](Self::can_migrate_from)
    /// is false. Otherwise returns any error from `deserialize_state`.
    fn migrate_state(
        &mut self,
        from_version: &SchemaVersion,
        data: &[u8],
    ) -> Result<(), StateError> {
        if !self.can_migrate_from(from_version) {
            return Err(StateError::IncompatibleVersion {
                current: self.schema_version(),
                required: *from_version,
            });
        }

        let snapshot = StateSnapshot {
            version: *from_version,
            checkpoint_id: 0,
            data: data.to_vec(),
            compression: CompressionType::None,
            checksum: StateSnapshot::calculate_checksum(data),
            metadata: self.component_metadata(),
        };

        self.deserialize_state(&snapshot)
    }

    /// Reset all mutable state to its initial empty value.
    ///
    /// Called during `clear_state()` to throw away state built up in memory,
    /// such as window buffers and aggregator totals, so the runtime can start
    /// fresh.
    fn reset_state(&self) {
        // Does nothing by default. State holders should override it.
    }
}

/// State management errors.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("Serialization failed: {message}")]
    SerializationError { message: String },

    #[error("Deserialization failed: {message}")]
    DeserializationError { message: String },

    #[error("Incompatible schema version: current {current}, required {required}")]
    IncompatibleVersion {
        current: SchemaVersion,
        required: SchemaVersion,
    },

    #[error("Checkpoint {checkpoint_id} not found")]
    CheckpointNotFound { checkpoint_id: CheckpointId },

    #[error("Invalid state data: {message}")]
    InvalidStateData { message: String },

    #[error("Compression failed: {message}")]
    CompressionError { message: String },

    #[error("Checksum verification failed")]
    ChecksumMismatch,

    #[error("IO error: {message}")]
    IoError { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version_compatibility() {
        let v1_0_0 = SchemaVersion::new(1, 0, 0);
        let v1_1_0 = SchemaVersion::new(1, 1, 0);
        let v2_0_0 = SchemaVersion::new(2, 0, 0);

        assert!(v1_1_0.is_compatible_with(&v1_0_0));
        assert!(!v1_0_0.is_compatible_with(&v1_1_0));
        assert!(!v2_0_0.is_compatible_with(&v1_0_0));
    }

    #[test]
    fn test_state_snapshot_integrity() {
        let data = b"test state data";
        let checksum = StateSnapshot::calculate_checksum(data);

        let snapshot = StateSnapshot {
            version: SchemaVersion::new(1, 0, 0),
            checkpoint_id: 1,
            data: data.to_vec(),
            compression: CompressionType::None,
            checksum,
            metadata: StateMetadata::new("test".to_string(), "TestComponent".to_string()),
        };

        assert!(snapshot.verify_integrity());

        let mut corrupted = snapshot.clone();
        corrupted.data[0] = corrupted.data[0].wrapping_add(1);
        assert!(!corrupted.verify_integrity());
    }

    #[test]
    fn test_changelog_operations() {
        let mut changelog = ChangeLog::new(1, 2);

        changelog.add_operation(StateOperation::Insert {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });

        changelog.add_operation(StateOperation::Update {
            key: b"key2".to_vec(),
            old_value: b"old".to_vec(),
            new_value: b"new".to_vec(),
        });

        assert_eq!(changelog.operations.len(), 2);
        assert_eq!(changelog.size_bytes, 4 + 6 + 4 + 3 + 3); // key1+value1 + key2+old+new
    }
}
