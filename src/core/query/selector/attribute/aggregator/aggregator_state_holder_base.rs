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

//! Shared base types and helpers for aggregator state holders.

use crate::core::persistence::state_holder::{
    ChangeLog, CheckpointId, CompressionType, SerializationHints, StateError, StateHolder,
    StateOperation, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

/// Shared fields for all aggregator state holders.
#[derive(Debug, Clone)]
pub(crate) struct StateHolderBase {
    pub component_id: String,
    pub last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,
    pub change_log: Arc<Mutex<Vec<StateOperation>>>,
    /// Flag set by state holder's `deserialize_state` to signal restoration.
    /// Executors check this with `swap(false, AcqRel)` instead of per-event mutex comparisons.
    pub restored: Arc<AtomicBool>,
}

impl StateHolderBase {
    pub fn new(component_id: String) -> Self {
        Self {
            component_id,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
            restored: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Clear the change log (called after successful checkpoint).
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }

    /// Get changelog since the given checkpoint.
    pub fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
        let last_checkpoint = self.last_checkpoint_id.lock().unwrap();
        if let Some(last_id) = *last_checkpoint {
            if since > last_id {
                return Err(StateError::CheckpointNotFound {
                    checkpoint_id: since,
                });
            }
        }

        let change_log = self.change_log.lock().unwrap();
        let mut changelog = ChangeLog::new(since, since + 1);
        for operation in change_log.iter() {
            changelog.add_operation(operation.clone());
        }
        Ok(changelog)
    }
}

/// Compress data and build a state snapshot.
pub(crate) fn compress_and_build_snapshot(
    holder: &(impl CompressibleStateHolder + StateHolder),
    data: Vec<u8>,
    hints: &SerializationHints,
) -> Result<StateSnapshot, StateError> {
    let (compressed_data, compression_type) =
        match holder.compress_state_data(&data, hints.prefer_compression.clone()) {
            Ok((compressed, comp_type)) => (compressed, comp_type),
            Err(_) => (data, CompressionType::None),
        };

    let checksum = StateSnapshot::calculate_checksum(&compressed_data);

    Ok(StateSnapshot {
        version: holder.schema_version(),
        checkpoint_id: 0,
        data: compressed_data,
        compression: compression_type,
        checksum,
        metadata: holder.component_metadata(),
    })
}

/// Verify snapshot integrity and decompress data.
pub(crate) fn verify_and_decompress(
    holder: &impl CompressibleStateHolder,
    snapshot: &StateSnapshot,
) -> Result<Vec<u8>, StateError> {
    if !snapshot.verify_integrity() {
        return Err(StateError::ChecksumMismatch);
    }
    holder.decompress_state_data(&snapshot.data, snapshot.compression.clone())
}

/// Default compression hints for aggregator state holders.
pub(crate) fn default_compression_hints(data_type: DataCharacteristics) -> CompressionHints {
    CompressionHints {
        prefer_speed: true,
        prefer_ratio: false,
        data_type,
        target_latency_ms: Some(1),
        min_compression_ratio: Some(0.2),
        expected_size_range: DataSizeRange::Small,
    }
}
