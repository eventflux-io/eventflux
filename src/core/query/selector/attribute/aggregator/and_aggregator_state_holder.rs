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

//! StateHolder implementation for AndAttributeAggregatorExecutor

use super::aggregator_state_holder_base::{
    compress_and_build_snapshot, default_compression_hints, verify_and_decompress, StateHolderBase,
};
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics,
};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct AndAggregatorStateHolder {
    true_count: Arc<Mutex<i64>>,
    false_count: Arc<Mutex<i64>>,
    pub(crate) base: StateHolderBase,
}

impl AndAggregatorStateHolder {
    pub fn new(
        true_count: Arc<Mutex<i64>>,
        false_count: Arc<Mutex<i64>>,
        component_id: String,
    ) -> Self {
        Self {
            true_count,
            false_count,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_added(&self, value: bool) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Insert {
            key: super::generate_operation_key(if value { "add_true" } else { "add_false" }),
            value: vec![u8::from(value)],
        });
    }

    pub fn record_value_removed(&self, value: bool) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Delete {
            key: super::generate_operation_key(if value { "remove_true" } else { "remove_false" }),
            old_value: vec![u8::from(value)],
        });
    }

    pub fn record_reset(&self, old_true_count: i64, old_false_count: i64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: to_bytes(&(old_true_count, old_false_count)).unwrap_or_default(),
            new_value: to_bytes(&(0i64, 0i64)).unwrap_or_default(),
        });
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_true_count(&self) -> i64 {
        *self.true_count.lock().unwrap()
    }

    pub fn get_false_count(&self) -> i64 {
        *self.false_count.lock().unwrap()
    }
}

impl StateHolder for AndAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let state_data = AndAggregatorStateData {
            true_count: *self.true_count.lock().unwrap(),
            false_count: *self.false_count.lock().unwrap(),
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize and aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: AndAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize and aggregator state: {e}"),
            })?;

        *self.true_count.lock().unwrap() = state_data.true_count;
        *self.false_count.lock().unwrap() = state_data.false_count;
        self.base
            .restored
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
        self.base.get_changelog(since)
    }

    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError> {
        let mut true_count = self.true_count.lock().unwrap();
        let mut false_count = self.false_count.lock().unwrap();

        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value, .. } => {
                    if value.first() == Some(&1) {
                        *true_count += 1;
                    } else {
                        *false_count += 1;
                    }
                }
                StateOperation::Delete { old_value, .. } => {
                    if old_value.first() == Some(&1) {
                        if *true_count > 0 {
                            *true_count -= 1;
                        }
                    } else if *false_count > 0 {
                        *false_count -= 1;
                    }
                }
                StateOperation::Update { .. } | StateOperation::Clear => {
                    *true_count = 0;
                    *false_count = 0;
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<i64>() * 2,
            entries: 1,
            estimated_growth_rate: 0.0,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.base.component_id.clone(),
            "AndAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "and".to_string());
        metadata.custom_metadata.insert(
            "current_true_count".to_string(),
            self.get_true_count().to_string(),
        );
        metadata.custom_metadata.insert(
            "current_false_count".to_string(),
            self.get_false_count().to_string(),
        );
        metadata
    }

    fn reset_state(&self) {
        *self.true_count.lock().unwrap() = 0;
        *self.false_count.lock().unwrap() = 0;
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for AndAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AndAggregatorStateData {
    true_count: i64,
    false_count: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_and_state_holder_creation() {
        let holder = AndAggregatorStateHolder::new(
            Arc::new(Mutex::new(0)),
            Arc::new(Mutex::new(0)),
            "test_and".to_string(),
        );
        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.get_true_count(), 0);
        assert_eq!(holder.get_false_count(), 0);
    }

    #[test]
    fn test_serialization_deserialization() {
        let holder = AndAggregatorStateHolder::new(
            Arc::new(Mutex::new(5)),
            Arc::new(Mutex::new(2)),
            "test_and".to_string(),
        );

        let hints = SerializationHints::default();
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        *holder.true_count.lock().unwrap() = 0;
        *holder.false_count.lock().unwrap() = 0;
        holder.deserialize_state(&snapshot).unwrap();
        assert_eq!(holder.get_true_count(), 5);
        assert_eq!(holder.get_false_count(), 2);
    }

    #[test]
    fn test_change_log_tracking() {
        let holder = AndAggregatorStateHolder::new(
            Arc::new(Mutex::new(0)),
            Arc::new(Mutex::new(0)),
            "test_and".to_string(),
        );

        holder.record_value_added(true);
        holder.record_value_added(false);
        holder.record_value_removed(true);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        holder.record_reset(1, 1);
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }

    #[test]
    fn test_metadata() {
        let holder = AndAggregatorStateHolder::new(
            Arc::new(Mutex::new(3)),
            Arc::new(Mutex::new(1)),
            "test_and".to_string(),
        );

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "AndAttributeAggregatorExecutor");
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "and"
        );
        assert_eq!(
            metadata.custom_metadata.get("current_true_count").unwrap(),
            "3"
        );
        assert_eq!(
            metadata.custom_metadata.get("current_false_count").unwrap(),
            "1"
        );
    }
}
