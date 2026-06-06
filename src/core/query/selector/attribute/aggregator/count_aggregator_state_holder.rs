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

//! StateHolder implementation for CountAttributeAggregatorExecutor

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
pub struct CountAggregatorStateHolder {
    count: Arc<Mutex<i64>>,
    pub(crate) base: StateHolderBase,
}

impl CountAggregatorStateHolder {
    pub fn new(count: Arc<Mutex<i64>>, component_id: String) -> Self {
        Self {
            count,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_increment(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Insert {
            key: super::generate_operation_key("increment"),
            value: vec![1],
        });
    }

    pub fn record_decrement(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Delete {
            key: super::generate_operation_key("decrement"),
            old_value: vec![1],
        });
    }

    pub fn record_reset(&self, old_count: i64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: to_bytes(&old_count).unwrap_or_default(),
            new_value: to_bytes(&0i64).unwrap_or_default(),
        });
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_count(&self) -> i64 {
        *self.count.lock().unwrap()
    }
}

impl StateHolder for CountAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let count = *self.count.lock().unwrap();
        let state_data = CountAggregatorStateData::new(count);
        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize count aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: CountAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize count aggregator state: {e}"),
            })?;

        *self.count.lock().unwrap() = state_data.count;
        self.base
            .restored
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
        self.base.get_changelog(since)
    }

    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let mut count = self.count.lock().unwrap();
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { .. } => {
                    *count += 1;
                }
                StateOperation::Delete { .. } => {
                    if *count > 0 {
                        *count -= 1;
                    }
                }
                StateOperation::Update { new_value, .. } => {
                    let new_count: i64 =
                        from_bytes(new_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new count: {e}"),
                        })?;
                    *count = new_count;
                }
                StateOperation::Clear => {
                    *count = 0;
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<i64>(),
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
            "CountAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "count".to_string());
        metadata
            .custom_metadata
            .insert("current_count".to_string(), self.get_count().to_string());
        metadata
    }

    fn reset_state(&self) {
        *self.count.lock().unwrap() = 0;
        self.base
            .restored
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for CountAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CountAggregatorStateData {
    count: i64,
}

impl CountAggregatorStateData {
    fn new(count: i64) -> Self {
        Self { count }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_count_aggregator_state_holder_creation() {
        let count = Arc::new(Mutex::new(0));
        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Random);
        assert_eq!(holder.get_count(), 0);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let count = Arc::new(Mutex::new(42));

        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        let hints = SerializationHints::default();

        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        assert_eq!(holder.get_count(), 42);
    }

    #[test]
    fn test_change_log_tracking() {
        let count = Arc::new(Mutex::new(0));
        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        holder.record_increment();
        holder.record_increment();

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        holder.record_decrement();

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        holder.record_reset(2);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }

    #[test]
    fn test_size_estimation() {
        let count = Arc::new(Mutex::new(100));
        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        let size = holder.estimate_size();
        assert_eq!(size.entries, 1);
        assert!(size.bytes > 0);
        assert_eq!(size.estimated_growth_rate, 0.0);
    }

    #[test]
    fn test_metadata() {
        let count = Arc::new(Mutex::new(99));
        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "CountAttributeAggregatorExecutor");
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "count"
        );
        assert_eq!(metadata.custom_metadata.get("current_count").unwrap(), "99");
    }
}
