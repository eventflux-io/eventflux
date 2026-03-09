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

//! StateHolder implementation for DistinctCountAttributeAggregatorExecutor

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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct DistinctCountAggregatorStateHolder {
    map: Arc<Mutex<HashMap<String, i64>>>,
    pub(crate) base: StateHolderBase,
}

impl DistinctCountAggregatorStateHolder {
    pub fn new(map: Arc<Mutex<HashMap<String, i64>>>, component_id: String) -> Self {
        Self {
            map,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_added(&self, key: &str, old_count: Option<i64>, new_count: i64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();

        if let Some(old) = old_count {
            change_log.push(StateOperation::Update {
                key: key.as_bytes().to_vec(),
                old_value: to_bytes(&old).unwrap_or_default(),
                new_value: to_bytes(&new_count).unwrap_or_default(),
            });
        } else {
            change_log.push(StateOperation::Insert {
                key: key.as_bytes().to_vec(),
                value: to_bytes(&new_count).unwrap_or_default(),
            });
        }
    }

    pub fn record_value_removed(&self, key: &str, old_count: i64, new_count: Option<i64>) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();

        if let Some(new) = new_count {
            change_log.push(StateOperation::Update {
                key: key.as_bytes().to_vec(),
                old_value: to_bytes(&old_count).unwrap_or_default(),
                new_value: to_bytes(&new).unwrap_or_default(),
            });
        } else {
            change_log.push(StateOperation::Delete {
                key: key.as_bytes().to_vec(),
                old_value: to_bytes(&old_count).unwrap_or_default(),
            });
        }
    }

    pub fn record_reset(&self, old_map: &HashMap<String, i64>) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: to_bytes(old_map).unwrap_or_default(),
            new_value: to_bytes(&HashMap::<String, i64>::new()).unwrap_or_default(),
        });
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_distinct_count(&self) -> i64 {
        self.map.lock().unwrap().len() as i64
    }

    pub fn get_distinct_values(&self) -> HashMap<String, i64> {
        self.map.lock().unwrap().clone()
    }

    pub fn get_value_count(&self, key: &str) -> Option<i64> {
        self.map.lock().unwrap().get(key).copied()
    }
}

impl StateHolder for DistinctCountAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let map = self.map.lock().unwrap().clone();
        let state_data = DistinctCountAggregatorStateData::new(map);
        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize distinct count aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: DistinctCountAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize distinct count aggregator state: {e}"),
            })?;

        *self.map.lock().unwrap() = state_data.map;
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

        let mut map = self.map.lock().unwrap();
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { key, value } => {
                    let key_str = String::from_utf8(key.clone()).map_err(|e| {
                        StateError::DeserializationError {
                            message: format!("Failed to deserialize key: {e}"),
                        }
                    })?;
                    let count: i64 =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize count: {e}"),
                        })?;
                    map.insert(key_str, count);
                }
                StateOperation::Delete { key, .. } => {
                    let key_str = String::from_utf8(key.clone()).map_err(|e| {
                        StateError::DeserializationError {
                            message: format!("Failed to deserialize key: {e}"),
                        }
                    })?;
                    map.remove(&key_str);
                }
                StateOperation::Update { key, new_value, .. } => {
                    if key == b"reset" {
                        let new_map: HashMap<String, i64> = from_bytes(new_value).map_err(|e| {
                            StateError::DeserializationError {
                                message: format!("Failed to deserialize new map: {e}"),
                            }
                        })?;
                        *map = new_map;
                    } else {
                        let key_str = String::from_utf8(key.clone()).map_err(|e| {
                            StateError::DeserializationError {
                                message: format!("Failed to deserialize key: {e}"),
                            }
                        })?;
                        let new_count: i64 = from_bytes(new_value).map_err(|e| {
                            StateError::DeserializationError {
                                message: format!("Failed to deserialize new count: {e}"),
                            }
                        })?;
                        map.insert(key_str, new_count);
                    }
                }
                StateOperation::Clear => {
                    map.clear();
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        let map = self.map.lock().unwrap();
        let entries = map.len();
        let key_size_estimate = 20;
        let value_size = std::mem::size_of::<i64>();
        StateSize {
            bytes: entries * (key_size_estimate + value_size),
            entries,
            estimated_growth_rate: 0.1,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.base.component_id.clone(),
            "DistinctCountAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "distinctcount".to_string());
        metadata.custom_metadata.insert(
            "current_distinct_count".to_string(),
            self.get_distinct_count().to_string(),
        );
        metadata.custom_metadata.insert(
            "map_size".to_string(),
            self.get_distinct_values().len().to_string(),
        );

        let distinct_values = self.get_distinct_values();
        if !distinct_values.is_empty() {
            let sample_keys: Vec<String> = distinct_values.keys().take(5).cloned().collect();
            metadata
                .custom_metadata
                .insert("sample_keys".to_string(), format!("{sample_keys:?}"));
        }

        metadata
    }

    fn reset_state(&self) {
        self.map.lock().unwrap().clear();
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for DistinctCountAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DistinctCountAggregatorStateData {
    map: HashMap<String, i64>,
}

impl DistinctCountAggregatorStateData {
    fn new(map: HashMap<String, i64>) -> Self {
        Self { map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_distinctcount_aggregator_state_holder_creation() {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Random);
        assert_eq!(holder.get_distinct_count(), 0);
        assert!(holder.get_distinct_values().is_empty());
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let mut initial_map = HashMap::new();
        initial_map.insert("value1".to_string(), 3);
        initial_map.insert("value2".to_string(), 1);
        initial_map.insert("value3".to_string(), 2);

        let map = Arc::new(Mutex::new(initial_map.clone()));

        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        let hints = SerializationHints::default();

        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        assert_eq!(holder.get_distinct_count(), 3);
        let restored_values = holder.get_distinct_values();
        assert_eq!(restored_values, initial_map);
        assert_eq!(holder.get_value_count("value1"), Some(3));
        assert_eq!(holder.get_value_count("value2"), Some(1));
        assert_eq!(holder.get_value_count("value3"), Some(2));
        assert_eq!(holder.get_value_count("nonexistent"), None);
    }

    #[test]
    fn test_change_log_tracking() {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        holder.record_value_added("value1", None, 1);
        holder.record_value_added("value1", Some(1), 2);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        holder.record_value_removed("value1", 2, Some(1));
        holder.record_value_removed("value1", 1, None);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);

        let mut old_map = HashMap::new();
        old_map.insert("value2".to_string(), 5);
        holder.record_reset(&old_map);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 5);
    }

    #[test]
    fn test_distinct_operations() {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let holder = DistinctCountAggregatorStateHolder::new(
            map.clone(),
            "test_distinctcount_aggregator".to_string(),
        );

        assert_eq!(holder.get_distinct_count(), 0);

        {
            let mut map_guard = map.lock().unwrap();
            map_guard.insert("apple".to_string(), 2);
            map_guard.insert("banana".to_string(), 1);
            map_guard.insert("cherry".to_string(), 3);
        }

        assert_eq!(holder.get_distinct_count(), 3);
        assert_eq!(holder.get_value_count("apple"), Some(2));
        assert_eq!(holder.get_value_count("banana"), Some(1));
        assert_eq!(holder.get_value_count("cherry"), Some(3));
        assert_eq!(holder.get_value_count("date"), None);

        let distinct_values = holder.get_distinct_values();
        assert_eq!(distinct_values.len(), 3);
        assert!(distinct_values.contains_key("apple"));
        assert!(distinct_values.contains_key("banana"));
        assert!(distinct_values.contains_key("cherry"));
    }

    #[test]
    fn test_size_estimation() {
        let mut initial_map = HashMap::new();
        for i in 0..100 {
            initial_map.insert(format!("key_{}", i), i);
        }

        let map = Arc::new(Mutex::new(initial_map));
        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        let size = holder.estimate_size();
        assert_eq!(size.entries, 100);
        assert!(size.bytes > 0);
        assert!(size.estimated_growth_rate > 0.0);
    }

    #[test]
    fn test_metadata() {
        let mut initial_map = HashMap::new();
        initial_map.insert("test_key1".to_string(), 5);
        initial_map.insert("test_key2".to_string(), 3);
        initial_map.insert("test_key3".to_string(), 7);

        let map = Arc::new(Mutex::new(initial_map));
        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        let metadata = holder.component_metadata();
        assert_eq!(
            metadata.component_type,
            "DistinctCountAttributeAggregatorExecutor"
        );
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "distinctcount"
        );
        assert_eq!(
            metadata
                .custom_metadata
                .get("current_distinct_count")
                .unwrap(),
            "3"
        );
        assert_eq!(metadata.custom_metadata.get("map_size").unwrap(), "3");

        assert!(metadata.custom_metadata.contains_key("sample_keys"));

        let empty_map = Arc::new(Mutex::new(HashMap::new()));
        let empty_holder = DistinctCountAggregatorStateHolder::new(
            empty_map,
            "test_empty_distinctcount_aggregator".to_string(),
        );

        let empty_metadata = empty_holder.component_metadata();
        assert_eq!(
            empty_metadata
                .custom_metadata
                .get("current_distinct_count")
                .unwrap(),
            "0"
        );
        assert_eq!(empty_metadata.custom_metadata.get("map_size").unwrap(), "0");
    }

    #[test]
    fn test_changelog_operations() {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        holder.record_value_added("new_key", None, 1);
        holder.record_value_added("new_key", Some(1), 3);
        holder.record_value_removed("new_key", 3, Some(1));
        holder.record_value_removed("new_key", 1, None);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);

        match &changelog.operations[0] {
            StateOperation::Insert { .. } => {}
            _ => panic!("Expected Insert operation"),
        }

        match &changelog.operations[1] {
            StateOperation::Update { .. } => {}
            _ => panic!("Expected Update operation"),
        }

        match &changelog.operations[2] {
            StateOperation::Update { .. } => {}
            _ => panic!("Expected Update operation"),
        }

        match &changelog.operations[3] {
            StateOperation::Delete { .. } => {}
            _ => panic!("Expected Delete operation"),
        }
    }
}
