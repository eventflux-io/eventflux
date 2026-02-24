// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/aggregator/distinctcount_aggregator_state_holder.rs

//! Enhanced StateHolder implementation for DistinctCountAttributeAggregatorExecutor
//!
//! This implementation provides enterprise-grade state management for distinct count aggregation
//! with versioning, incremental checkpointing, and comprehensive metadata.

use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Enhanced state holder for DistinctCountAttributeAggregatorExecutor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct DistinctCountAggregatorStateHolder {
    /// Map of distinct values to their counts
    map: Arc<Mutex<HashMap<String, i64>>>,

    /// Component identifier
    component_id: String,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,
}

impl DistinctCountAggregatorStateHolder {
    /// Create a new enhanced state holder
    pub fn new(map: Arc<Mutex<HashMap<String, i64>>>, component_id: String) -> Self {
        Self {
            map,
            component_id,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a value addition for incremental checkpointing
    pub fn record_value_added(&self, key: &str, old_count: Option<i64>, new_count: i64) {
        let mut change_log = self.change_log.lock().unwrap();

        if old_count.is_none() {
            // New distinct value
            change_log.push(StateOperation::Insert {
                key: self.serialize_key(key),
                value: self.serialize_count(new_count),
            });
        } else {
            // Existing value count updated
            change_log.push(StateOperation::Update {
                key: self.serialize_key(key),
                old_value: self.serialize_count(old_count.unwrap()),
                new_value: self.serialize_count(new_count),
            });
        }
    }

    /// Record a value removal for incremental checkpointing
    pub fn record_value_removed(&self, key: &str, old_count: i64, new_count: Option<i64>) {
        let mut change_log = self.change_log.lock().unwrap();

        if new_count.is_none() {
            // Value completely removed
            change_log.push(StateOperation::Delete {
                key: self.serialize_key(key),
                old_value: self.serialize_count(old_count),
            });
        } else {
            // Value count decreased
            change_log.push(StateOperation::Update {
                key: self.serialize_key(key),
                old_value: self.serialize_count(old_count),
                new_value: self.serialize_count(new_count.unwrap()),
            });
        }
    }

    /// Record a state reset for incremental checkpointing
    pub fn record_reset(&self, old_map: &HashMap<String, i64>) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: self.serialize_map(old_map),
            new_value: self.serialize_map(&HashMap::new()),
        });
    }

    /// Serialize a key to bytes
    fn serialize_key(&self, key: &str) -> Vec<u8> {
        key.as_bytes().to_vec()
    }

    /// Serialize a count value to bytes
    fn serialize_count(&self, count: i64) -> Vec<u8> {
        use crate::core::util::to_bytes;
        to_bytes(&count).unwrap_or_default()
    }

    /// Serialize a map to bytes
    fn serialize_map(&self, map: &HashMap<String, i64>) -> Vec<u8> {
        use crate::core::util::to_bytes;
        to_bytes(map).unwrap_or_default()
    }

    /// Clear the change log (called after successful checkpoint)
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }

    /// Get current distinct count
    pub fn get_distinct_count(&self) -> i64 {
        self.map.lock().unwrap().len() as i64
    }

    /// Get current map of distinct values
    pub fn get_distinct_values(&self) -> HashMap<String, i64> {
        self.map.lock().unwrap().clone()
    }

    /// Get value count for a specific key
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

        // Create state data structure
        let state_data = DistinctCountAggregatorStateData::new(map);

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize distinct count aggregator state: {e}"),
        })?;

        // Apply compression if requested using the shared compression utility
        let (compressed_data, compression_type) =
            if let Some(ref compression) = hints.prefer_compression {
                match self.compress_state_data(&data, Some(compression.clone())) {
                    Ok((compressed, comp_type)) => (compressed, comp_type),
                    Err(_) => {
                        // Fall back to no compression if compression fails
                        (data, CompressionType::None)
                    }
                }
            } else {
                // Use intelligent compression selection
                match self.compress_state_data(&data, None) {
                    Ok((compressed, comp_type)) => (compressed, comp_type),
                    Err(_) => (data, CompressionType::None),
                }
            };

        let data = compressed_data;
        let compression = compression_type;

        let checksum = StateSnapshot::calculate_checksum(&data);

        Ok(StateSnapshot {
            version: self.schema_version(),
            checkpoint_id: 0, // Will be set by the checkpoint coordinator
            data,
            compression,
            checksum,
            metadata: self.component_metadata(),
        })
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        // Verify integrity
        if !snapshot.verify_integrity() {
            return Err(StateError::ChecksumMismatch);
        }

        // Decompress data if needed using the shared compression utility
        let data = self.decompress_state_data(&snapshot.data, snapshot.compression.clone())?;

        // Deserialize state data
        let state_data: DistinctCountAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize distinct count aggregator state: {e}"),
            })?;

        // Restore aggregator state
        *self.map.lock().unwrap() = state_data.map;

        Ok(())
    }

    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
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

    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        // Lock state structure once for efficiency
        let mut map = self.map.lock().unwrap();

        // Apply each operation in order
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { key, value } => {
                    // Deserialize key and count
                    let key_str = String::from_utf8(key.clone()).map_err(|e| {
                        StateError::DeserializationError {
                            message: format!("Failed to deserialize key: {e}"),
                        }
                    })?;

                    let count: i64 =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize count: {e}"),
                        })?;

                    // Insert new distinct value with its count
                    map.insert(key_str, count);
                }
                StateOperation::Delete { key, .. } => {
                    // Deserialize key
                    let key_str = String::from_utf8(key.clone()).map_err(|e| {
                        StateError::DeserializationError {
                            message: format!("Failed to deserialize key: {e}"),
                        }
                    })?;

                    // Remove distinct value from map
                    map.remove(&key_str);
                }
                StateOperation::Update { key, new_value, .. } => {
                    // Check if this is a reset operation
                    if key == b"reset" {
                        // Deserialize new map (used for reset operations)
                        let new_map: HashMap<String, i64> = from_bytes(new_value).map_err(|e| {
                            StateError::DeserializationError {
                                message: format!("Failed to deserialize new map: {e}"),
                            }
                        })?;

                        // Replace current map with new map
                        *map = new_map;
                    } else {
                        // Deserialize key and new count
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

                        // Update the count for existing distinct value
                        map.insert(key_str, new_count);
                    }
                }
                StateOperation::Clear => {
                    // Reset to initial state
                    map.clear();
                }
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        // Distinct count aggregator memory footprint depends on number of distinct values
        let map = self.map.lock().unwrap();
        let entries = map.len();

        // Estimate size based on map entries
        let key_size_estimate = 20; // Average key size estimate
        let value_size = std::mem::size_of::<i64>();
        let base_size = entries * (key_size_estimate + value_size);

        // Growth rate depends on data cardinality - could be high for high-cardinality data
        let growth_rate = 0.1; // Moderate growth as new distinct values are added

        StateSize {
            bytes: base_size,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Distinct count aggregators have random access pattern for lookups
        // Map operations are typically random access
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "DistinctCountAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
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

        // Add sample of distinct values (limited to avoid large metadata)
        let distinct_values = self.get_distinct_values();
        if !distinct_values.is_empty() {
            let sample_keys: Vec<String> = distinct_values.keys().take(5).cloned().collect();
            metadata
                .custom_metadata
                .insert("sample_keys".to_string(), format!("{sample_keys:?}"));
        }

        metadata
    }
}

impl CompressibleStateHolder for DistinctCountAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Aggregators need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::Numeric, // DistinctCount aggregators work with key-value numeric data
            target_latency_ms: Some(1),              // Target < 1ms compression time
            min_compression_ratio: Some(0.2),        // At least 20% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // DistinctCount aggregator state is typically small
        }
    }
}

/// Serializable state data for DistinctCountAttributeAggregatorExecutor
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

        let mut holder = DistinctCountAggregatorStateHolder::new(
            map,
            "test_distinctcount_aggregator".to_string(),
        );

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the data was restored
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

        // Record value additions
        holder.record_value_added("value1", None, 1);
        holder.record_value_added("value1", Some(1), 2);

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        // Record value removal
        holder.record_value_removed("value1", 2, Some(1));
        holder.record_value_removed("value1", 1, None);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);

        // Record reset
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

        // Initially empty
        assert_eq!(holder.get_distinct_count(), 0);

        // Add values to the map directly (simulating aggregator behavior)
        {
            let mut map_guard = map.lock().unwrap();
            map_guard.insert("apple".to_string(), 2);
            map_guard.insert("banana".to_string(), 1);
            map_guard.insert("cherry".to_string(), 3);
        }

        // Check distinct count and values
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
        assert!(size.bytes > 0); // Should have substantial size with 100 entries
        assert!(size.estimated_growth_rate > 0.0); // Distinct count can grow
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

        // Should have sample keys
        assert!(metadata.custom_metadata.contains_key("sample_keys"));

        // Test with empty map
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

        // Test various operation types
        holder.record_value_added("new_key", None, 1); // Insert
        holder.record_value_added("new_key", Some(1), 3); // Update (increment)
        holder.record_value_removed("new_key", 3, Some(1)); // Update (decrement)
        holder.record_value_removed("new_key", 1, None); // Delete

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);

        // Verify operation types
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
