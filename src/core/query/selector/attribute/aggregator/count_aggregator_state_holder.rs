// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/aggregator/count_aggregator_state_holder.rs

//! Enhanced StateHolder implementation for CountAttributeAggregatorExecutor
//!
//! This implementation provides enterprise-grade state management for count aggregation
//! with versioning, incremental checkpointing, and comprehensive metadata.

use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use std::sync::{Arc, Mutex};

/// Enhanced state holder for CountAttributeAggregatorExecutor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct CountAggregatorStateHolder {
    /// Count value
    count: Arc<Mutex<i64>>,

    /// Component identifier
    component_id: String,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,
}

impl CountAggregatorStateHolder {
    /// Create a new enhanced state holder
    pub fn new(count: Arc<Mutex<i64>>, component_id: String) -> Self {
        Self {
            count,
            component_id,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a count increment for incremental checkpointing
    pub fn record_increment(&self) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Insert {
            key: self.generate_operation_key("increment"),
            value: vec![1], // Simple increment operation
        });
    }

    /// Record a count decrement for incremental checkpointing
    pub fn record_decrement(&self) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Delete {
            key: self.generate_operation_key("decrement"),
            old_value: vec![1], // Simple decrement operation
        });
    }

    /// Record a state reset for incremental checkpointing
    pub fn record_reset(&self, old_count: i64) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: self.serialize_count(old_count),
            new_value: self.serialize_count(0),
        });
    }

    /// Generate a unique key for an operation
    fn generate_operation_key(&self, operation_type: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(operation_type.as_bytes());
        key.push(b'_');

        // Add timestamp for uniqueness
        let timestamp = chrono::Utc::now().timestamp_millis();
        key.extend_from_slice(&timestamp.to_le_bytes());

        key
    }

    /// Serialize count value to bytes
    fn serialize_count(&self, count: i64) -> Vec<u8> {
        use crate::core::util::to_bytes;
        to_bytes(&count).unwrap_or_default()
    }

    /// Clear the change log (called after successful checkpoint)
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }

    /// Get current count value
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

        // Create state data structure
        let state_data = CountAggregatorStateData::new(count);

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize count aggregator state: {e}"),
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
        let state_data: CountAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize count aggregator state: {e}"),
            })?;

        // Restore aggregator state
        *self.count.lock().unwrap() = state_data.count;

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
        let mut count = self.count.lock().unwrap();

        // Apply each operation in order
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { .. } => {
                    // Insert represents an increment operation
                    *count += 1;
                }
                StateOperation::Delete { .. } => {
                    // Delete represents a decrement operation
                    if *count > 0 {
                        *count -= 1;
                    }
                }
                StateOperation::Update { new_value, .. } => {
                    // Deserialize new count (used for reset operations)
                    let new_count: i64 =
                        from_bytes(new_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new count: {e}"),
                        })?;

                    // Replace current count with new count
                    *count = new_count;
                }
                StateOperation::Clear => {
                    // Reset to initial state
                    *count = 0;
                }
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        // Count aggregator has minimal memory footprint - just stores i64 value
        let base_size = std::mem::size_of::<i64>();
        let entries = 1; // Single count value

        // Growth rate is minimal - aggregators don't grow in size
        let growth_rate = 0.0; // Aggregators don't grow in size

        StateSize {
            bytes: base_size,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Count aggregators have random access pattern for value updates
        // But typically accessed sequentially for query results
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "CountAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "count".to_string());
        metadata
            .custom_metadata
            .insert("current_count".to_string(), self.get_count().to_string());

        metadata
    }
}

impl CompressibleStateHolder for CountAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Aggregators need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::Numeric, // Count aggregators work with numeric data
            target_latency_ms: Some(1),              // Target < 1ms compression time
            min_compression_ratio: Some(0.2),        // At least 20% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // Aggregator state is very small
        }
    }
}

/// Serializable state data for CountAttributeAggregatorExecutor
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

        let mut holder =
            CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the data was restored
        assert_eq!(holder.get_count(), 42);
    }

    #[test]
    fn test_change_log_tracking() {
        let count = Arc::new(Mutex::new(0));
        let holder = CountAggregatorStateHolder::new(count, "test_count_aggregator".to_string());

        // Record increments
        holder.record_increment();
        holder.record_increment();

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        // Record decrement
        holder.record_decrement();

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        // Record reset
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
        assert!(size.bytes > 0); // Should have some base size
        assert_eq!(size.estimated_growth_rate, 0.0); // Aggregators don't grow
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
