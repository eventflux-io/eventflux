// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/aggregator/avg_aggregator_state_holder.rs

//! Enhanced StateHolder implementation for AvgAttributeAggregatorExecutor
//!
//! This implementation provides enterprise-grade state management for average aggregation
//! with versioning, incremental checkpointing, and comprehensive metadata.

use crate::core::event::value::AttributeValue;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use std::sync::{Arc, Mutex};

/// Enhanced state holder for AvgAttributeAggregatorExecutor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct AvgAggregatorStateHolder {
    /// Sum value
    sum: Arc<Mutex<f64>>,

    /// Count of values added
    count: Arc<Mutex<u64>>,

    /// Component identifier
    component_id: String,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,
}

impl AvgAggregatorStateHolder {
    /// Create a new enhanced state holder
    pub fn new(sum: Arc<Mutex<f64>>, count: Arc<Mutex<u64>>, component_id: String) -> Self {
        Self {
            sum,
            count,
            component_id,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a value addition for incremental checkpointing
    pub fn record_value_added(&self, value: f64) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Insert {
            key: self.generate_operation_key("add"),
            value: self.serialize_value_operation(value),
        });
    }

    /// Record a value removal for incremental checkpointing
    pub fn record_value_removed(&self, value: f64) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Delete {
            key: self.generate_operation_key("remove"),
            old_value: self.serialize_value_operation(value),
        });
    }

    /// Record a state reset for incremental checkpointing
    pub fn record_reset(&self, old_sum: f64, old_count: u64) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: self.serialize_state_values(old_sum, old_count),
            new_value: self.serialize_state_values(0.0, 0),
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

    /// Serialize a value operation to bytes
    fn serialize_value_operation(&self, value: f64) -> Vec<u8> {
        use crate::core::util::to_bytes;
        to_bytes(&value).unwrap_or_default()
    }

    /// Serialize state values to bytes
    fn serialize_state_values(&self, sum: f64, count: u64) -> Vec<u8> {
        use crate::core::util::to_bytes;
        let state_data = (sum, count);
        to_bytes(&state_data).unwrap_or_default()
    }

    /// Clear the change log (called after successful checkpoint)
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }

    /// Get current sum value
    pub fn get_sum(&self) -> f64 {
        *self.sum.lock().unwrap()
    }

    /// Get current count value
    pub fn get_count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

    /// Get current average value
    pub fn get_average(&self) -> Option<f64> {
        let sum = *self.sum.lock().unwrap();
        let count = *self.count.lock().unwrap();

        if count == 0 {
            None
        } else {
            Some(sum / count as f64)
        }
    }

    /// Get current aggregated value as AttributeValue
    pub fn get_aggregated_value(&self) -> Option<AttributeValue> {
        self.get_average().map(AttributeValue::Double)
    }
}

impl StateHolder for AvgAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let sum = *self.sum.lock().unwrap();
        let count = *self.count.lock().unwrap();

        // Create state data structure
        let state_data = AvgAggregatorStateData::new(sum, count);

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize avg aggregator state: {e}"),
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
        let state_data: AvgAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize avg aggregator state: {e}"),
            })?;

        // Restore aggregator state
        *self.sum.lock().unwrap() = state_data.sum;
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

        // Lock state structures once for efficiency
        let mut sum = self.sum.lock().unwrap();
        let mut count = self.count.lock().unwrap();

        // Apply each operation in order
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value, .. } => {
                    // Deserialize the value that was added
                    let added_value: f64 =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize added value: {e}"),
                        })?;

                    // Add to sum and increment count
                    *sum += added_value;
                    *count += 1;
                }
                StateOperation::Delete { old_value, .. } => {
                    // Deserialize the value that was removed
                    let removed_value: f64 =
                        from_bytes(old_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize removed value: {e}"),
                        })?;

                    // Subtract from sum and decrement count
                    *sum -= removed_value;
                    if *count > 0 {
                        *count -= 1;
                    }
                }
                StateOperation::Update { new_value, .. } => {
                    // Deserialize new state (used for reset operations)
                    let new_state: (f64, u64) =
                        from_bytes(new_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new state: {e}"),
                        })?;

                    // Replace current state with new state
                    *sum = new_state.0;
                    *count = new_state.1;
                }
                StateOperation::Clear => {
                    // Reset to initial state
                    *sum = 0.0;
                    *count = 0;
                }
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        // Avg aggregator has minimal memory footprint
        // Just stores sum (f64) and count (u64) values
        let base_size = std::mem::size_of::<f64>() + std::mem::size_of::<u64>();
        let entries = 1; // Single aggregation value

        // Growth rate is minimal - aggregators don't grow in size
        let growth_rate = 0.0; // Aggregators don't grow in size

        StateSize {
            bytes: base_size,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Avg aggregators have random access pattern for value updates
        // But typically accessed sequentially for query results
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "AvgAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "avg".to_string());
        metadata
            .custom_metadata
            .insert("current_sum".to_string(), self.get_sum().to_string());
        metadata
            .custom_metadata
            .insert("current_count".to_string(), self.get_count().to_string());
        if let Some(avg) = self.get_average() {
            metadata
                .custom_metadata
                .insert("current_average".to_string(), avg.to_string());
        }

        metadata
    }
}

impl CompressibleStateHolder for AvgAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Aggregators need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::Numeric, // Avg aggregators work with numeric data
            target_latency_ms: Some(1),              // Target < 1ms compression time
            min_compression_ratio: Some(0.2),        // At least 20% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // Aggregator state is very small
        }
    }
}

/// Serializable state data for AvgAttributeAggregatorExecutor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AvgAggregatorStateData {
    sum: f64,
    count: u64,
}

impl AvgAggregatorStateData {
    fn new(sum: f64, count: u64) -> Self {
        Self { sum, count }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_avg_aggregator_state_holder_creation() {
        let sum = Arc::new(Mutex::new(0.0));
        let count = Arc::new(Mutex::new(0));
        let holder = AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Random);
        assert_eq!(holder.get_sum(), 0.0);
        assert_eq!(holder.get_count(), 0);
        assert_eq!(holder.get_average(), None);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let sum = Arc::new(Mutex::new(100.0));
        let count = Arc::new(Mutex::new(4));

        let mut holder =
            AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the data was restored
        assert_eq!(holder.get_sum(), 100.0);
        assert_eq!(holder.get_count(), 4);
        assert_eq!(holder.get_average(), Some(25.0));
    }

    #[test]
    fn test_change_log_tracking() {
        let sum = Arc::new(Mutex::new(0.0));
        let count = Arc::new(Mutex::new(0));
        let holder = AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        // Record value additions
        holder.record_value_added(10.0);
        holder.record_value_added(20.0);

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        // Record value removal
        holder.record_value_removed(5.0);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        // Record reset
        holder.record_reset(25.0, 2);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }

    #[test]
    fn test_average_calculation() {
        let sum = Arc::new(Mutex::new(30.0));
        let count = Arc::new(Mutex::new(3));
        let holder = AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        // Test average calculation
        assert_eq!(holder.get_average(), Some(10.0));

        // Test aggregated value
        let value = holder.get_aggregated_value().unwrap();
        match value {
            AttributeValue::Double(d) => assert!((d - 10.0).abs() < f64::EPSILON),
            _ => panic!("Expected Double value"),
        }

        // Test empty case
        let empty_sum = Arc::new(Mutex::new(0.0));
        let empty_count = Arc::new(Mutex::new(0));
        let empty_holder = AvgAggregatorStateHolder::new(
            empty_sum,
            empty_count,
            "test_empty_avg_aggregator".to_string(),
        );

        assert_eq!(empty_holder.get_average(), None);
        assert_eq!(empty_holder.get_aggregated_value(), None);
    }

    #[test]
    fn test_size_estimation() {
        let sum = Arc::new(Mutex::new(100.0));
        let count = Arc::new(Mutex::new(5));
        let holder = AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        let size = holder.estimate_size();
        assert_eq!(size.entries, 1);
        assert!(size.bytes > 0); // Should have some base size
        assert_eq!(size.estimated_growth_rate, 0.0); // Aggregators don't grow
    }

    #[test]
    fn test_metadata() {
        let sum = Arc::new(Mutex::new(75.0));
        let count = Arc::new(Mutex::new(3));
        let holder = AvgAggregatorStateHolder::new(sum, count, "test_avg_aggregator".to_string());

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "AvgAttributeAggregatorExecutor");
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "avg"
        );
        assert_eq!(metadata.custom_metadata.get("current_sum").unwrap(), "75");
        assert_eq!(metadata.custom_metadata.get("current_count").unwrap(), "3");
        assert_eq!(
            metadata.custom_metadata.get("current_average").unwrap(),
            "25"
        );
    }
}
