// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/aggregator/sum_aggregator_state_holder.rs

//! Enhanced StateHolder implementation for SumAttributeAggregatorExecutor
//!
//! This implementation provides enterprise-grade state management for sum aggregation
//! with versioning, incremental checkpointing, and comprehensive metadata.

use crate::core::event::value::AttributeValue;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::{Arc, Mutex};

/// Enhanced state holder for SumAttributeAggregatorExecutor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct SumAggregatorStateHolder {
    /// Sum value
    sum: Arc<Mutex<f64>>,

    /// Count of values added
    count: Arc<Mutex<u64>>,

    /// Return type for the aggregator
    return_type: ApiAttributeType,

    /// Component identifier
    component_id: String,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,
}

impl SumAggregatorStateHolder {
    /// Create a new enhanced state holder
    pub fn new(
        sum: Arc<Mutex<f64>>,
        count: Arc<Mutex<u64>>,
        component_id: String,
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            sum,
            count,
            return_type,
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

    /// Get current aggregated value based on return type
    pub fn get_aggregated_value(&self) -> Option<AttributeValue> {
        let sum = *self.sum.lock().unwrap();
        match self.return_type {
            ApiAttributeType::LONG => Some(AttributeValue::Long(sum as i64)),
            ApiAttributeType::DOUBLE => Some(AttributeValue::Double(sum)),
            _ => None,
        }
    }
}

impl StateHolder for SumAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let sum = *self.sum.lock().unwrap();
        let count = *self.count.lock().unwrap();

        // Create state data structure
        let state_data = SumAggregatorStateData::new(sum, count, self.return_type);

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize sum aggregator state: {e}"),
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
        let state_data: SumAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize sum aggregator state: {e}"),
            })?;

        // Restore aggregator state (return_type is configuration and doesn't need to be restored)
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
        // Sum aggregator has minimal memory footprint
        // Just stores sum (f64) and count (u64) values
        let base_size = std::mem::size_of::<f64>() + std::mem::size_of::<u64>();
        let entries = 1; // Single aggregation value

        // Growth rate is minimal - just accumulates values
        let growth_rate = 0.0; // Aggregators don't grow in size

        StateSize {
            bytes: base_size,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Sum aggregators have random access pattern for value updates
        // But typically accessed sequentially for query results
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "SumAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "sum".to_string());
        metadata
            .custom_metadata
            .insert("return_type".to_string(), format!("{:?}", self.return_type));
        metadata
            .custom_metadata
            .insert("current_sum".to_string(), self.get_sum().to_string());
        metadata
            .custom_metadata
            .insert("current_count".to_string(), self.get_count().to_string());

        metadata
    }
}

impl CompressibleStateHolder for SumAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Aggregators need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::Numeric, // Sum aggregators work with numeric data
            target_latency_ms: Some(1),              // Target < 1ms compression time
            min_compression_ratio: Some(0.2),        // At least 20% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // Aggregator state is very small
        }
    }
}

/// Serializable state data for SumAttributeAggregatorExecutor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SumAggregatorStateData {
    sum: f64,
    count: u64,
    return_type: String, // Serialize as string to avoid serialization issues
}

impl SumAggregatorStateData {
    fn new(sum: f64, count: u64, return_type: ApiAttributeType) -> Self {
        Self {
            sum,
            count,
            return_type: format!("{return_type:?}"),
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        match self.return_type.as_str() {
            "LONG" => ApiAttributeType::LONG,
            "DOUBLE" => ApiAttributeType::DOUBLE,
            "INT" => ApiAttributeType::INT,
            "FLOAT" => ApiAttributeType::FLOAT,
            _ => ApiAttributeType::DOUBLE, // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_sum_aggregator_state_holder_creation() {
        let sum = Arc::new(Mutex::new(0.0));
        let count = Arc::new(Mutex::new(0));
        let holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Random);
        assert_eq!(holder.get_sum(), 0.0);
        assert_eq!(holder.get_count(), 0);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let sum = Arc::new(Mutex::new(42.5));
        let count = Arc::new(Mutex::new(3));

        let mut holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the data was restored
        assert_eq!(holder.get_sum(), 42.5);
        assert_eq!(holder.get_count(), 3);
    }

    #[test]
    fn test_change_log_tracking() {
        let sum = Arc::new(Mutex::new(0.0));
        let count = Arc::new(Mutex::new(0));
        let holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

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
    fn test_aggregated_value_conversion() {
        let sum = Arc::new(Mutex::new(42.7));
        let count = Arc::new(Mutex::new(3));

        // Test DOUBLE return type
        let holder_double = SumAggregatorStateHolder::new(
            sum.clone(),
            count.clone(),
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let value = holder_double.get_aggregated_value().unwrap();
        match value {
            AttributeValue::Double(d) => assert!((d - 42.7).abs() < f64::EPSILON),
            _ => panic!("Expected Double value"),
        }

        // Test LONG return type
        let holder_long = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::LONG,
        );

        let value = holder_long.get_aggregated_value().unwrap();
        match value {
            AttributeValue::Long(l) => assert_eq!(l, 42),
            _ => panic!("Expected Long value"),
        }
    }

    #[test]
    fn test_size_estimation() {
        let sum = Arc::new(Mutex::new(100.0));
        let count = Arc::new(Mutex::new(5));
        let holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let size = holder.estimate_size();
        assert_eq!(size.entries, 1);
        assert!(size.bytes > 0); // Should have some base size
        assert_eq!(size.estimated_growth_rate, 0.0); // Aggregators don't grow
    }

    #[test]
    fn test_metadata() {
        let sum = Arc::new(Mutex::new(123.45));
        let count = Arc::new(Mutex::new(7));
        let holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "SumAttributeAggregatorExecutor");
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "sum"
        );
        assert_eq!(
            metadata.custom_metadata.get("current_sum").unwrap(),
            "123.45"
        );
        assert_eq!(metadata.custom_metadata.get("current_count").unwrap(), "7");
    }
}
