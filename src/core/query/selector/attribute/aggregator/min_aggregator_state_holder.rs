// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/aggregator/min_aggregator_state_holder.rs

//! Enhanced StateHolder implementation for MinAttributeAggregatorExecutor
//!
//! This implementation provides enterprise-grade state management for min aggregation
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

/// Enhanced state holder for MinAttributeAggregatorExecutor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct MinAggregatorStateHolder {
    /// Current minimum value
    value: Arc<Mutex<Option<f64>>>,

    /// Return type for the aggregator
    return_type: ApiAttributeType,

    /// Component identifier
    component_id: String,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,
}

impl MinAggregatorStateHolder {
    /// Create a new enhanced state holder
    pub fn new(
        value: Arc<Mutex<Option<f64>>>,
        component_id: String,
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            value,
            return_type,
            component_id,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a value update for incremental checkpointing
    pub fn record_value_updated(&self, old_value: Option<f64>, new_value: Option<f64>) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Update {
            key: self.generate_operation_key("update"),
            old_value: self.serialize_min_value(old_value),
            new_value: self.serialize_min_value(new_value),
        });
    }

    /// Record a state reset for incremental checkpointing
    pub fn record_reset(&self, old_value: Option<f64>) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: self.serialize_min_value(old_value),
            new_value: self.serialize_min_value(None),
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

    /// Serialize a min value to bytes
    fn serialize_min_value(&self, value: Option<f64>) -> Vec<u8> {
        use crate::core::util::to_bytes;
        to_bytes(&value).unwrap_or_default()
    }

    /// Clear the change log (called after successful checkpoint)
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }

    /// Get current minimum value
    pub fn get_min_value(&self) -> Option<f64> {
        *self.value.lock().unwrap()
    }

    /// Get current aggregated value as AttributeValue
    pub fn get_aggregated_value(&self) -> Option<AttributeValue> {
        let value = *self.value.lock().unwrap();
        let value = value?;
        match self.return_type {
            ApiAttributeType::INT => Some(AttributeValue::Int(value as i32)),
            ApiAttributeType::LONG => Some(AttributeValue::Long(value as i64)),
            ApiAttributeType::FLOAT => Some(AttributeValue::Float(value as f32)),
            _ => Some(AttributeValue::Double(value)),
        }
    }
}

impl StateHolder for MinAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let value = *self.value.lock().unwrap();

        // Create state data structure
        let state_data = MinAggregatorStateData::new(value, self.return_type);

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize min aggregator state: {e}"),
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
        let state_data: MinAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize min aggregator state: {e}"),
            })?;

        // Restore aggregator state (return_type is configuration and doesn't need to be restored)
        *self.value.lock().unwrap() = state_data.value;

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
        let mut value = self.value.lock().unwrap();

        // Apply each operation in order
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value: new_val, .. } => {
                    // Deserialize the new value
                    let new_value: Option<f64> =
                        from_bytes(new_val).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new value: {e}"),
                        })?;

                    // Update min value (insert is used when updating the min)
                    if let Some(new_v) = new_value {
                        if let Some(current_v) = *value {
                            *value = Some(current_v.min(new_v));
                        } else {
                            *value = Some(new_v);
                        }
                    }
                }
                StateOperation::Delete { .. } => {
                    // For min aggregator, delete might indicate value removal
                    // However, min typically doesn't support removal in the traditional sense
                    // This could be a no-op or reset depending on implementation
                }
                StateOperation::Update {
                    new_value: new_val, ..
                } => {
                    // Deserialize new value (used for update/reset operations)
                    let new_value: Option<f64> =
                        from_bytes(new_val).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new value: {e}"),
                        })?;

                    // Replace current value with new value
                    *value = new_value;
                }
                StateOperation::Clear => {
                    // Reset to initial state
                    *value = None;
                }
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        // Min aggregator has minimal memory footprint
        // Just stores an optional f64 value
        let base_size = std::mem::size_of::<Option<f64>>();
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
        // Min aggregators have random access pattern for value updates
        // But typically accessed sequentially for query results
        AccessPattern::Random
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "MinAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "min".to_string());
        metadata
            .custom_metadata
            .insert("return_type".to_string(), format!("{:?}", self.return_type));
        if let Some(min_value) = self.get_min_value() {
            metadata
                .custom_metadata
                .insert("current_min".to_string(), min_value.to_string());
        } else {
            metadata
                .custom_metadata
                .insert("current_min".to_string(), "None".to_string());
        }

        metadata
    }
}

impl CompressibleStateHolder for MinAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Aggregators need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::Numeric, // Min aggregators work with numeric data
            target_latency_ms: Some(1),              // Target < 1ms compression time
            min_compression_ratio: Some(0.2),        // At least 20% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // Aggregator state is very small
        }
    }
}

/// Serializable state data for MinAttributeAggregatorExecutor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MinAggregatorStateData {
    value: Option<f64>,
    return_type: String, // Serialize as string to avoid serialization issues
}

impl MinAggregatorStateData {
    fn new(value: Option<f64>, return_type: ApiAttributeType) -> Self {
        Self {
            value,
            return_type: format!("{return_type:?}"),
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        match self.return_type.as_str() {
            "INT" => ApiAttributeType::INT,
            "LONG" => ApiAttributeType::LONG,
            "FLOAT" => ApiAttributeType::FLOAT,
            "DOUBLE" => ApiAttributeType::DOUBLE,
            _ => ApiAttributeType::DOUBLE, // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_min_aggregator_state_holder_creation() {
        let value = Arc::new(Mutex::new(None));
        let holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Random);
        assert_eq!(holder.get_min_value(), None);
        assert_eq!(holder.get_aggregated_value(), None);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let value = Arc::new(Mutex::new(Some(42.5)));

        let mut holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
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
        assert_eq!(holder.get_min_value(), Some(42.5));

        let value = holder.get_aggregated_value().unwrap();
        match value {
            AttributeValue::Double(d) => assert!((d - 42.5).abs() < f64::EPSILON),
            _ => panic!("Expected Double value"),
        }
    }

    #[test]
    fn test_change_log_tracking() {
        let value = Arc::new(Mutex::new(None));
        let holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        // Record value updates
        holder.record_value_updated(None, Some(10.0));
        holder.record_value_updated(Some(10.0), Some(5.0));

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        // Record reset
        holder.record_reset(Some(5.0));

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);
    }

    #[test]
    fn test_return_type_conversion() {
        let value = Arc::new(Mutex::new(Some(42.7)));

        // Test INT return type
        let holder_int = MinAggregatorStateHolder::new(
            value.clone(),
            "test_min_aggregator".to_string(),
            ApiAttributeType::INT,
        );

        let value_int = holder_int.get_aggregated_value().unwrap();
        match value_int {
            AttributeValue::Int(i) => assert_eq!(i, 42),
            _ => panic!("Expected Int value"),
        }

        // Test LONG return type
        let holder_long = MinAggregatorStateHolder::new(
            value.clone(),
            "test_min_aggregator".to_string(),
            ApiAttributeType::LONG,
        );

        let value_long = holder_long.get_aggregated_value().unwrap();
        match value_long {
            AttributeValue::Long(l) => assert_eq!(l, 42),
            _ => panic!("Expected Long value"),
        }

        // Test FLOAT return type
        let holder_float = MinAggregatorStateHolder::new(
            value.clone(),
            "test_min_aggregator".to_string(),
            ApiAttributeType::FLOAT,
        );

        let value_float = holder_float.get_aggregated_value().unwrap();
        match value_float {
            AttributeValue::Float(f) => assert!((f - 42.7).abs() < 0.01),
            _ => panic!("Expected Float value"),
        }

        // Test DOUBLE return type
        let holder_double = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let value_double = holder_double.get_aggregated_value().unwrap();
        match value_double {
            AttributeValue::Double(d) => assert!((d - 42.7).abs() < f64::EPSILON),
            _ => panic!("Expected Double value"),
        }
    }

    #[test]
    fn test_size_estimation() {
        let value = Arc::new(Mutex::new(Some(100.0)));
        let holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let size = holder.estimate_size();
        assert_eq!(size.entries, 1);
        assert!(size.bytes > 0); // Should have some base size
        assert_eq!(size.estimated_growth_rate, 0.0); // Aggregators don't grow
    }

    #[test]
    fn test_metadata() {
        let value = Arc::new(Mutex::new(Some(123.45)));
        let holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "MinAttributeAggregatorExecutor");
        assert_eq!(
            metadata.custom_metadata.get("aggregator_type").unwrap(),
            "min"
        );
        assert_eq!(
            metadata.custom_metadata.get("current_min").unwrap(),
            "123.45"
        );

        // Test with None value
        let empty_value = Arc::new(Mutex::new(None));
        let empty_holder = MinAggregatorStateHolder::new(
            empty_value,
            "test_empty_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let empty_metadata = empty_holder.component_metadata();
        assert_eq!(
            empty_metadata.custom_metadata.get("current_min").unwrap(),
            "None"
        );
    }
}
