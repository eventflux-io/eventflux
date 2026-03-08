// SPDX-License-Identifier: MIT OR Apache-2.0

//! StateHolder implementation for MinAttributeAggregatorExecutor

use super::aggregator_state_holder_base::{
    compress_and_build_snapshot, default_compression_hints, verify_and_decompress, StateHolderBase,
};
use crate::core::event::value::AttributeValue;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct MinAggregatorStateHolder {
    value: Arc<Mutex<Option<f64>>>,
    return_type: ApiAttributeType,
    pub(crate) base: StateHolderBase,
}

impl MinAggregatorStateHolder {
    pub fn new(
        value: Arc<Mutex<Option<f64>>>,
        component_id: String,
        return_type: ApiAttributeType,
    ) -> Self {
        Self {
            value,
            return_type,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_updated(&self, old_value: Option<f64>, new_value: Option<f64>) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: super::generate_operation_key("update"),
            old_value: to_bytes(&old_value).unwrap_or_default(),
            new_value: to_bytes(&new_value).unwrap_or_default(),
        });
    }

    pub fn record_reset(&self, old_value: Option<f64>) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: to_bytes(&old_value).unwrap_or_default(),
            new_value: to_bytes(&None::<f64>).unwrap_or_default(),
        });
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_min_value(&self) -> Option<f64> {
        *self.value.lock().unwrap()
    }

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
        let state_data = MinAggregatorStateData::new(value, self.return_type);
        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize min aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: MinAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize min aggregator state: {e}"),
            })?;

        *self.value.lock().unwrap() = state_data.value;
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

        let mut value = self.value.lock().unwrap();
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value: new_val, .. } => {
                    let new_value: Option<f64> =
                        from_bytes(new_val).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new value: {e}"),
                        })?;
                    if let Some(new_v) = new_value {
                        if let Some(current_v) = *value {
                            *value = Some(current_v.min(new_v));
                        } else {
                            *value = Some(new_v);
                        }
                    }
                }
                StateOperation::Delete { .. } => {}
                StateOperation::Update {
                    new_value: new_val, ..
                } => {
                    let new_value: Option<f64> =
                        from_bytes(new_val).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new value: {e}"),
                        })?;
                    *value = new_value;
                }
                StateOperation::Clear => {
                    *value = None;
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<Option<f64>>(),
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
            "MinAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
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

    fn reset_state(&self) {
        *self.value.lock().unwrap() = None;
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for MinAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MinAggregatorStateData {
    value: Option<f64>,
    return_type: String,
}

impl MinAggregatorStateData {
    fn new(value: Option<f64>, return_type: ApiAttributeType) -> Self {
        Self {
            value,
            return_type: format!("{return_type:?}"),
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

        let holder = MinAggregatorStateHolder::new(
            value,
            "test_min_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let hints = SerializationHints::default();

        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

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

        holder.record_value_updated(None, Some(10.0));
        holder.record_value_updated(Some(10.0), Some(5.0));

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        holder.record_reset(Some(5.0));

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);
    }

    #[test]
    fn test_return_type_conversion() {
        let value = Arc::new(Mutex::new(Some(42.7)));

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
        assert!(size.bytes > 0);
        assert_eq!(size.estimated_growth_rate, 0.0);
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
