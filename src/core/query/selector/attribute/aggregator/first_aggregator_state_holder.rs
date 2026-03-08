// SPDX-License-Identifier: MIT OR Apache-2.0

//! StateHolder implementation for FirstAttributeAggregatorExecutor

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
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct FirstAggregatorStateHolder {
    values: Arc<Mutex<VecDeque<AttributeValue>>>,
    pub(crate) base: StateHolderBase,
}

impl FirstAggregatorStateHolder {
    pub fn new(values: Arc<Mutex<VecDeque<AttributeValue>>>, component_id: String) -> Self {
        Self {
            values,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_added(&self, value: &AttributeValue) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Insert {
            key: super::generate_operation_key("add"),
            value: to_bytes(value).unwrap_or_default(),
        });
    }

    pub fn record_value_removed(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Delete {
            key: super::generate_operation_key("remove"),
            old_value: vec![],
        });
    }

    pub fn record_reset(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Clear);
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_values(&self) -> VecDeque<AttributeValue> {
        self.values.lock().unwrap().clone()
    }
}

impl StateHolder for FirstAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let state_data = {
            let values = self.values.lock().unwrap();
            FirstAggregatorStateData {
                values: values.iter().cloned().collect(),
            }
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize first aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: FirstAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize first aggregator state: {e}"),
            })?;

        let mut values = self.values.lock().unwrap();
        values.clear();
        values.extend(state_data.values);
        drop(values);
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

        let mut values = self.values.lock().unwrap();
        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value, .. } => {
                    let v: AttributeValue =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize value: {e}"),
                        })?;
                    values.push_back(v);
                }
                StateOperation::Delete { .. } => {
                    values.pop_front();
                }
                StateOperation::Clear => {
                    values.clear();
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        let values = self.values.lock().unwrap();
        StateSize {
            bytes: values.len() * std::mem::size_of::<AttributeValue>(),
            entries: values.len(),
            estimated_growth_rate: 0.1,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.base.component_id.clone(),
            "FirstAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "first".to_string());
        let values = self.values.lock().unwrap();
        metadata
            .custom_metadata
            .insert("value_count".to_string(), values.len().to_string());
        metadata
    }

    fn reset_state(&self) {
        self.values.lock().unwrap().clear();
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for FirstAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Mixed)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FirstAggregatorStateData {
    values: Vec<AttributeValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_state_holder_creation() {
        let holder = FirstAggregatorStateHolder::new(
            Arc::new(Mutex::new(VecDeque::new())),
            "test_first".to_string(),
        );
        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert!(holder.get_values().is_empty());
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut values = VecDeque::new();
        values.push_back(AttributeValue::Int(10));
        values.push_back(AttributeValue::Int(20));

        let holder =
            FirstAggregatorStateHolder::new(Arc::new(Mutex::new(values)), "test_first".to_string());

        let hints = SerializationHints::default();
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        holder.values.lock().unwrap().clear();
        holder.deserialize_state(&snapshot).unwrap();

        let restored = holder.get_values();
        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0], AttributeValue::Int(10));
        assert_eq!(restored[1], AttributeValue::Int(20));
    }

    #[test]
    fn test_change_log_tracking() {
        let holder = FirstAggregatorStateHolder::new(
            Arc::new(Mutex::new(VecDeque::new())),
            "test_first".to_string(),
        );

        holder.record_value_added(&AttributeValue::Int(1));
        holder.record_value_added(&AttributeValue::Int(2));
        holder.record_value_removed();

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        holder.record_reset();
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }
}
