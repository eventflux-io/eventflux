// SPDX-License-Identifier: MIT OR Apache-2.0

//! StateHolder implementation for LastAttributeAggregatorExecutor

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
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct LastAggregatorStateHolder {
    last_value: Arc<Mutex<Option<AttributeValue>>>,
    pub(crate) base: StateHolderBase,
}

impl LastAggregatorStateHolder {
    pub fn new(last_value: Arc<Mutex<Option<AttributeValue>>>, component_id: String) -> Self {
        Self {
            last_value,
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_updated(&self, value: &AttributeValue) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: super::generate_operation_key("update"),
            old_value: vec![],
            new_value: to_bytes(value).unwrap_or_default(),
        });
    }

    pub fn record_reset(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Clear);
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_last_value(&self) -> Option<AttributeValue> {
        self.last_value.lock().unwrap().clone()
    }
}

impl StateHolder for LastAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let value = self.last_value.lock().unwrap().clone();
        let state_data = LastAggregatorStateData { last_value: value };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize last aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: LastAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize last aggregator state: {e}"),
            })?;

        *self.last_value.lock().unwrap() = state_data.last_value;
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

        let mut last_value = self.last_value.lock().unwrap();
        for operation in &changes.operations {
            match operation {
                StateOperation::Update { new_value, .. } => {
                    let v: AttributeValue =
                        from_bytes(new_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize value: {e}"),
                        })?;
                    *last_value = Some(v);
                }
                StateOperation::Clear => {
                    *last_value = None;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<Option<AttributeValue>>(),
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
            "LastAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "last".to_string());
        metadata
    }

    fn reset_state(&self) {
        *self.last_value.lock().unwrap() = None;
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for LastAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Mixed)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LastAggregatorStateData {
    last_value: Option<AttributeValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_last_state_holder_creation() {
        let holder =
            LastAggregatorStateHolder::new(Arc::new(Mutex::new(None)), "test_last".to_string());
        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.get_last_value(), None);
    }

    #[test]
    fn test_serialization_deserialization() {
        let holder = LastAggregatorStateHolder::new(
            Arc::new(Mutex::new(Some(AttributeValue::Int(42)))),
            "test_last".to_string(),
        );

        let hints = SerializationHints::default();
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        *holder.last_value.lock().unwrap() = None;
        holder.deserialize_state(&snapshot).unwrap();
        assert_eq!(holder.get_last_value(), Some(AttributeValue::Int(42)));
    }

    #[test]
    fn test_change_log_tracking() {
        let holder =
            LastAggregatorStateHolder::new(Arc::new(Mutex::new(None)), "test_last".to_string());

        holder.record_value_updated(&AttributeValue::Int(1));
        holder.record_value_updated(&AttributeValue::Int(2));

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        holder.record_reset();
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);
    }
}
