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

//! StateHolder implementation for SumAttributeAggregatorExecutor

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
pub struct SumAggregatorStateHolder {
    sum: Arc<Mutex<f64>>,
    count: Arc<Mutex<u64>>,
    return_type: ApiAttributeType,
    pub(crate) base: StateHolderBase,
}

impl SumAggregatorStateHolder {
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
            base: StateHolderBase::new(component_id),
        }
    }

    pub fn record_value_added(&self, value: f64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Insert {
            key: super::generate_operation_key("add"),
            value: to_bytes(&value).unwrap_or_default(),
        });
    }

    pub fn record_value_removed(&self, value: f64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Delete {
            key: super::generate_operation_key("remove"),
            old_value: to_bytes(&value).unwrap_or_default(),
        });
    }

    pub fn record_reset(&self, old_sum: f64, old_count: u64) {
        use crate::core::util::to_bytes;
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Update {
            key: b"reset".to_vec(),
            old_value: to_bytes(&(old_sum, old_count)).unwrap_or_default(),
            new_value: to_bytes(&(0.0f64, 0u64)).unwrap_or_default(),
        });
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_sum(&self) -> f64 {
        *self.sum.lock().unwrap()
    }

    pub fn get_count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

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
        let state_data = SumAggregatorStateData::new(sum, count, self.return_type);
        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize sum aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: SumAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize sum aggregator state: {e}"),
            })?;

        *self.sum.lock().unwrap() = state_data.sum;
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

        let mut sum = self.sum.lock().unwrap();
        let mut count = self.count.lock().unwrap();

        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value, .. } => {
                    let added_value: f64 =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize added value: {e}"),
                        })?;
                    *sum += added_value;
                    *count += 1;
                }
                StateOperation::Delete { old_value, .. } => {
                    let removed_value: f64 =
                        from_bytes(old_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize removed value: {e}"),
                        })?;
                    *sum -= removed_value;
                    if *count > 0 {
                        *count -= 1;
                    }
                }
                StateOperation::Update { new_value, .. } => {
                    let new_state: (f64, u64) =
                        from_bytes(new_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize new state: {e}"),
                        })?;
                    *sum = new_state.0;
                    *count = new_state.1;
                }
                StateOperation::Clear => {
                    *sum = 0.0;
                    *count = 0;
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<f64>() + std::mem::size_of::<u64>(),
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
            "SumAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
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

    fn reset_state(&self) {
        *self.sum.lock().unwrap() = 0.0;
        *self.count.lock().unwrap() = 0;
        self.base.restored.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl CompressibleStateHolder for SumAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SumAggregatorStateData {
    sum: f64,
    count: u64,
    return_type: String,
}

impl SumAggregatorStateData {
    fn new(sum: f64, count: u64, return_type: ApiAttributeType) -> Self {
        Self {
            sum,
            count,
            return_type: format!("{return_type:?}"),
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

        let holder = SumAggregatorStateHolder::new(
            sum,
            count,
            "test_sum_aggregator".to_string(),
            ApiAttributeType::DOUBLE,
        );

        let hints = SerializationHints::default();

        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

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

        holder.record_value_added(10.0);
        holder.record_value_added(20.0);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        holder.record_value_removed(5.0);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        holder.record_reset(25.0, 2);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }

    #[test]
    fn test_aggregated_value_conversion() {
        let sum = Arc::new(Mutex::new(42.7));
        let count = Arc::new(Mutex::new(3));

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
        assert!(size.bytes > 0);
        assert_eq!(size.estimated_growth_rate, 0.0);
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
