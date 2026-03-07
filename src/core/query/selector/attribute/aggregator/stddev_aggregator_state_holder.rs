// SPDX-License-Identifier: MIT OR Apache-2.0

//! StateHolder implementation for StdDevAttributeAggregatorExecutor

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
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct StdDevAggregatorStateHolder {
    mean: Arc<Mutex<f64>>,
    m2: Arc<Mutex<f64>>,
    sum: Arc<Mutex<f64>>,
    count: Arc<Mutex<u64>>,
    pub(crate) base: StateHolderBase,
}

impl StdDevAggregatorStateHolder {
    pub fn new(
        mean: Arc<Mutex<f64>>,
        m2: Arc<Mutex<f64>>,
        sum: Arc<Mutex<f64>>,
        count: Arc<Mutex<u64>>,
        component_id: String,
    ) -> Self {
        Self {
            mean,
            m2,
            sum,
            count,
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

    pub fn record_reset(&self) {
        let mut change_log = self.base.change_log.lock().unwrap();
        change_log.push(StateOperation::Clear);
    }

    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        self.base.clear_change_log(checkpoint_id);
    }

    pub fn get_mean(&self) -> f64 {
        *self.mean.lock().unwrap()
    }

    pub fn get_m2(&self) -> f64 {
        *self.m2.lock().unwrap()
    }

    pub fn get_sum(&self) -> f64 {
        *self.sum.lock().unwrap()
    }

    pub fn get_count(&self) -> u64 {
        *self.count.lock().unwrap()
    }
}

impl StateHolder for StdDevAggregatorStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let state_data = StdDevAggregatorStateData {
            mean: *self.mean.lock().unwrap(),
            m2: *self.m2.lock().unwrap(),
            sum: *self.sum.lock().unwrap(),
            count: *self.count.lock().unwrap(),
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize stddev aggregator state: {e}"),
        })?;

        compress_and_build_snapshot(self, data, hints)
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        let data = verify_and_decompress(self, snapshot)?;
        let state_data: StdDevAggregatorStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize stddev aggregator state: {e}"),
            })?;

        *self.mean.lock().unwrap() = state_data.mean;
        *self.m2.lock().unwrap() = state_data.m2;
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

        let mut mean = self.mean.lock().unwrap();
        let mut m2 = self.m2.lock().unwrap();
        let mut sum = self.sum.lock().unwrap();
        let mut count = self.count.lock().unwrap();

        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { value, .. } => {
                    let v: f64 =
                        from_bytes(value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize added value: {e}"),
                        })?;
                    *count += 1;
                    if *count == 1 {
                        *mean = v;
                        *m2 = 0.0;
                        *sum = v;
                    } else {
                        let old_mean = *mean;
                        *sum += v;
                        *mean = *sum / *count as f64;
                        *m2 += (v - old_mean) * (v - *mean);
                    }
                }
                StateOperation::Delete { old_value, .. } => {
                    let v: f64 =
                        from_bytes(old_value).map_err(|e| StateError::DeserializationError {
                            message: format!("Failed to deserialize removed value: {e}"),
                        })?;
                    if *count > 0 {
                        *count -= 1;
                        if *count == 0 {
                            *mean = 0.0;
                            *m2 = 0.0;
                            *sum = 0.0;
                        } else {
                            let old_mean = *mean;
                            *sum -= v;
                            *mean = *sum / *count as f64;
                            *m2 = (*m2 - (v - old_mean) * (v - *mean)).max(0.0);
                        }
                    }
                }
                StateOperation::Clear | StateOperation::Update { .. } => {
                    *mean = 0.0;
                    *m2 = 0.0;
                    *sum = 0.0;
                    *count = 0;
                }
            }
        }
        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        StateSize {
            bytes: std::mem::size_of::<f64>() * 3 + std::mem::size_of::<u64>(),
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
            "StdDevAttributeAggregatorExecutor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();
        metadata
            .custom_metadata
            .insert("aggregator_type".to_string(), "stddev".to_string());
        metadata
    }
}

impl CompressibleStateHolder for StdDevAggregatorStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        default_compression_hints(DataCharacteristics::Numeric)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StdDevAggregatorStateData {
    mean: f64,
    m2: f64,
    sum: f64,
    count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stddev_state_holder_creation() {
        let holder = StdDevAggregatorStateHolder::new(
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0)),
            "test_stddev".to_string(),
        );
        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.get_count(), 0);
    }

    #[test]
    fn test_serialization_deserialization() {
        let holder = StdDevAggregatorStateHolder::new(
            Arc::new(Mutex::new(5.0)),
            Arc::new(Mutex::new(10.0)),
            Arc::new(Mutex::new(25.0)),
            Arc::new(Mutex::new(5)),
            "test_stddev".to_string(),
        );

        let hints = SerializationHints::default();
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        *holder.mean.lock().unwrap() = 0.0;
        *holder.count.lock().unwrap() = 0;
        holder.deserialize_state(&snapshot).unwrap();
        assert_eq!(holder.get_mean(), 5.0);
        assert_eq!(holder.get_m2(), 10.0);
        assert_eq!(holder.get_sum(), 25.0);
        assert_eq!(holder.get_count(), 5);
    }

    #[test]
    fn test_change_log_tracking() {
        let holder = StdDevAggregatorStateHolder::new(
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0)),
            "test_stddev".to_string(),
        );

        holder.record_value_added(5.0);
        holder.record_value_added(10.0);
        holder.record_value_removed(5.0);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);

        holder.record_reset();
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 4);
    }
}
