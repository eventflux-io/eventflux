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

// src/core/query/processor/stream/window/length_batch_window_state_holder.rs

//! StateHolder for LengthBatchWindowProcessor, so its batches can be checkpointed and
//! restored.
//!
//! Adds a schema version, a change log for incremental checkpoints, and metadata about
//! the window.

use std::sync::{Arc, Mutex};

use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use crate::core::util::event_serialization::{EventSerializationService, StorageStrategy};

/// State holder for LengthBatchWindowProcessor. Saves and restores the current batch,
/// the expired batch and the reset event, and records changes for incremental
/// checkpoints.
#[derive(Debug, Clone)]
pub struct LengthBatchWindowStateHolder {
    /// Batch being filled
    buffer: Arc<Mutex<Vec<StreamEvent>>>,

    /// Last flushed batch, sent out as expired events
    expired: Arc<Mutex<Vec<StreamEvent>>>,

    /// Reset event template. Sent between the expired and current events so
    /// aggregators clear their state.
    reset_event: Arc<Mutex<Option<StreamEvent>>>,

    component_id: String,

    /// Events per batch
    batch_length: usize,

    /// Checkpoint the change log was last cleared at
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Changes since the last checkpoint, for incremental checkpoints
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Events added so far. Saved in snapshots.
    total_events_processed: Arc<Mutex<u64>>,

    /// Encodes and decodes events, including every AttributeValue type
    serialization_service: EventSerializationService,
}

impl LengthBatchWindowStateHolder {
    /// Create a holder for the given buffers with an empty change log. Events are
    /// encoded with StorageStrategy::Essential.
    pub fn new(
        buffer: Arc<Mutex<Vec<StreamEvent>>>,
        expired: Arc<Mutex<Vec<StreamEvent>>>,
        reset_event: Arc<Mutex<Option<StreamEvent>>>,
        component_id: String,
        batch_length: usize,
    ) -> Self {
        Self {
            buffer,
            expired,
            reset_event,
            component_id,
            batch_length,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
            total_events_processed: Arc::new(Mutex::new(0)),
            serialization_service: EventSerializationService::new(StorageStrategy::Essential),
        }
    }

    /// Log an Insert into the current batch and count the event
    pub fn record_event_added(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: self.generate_event_key(event, "current_batch"),
            value: event_data,
        });

        *self.total_events_processed.lock().unwrap() += 1;
    }

    /// Log a batch flush.
    ///
    /// Written as a Delete with the key `batch_flush_marker`. Replaying it moves the
    /// current batch into expired. The value holds only the two batch sizes and the
    /// wall clock time, not the events.
    pub fn record_batch_flushed(
        &self,
        current_batch: &[StreamEvent],
        expired_batch: &[StreamEvent],
    ) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Delete {
            key: b"batch_flush_marker".to_vec(),
            old_value: self.serialize_batch_transition(current_batch, expired_batch),
        });
    }

    /// Log that the reset event template was set. Called when the first event of a new
    /// batch becomes the template.
    pub fn record_reset_event_set(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: b"reset_event".to_vec(),
            value: event_data,
        });
    }

    /// Log that the reset event template was cleared. Called when a batch flush uses
    /// the reset event up.
    pub fn record_reset_event_cleared(&self) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Delete {
            key: b"reset_event".to_vec(),
            old_value: Vec::new(), // replay matches on the key, so no old value is needed
        });
    }

    /// Changelog key for an event, made of the buffer name, `_`, the timestamp, then a
    /// hash of before_window_data, numbers little endian. apply_changelog reads the buffer name
    /// back from the key.
    fn generate_event_key(&self, event: &StreamEvent, buffer_type: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(buffer_type.as_bytes());
        key.push(b'_');
        key.extend_from_slice(&event.timestamp.to_le_bytes());

        let data_hash = self.hash_event_data(&event.before_window_data);
        key.extend_from_slice(&data_hash.to_le_bytes());

        key
    }

    /// Hash the value count, then each value through its Debug text. DefaultHasher
    /// output can change between Rust versions, so the key is not stable across builds.
    fn hash_event_data(&self, data: &[crate::core::event::value::AttributeValue]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        data.len().hash(&mut hasher);

        for attr in data {
            let attr_str = format!("{attr:?}");
            attr_str.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Encode an event with the serialization service. Returns empty bytes if encoding
    /// fails.
    fn serialize_event(&self, event: &StreamEvent) -> Vec<u8> {
        self.serialization_service
            .serialize_event(event)
            .unwrap_or_default()
    }

    /// Encode the batch sizes and the current wall clock time in milliseconds for a
    /// flush entry. Returns empty bytes if encoding fails.
    fn serialize_batch_transition(
        &self,
        current_batch: &[StreamEvent],
        expired_batch: &[StreamEvent],
    ) -> Vec<u8> {
        use crate::core::util::to_bytes;

        let transition_data = (
            current_batch.len(),
            expired_batch.len(),
            chrono::Utc::now().timestamp_millis(),
        );

        to_bytes(&transition_data).unwrap_or_default()
    }

    /// Decode an event with the serialization service
    fn deserialize_event(&self, data: &[u8]) -> Result<StreamEvent, StateError> {
        self.serialization_service.deserialize_event(data)
    }

    /// Empty the change log and remember `checkpoint_id`. Call after a checkpoint
    /// succeeds.
    #[allow(dead_code)]
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }
}

impl StateHolder for LengthBatchWindowStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        // Any compression preference switches events to the Compressed strategy
        let storage_strategy = hints
            .prefer_compression
            .as_ref()
            .map(|_| StorageStrategy::Compressed)
            .unwrap_or(StorageStrategy::Essential);

        let current_batch = {
            let buffer = self.buffer.lock().unwrap();
            let mut serialized_events = Vec::new();
            for event in buffer.iter() {
                match self
                    .serialization_service
                    .serialize_event_with_strategy(event, storage_strategy.clone())
                {
                    Ok(data) => serialized_events.push(data),
                    Err(e) => {
                        eprintln!("Warning: Failed to serialize event: {e}");
                        // Skip this event and keep the rest, so one bad event does not
                        // fail the snapshot
                    }
                }
            }
            serialized_events
        };

        let expired_batch = {
            let expired = self.expired.lock().unwrap();
            let mut serialized_events = Vec::new();
            for event in expired.iter() {
                match self
                    .serialization_service
                    .serialize_event_with_strategy(event, storage_strategy.clone())
                {
                    Ok(data) => serialized_events.push(data),
                    Err(e) => {
                        eprintln!("Warning: Failed to serialize expired event: {e}");
                        // Skip this event and keep the rest
                    }
                }
            }
            serialized_events
        };

        // A reset event that fails to encode is saved as None
        let reset_event = {
            let reset_guard = self.reset_event.lock().unwrap();
            if let Some(ref event) = *reset_guard {
                self.serialization_service
                    .serialize_event_with_strategy(event, storage_strategy.clone())
                    .ok()
            } else {
                None
            }
        };

        let state_data = LengthBatchWindowStateData {
            current_batch,
            expired_batch,
            batch_length: self.batch_length,
            total_events_processed: *self.total_events_processed.lock().unwrap(),
            reset_event,
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize length batch window state: {e}"),
        })?;

        let (compressed_data, compression_type) =
            self.compress_state_data(&data, hints.prefer_compression.clone())?;

        let data = compressed_data;
        let compression = compression_type;

        let checksum = StateSnapshot::calculate_checksum(&data);

        Ok(StateSnapshot {
            version: self.schema_version(),
            checkpoint_id: 0, // the checkpoint coordinator sets the real id
            data,
            compression,
            checksum,
            metadata: self.component_metadata(),
        })
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        use crate::core::util::from_bytes;

        log::info!(
            "LengthBatchWindow: deserialize_state called for component {}",
            self.component_id
        );

        if !snapshot.verify_integrity() {
            log::error!("LengthBatchWindow: Checksum mismatch");
            return Err(StateError::ChecksumMismatch);
        }

        let data = self.decompress_state_data(&snapshot.data, snapshot.compression.clone())?;

        let state_data: LengthBatchWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize length batch window state: {e}"),
            })?;

        log::info!(
            "LengthBatchWindow: Deserialized {} current batch events, {} expired events",
            state_data.current_batch.len(),
            state_data.expired_batch.len()
        );

        // Replace both batches with the saved events. An event that cannot be decoded
        // is skipped with a warning, so the rest still restore.
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.clear();

            for serialized_event in state_data.current_batch {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => {
                        buffer.push(event);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to deserialize current batch event: {e}");
                    }
                }
            }
        }

        {
            let mut expired = self.expired.lock().unwrap();
            expired.clear();

            for serialized_event in state_data.expired_batch {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => expired.push(event),
                    Err(e) => {
                        eprintln!("Warning: Failed to deserialize expired batch event: {e}");
                    }
                }
            }
        }

        // A reset event that cannot be decoded is restored as None
        {
            let mut reset_guard = self.reset_event.lock().unwrap();
            *reset_guard = if let Some(serialized_event) = state_data.reset_event {
                self.deserialize_event(&serialized_event).ok()
            } else {
                None
            };
        }

        // batch_length comes from the query config, so it is not restored
        *self.total_events_processed.lock().unwrap() = state_data.total_events_processed;

        log::info!(
            "LengthBatchWindow: Successfully restored state for component {}",
            self.component_id
        );
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
        let mut buffer = self.buffer.lock().unwrap();
        let mut expired = self.expired.lock().unwrap();

        for operation in &changes.operations {
            match operation {
                StateOperation::Insert { key, value } => {
                    if key == b"reset_event" {
                        if let Ok(event) = self.deserialize_event(value) {
                            let mut reset_guard = self.reset_event.lock().unwrap();
                            *reset_guard = Some(event);
                        }
                        continue;
                    }

                    match self.deserialize_event(value) {
                        Ok(event) => {
                            // The key prefix names the target buffer. Unknown prefixes go
                            // to the current batch.
                            if key.starts_with(b"current_batch") {
                                buffer.push(event);
                            } else if key.starts_with(b"expired_batch") {
                                expired.push(event);
                            } else {
                                buffer.push(event);
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Warning: Failed to deserialize event during changelog apply: {:?}",
                                e
                            );
                            continue;
                        }
                    }
                }

                StateOperation::Delete { key, old_value } => {
                    if key == b"batch_flush_marker" {
                        // A flush. The current batch replaces the expired batch.
                        expired.clear();
                        expired.extend(buffer.drain(..));
                    } else if key == b"reset_event" {
                        let mut reset_guard = self.reset_event.lock().unwrap();
                        *reset_guard = None;
                    } else {
                        // Remove every matching event from both buffers
                        if let Ok(event_to_remove) = self.deserialize_event(old_value) {
                            buffer.retain(|e| {
                                !(e.timestamp == event_to_remove.timestamp
                                    && e.before_window_data == event_to_remove.before_window_data)
                            });
                            expired.retain(|e| {
                                !(e.timestamp == event_to_remove.timestamp
                                    && e.before_window_data == event_to_remove.before_window_data)
                            });
                        }
                    }
                }

                StateOperation::Update {
                    key: _,
                    old_value,
                    new_value,
                } => {
                    // Remove the old event from both buffers, then add the new one to the
                    // current batch
                    if let Ok(old_event) = self.deserialize_event(old_value) {
                        buffer.retain(|e| {
                            !(e.timestamp == old_event.timestamp
                                && e.before_window_data == old_event.before_window_data)
                        });
                        expired.retain(|e| {
                            !(e.timestamp == old_event.timestamp
                                && e.before_window_data == old_event.before_window_data)
                        });
                    }

                    if let Ok(new_event) = self.deserialize_event(new_value) {
                        buffer.push(new_event);
                    }
                }

                StateOperation::Clear => {
                    buffer.clear();
                    expired.clear();
                    *self.reset_event.lock().unwrap() = None;
                }
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        let buffer = self.buffer.lock().unwrap();
        let expired = self.expired.lock().unwrap();
        let entries = buffer.len() + expired.len();

        // Rough guess of bytes per event
        let estimated_bytes_per_event = 200;
        let total_bytes = entries * estimated_bytes_per_event;

        // The batch fills up to batch_length and then flushes, so growth is the room
        // left in the current batch
        let growth_rate = if self.batch_length > 0 {
            let current_fill = buffer.len();
            if current_fill >= self.batch_length {
                0.0 // full, about to flush
            } else {
                ((self.batch_length - current_fill) * estimated_bytes_per_event) as f64
            }
        } else {
            estimated_bytes_per_event as f64
        };

        StateSize {
            bytes: total_bytes,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Events are added in order and flushed as whole batches
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "LengthBatchWindowProcessor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        metadata
            .custom_metadata
            .insert("batch_length".to_string(), self.batch_length.to_string());
        metadata
            .custom_metadata
            .insert("window_type".to_string(), "lengthBatch".to_string());

        let buffer_len = self.buffer.lock().unwrap().len();
        let expired_len = self.expired.lock().unwrap().len();
        metadata
            .custom_metadata
            .insert("current_batch_size".to_string(), buffer_len.to_string());
        metadata
            .custom_metadata
            .insert("expired_batch_size".to_string(), expired_len.to_string());

        metadata
    }

    fn reset_state(&self) {
        self.buffer.lock().unwrap().clear();
        self.expired.lock().unwrap().clear();
        *self.reset_event.lock().unwrap() = None;
        *self.last_checkpoint_id.lock().unwrap() = None;
        self.change_log.lock().unwrap().clear();
        *self.total_events_processed.lock().unwrap() = 0;
    }
}

impl CompressibleStateHolder for LengthBatchWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // batch windows handle many events, so favor speed
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // events repeat somewhat
            target_latency_ms: Some(2),                           // aim for under 2 ms
            min_compression_ratio: Some(0.4), // worth it only if it saves at least 40%
            expected_size_range: DataSizeRange::Large, // batches can be large
        }
    }
}

/// Snapshot contents for LengthBatchWindowProcessor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LengthBatchWindowStateData {
    current_batch: Vec<Vec<u8>>,
    expired_batch: Vec<Vec<u8>>,
    batch_length: usize,
    total_events_processed: u64,
    /// Encoded reset event template, if one is set.
    /// serde(default) lets checkpoints saved before this field existed still load.
    #[serde(default)]
    reset_event: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::complex_event::ComplexEventType;
    use crate::core::event::value::AttributeValue;
    use std::sync::Arc;

    #[test]
    fn test_length_batch_window_state_holder_creation() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));
        let reset_event = Arc::new(Mutex::new(None));
        let holder = LengthBatchWindowStateHolder::new(
            buffer,
            expired,
            reset_event,
            "test_length_batch_window".to_string(),
            5, // batch size of 5
        );

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Sequential);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));

        // Add some test events to current batch
        {
            let mut buf = buffer.lock().unwrap();
            let mut event1 = StreamEvent::new(1000, 2, 0, 0);
            event1.before_window_data = vec![
                AttributeValue::String("test1".to_string()),
                AttributeValue::Int(42),
            ];
            buf.push(event1);

            let mut event2 = StreamEvent::new(2000, 2, 0, 0);
            event2.before_window_data = vec![
                AttributeValue::String("test2".to_string()),
                AttributeValue::Int(84),
            ];
            buf.push(event2);
        }

        // Add some test events to expired batch
        {
            let mut exp = expired.lock().unwrap();
            let mut event3 = StreamEvent::new(3000, 1, 0, 0);
            event3.before_window_data = vec![AttributeValue::String("expired1".to_string())];
            event3.event_type = ComplexEventType::Expired;
            exp.push(event3);
        }

        let reset_event = Arc::new(Mutex::new(None));
        let holder = LengthBatchWindowStateHolder::new(
            buffer,
            expired,
            reset_event,
            "test_length_batch_window".to_string(),
            5,
        );

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the events were properly restored
        let buffer = holder.buffer.lock().unwrap();
        let expired = holder.expired.lock().unwrap();
        assert_eq!(buffer.len(), 2); // Current batch events should be restored
        assert_eq!(expired.len(), 1); // Expired batch events should be restored

        // Verify event data integrity
        if let Some(event) = buffer.first() {
            assert_eq!(event.timestamp, 1000);
            assert_eq!(event.before_window_data.len(), 2);
        }

        if let Some(event) = expired.first() {
            assert_eq!(event.timestamp, 3000);
            assert_eq!(event.before_window_data.len(), 1);
        }
    }

    #[test]
    fn test_change_log_tracking() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));
        let reset_event = Arc::new(Mutex::new(None));
        let holder = LengthBatchWindowStateHolder::new(
            buffer,
            expired,
            reset_event,
            "test_length_batch_window".to_string(),
            3,
        );

        // Create test events
        let mut event1 = StreamEvent::new(1000, 1, 0, 0);
        event1.before_window_data = vec![AttributeValue::Int(42)];

        let mut event2 = StreamEvent::new(2000, 1, 0, 0);
        event2.before_window_data = vec![AttributeValue::Int(84)];

        // Record event additions
        holder.record_event_added(&event1);
        holder.record_event_added(&event2);

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);

        // Record batch flush
        let current_batch = vec![event1, event2];
        let expired_batch = vec![];
        holder.record_batch_flushed(&current_batch, &expired_batch);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 3);
    }

    #[test]
    fn test_size_estimation() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));
        let reset_event = Arc::new(Mutex::new(None));
        let holder = LengthBatchWindowStateHolder::new(
            buffer.clone(),
            expired.clone(),
            reset_event,
            "test_length_batch_window".to_string(),
            5, // batch size of 5
        );

        // Test empty state
        let size = holder.estimate_size();
        assert_eq!(size.entries, 0);
        assert_eq!(size.bytes, 0);

        // Add some events to current batch
        {
            let mut buf = buffer.lock().unwrap();
            for i in 0..3 {
                let mut event = StreamEvent::new(1000 + i * 100, 1, 0, 0);
                event.before_window_data = vec![AttributeValue::Int(i as i32)];
                buf.push(event);
            }
        }

        // Add one expired event
        {
            let mut exp = expired.lock().unwrap();
            let mut event = StreamEvent::new(500, 1, 0, 0);
            event.before_window_data = vec![AttributeValue::Int(99)];
            exp.push(event);
        }

        let size = holder.estimate_size();
        assert_eq!(size.entries, 4); // 3 current + 1 expired
        assert_eq!(size.bytes, 4 * 200); // 200 bytes per event estimate
        assert!(size.estimated_growth_rate > 0.0); // Still room to grow in batch
    }

    #[test]
    fn test_batch_metadata() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));

        // Add some events to both buffers
        {
            let mut buf = buffer.lock().unwrap();
            for i in 0..2 {
                let mut event = StreamEvent::new(1000 + i * 100, 1, 0, 0);
                event.before_window_data = vec![AttributeValue::Int(i as i32)];
                buf.push(event);
            }
        }

        {
            let mut exp = expired.lock().unwrap();
            let mut event = StreamEvent::new(500, 1, 0, 0);
            event.before_window_data = vec![AttributeValue::Int(99)];
            exp.push(event);
        }

        let reset_event = Arc::new(Mutex::new(None));
        let holder = LengthBatchWindowStateHolder::new(
            buffer,
            expired,
            reset_event,
            "test_length_batch_window".to_string(),
            5,
        );

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "LengthBatchWindowProcessor");
        assert_eq!(metadata.custom_metadata.get("batch_length").unwrap(), "5");
        assert_eq!(
            metadata.custom_metadata.get("window_type").unwrap(),
            "lengthBatch"
        );
        assert_eq!(
            metadata.custom_metadata.get("current_batch_size").unwrap(),
            "2"
        );
        assert_eq!(
            metadata.custom_metadata.get("expired_batch_size").unwrap(),
            "1"
        );
    }
}
