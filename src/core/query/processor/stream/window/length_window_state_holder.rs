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

// src/core/query/processor/stream/window/length_window_state_holder.rs

//! StateHolder for LengthWindowProcessor, so its buffer can be checkpointed and
//! restored.
//!
//! Adds a schema version, a change log for incremental checkpoints, and metadata about
//! the window.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::core::event::complex_event::ComplexEventType;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};

/// State holder for LengthWindowProcessor. Saves and restores the window buffer and
/// records changes for incremental checkpoints.
#[derive(Debug, Clone)]
pub struct LengthWindowStateHolder {
    /// Events in the window, shared with the processor
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,

    component_id: String,

    /// Most events the window holds
    window_length: usize,

    /// Checkpoint the change log was last cleared at
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Changes since the last checkpoint, for incremental checkpoints
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Events added so far. Saved in snapshots.
    total_events_processed: Arc<Mutex<u64>>,
}

impl LengthWindowStateHolder {
    /// Create a holder for `buffer` with an empty change log
    pub fn new(
        buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
        component_id: String,
        window_length: usize,
    ) -> Self {
        Self {
            buffer,
            component_id,
            window_length,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
            total_events_processed: Arc::new(Mutex::new(0)),
        }
    }

    /// Log an Insert for an added event and count it
    #[allow(dead_code)]
    pub fn record_event_added(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: self.generate_event_key(event),
            value: event_data,
        });

        *self.total_events_processed.lock().unwrap() += 1;
    }

    /// Log a Delete for an event that left the window
    #[allow(dead_code)]
    pub fn record_event_removed(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Delete {
            key: self.generate_event_key(event),
            old_value: event_data,
        });
    }

    /// Changelog key for an event, made of the timestamp followed by a hash of
    /// before_window_data, both little endian. Two equal events get the same key.
    #[allow(dead_code)]
    fn generate_event_key(&self, event: &StreamEvent) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&event.timestamp.to_le_bytes());

        let data_hash = self.hash_event_data(&event.before_window_data);
        key.extend_from_slice(&data_hash.to_le_bytes());

        key
    }

    /// Hash the value count, then each value through its Debug text. DefaultHasher
    /// output can change between Rust versions, so the key is not stable across builds.
    #[allow(dead_code)]
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

    /// Encode the timestamp, before_window_data and expired flag. Returns empty bytes
    /// if encoding fails.
    fn serialize_event(&self, event: &StreamEvent) -> Vec<u8> {
        use crate::core::util::to_bytes;

        let event_data = (
            event.timestamp,
            &event.before_window_data,
            event.event_type == ComplexEventType::Expired,
        );

        to_bytes(&event_data).unwrap_or_default()
    }

    /// Decode bytes from serialize_event back into a StreamEvent. Other fields are left
    /// at their defaults.
    ///
    /// # Errors
    /// Returns StateError::DeserializationError when the bytes cannot be decoded.
    fn deserialize_event(&self, data: &[u8]) -> Result<StreamEvent, StateError> {
        use crate::core::util::from_bytes;

        if let Ok((timestamp, data, is_expired)) =
            from_bytes::<(i64, Vec<crate::core::event::value::AttributeValue>, bool)>(data)
        {
            let mut se = StreamEvent::new(timestamp, data.len(), 0, 0);
            se.before_window_data = data;
            se.event_type = if is_expired {
                ComplexEventType::Expired
            } else {
                ComplexEventType::Current
            };
            return Ok(se);
        }

        Err(StateError::DeserializationError {
            message: "Failed to deserialize StreamEvent".to_string(),
        })
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

impl StateHolder for LengthWindowStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        // Copy what is needed in a short block so the buffer lock is held briefly
        let (serialized_events, total_events) = {
            let buffer = self.buffer.lock().unwrap();

            let mut serialized_events = Vec::new();
            for event in buffer.iter() {
                let event_data = self.serialize_event(event);
                serialized_events.push(event_data);
            }

            let total_events = *self.total_events_processed.lock().unwrap();
            (serialized_events, total_events)
        }; // buffer lock released here

        let state_data = LengthWindowStateData {
            events: serialized_events,
            window_length: self.window_length,
            total_events_processed: total_events,
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize length window state: {e}"),
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

        if !snapshot.verify_integrity() {
            return Err(StateError::ChecksumMismatch);
        }

        let data = self.decompress_state_data(&snapshot.data, snapshot.compression.clone())?;

        let state_data: LengthWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize length window state: {e}"),
            })?;

        // Replace the buffer contents with the saved events
        let mut buffer = self.buffer.lock().unwrap();
        buffer.clear();

        for event_data in &state_data.events {
            match self.deserialize_event(event_data) {
                Ok(event) => buffer.push_back(Arc::new(event)),
                Err(_e) => {
                    // Skip an event that cannot be decoded, without logging
                    continue;
                }
            }
        }

        // window_length comes from the query config, so it is not restored
        *self.total_events_processed.lock().unwrap() = state_data.total_events_processed;

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
        use super::changelog_helpers::apply_operation_to_simple_window;

        let mut buffer = self.buffer.lock().unwrap();

        for operation in &changes.operations {
            apply_operation_to_simple_window(&mut buffer, operation, &|data| {
                self.deserialize_event(data)
            })?;
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        let entries = if let Ok(buffer) = self.buffer.try_lock() {
            buffer.len()
        } else {
            // The buffer is locked. Guess instead of waiting, so this never hangs.
            5
        };

        // Rough guess of bytes per event
        let estimated_bytes_per_event = 200;
        let total_bytes = entries * estimated_bytes_per_event;

        // A full window stops growing. Otherwise assume one event's worth of growth.
        let growth_rate = if self.window_length > 0 {
            if entries >= self.window_length {
                0.0
            } else {
                estimated_bytes_per_event as f64
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
        // Events are added at the back and removed from the front
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "LengthWindowProcessor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        metadata
            .custom_metadata
            .insert("window_length".to_string(), self.window_length.to_string());
        metadata
            .custom_metadata
            .insert("window_type".to_string(), "length".to_string());

        metadata
    }

    fn reset_state(&self) {
        self.buffer.lock().unwrap().clear();
        *self.last_checkpoint_id.lock().unwrap() = None;
        self.change_log.lock().unwrap().clear();
        *self.total_events_processed.lock().unwrap() = 0;
    }
}

impl CompressibleStateHolder for LengthWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // live processing needs low latency
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // events repeat somewhat
            target_latency_ms: Some(1),                           // aim for under 1 ms
            min_compression_ratio: Some(0.3), // worth it only if it saves at least 30%
            expected_size_range: DataSizeRange::Small, // length windows usually hold little
        }
    }
}

/// Snapshot contents for LengthWindowProcessor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LengthWindowStateData {
    events: Vec<Vec<u8>>,
    window_length: usize,
    total_events_processed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use std::collections::VecDeque;
    use std::sync::Arc;

    #[test]
    fn test_length_window_state_holder_creation() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = LengthWindowStateHolder::new(buffer, "test_length_window".to_string(), 10);

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Sequential);
    }

    #[test]
    fn test_state_serialization_and_deserialization() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));

        // Add some test events
        {
            let mut buf = buffer.lock().unwrap();
            let mut event = StreamEvent::new(1000, 2, 0, 0);
            event.before_window_data = vec![
                AttributeValue::String("test".to_string()),
                AttributeValue::Int(42),
            ];
            buf.push_back(Arc::new(event));
        }

        let holder = LengthWindowStateHolder::new(buffer, "test_length_window".to_string(), 10);

        let hints = SerializationHints::default();

        // Test serialization with shared compression utility
        let snapshot = holder
            .serialize_state(&hints)
            .expect("Serialization should succeed");

        assert!(snapshot.verify_integrity());

        // Test deserialization
        holder
            .deserialize_state(&snapshot)
            .expect("Deserialization should succeed");

        // Verify the data was restored
        let buffer = holder.buffer.lock().unwrap();
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_change_log_tracking() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = LengthWindowStateHolder::new(buffer, "test_length_window".to_string(), 10);

        // Create a test event
        let mut event = StreamEvent::new(1000, 1, 0, 0);
        event.before_window_data = vec![AttributeValue::Int(42)];

        // Record event addition
        holder.record_event_added(&event);

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 1);

        // Record event removal
        holder.record_event_removed(&event);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);
    }

    #[test]
    fn test_size_estimation() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder =
            LengthWindowStateHolder::new(buffer.clone(), "test_length_window".to_string(), 5);

        // Test empty window
        let size = holder.estimate_size();
        assert_eq!(size.entries, 0);
        assert_eq!(size.bytes, 0);

        // Add some events
        {
            let mut buf = buffer.lock().unwrap();
            for i in 0..3 {
                let mut event = StreamEvent::new(1000 + i, 1, 0, 0);
                event.before_window_data = vec![AttributeValue::Int(i as i32)];
                buf.push_back(Arc::new(event));
            }
        }

        let size = holder.estimate_size();
        assert_eq!(size.entries, 3);
        assert_eq!(size.bytes, 3 * 200); // 200 bytes per event estimate
        assert!(size.estimated_growth_rate > 0.0); // Still room to grow
    }
}
