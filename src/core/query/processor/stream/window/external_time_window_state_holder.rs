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

// src/core/query/processor/stream/window/external_time_window_state_holder.rs

//! StateHolder for ExternalTimeWindowProcessor, so its buffer can be checkpointed and
//! restored.
//!
//! Like the time window holder, but the window uses event timestamps for its bounds.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::core::event::complex_event::ComplexEventType;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};

/// State holder for ExternalTimeWindowProcessor. Saves and restores the window buffer
/// and records changes for incremental checkpoints.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExternalTimeWindowStateHolder {
    /// Events in the window, shared with the processor
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,

    component_id: String,

    /// Window length in milliseconds
    window_duration_ms: i64,

    /// Checkpoint the change log was last cleared at
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Changes since the last checkpoint, for incremental checkpoints
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Events added so far. Saved in snapshots.
    total_events_processed: Arc<Mutex<u64>>,
}

#[allow(dead_code)]
impl ExternalTimeWindowStateHolder {
    /// Create a holder for `buffer` with an empty change log
    pub fn new(
        buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
        component_id: String,
        window_duration_ms: i64,
    ) -> Self {
        Self {
            buffer,
            component_id,
            window_duration_ms,
            last_checkpoint_id: Arc::new(Mutex::new(None)),
            change_log: Arc::new(Mutex::new(Vec::new())),
            total_events_processed: Arc::new(Mutex::new(0)),
        }
    }

    /// Log an Insert for an added event and count it
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
    pub fn record_event_expired(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Delete {
            key: self.generate_event_key(event),
            old_value: event_data,
        });
    }

    /// Changelog key for an event, made of the timestamp followed by a hash of
    /// before_window_data, both little endian. Two equal events get the same key.
    fn generate_event_key(&self, event: &StreamEvent) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(&event.timestamp.to_le_bytes());

        let data_hash = self.hash_event_data(&event.before_window_data);
        key.extend_from_slice(&data_hash.to_le_bytes());

        key
    }

    /// Hash the attribute values through their Debug text. DefaultHasher output can
    /// change between Rust versions, so the key is not stable across builds.
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
    pub fn clear_change_log(&self, checkpoint_id: CheckpointId) {
        let mut change_log = self.change_log.lock().unwrap();
        change_log.clear();
        *self.last_checkpoint_id.lock().unwrap() = Some(checkpoint_id);
    }
}

impl StateHolder for ExternalTimeWindowStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let buffer = self.buffer.lock().unwrap();

        let mut serialized_events = Vec::new();
        for event in buffer.iter() {
            let event_data = self.serialize_event(event);
            serialized_events.push(event_data);
        }

        let state_data = ExternalTimeWindowStateData {
            events: serialized_events,
            window_duration_ms: self.window_duration_ms,
            total_events_processed: *self.total_events_processed.lock().unwrap(),
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize external time window state: {e}"),
        })?;

        // Compress with the requested type, or let the shared helper pick one
        let (compressed_data, compression_type) =
            if let Some(ref compression) = hints.prefer_compression {
                match self.compress_state_data(&data, Some(compression.clone())) {
                    Ok((compressed, comp_type)) => (compressed, comp_type),
                    Err(_) => {
                        // Store uncompressed if compression fails
                        (data, CompressionType::None)
                    }
                }
            } else {
                // No preference, so the helper picks based on compression_hints
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

        let state_data: ExternalTimeWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize external time window state: {e}"),
            })?;

        // Replace the buffer contents with the saved events
        let mut buffer = self.buffer.lock().unwrap();
        buffer.clear();

        for event_data in &state_data.events {
            match self.deserialize_event(event_data) {
                Ok(event) => buffer.push_back(Arc::new(event)),
                Err(_e) => {
                    // Skip an event that cannot be decoded, without logging
                }
            }
        }

        // window_duration_ms comes from the query config, so it is not restored
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
        let buffer = self.buffer.lock().unwrap();
        let entries = buffer.len();
        let estimated_bytes_per_event = 200;
        let total_bytes = entries * estimated_bytes_per_event;
        let growth_rate = estimated_bytes_per_event as f64;

        StateSize {
            bytes: total_bytes,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "ExternalTimeWindowProcessor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        metadata.custom_metadata.insert(
            "window_duration_ms".to_string(),
            self.window_duration_ms.to_string(),
        );
        metadata
            .custom_metadata
            .insert("window_type".to_string(), "externalTime".to_string());

        metadata
    }

    fn reset_state(&self) {
        self.buffer.lock().unwrap().clear();
        *self.last_checkpoint_id.lock().unwrap() = None;
        self.change_log.lock().unwrap().clear();
        *self.total_events_processed.lock().unwrap() = 0;
    }
}

impl CompressibleStateHolder for ExternalTimeWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // live processing needs low latency
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // events repeat somewhat
            target_latency_ms: Some(1),                           // aim for under 1 ms
            min_compression_ratio: Some(0.3), // worth it only if it saves at least 30%
            expected_size_range: DataSizeRange::Small, // these windows usually hold little
        }
    }
}

/// Snapshot contents for ExternalTimeWindowProcessor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ExternalTimeWindowStateData {
    events: Vec<Vec<u8>>,
    window_duration_ms: i64,
    total_events_processed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use std::collections::VecDeque;
    use std::sync::Arc;

    #[test]
    fn test_external_time_window_state_holder_creation() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = ExternalTimeWindowStateHolder::new(
            buffer,
            "test_external_time_window".to_string(),
            1000, // 1 second window
        );

        assert_eq!(holder.schema_version(), SchemaVersion::new(1, 0, 0));
        assert_eq!(holder.access_pattern(), AccessPattern::Sequential);
    }

    #[test]
    fn test_change_log_tracking() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = ExternalTimeWindowStateHolder::new(
            buffer,
            "test_external_time_window".to_string(),
            1000,
        );

        // Create a test event
        let mut event = StreamEvent::new(1000, 1, 0, 0);
        event.before_window_data = vec![AttributeValue::Int(42)];

        // Record event addition
        holder.record_event_added(&event);

        // Get changelog
        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 1);

        // Record event expiration
        holder.record_event_expired(&event);

        let changelog = holder.get_changelog(0).unwrap();
        assert_eq!(changelog.operations.len(), 2);
    }
}
