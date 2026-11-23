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

// src/core/query/processor/stream/window/time_window_state_holder.rs

//! StateHolder for TimeWindowProcessor, so its buffer can be checkpointed and
//! restored.
//!
//! Adds a schema version, a change log for incremental checkpoints, and metadata about
//! the window.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, CompressionType, SchemaVersion, SerializationHints,
    StateError, StateHolder, StateMetadata, StateOperation, StateSize, StateSnapshot,
};
use crate::core::util::compression::{
    CompressibleStateHolder, CompressionHints, DataCharacteristics, DataSizeRange,
};
use crate::core::util::event_serialization::{EventSerializationService, StorageStrategy};

/// State holder for TimeWindowProcessor. Saves and restores the window buffer and
/// records changes for incremental checkpoints.
#[derive(Debug, Clone)]
pub struct TimeWindowStateHolder {
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

    /// Window start time in milliseconds, if set. Saved in snapshots.
    window_start_time: Arc<Mutex<Option<i64>>>,

    /// Encodes and decodes events, including every AttributeValue type
    serialization_service: EventSerializationService,
}

impl TimeWindowStateHolder {
    /// Create a holder for `buffer` with an empty change log. Events are encoded with
    /// StorageStrategy::Essential.
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
            window_start_time: Arc::new(Mutex::new(None)),
            serialization_service: EventSerializationService::new(StorageStrategy::Essential),
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

    /// Log a Delete for an event that expired from the window
    pub fn record_event_expired(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Delete {
            key: self.generate_event_key(event),
            old_value: event_data,
        });
    }

    /// Set the window start time. Not logged in the change log.
    #[allow(dead_code)]
    pub fn update_window_start_time(&self, timestamp: i64) {
        *self.window_start_time.lock().unwrap() = Some(timestamp);
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

impl StateHolder for TimeWindowStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        let window_start_time = *self.window_start_time.lock().unwrap();

        // Any compression preference switches events to the Compressed strategy
        let storage_strategy = hints
            .prefer_compression
            .as_ref()
            .map(|_| StorageStrategy::Compressed)
            .unwrap_or(StorageStrategy::Essential);

        let events = {
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

        let state_data = TimeWindowStateData {
            events,
            window_duration_ms: self.window_duration_ms,
            window_start_time,
            total_events_processed: *self.total_events_processed.lock().unwrap(),
        };

        let data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize time window state: {e}"),
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

        let state_data: TimeWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize time window state: {e}"),
            })?;

        // Replace the buffer contents with the saved events
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.clear();

            for serialized_event in state_data.events {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => buffer.push_back(Arc::new(event)),
                    Err(e) => {
                        eprintln!("Warning: Failed to deserialize window event: {e}");
                        // Skip this event and keep the rest
                    }
                }
            }
        }

        // window_duration_ms comes from the query config, so it is not restored
        *self.window_start_time.lock().unwrap() = state_data.window_start_time;
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
            // Clear also resets window_start_time, so it cannot use the shared helper
            if matches!(operation, StateOperation::Clear) {
                buffer.clear();
                *self.window_start_time.lock().unwrap() = None;
            } else {
                apply_operation_to_simple_window(&mut buffer, operation, &|data| {
                    self.deserialize_event(data)
                })?;
            }
        }

        Ok(())
    }

    fn estimate_size(&self) -> StateSize {
        let buffer = self.buffer.lock().unwrap();
        let entries = buffer.len();

        // Rough guess of bytes per event
        let estimated_bytes_per_event = 200;
        let total_bytes = entries * estimated_bytes_per_event;

        // Real growth depends on the event rate, which is not tracked, so guess one
        // event's size
        let growth_rate = estimated_bytes_per_event as f64;

        StateSize {
            bytes: total_bytes,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Events are added and expired in time order
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata =
            StateMetadata::new(self.component_id.clone(), "TimeWindowProcessor".to_string());
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        metadata.custom_metadata.insert(
            "window_duration_ms".to_string(),
            self.window_duration_ms.to_string(),
        );
        metadata
            .custom_metadata
            .insert("window_type".to_string(), "time".to_string());

        if let Some(start_time) = *self.window_start_time.lock().unwrap() {
            metadata
                .custom_metadata
                .insert("window_start_time".to_string(), start_time.to_string());
        }

        metadata
    }

    fn reset_state(&self) {
        self.buffer.lock().unwrap().clear();
        *self.last_checkpoint_id.lock().unwrap() = None;
        self.change_log.lock().unwrap().clear();
        *self.total_events_processed.lock().unwrap() = 0;
        *self.window_start_time.lock().unwrap() = None;
    }
}

impl CompressibleStateHolder for TimeWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // live processing needs low latency
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // events repeat somewhat
            target_latency_ms: Some(1),                           // aim for under 1 ms
            min_compression_ratio: Some(0.3),                     // save at least 30%
            expected_size_range: DataSizeRange::Medium, // time windows can hold more events
        }
    }
}

/// Snapshot contents for TimeWindowProcessor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TimeWindowStateData {
    events: Vec<Vec<u8>>,
    window_duration_ms: i64,
    window_start_time: Option<i64>,
    total_events_processed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use std::collections::VecDeque;
    use std::sync::Arc;

    #[test]
    fn test_time_window_state_holder_creation() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = TimeWindowStateHolder::new(
            buffer,
            "test_time_window".to_string(),
            1000, // 1 second window
        );

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

        let holder = TimeWindowStateHolder::new(buffer, "test_time_window".to_string(), 1000);

        // Set window start time
        holder.update_window_start_time(900);

        let hints = SerializationHints::default();

        // Test serialization
        let snapshot = holder.serialize_state(&hints).unwrap();
        assert!(snapshot.verify_integrity());

        // Test deserialization
        let result = holder.deserialize_state(&snapshot);
        assert!(result.is_ok());

        // Verify the events were properly restored
        let buffer = holder.buffer.lock().unwrap();
        assert_eq!(buffer.len(), 1); // Window events should be restored

        // Verify event data integrity
        if let Some(event) = buffer.front() {
            assert_eq!(event.timestamp, 1000);
            assert_eq!(event.before_window_data.len(), 2);
        }

        // Verify window start time was restored
        let start_time = *holder.window_start_time.lock().unwrap();
        assert_eq!(start_time, Some(900));
    }

    #[test]
    fn test_change_log_tracking() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = TimeWindowStateHolder::new(buffer, "test_time_window".to_string(), 1000);

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

    #[test]
    fn test_size_estimation() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = TimeWindowStateHolder::new(
            buffer.clone(),
            "test_time_window".to_string(),
            5000, // 5 second window
        );

        // Test empty window
        let size = holder.estimate_size();
        assert_eq!(size.entries, 0);
        assert_eq!(size.bytes, 0);

        // Add some events
        {
            let mut buf = buffer.lock().unwrap();
            for i in 0..3 {
                let mut event = StreamEvent::new(1000 + i * 100, 1, 0, 0);
                event.before_window_data = vec![AttributeValue::Int(i as i32)];
                buf.push_back(Arc::new(event));
            }
        }

        let size = holder.estimate_size();
        assert_eq!(size.entries, 3);
        assert_eq!(size.bytes, 3 * 200); // 200 bytes per event estimate
        assert!(size.estimated_growth_rate > 0.0); // Time windows can grow based on rate
    }

    #[test]
    fn test_window_metadata() {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let holder = TimeWindowStateHolder::new(
            buffer,
            "test_time_window".to_string(),
            2000, // 2 second window
        );

        holder.update_window_start_time(1000);

        let metadata = holder.component_metadata();
        assert_eq!(metadata.component_type, "TimeWindowProcessor");
        assert_eq!(
            metadata.custom_metadata.get("window_duration_ms").unwrap(),
            "2000"
        );
        assert_eq!(metadata.custom_metadata.get("window_type").unwrap(), "time");
        assert_eq!(
            metadata.custom_metadata.get("window_start_time").unwrap(),
            "1000"
        );
    }
}
