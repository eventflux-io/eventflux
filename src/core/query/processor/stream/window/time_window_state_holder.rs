// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/time_window_state_holder.rs

//! Enhanced StateHolder implementation for TimeWindowProcessor
//!
//! This implementation provides enterprise-grade state management for time windows
//! with versioning, incremental checkpointing, and comprehensive metadata.

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

/// Enhanced state holder for TimeWindowProcessor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct TimeWindowStateHolder {
    /// Window buffer containing events
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,

    /// Component identifier
    component_id: String,

    /// Window duration in milliseconds
    window_duration_ms: i64,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Event counter for size estimation
    total_events_processed: Arc<Mutex<u64>>,

    /// Window start time tracking
    window_start_time: Arc<Mutex<Option<i64>>>,

    /// Event serialization service with proper AttributeValue handling
    serialization_service: EventSerializationService,
}

impl TimeWindowStateHolder {
    /// Create a new enhanced state holder
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

    /// Record an event addition for incremental checkpointing
    pub fn record_event_added(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: self.generate_event_key(event),
            value: event_data,
        });

        // Update event counter
        *self.total_events_processed.lock().unwrap() += 1;
    }

    /// Record an event expiration for incremental checkpointing
    pub fn record_event_expired(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Delete {
            key: self.generate_event_key(event),
            old_value: event_data,
        });
    }

    /// Update window start time
    pub fn update_window_start_time(&self, timestamp: i64) {
        *self.window_start_time.lock().unwrap() = Some(timestamp);
    }

    /// Generate a unique key for an event
    fn generate_event_key(&self, event: &StreamEvent) -> Vec<u8> {
        // Use timestamp and a hash of the event data as key
        let mut key = Vec::new();
        key.extend_from_slice(&event.timestamp.to_le_bytes());

        // Add a simple hash of the event data
        let data_hash = self.hash_event_data(&event.before_window_data);
        key.extend_from_slice(&data_hash.to_le_bytes());

        key
    }

    /// Simple hash function for event data
    fn hash_event_data(&self, data: &[crate::core::event::value::AttributeValue]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Hash the length first
        data.len().hash(&mut hasher);

        // Hash each attribute value
        for attr in data {
            // Create a string representation for hashing
            let attr_str = format!("{attr:?}");
            attr_str.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Serialize an event to bytes using the enhanced serialization service
    fn serialize_event(&self, event: &StreamEvent) -> Vec<u8> {
        self.serialization_service
            .serialize_event(event)
            .unwrap_or_default()
    }

    /// Deserialize an event from bytes using the enhanced serialization service
    fn deserialize_event(&self, data: &[u8]) -> Result<StreamEvent, StateError> {
        self.serialization_service.deserialize_event(data)
    }

    /// Clear the change log (called after successful checkpoint)
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

        // Determine storage strategy based on hints
        let storage_strategy = hints
            .prefer_compression
            .as_ref()
            .map(|_| StorageStrategy::Compressed)
            .unwrap_or(StorageStrategy::Essential);

        // Serialize window events
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
                        // Continue with other events rather than failing completely
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

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize time window state: {e}"),
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
        let state_data: TimeWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize time window state: {e}"),
            })?;

        // Deserialize and restore window events
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.clear();

            for serialized_event in state_data.events {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => buffer.push_back(Arc::new(event)),
                    Err(e) => {
                        eprintln!("Warning: Failed to deserialize window event: {e}");
                        // Continue with other events rather than failing completely
                    }
                }
            }
        }

        // Restore metadata (window_duration_ms is configuration and doesn't need to be restored)
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
            // Handle Clear specially to also reset window_start_time
            if matches!(operation, StateOperation::Clear) {
                buffer.clear();
                *self.window_start_time.lock().unwrap() = None;
            } else {
                // Use shared helper for other operations
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

        // Estimate bytes per event (rough calculation)
        let estimated_bytes_per_event = 200; // Conservative estimate
        let total_bytes = entries * estimated_bytes_per_event;

        // Estimate growth rate based on window duration
        // Time windows have variable growth based on event rate
        let growth_rate = estimated_bytes_per_event as f64; // Simplified estimate

        StateSize {
            bytes: total_bytes,
            entries,
            estimated_growth_rate: growth_rate,
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        // Time windows have a sequential access pattern
        // Events are added in time order and expired in time order
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata =
            StateMetadata::new(self.component_id.clone(), "TimeWindowProcessor".to_string());
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
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
}

impl CompressibleStateHolder for TimeWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Time windows need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // Time-based event streams
            target_latency_ms: Some(1),                           // Target < 1ms compression time
            min_compression_ratio: Some(0.3),                     // At least 30% space savings
            expected_size_range: DataSizeRange::Medium, // Time windows can have more events
        }
    }
}

/// Serializable state data for TimeWindowProcessor
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

        let mut holder = TimeWindowStateHolder::new(buffer, "test_time_window".to_string(), 1000);

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
        if let Some(event) = buffer.get(0) {
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
