// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/length_window_state_holder.rs

//! Enhanced StateHolder implementation for LengthWindowProcessor
//!
//! This implementation provides enterprise-grade state management for length windows
//! with versioning, incremental checkpointing, and comprehensive metadata.

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

/// Enhanced state holder for LengthWindowProcessor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct LengthWindowStateHolder {
    /// Window buffer containing events
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,

    /// Component identifier
    component_id: String,

    /// Window length configuration
    window_length: usize,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Event counter for size estimation
    total_events_processed: Arc<Mutex<u64>>,
}

impl LengthWindowStateHolder {
    /// Create a new enhanced state holder
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

    /// Record an event removal for incremental checkpointing
    pub fn record_event_removed(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Delete {
            key: self.generate_event_key(event),
            old_value: event_data,
        });
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

    /// Serialize an event to bytes
    fn serialize_event(&self, event: &StreamEvent) -> Vec<u8> {
        use crate::core::util::to_bytes;

        // Simple serialization using bincode
        // Serialize the essential event data directly
        let event_data = (
            event.timestamp,
            &event.before_window_data,
            event.event_type == ComplexEventType::Expired,
        );

        to_bytes(&event_data).unwrap_or_default()
    }

    /// Deserialize an event from bytes
    fn deserialize_event(&self, data: &[u8]) -> Result<StreamEvent, StateError> {
        use crate::core::util::from_bytes;

        // Deserialize using bincode - direct approach
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

    /// Clear the change log (called after successful checkpoint)
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

        // Create state data structure by gathering data with minimal lock time
        let (serialized_events, total_events) = {
            let buffer = self.buffer.lock().unwrap();

            // Serialize all events in the buffer
            let mut serialized_events = Vec::new();
            for event in buffer.iter() {
                let event_data = self.serialize_event(event);
                serialized_events.push(event_data);
            }

            let total_events = *self.total_events_processed.lock().unwrap();
            (serialized_events, total_events)
        }; // Release buffer lock early

        let state_data = LengthWindowStateData {
            events: serialized_events,
            window_length: self.window_length,
            total_events_processed: total_events,
        };

        // Serialize to bytes
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
        let state_data: LengthWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize length window state: {e}"),
            })?;

        // Restore window buffer
        let mut buffer = self.buffer.lock().unwrap();
        buffer.clear();

        for event_data in &state_data.events {
            match self.deserialize_event(event_data) {
                Ok(event) => buffer.push_back(Arc::new(event)),
                Err(_e) => {
                    // Warning: Failed to deserialize event, skipping
                    continue;
                }
            }
        }

        // Restore metadata (window_length is configuration and doesn't need to be restored)
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
            // Use shared helper for all simple window operations
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
            // Return conservative estimate if buffer is locked to avoid hanging
            5 // Conservative estimate
        };

        // Estimate bytes per event (rough calculation)
        let estimated_bytes_per_event = 200; // Conservative estimate
        let total_bytes = entries * estimated_bytes_per_event;

        // Estimate growth rate based on window length
        let growth_rate = if self.window_length > 0 {
            // If we're at capacity, growth rate is 0 (stable)
            if entries >= self.window_length {
                0.0
            } else {
                // Otherwise, estimate based on typical event size
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
        // Length windows have a sequential access pattern
        // Events are added to the end and removed from the front
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "LengthWindowProcessor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
        metadata
            .custom_metadata
            .insert("window_length".to_string(), self.window_length.to_string());
        metadata
            .custom_metadata
            .insert("window_type".to_string(), "length".to_string());

        metadata
    }
}

impl CompressibleStateHolder for LengthWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Length windows need low latency for real-time processing
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // Event streams have moderate patterns
            target_latency_ms: Some(1),                           // Target < 1ms compression time
            min_compression_ratio: Some(0.3), // At least 30% space savings to be worthwhile
            expected_size_range: DataSizeRange::Small, // Length windows typically have small state
        }
    }
}

/// Serializable state data for LengthWindowProcessor
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

        let mut holder = LengthWindowStateHolder::new(buffer, "test_length_window".to_string(), 10);

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
