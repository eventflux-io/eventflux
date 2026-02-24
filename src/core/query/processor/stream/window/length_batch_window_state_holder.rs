// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/length_batch_window_state_holder.rs

//! Enhanced StateHolder implementation for LengthBatchWindowProcessor
//!
//! This implementation provides enterprise-grade state management for length batch windows
//! with versioning, incremental checkpointing, and comprehensive metadata.

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

/// Enhanced state holder for LengthBatchWindowProcessor with StateHolder capabilities
#[derive(Debug, Clone)]
pub struct LengthBatchWindowStateHolder {
    /// Current batch buffer
    buffer: Arc<Mutex<Vec<StreamEvent>>>,

    /// Expired batch buffer
    expired: Arc<Mutex<Vec<StreamEvent>>>,

    /// Reset event template - emitted between expired and current to clear aggregator state
    reset_event: Arc<Mutex<Option<StreamEvent>>>,

    /// Component identifier
    component_id: String,

    /// Batch length configuration
    batch_length: usize,

    /// Last checkpoint ID for incremental tracking
    last_checkpoint_id: Arc<Mutex<Option<CheckpointId>>>,

    /// Change log for incremental checkpointing
    change_log: Arc<Mutex<Vec<StateOperation>>>,

    /// Event counter for size estimation
    total_events_processed: Arc<Mutex<u64>>,

    /// Event serialization service with proper AttributeValue handling
    serialization_service: EventSerializationService,
}

impl LengthBatchWindowStateHolder {
    /// Create a new enhanced state holder
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

    /// Record an event addition to current batch for incremental checkpointing
    pub fn record_event_added(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: self.generate_event_key(event, "current_batch"),
            value: event_data,
        });

        // Update event counter
        *self.total_events_processed.lock().unwrap() += 1;
    }

    /// Record a batch flush for incremental checkpointing
    pub fn record_batch_flushed(
        &self,
        current_batch: &[StreamEvent],
        expired_batch: &[StreamEvent],
    ) {
        let mut change_log = self.change_log.lock().unwrap();

        // Record batch transition operations
        change_log.push(StateOperation::Delete {
            key: b"batch_flush_marker".to_vec(),
            old_value: self.serialize_batch_transition(current_batch, expired_batch),
        });
    }

    /// Record reset event template change for incremental checkpointing
    /// Called when the first event of a new batch is captured as the reset template
    pub fn record_reset_event_set(&self, event: &StreamEvent) {
        let mut change_log = self.change_log.lock().unwrap();
        let event_data = self.serialize_event(event);

        change_log.push(StateOperation::Insert {
            key: b"reset_event".to_vec(),
            value: event_data,
        });
    }

    /// Record reset event template cleared for incremental checkpointing
    /// Called when the reset event is consumed during batch flush
    pub fn record_reset_event_cleared(&self) {
        let mut change_log = self.change_log.lock().unwrap();

        change_log.push(StateOperation::Delete {
            key: b"reset_event".to_vec(),
            old_value: Vec::new(), // We don't need the old value for delete
        });
    }

    /// Generate a unique key for an event
    fn generate_event_key(&self, event: &StreamEvent, buffer_type: &str) -> Vec<u8> {
        // Use timestamp, buffer type, and a hash of event data as key
        let mut key = Vec::new();
        key.extend_from_slice(buffer_type.as_bytes());
        key.push(b'_');
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

    /// Serialize batch transition for changelog
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

impl StateHolder for LengthBatchWindowStateHolder {
    fn schema_version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        use crate::core::util::to_bytes;

        // Determine storage strategy based on hints
        let storage_strategy = hints
            .prefer_compression
            .as_ref()
            .map(|_| StorageStrategy::Compressed)
            .unwrap_or(StorageStrategy::Essential);

        // Serialize current batch events
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
                        // Continue with other events rather than failing completely
                    }
                }
            }
            serialized_events
        };

        // Serialize expired batch events
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
                        // Continue with other events rather than failing completely
                    }
                }
            }
            serialized_events
        };

        // Serialize reset event if present
        let reset_event = {
            let reset_guard = self.reset_event.lock().unwrap();
            if let Some(ref event) = *reset_guard {
                match self
                    .serialization_service
                    .serialize_event_with_strategy(event, storage_strategy.clone())
                {
                    Ok(data) => Some(data),
                    Err(_e) => None, // Skip if serialization fails
                }
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

        // Serialize to bytes
        let mut data = to_bytes(&state_data).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize length batch window state: {e}"),
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

        log::info!(
            "LengthBatchWindow: deserialize_state called for component {}",
            self.component_id
        );

        // Verify integrity
        if !snapshot.verify_integrity() {
            log::error!("LengthBatchWindow: Checksum mismatch");
            return Err(StateError::ChecksumMismatch);
        }

        // Decompress data if needed using the shared compression utility
        let data = self.decompress_state_data(&snapshot.data, snapshot.compression.clone())?;

        // Deserialize state data
        let state_data: LengthBatchWindowStateData =
            from_bytes(&data).map_err(|e| StateError::DeserializationError {
                message: format!("Failed to deserialize length batch window state: {e}"),
            })?;

        log::info!(
            "LengthBatchWindow: Deserialized {} current batch events, {} expired events",
            state_data.current_batch.len(),
            state_data.expired_batch.len()
        );

        // Deserialize and restore current batch events
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
                        // Continue with other events rather than failing completely
                    }
                }
            }
        }

        // Deserialize and restore expired batch events
        {
            let mut expired = self.expired.lock().unwrap();
            expired.clear();

            for serialized_event in state_data.expired_batch {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => expired.push(event),
                    Err(e) => {
                        eprintln!("Warning: Failed to deserialize expired batch event: {e}");
                        // Continue with other events rather than failing completely
                    }
                }
            }
        }

        // Restore reset event template if present
        {
            let mut reset_guard = self.reset_event.lock().unwrap();
            *reset_guard = if let Some(serialized_event) = state_data.reset_event {
                match self.deserialize_event(&serialized_event) {
                    Ok(event) => Some(event),
                    Err(_e) => None, // Skip if deserialization fails
                }
            } else {
                None
            };
        }

        // Restore metadata (batch_length is configuration and doesn't need to be restored)
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
                    // Handle reset_event insert
                    if key == b"reset_event" {
                        if let Ok(event) = self.deserialize_event(value) {
                            let mut reset_guard = self.reset_event.lock().unwrap();
                            *reset_guard = Some(event);
                        }
                        continue;
                    }

                    // Deserialize and insert event into appropriate buffer
                    match self.deserialize_event(value) {
                        Ok(event) => {
                            // Check key to determine target buffer
                            if key.starts_with(b"current_batch") {
                                buffer.push(event);
                            } else if key.starts_with(b"expired_batch") {
                                expired.push(event);
                            } else {
                                // Default to current buffer
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
                        // Handle batch flush: move current to expired
                        expired.clear();
                        expired.extend(buffer.drain(..));
                    } else if key == b"reset_event" {
                        // Clear reset event template
                        let mut reset_guard = self.reset_event.lock().unwrap();
                        *reset_guard = None;
                    } else {
                        // Deserialize and remove the event
                        if let Ok(event_to_remove) = self.deserialize_event(old_value) {
                            // Remove from both buffers
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
                    // Remove old event and insert new one
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
                    // Clear both buffers and reset event template
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

        // Estimate bytes per event (rough calculation)
        let estimated_bytes_per_event = 200; // Conservative estimate
        let total_bytes = entries * estimated_bytes_per_event;

        // Estimate growth rate based on batch length
        let growth_rate = if self.batch_length > 0 {
            // Growth is cyclic - builds up to batch_length, then resets
            // Estimate based on current buffer fill level
            let current_fill = buffer.len();
            if current_fill >= self.batch_length {
                0.0 // At capacity, about to flush
            } else {
                // Estimate remaining space times event size
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
        // Length batch windows have a sequential access pattern
        // Events are added in order and flushed as batches
        AccessPattern::Sequential
    }

    fn component_metadata(&self) -> StateMetadata {
        let mut metadata = StateMetadata::new(
            self.component_id.clone(),
            "LengthBatchWindowProcessor".to_string(),
        );
        metadata.access_pattern = self.access_pattern();
        metadata.size_estimation = self.estimate_size();

        // Add custom metadata
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
}

impl CompressibleStateHolder for LengthBatchWindowStateHolder {
    fn compression_hints(&self) -> CompressionHints {
        CompressionHints {
            prefer_speed: true, // Batch windows process many events, need speed
            prefer_ratio: false,
            data_type: DataCharacteristics::ModeratelyRepetitive, // Batch event streams
            target_latency_ms: Some(2), // Target < 2ms for batch processing
            min_compression_ratio: Some(0.4), // At least 40% space savings for batches
            expected_size_range: DataSizeRange::Large, // Batches can be large
        }
    }
}

/// Serializable state data for LengthBatchWindowProcessor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LengthBatchWindowStateData {
    current_batch: Vec<Vec<u8>>,
    expired_batch: Vec<Vec<u8>>,
    batch_length: usize,
    total_events_processed: u64,
    /// Serialized reset event template (if present)
    /// Uses serde(default) for backward compatibility with checkpoints created before this field existed
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
        let mut holder = LengthBatchWindowStateHolder::new(
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
        if let Some(event) = buffer.get(0) {
            assert_eq!(event.timestamp, 1000);
            assert_eq!(event.before_window_data.len(), 2);
        }

        if let Some(event) = expired.get(0) {
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
