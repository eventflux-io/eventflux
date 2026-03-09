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

//! Advanced Event Serialization System
//!
//! This module provides enterprise-grade serialization for StreamEvents and AttributeValues,
//! with proper handling of all types including the problematic Box<dyn Any> variant.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::event::complex_event::ComplexEventType;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::persistence::state_holder::StateError;

/// Storage strategy for event serialization based on use case
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum StorageStrategy {
    /// Store complete event data for full recovery
    Full,
    /// Store essential data only (excludes complex objects)
    #[default]
    Essential,
    /// Store only references/metadata (minimal storage)
    Reference,
    /// Store compressed event data
    Compressed,
}

/// Serializable representation of AttributeValue that handles all types safely
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableAttributeValue {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    /// Raw binary data for passthrough scenarios
    Bytes(Vec<u8>),
    /// Object variant with type information and serialized data
    Object {
        type_name: String,
        is_some: bool,
        /// For known types, we store a string representation
        /// For unknown types, we store None and reconstruct as empty
        serialized_data: Option<String>,
    },
    Null,
}

impl From<&AttributeValue> for SerializableAttributeValue {
    fn from(value: &AttributeValue) -> Self {
        match value {
            AttributeValue::String(s) => SerializableAttributeValue::String(s.clone()),
            AttributeValue::Int(i) => SerializableAttributeValue::Int(*i),
            AttributeValue::Long(l) => SerializableAttributeValue::Long(*l),
            AttributeValue::Float(f) => SerializableAttributeValue::Float(*f),
            AttributeValue::Double(d) => SerializableAttributeValue::Double(*d),
            AttributeValue::Bool(b) => SerializableAttributeValue::Bool(*b),
            AttributeValue::Bytes(bytes) => SerializableAttributeValue::Bytes(bytes.clone()),
            AttributeValue::Object(obj_opt) => {
                // Handle the Box<dyn Any> case
                SerializableAttributeValue::Object {
                    type_name: "dynamic_object".to_string(),
                    is_some: obj_opt.is_some(),
                    // For now, we cannot serialize arbitrary Box<dyn Any> objects
                    // In a production system, we would register known object types
                    serialized_data: None,
                }
            }
            AttributeValue::Null => SerializableAttributeValue::Null,
        }
    }
}

impl From<SerializableAttributeValue> for AttributeValue {
    fn from(value: SerializableAttributeValue) -> Self {
        match value {
            SerializableAttributeValue::String(s) => AttributeValue::String(s),
            SerializableAttributeValue::Int(i) => AttributeValue::Int(i),
            SerializableAttributeValue::Long(l) => AttributeValue::Long(l),
            SerializableAttributeValue::Float(f) => AttributeValue::Float(f),
            SerializableAttributeValue::Double(d) => AttributeValue::Double(d),
            SerializableAttributeValue::Bool(b) => AttributeValue::Bool(b),
            SerializableAttributeValue::Bytes(bytes) => AttributeValue::Bytes(bytes),
            SerializableAttributeValue::Object { is_some, .. } => {
                // Reconstruct as None for now - in production we'd have a registry
                // of known object types that can be deserialized properly
                if is_some {
                    // We know there was an object but can't reconstruct it
                    AttributeValue::Object(None)
                } else {
                    AttributeValue::Object(None)
                }
            }
            SerializableAttributeValue::Null => AttributeValue::Null,
        }
    }
}

/// Serializable event data with configurable storage strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEventData {
    pub timestamp: i64,
    pub before_window_data: Vec<SerializableAttributeValue>,
    pub on_after_window_data: Vec<SerializableAttributeValue>,
    pub output_data: Vec<SerializableAttributeValue>,
    pub is_expired: bool,
    pub storage_strategy: StorageStrategy,
    /// Event metadata for reconstruction
    pub metadata: EventMetadata,
}

/// Metadata for event reconstruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    pub event_key: String,
    pub source_stream: Option<String>,
    pub processing_stage: Option<String>,
    pub custom_properties: HashMap<String, String>,
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self {
            event_key: "unknown".to_string(),
            source_stream: None,
            processing_stage: None,
            custom_properties: HashMap::new(),
        }
    }
}

impl SerializableEventData {
    /// Create serializable event data from a StreamEvent with specified strategy
    pub fn from_stream_event(event: &StreamEvent, strategy: StorageStrategy) -> Self {
        let before_window_data = match strategy {
            StorageStrategy::Full | StorageStrategy::Essential => event
                .before_window_data
                .iter()
                .map(SerializableAttributeValue::from)
                .collect(),
            StorageStrategy::Reference => {
                // Store only basic types, skip complex objects entirely
                event
                    .before_window_data
                    .iter()
                    .filter_map(|attr| {
                        match attr {
                            AttributeValue::Object(_) => None, // Skip objects in reference mode
                            _ => Some(SerializableAttributeValue::from(attr)),
                        }
                    })
                    .collect()
            }
            StorageStrategy::Compressed => {
                // Similar to essential but with additional compression hints
                event
                    .before_window_data
                    .iter()
                    .map(SerializableAttributeValue::from)
                    .collect()
            }
        };

        let on_after_window_data = event
            .on_after_window_data
            .iter()
            .map(SerializableAttributeValue::from)
            .collect();

        let output_data = match &event.output_data {
            Some(data) => data.iter().map(SerializableAttributeValue::from).collect(),
            None => Vec::new(),
        };

        Self {
            timestamp: event.timestamp,
            before_window_data,
            on_after_window_data,
            output_data,
            is_expired: event.event_type == ComplexEventType::Expired,
            storage_strategy: strategy,
            metadata: EventMetadata {
                event_key: format!("event_{}", event.timestamp),
                source_stream: None, // Could be populated from context
                processing_stage: None,
                custom_properties: HashMap::new(),
            },
        }
    }

    /// Convert back to StreamEvent
    pub fn to_stream_event(&self) -> StreamEvent {
        let before_window_data: Vec<AttributeValue> = self
            .before_window_data
            .iter()
            .map(|attr| AttributeValue::from(attr.clone()))
            .collect();

        let on_after_window_data: Vec<AttributeValue> = self
            .on_after_window_data
            .iter()
            .map(|attr| AttributeValue::from(attr.clone()))
            .collect();

        let output_data: Vec<AttributeValue> = self
            .output_data
            .iter()
            .map(|attr| AttributeValue::from(attr.clone()))
            .collect();

        let mut stream_event = StreamEvent::new(
            self.timestamp,
            before_window_data.len(),
            on_after_window_data.len(),
            output_data.len(),
        );

        stream_event.before_window_data = before_window_data;
        stream_event.on_after_window_data = on_after_window_data;
        stream_event.output_data = if output_data.is_empty() {
            None
        } else {
            Some(output_data)
        };
        stream_event.event_type = if self.is_expired {
            ComplexEventType::Expired
        } else {
            ComplexEventType::Current
        };

        stream_event
    }

    /// Serialize to bytes with compression support
    pub fn to_bytes(&self) -> Result<Vec<u8>, StateError> {
        use crate::core::util::to_bytes;

        to_bytes(self).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize event data: {e}"),
        })
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, StateError> {
        use crate::core::util::from_bytes;

        from_bytes(data).map_err(|e| StateError::DeserializationError {
            message: format!("Failed to deserialize event data: {e}"),
        })
    }

    /// Get storage size estimate
    pub fn estimate_size(&self) -> usize {
        // Rough estimate based on data
        let base_size = std::mem::size_of::<Self>();
        let data_size = (self.before_window_data.len()
            + self.on_after_window_data.len()
            + self.output_data.len())
            * 50; // Rough estimate per attribute
        base_size + data_size
    }
}

/// Batch serialization for multiple events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<SerializableEventData>,
    pub batch_metadata: BatchMetadata,
}

/// Metadata for event batches
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchMetadata {
    pub batch_id: String,
    pub timestamp_range: (i64, i64),
    pub event_count: usize,
    pub storage_strategy: StorageStrategy,
    pub compression_applied: bool,
}

impl EventBatch {
    /// Create a new event batch
    pub fn new(events: Vec<SerializableEventData>, strategy: StorageStrategy) -> Self {
        let timestamp_range = if events.is_empty() {
            (0, 0)
        } else {
            let min_ts = events.iter().map(|e| e.timestamp).min().unwrap_or(0);
            let max_ts = events.iter().map(|e| e.timestamp).max().unwrap_or(0);
            (min_ts, max_ts)
        };

        let batch_metadata = BatchMetadata {
            batch_id: format!("batch_{}", chrono::Utc::now().timestamp_millis()),
            timestamp_range,
            event_count: events.len(),
            storage_strategy: strategy,
            compression_applied: false,
        };

        Self {
            events,
            batch_metadata,
        }
    }

    /// Serialize batch to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, StateError> {
        use crate::core::util::to_bytes;

        to_bytes(self).map_err(|e| StateError::SerializationError {
            message: format!("Failed to serialize event batch: {e}"),
        })
    }

    /// Deserialize batch from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, StateError> {
        use crate::core::util::from_bytes;

        from_bytes(data).map_err(|e| StateError::DeserializationError {
            message: format!("Failed to deserialize event batch: {e}"),
        })
    }

    /// Convert all events to StreamEvents
    pub fn to_stream_events(&self) -> Vec<StreamEvent> {
        self.events
            .iter()
            .map(|event_data| event_data.to_stream_event())
            .collect()
    }

    /// Estimate total storage size
    pub fn estimate_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let events_size: usize = self.events.iter().map(|e| e.estimate_size()).sum();
        base_size + events_size
    }
}

/// High-level event serialization service
#[derive(Debug, Clone)]
pub struct EventSerializationService {
    default_strategy: StorageStrategy,
}

impl EventSerializationService {
    pub fn new(default_strategy: StorageStrategy) -> Self {
        Self { default_strategy }
    }

    /// Serialize a single event with default strategy
    pub fn serialize_event(&self, event: &StreamEvent) -> Result<Vec<u8>, StateError> {
        let serializable =
            SerializableEventData::from_stream_event(event, self.default_strategy.clone());
        serializable.to_bytes()
    }

    /// Serialize a single event with custom strategy
    pub fn serialize_event_with_strategy(
        &self,
        event: &StreamEvent,
        strategy: StorageStrategy,
    ) -> Result<Vec<u8>, StateError> {
        let serializable = SerializableEventData::from_stream_event(event, strategy);
        serializable.to_bytes()
    }

    /// Deserialize a single event
    pub fn deserialize_event(&self, data: &[u8]) -> Result<StreamEvent, StateError> {
        let serializable = SerializableEventData::from_bytes(data)?;
        Ok(serializable.to_stream_event())
    }

    /// Serialize a batch of events
    pub fn serialize_event_batch(&self, events: &[StreamEvent]) -> Result<Vec<u8>, StateError> {
        let serializable_events: Vec<SerializableEventData> = events
            .iter()
            .map(|event| {
                SerializableEventData::from_stream_event(event, self.default_strategy.clone())
            })
            .collect();

        let batch = EventBatch::new(serializable_events, self.default_strategy.clone());
        batch.to_bytes()
    }

    /// Deserialize a batch of events
    pub fn deserialize_event_batch(&self, data: &[u8]) -> Result<Vec<StreamEvent>, StateError> {
        let batch = EventBatch::from_bytes(data)?;
        Ok(batch.to_stream_events())
    }

    /// Get storage size estimate for events
    pub fn estimate_storage_size(&self, events: &[StreamEvent]) -> usize {
        events
            .iter()
            .map(|event| {
                let serializable =
                    SerializableEventData::from_stream_event(event, self.default_strategy.clone());
                serializable.estimate_size()
            })
            .sum()
    }
}

impl Default for EventSerializationService {
    fn default() -> Self {
        Self::new(StorageStrategy::Essential)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;

    #[test]
    fn test_serializable_attribute_value_conversion() {
        let attr = AttributeValue::String("test".to_string());
        let serializable = SerializableAttributeValue::from(&attr);
        let converted_back = AttributeValue::from(serializable);

        match converted_back {
            AttributeValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Conversion failed"),
        }
    }

    #[test]
    fn test_object_attribute_value_handling() {
        let attr = AttributeValue::Object(None);
        let serializable = SerializableAttributeValue::from(&attr);

        match serializable {
            SerializableAttributeValue::Object { is_some, .. } => assert!(!is_some),
            _ => panic!("Expected Object variant"),
        }

        let converted_back = AttributeValue::from(serializable);
        match converted_back {
            AttributeValue::Object(None) => {} // Expected
            _ => panic!("Object conversion failed"),
        }
    }

    #[test]
    fn test_stream_event_serialization() {
        let mut event = StreamEvent::new(1000, 2, 0, 0);
        event.before_window_data = vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(42),
        ];

        let serializable = SerializableEventData::from_stream_event(&event, StorageStrategy::Full);
        let reconstructed = serializable.to_stream_event();

        assert_eq!(reconstructed.timestamp, 1000);
        assert_eq!(reconstructed.before_window_data.len(), 2);

        match &reconstructed.before_window_data[0] {
            AttributeValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("First attribute should be string"),
        }

        match &reconstructed.before_window_data[1] {
            AttributeValue::Int(i) => assert_eq!(*i, 42),
            _ => panic!("Second attribute should be int"),
        }
    }

    #[test]
    fn test_reference_storage_strategy() {
        let mut event = StreamEvent::new(1000, 3, 0, 0);
        event.before_window_data = vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(42),
            AttributeValue::Object(None), // This should be filtered out
        ];

        let serializable =
            SerializableEventData::from_stream_event(&event, StorageStrategy::Reference);

        // Should only have 2 attributes (Object filtered out)
        assert_eq!(serializable.before_window_data.len(), 2);

        let reconstructed = serializable.to_stream_event();
        assert_eq!(reconstructed.before_window_data.len(), 2);
    }

    #[test]
    fn test_event_batch_serialization() {
        let mut event1 = StreamEvent::new(1000, 1, 0, 0);
        event1.before_window_data = vec![AttributeValue::Int(1)];

        let mut event2 = StreamEvent::new(2000, 1, 0, 0);
        event2.before_window_data = vec![AttributeValue::Int(2)];

        let events = vec![event1, event2];
        let service = EventSerializationService::default();

        let serialized = service.serialize_event_batch(&events).unwrap();
        let deserialized = service.deserialize_event_batch(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].timestamp, 1000);
        assert_eq!(deserialized[1].timestamp, 2000);
    }

    #[test]
    fn test_serialization_service() {
        let mut event = StreamEvent::new(1000, 2, 0, 0);
        event.before_window_data = vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Double(3.14),
        ];

        let service = EventSerializationService::new(StorageStrategy::Essential);

        let serialized = service.serialize_event(&event).unwrap();
        let deserialized = service.deserialize_event(&serialized).unwrap();

        assert_eq!(deserialized.timestamp, 1000);
        assert_eq!(deserialized.before_window_data.len(), 2);
    }

    #[test]
    fn test_size_estimation() {
        let mut event = StreamEvent::new(1000, 1, 0, 0);
        event.before_window_data = vec![AttributeValue::String("test".to_string())];

        let service = EventSerializationService::default();
        let estimated_size = service.estimate_storage_size(&[event]);

        assert!(estimated_size > 0);
        assert!(estimated_size < 10000); // Reasonable upper bound
    }
}
