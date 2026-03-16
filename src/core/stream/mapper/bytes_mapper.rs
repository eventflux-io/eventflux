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

//! Bytes Mapper - Raw binary passthrough for external systems
//!
//! This mapper provides raw bytes passthrough for external message systems like RabbitMQ,
//! Kafka, etc. Unlike the internal PassthroughMapper (which uses bincode), this mapper:
//!
//! - **BytesSourceMapper**: Stores raw bytes in `AttributeValue::Bytes` to preserve
//!   binary data exactly without any UTF-8 conversion or mutation
//! - **BytesSinkMapper**: Extracts the raw bytes and outputs them unchanged
//!
//! ## Use Cases
//!
//! - Binary protocol messages (protobuf, msgpack, etc.)
//! - Pre-formatted payloads that should pass through unchanged
//! - Custom serialization formats handled by downstream processors
//!
//! ## Example
//!
//! ```sql
//! CREATE STREAM RawMessages (payload OBJECT)
//! WITH (
//!     type = 'rabbitmq',
//!     format = 'bytes',
//!     'rabbitmq.host' = 'localhost',
//!     'rabbitmq.queue' = 'raw-queue'
//! );
//! ```

use super::{SinkMapper, SourceMapper};
use crate::core::event::AttributeValue;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source mapper for raw binary payloads
///
/// Stores incoming bytes in an `AttributeValue::Bytes` variant.
/// This preserves binary data exactly without any UTF-8 conversion or mutation.
#[derive(Debug, Clone, Default)]
pub struct BytesSourceMapper;

impl BytesSourceMapper {
    pub fn new() -> Self {
        Self
    }
}

impl SourceMapper for BytesSourceMapper {
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError> {
        // Create timestamp
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Store raw bytes directly in AttributeValue::Bytes
        let event = Event::new_with_data(timestamp, vec![AttributeValue::Bytes(input.to_vec())]);

        Ok(vec![event])
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(self.clone())
    }
}

/// Sink mapper for raw binary output
///
/// Extracts raw bytes from `AttributeValue::Bytes`,
/// or converts other attribute types to their byte representation.
#[derive(Debug, Clone)]
pub struct BytesSinkMapper {
    /// Field index to extract (default: 0 = first field)
    field_index: usize,
}

impl BytesSinkMapper {
    pub fn new() -> Self {
        Self { field_index: 0 }
    }

    /// Create mapper that extracts a specific field by index
    pub fn with_field_index(index: usize) -> Self {
        Self { field_index: index }
    }

    /// Set the field index to extract
    pub fn set_field_index(&mut self, index: usize) {
        self.field_index = index;
    }
}

impl Default for BytesSinkMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkMapper for BytesSinkMapper {
    fn map(&self, events: &[Event]) -> Result<Vec<u8>, EventFluxError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // Bytes mapper only supports single-event batches to preserve raw binary
        // passthrough behavior. Binary data cannot be safely concatenated with
        // separators as it would corrupt non-text formats like protobuf, msgpack,
        // or other binary protocols.
        //
        // If you're seeing this error, ensure your sink receives events one at a
        // time rather than in batches, or use a text-based format like JSON or CSV
        // that can safely handle multiple events.
        if events.len() > 1 {
            return Err(EventFluxError::MappingFailed {
                message: format!(
                    "Bytes mapper received {} events but only supports single-event batches. \
                    Binary data cannot be safely concatenated. Use JSON or CSV format for \
                    batched events, or ensure events are sent individually.",
                    events.len()
                ),
                source: None,
            });
        }

        let event = &events[0];

        if self.field_index >= event.data.len() {
            return Err(EventFluxError::MappingFailed {
                message: format!(
                    "Field index {} out of bounds (event has {} fields)",
                    self.field_index,
                    event.data.len()
                ),
                source: None,
            });
        }

        // Extract bytes from the specified field
        let bytes = match &event.data[self.field_index] {
            AttributeValue::Bytes(bytes) => bytes.clone(),
            AttributeValue::String(s) => s.as_bytes().to_vec(),
            AttributeValue::Int(n) => n.to_string().into_bytes(),
            AttributeValue::Long(n) => n.to_string().into_bytes(),
            AttributeValue::Float(f) => f.to_string().into_bytes(),
            AttributeValue::Double(d) => d.to_string().into_bytes(),
            AttributeValue::Bool(b) => b.to_string().into_bytes(),
            AttributeValue::Null => b"null".to_vec(),
            AttributeValue::Object(_) => b"<object>".to_vec(),
        };

        Ok(bytes)
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_source_mapper_utf8() {
        let mapper = BytesSourceMapper::new();
        let input = b"Hello, World!";

        let events = mapper.map(input).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 1);

        // Verify it's stored as Bytes
        if let AttributeValue::Bytes(bytes) = &events[0].data[0] {
            assert_eq!(bytes, b"Hello, World!");
        } else {
            panic!("Expected Bytes attribute");
        }
    }

    #[test]
    fn test_bytes_source_mapper_binary_passthrough() {
        let mapper = BytesSourceMapper::new();
        // Binary data with invalid UTF-8 sequences
        let input = vec![0x00, 0x01, 0xff, 0xfe, 0x80, 0x90, 0xa0, 0xb0];

        let events = mapper.map(&input).unwrap();
        assert_eq!(events.len(), 1);

        // Verify binary data is preserved exactly
        if let AttributeValue::Bytes(bytes) = &events[0].data[0] {
            assert_eq!(bytes, &input, "Binary data should be preserved exactly");
        } else {
            panic!("Expected Bytes attribute");
        }
    }

    #[test]
    fn test_bytes_roundtrip_binary_data() {
        let source_mapper = BytesSourceMapper::new();
        let sink_mapper = BytesSinkMapper::new();

        // Test with various binary patterns including invalid UTF-8
        let test_cases: Vec<Vec<u8>> = vec![
            vec![0x00, 0x01, 0x02, 0x03],        // null bytes
            vec![0xff, 0xfe, 0xfd],              // high bytes
            vec![0x80, 0x81, 0x82],              // continuation bytes without start
            vec![0xc0, 0xc1],                    // overlong encodings
            (0..256).map(|b| b as u8).collect(), // all byte values
        ];

        for input in test_cases {
            let events = source_mapper.map(&input).unwrap();
            let output = sink_mapper.map(&events).unwrap();
            assert_eq!(
                output, input,
                "Binary data should round-trip exactly: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_bytes_clone_preserves_data() {
        let mapper = BytesSourceMapper::new();
        // Binary data with invalid UTF-8 sequences
        let input = vec![0x00, 0x01, 0xff, 0xfe, 0x80, 0x90, 0xa0, 0xb0];

        let events = mapper.map(&input).unwrap();
        let cloned_events: Vec<Event> = events.to_vec();

        // Verify cloned data is preserved exactly
        if let AttributeValue::Bytes(bytes) = &cloned_events[0].data[0] {
            assert_eq!(
                bytes, &input,
                "Cloned binary data should be preserved exactly"
            );
        } else {
            panic!("Expected Bytes attribute after clone");
        }
    }

    #[test]
    fn test_bytes_sink_mapper_bytes() {
        let mapper = BytesSinkMapper::new();
        let raw_data = vec![0x00, 0xff, 0x80, 0x7f];
        let event = Event::new_with_data(123, vec![AttributeValue::Bytes(raw_data.clone())]);

        let result = mapper.map(&[event]).unwrap();
        assert_eq!(result, raw_data);
    }

    #[test]
    fn test_bytes_sink_mapper_string() {
        let mapper = BytesSinkMapper::new();
        let event = Event::new_with_data(123, vec![AttributeValue::String("Hello".to_string())]);

        let result = mapper.map(&[event]).unwrap();
        assert_eq!(result, b"Hello");
    }

    #[test]
    fn test_bytes_sink_mapper_number() {
        let mapper = BytesSinkMapper::new();
        let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);

        let result = mapper.map(&[event]).unwrap();
        assert_eq!(result, b"42");
    }

    #[test]
    fn test_bytes_sink_mapper_multiple_events_errors() {
        // Bytes mapper rejects multi-event batches to prevent silent data loss.
        // Binary data cannot be safely concatenated, so batching must be handled
        // at the sink level (sending events individually) rather than silently
        // dropping events.
        let mapper = BytesSinkMapper::new();
        let events = vec![
            Event::new_with_data(1, vec![AttributeValue::Bytes(b"First".to_vec())]),
            Event::new_with_data(2, vec![AttributeValue::Bytes(b"Second".to_vec())]),
        ];

        let result = mapper.map(&events);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("only supports single-event batches"),
            "Expected error about single-event batches, got: {}",
            err
        );
    }

    #[test]
    fn test_bytes_sink_mapper_empty() {
        let mapper = BytesSinkMapper::new();
        let result = mapper.map(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_bytes_sink_mapper_field_index() {
        let mapper = BytesSinkMapper::with_field_index(1);
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::String("ignore".to_string()),
                AttributeValue::Bytes(b"extract_me".to_vec()),
            ],
        );

        let result = mapper.map(&[event]).unwrap();
        assert_eq!(result, b"extract_me");
    }

    #[test]
    fn test_bytes_sink_mapper_field_out_of_bounds() {
        let mapper = BytesSinkMapper::with_field_index(5);
        let event = Event::new_with_data(123, vec![AttributeValue::Bytes(b"only_one".to_vec())]);

        let result = mapper.map(&[event]);
        assert!(result.is_err());
    }
}
