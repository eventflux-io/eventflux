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

//! Bytes Mapper. Passes raw binary payloads to and from outside systems.
//!
//! Used with message systems like RabbitMQ and Kafka. Unlike the internal
//! PassthroughMapper, which uses bincode, this mapper keeps the bytes as they are.
//!
//! - **BytesSourceMapper**: puts the raw bytes in `AttributeValue::Bytes`, with no
//!   UTF-8 conversion or other change
//! - **BytesSinkMapper**: takes the raw bytes from one field and outputs them
//!   unchanged
//!
//! ## Use Cases
//!
//! - Binary protocol messages, such as protobuf or msgpack
//! - Payloads already in their final format that should pass through unchanged
//! - Custom encodings that later processors handle
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

use super::{now_millis, SinkMapper, SourceMapper};
use crate::core::event::AttributeValue;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;

/// Source mapper for raw binary payloads.
///
/// Turns each payload into one event with a single `AttributeValue::Bytes` field
/// and the current time as its timestamp. The bytes are kept exactly, with no
/// UTF-8 conversion or other change.
#[derive(Debug, Clone, Default)]
pub struct BytesSourceMapper;

impl BytesSourceMapper {
    pub fn new() -> Self {
        Self
    }
}

impl SourceMapper for BytesSourceMapper {
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError> {
        let event = Event::new_with_data(now_millis(), vec![AttributeValue::Bytes(input.to_vec())]);

        Ok(vec![event])
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(self.clone())
    }
}

/// Sink mapper for raw binary output.
///
/// Outputs one field of the event. `AttributeValue::Bytes` is output as is.
/// Strings are output as their UTF-8 bytes, numbers and bools as their text
/// form, Null as `null`, and Object as `<object>`.
#[derive(Debug, Clone)]
pub struct BytesSinkMapper {
    /// Index of the field to output. Default 0, the first field.
    field_index: usize,
}

impl BytesSinkMapper {
    /// Creates a mapper that outputs the first field
    pub fn new() -> Self {
        Self { field_index: 0 }
    }

    /// Creates a mapper that outputs the field at `index`
    pub fn with_field_index(index: usize) -> Self {
        Self { field_index: index }
    }

    /// Sets the index of the field to output
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
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError> {
        // One event gives one binary payload. SinkCallbackAdapter walks through
        // batches one event at a time, so the old "cannot concatenate binary
        // batches" failure can no longer happen.
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

        // Must be stored as Bytes
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

        // The binary data must be kept exactly
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

        // Several binary patterns, including invalid UTF-8
        let test_cases: Vec<Vec<u8>> = vec![
            vec![0x00, 0x01, 0x02, 0x03],        // null bytes
            vec![0xff, 0xfe, 0xfd],              // high bytes
            vec![0x80, 0x81, 0x82],              // continuation bytes without start
            vec![0xc0, 0xc1],                    // overlong encodings
            (0..256).map(|b| b as u8).collect(), // all byte values
        ];

        for input in test_cases {
            let events = source_mapper.map(&input).unwrap();
            assert_eq!(events.len(), 1);
            let output = sink_mapper.map_event(&events[0]).unwrap();
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

        // The cloned data must be kept exactly
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

        let result = mapper.map_event(&event).unwrap();
        assert_eq!(result, raw_data);
    }

    #[test]
    fn test_bytes_sink_mapper_string() {
        let mapper = BytesSinkMapper::new();
        let event = Event::new_with_data(123, vec![AttributeValue::String("Hello".to_string())]);

        let result = mapper.map_event(&event).unwrap();
        assert_eq!(result, b"Hello");
    }

    #[test]
    fn test_bytes_sink_mapper_number() {
        let mapper = BytesSinkMapper::new();
        let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);

        let result = mapper.map_event(&event).unwrap();
        assert_eq!(result, b"42");
    }

    #[test]
    fn test_bytes_sink_mapper_events_map_independently() {
        // Each event in a batch maps to its own payload, since
        // SinkCallbackAdapter walks through the batch. The old "cannot
        // concatenate binary batches" failure can no longer happen.
        let mapper = BytesSinkMapper::new();
        let events = [
            Event::new_with_data(1, vec![AttributeValue::Bytes(b"First".to_vec())]),
            Event::new_with_data(2, vec![AttributeValue::Bytes(b"Second".to_vec())]),
        ];

        let payloads: Vec<Vec<u8>> = events
            .iter()
            .map(|e| mapper.map_event(e).unwrap())
            .collect();
        assert_eq!(payloads, vec![b"First".to_vec(), b"Second".to_vec()]);
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

        let result = mapper.map_event(&event).unwrap();
        assert_eq!(result, b"extract_me");
    }

    #[test]
    fn test_bytes_sink_mapper_field_out_of_bounds() {
        let mapper = BytesSinkMapper::with_field_index(5);
        let event = Event::new_with_data(123, vec![AttributeValue::Bytes(b"only_one".to_vec())]);

        let result = mapper.map_event(&event);
        assert!(result.is_err());
    }
}
