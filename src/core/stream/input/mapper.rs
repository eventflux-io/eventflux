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

//! Source mappers.
//!
//! Re-exports the `SourceMapper` trait from the `stream::mapper` module, which is
//! where it is defined, and adds the default `PassthroughMapper`.

pub use crate::core::stream::mapper::SourceMapper;

use crate::core::event::event::Event;
use crate::core::exception::EventFluxError;

/// Default source mapper, used when no format such as JSON or CSV is set.
///
/// Reads events from the compact bincode binary encoding. It is the reverse of
/// `output::mapper::PassthroughMapper`. It is used for
/// - debug sources like TimerSource that build events directly
/// - passing events inside the engine where no outside format is needed
///
/// # Example
///
/// ```ignore
/// let mapper = PassthroughMapper::new();
/// let bytes = eventflux::core::util::serialization::to_bytes(&events)?;
/// let recovered: Vec<Event> = mapper.map(&bytes);
/// ```
#[derive(Debug, Clone)]
pub struct PassthroughMapper;

impl PassthroughMapper {
    pub fn new() -> Self {
        Self
    }

    /// Encodes events to bytes that `map` can read back.
    ///
    /// Used by sources that build events themselves and must pass them through
    /// the mapper step as bytes. Returns an error string if encoding fails.
    pub fn serialize(events: &[Event]) -> Result<Vec<u8>, String> {
        crate::core::util::serialization::to_bytes(events)
            .map_err(|e| format!("Failed to serialize events: {}", e))
    }
}

impl Default for PassthroughMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMapper for PassthroughMapper {
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError> {
        // Bad bytes return MappingFailed instead of being dropped silently
        crate::core::util::serialization::from_bytes(input).map_err(|e| {
            EventFluxError::MappingFailed {
                message: format!("Failed to deserialize events from binary format: {}", e),
                source: Some(Box::new(e)),
            }
        })
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;

    #[test]
    fn test_passthrough_mapper_valid_input() {
        let mapper = PassthroughMapper::new();
        let events = vec![Event::new_with_data(
            123,
            vec![AttributeValue::String("test".to_string())],
        )];

        let bytes = PassthroughMapper::serialize(&events).unwrap();
        let result = mapper.map(&bytes);

        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].timestamp, 123);
    }

    #[test]
    fn test_passthrough_mapper_invalid_input_returns_error() {
        let mapper = PassthroughMapper::new();

        let invalid_bytes = b"this is not valid bincode data";
        let result = mapper.map(invalid_bytes);

        // Must return an error, not drop the data silently
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(matches!(error, EventFluxError::MappingFailed { .. }));
        assert!(error
            .to_string()
            .contains("Failed to deserialize events from binary format"));
    }

    #[test]
    fn test_passthrough_mapper_empty_input_returns_error() {
        let mapper = PassthroughMapper::new();

        let result = mapper.map(&[]);

        assert!(result.is_err());
    }
}
