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

//! Source mapper trait and implementations
//!
//! Re-exports the canonical `SourceMapper` trait from `stream::mapper` module.

// Re-export the canonical SourceMapper trait from mapper module
pub use crate::core::stream::mapper::SourceMapper;

use crate::core::event::event::Event;
use crate::core::exception::EventFluxError;

/// Default passthrough mapper for Sources
///
/// When no format is specified (no JSON/CSV/XML mapper), this mapper
/// deserializes Events from an efficient binary format using bincode.
///
/// This is the inverse of output::mapper::PassthroughMapper and is used for:
/// - Debug sources like TimerSource that produce Events directly
/// - Internal event passing where no external format is needed
///
/// # Example
///
/// ```ignore
/// let mapper = PassthroughMapper::new();
/// let bytes = bincode::serialize(&events)?;
/// let recovered: Vec<Event> = mapper.map(&bytes);
/// ```
#[derive(Debug, Clone)]
pub struct PassthroughMapper;

impl PassthroughMapper {
    pub fn new() -> Self {
        Self
    }

    /// Serialize Events to bytes
    ///
    /// Used by sources that generate Events internally and need to convert
    /// them to binary format for the mapper pipeline.
    pub fn serialize(events: &[Event]) -> Result<Vec<u8>, String> {
        bincode::serialize(events).map_err(|e| format!("Failed to serialize events: {}", e))
    }
}

impl Default for PassthroughMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMapper for PassthroughMapper {
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError> {
        // Use bincode for efficient binary deserialization
        bincode::deserialize(input).map_err(|e| EventFluxError::MappingFailed {
            message: format!("Failed to deserialize events from binary format: {}", e),
            source: Some(Box::new(e)),
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

        // Serialize and deserialize
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

        // Invalid bincode data
        let invalid_bytes = b"this is not valid bincode data";
        let result = mapper.map(invalid_bytes);

        // Should return error instead of silently dropping data
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

        // Empty input
        let result = mapper.map(&[]);

        // Should return error
        assert!(result.is_err());
    }
}
