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

//! Sink mappers.
//!
//! Re-exports the `SinkMapper` trait from the `stream::mapper` module, which is
//! where it is defined, and adds the default `PassthroughMapper`.

pub use crate::core::stream::mapper::SinkMapper;

use crate::core::event::event::Event;
use crate::core::exception::EventFluxError;

/// Default sink mapper, used when no format such as JSON or CSV is set.
///
/// Writes events in the compact bincode binary encoding. It is used for
/// - debug sinks like LogSink that decode the bytes back into events
/// - passing events inside the engine where no outside format is needed
///
/// # Example
///
/// ```ignore
/// let mapper = PassthroughMapper::new();
/// let bytes = mapper.map_event(&event)?;
/// let recovered: Vec<Event> = PassthroughMapper::deserialize(&bytes)?;
/// ```
#[derive(Debug, Clone)]
pub struct PassthroughMapper;

impl PassthroughMapper {
    pub fn new() -> Self {
        Self
    }

    /// Decodes bytes from `map_event` back into events.
    ///
    /// Used by debug sinks that need the events back. Returns an error string
    /// if the bytes do not decode.
    pub fn deserialize(bytes: &[u8]) -> Result<Vec<Event>, String> {
        crate::core::util::serialization::from_bytes(bytes)
            .map_err(|e| format!("Failed to deserialize events: {}", e))
    }
}

impl Default for PassthroughMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkMapper for PassthroughMapper {
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError> {
        // Encode as a slice of one event so the payload stays a bincode
        // sequence, since `deserialize` (used by LogSink) returns Vec<Event>.
        crate::core::util::serialization::to_bytes(std::slice::from_ref(event))
            .map_err(|e| EventFluxError::app_runtime(format!("Failed to serialize event: {}", e)))
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(self.clone())
    }
}
