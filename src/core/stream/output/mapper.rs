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

//! Sink mapper trait and implementations
//!
//! Re-exports the canonical `SinkMapper` trait from `stream::mapper` module.

// Re-export the canonical SinkMapper trait from mapper module
pub use crate::core::stream::mapper::SinkMapper;

use crate::core::event::event::Event;
use crate::core::exception::EventFluxError;

/// Default passthrough mapper for Events
///
/// When no format is specified (no JSON/CSV/XML mapper), this mapper
/// serializes Events to an efficient binary format using bincode.
///
/// This is used for:
/// - Debug sinks like LogSink that need to deserialize back to Events
/// - Internal event passing where no external format is needed
///
/// # Example
///
/// ```ignore
/// let mapper = PassthroughMapper::new();
/// let bytes = mapper.map(&events);
/// let recovered: Vec<Event> = PassthroughMapper::deserialize(&bytes)?;
/// ```
#[derive(Debug, Clone)]
pub struct PassthroughMapper;

impl PassthroughMapper {
    pub fn new() -> Self {
        Self
    }

    /// Deserialize bytes back to Events
    ///
    /// Used by debug sinks that need to recover Events from binary format.
    pub fn deserialize(bytes: &[u8]) -> Result<Vec<Event>, String> {
        bincode::deserialize(bytes).map_err(|e| format!("Failed to deserialize events: {}", e))
    }
}

impl Default for PassthroughMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkMapper for PassthroughMapper {
    fn map(&self, events: &[Event]) -> Result<Vec<u8>, EventFluxError> {
        // Use bincode for efficient binary serialization
        bincode::serialize(events)
            .map_err(|e| EventFluxError::app_runtime(format!("Failed to serialize events: {}", e)))
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(self.clone())
    }
}
