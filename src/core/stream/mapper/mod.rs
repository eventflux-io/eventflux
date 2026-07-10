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

//! # Data Mapping System
//!
//! Provides mappers for transforming raw data to/from EventFlux events.
//!
//! ## Mapper Types
//!
//! - **SourceMapper**: Maps raw bytes (JSON, CSV, etc.) → EventFlux events
//! - **SinkMapper**: Maps EventFlux events → raw bytes for external systems
//!
//! ## Auto-Mapping Policy (All-or-Nothing)
//!
//! EventFlux enforces strict auto-mapping rules:
//! - **NO `mapping.*` properties** → Auto-map ALL top-level fields by name
//! - **ANY `mapping.*` properties** → Explicitly map ALL fields (no auto-mapping)
//!
//! ## Supported Formats
//!
//! - JSON (with JSONPath extraction)
//! - CSV (with field mapping)
//!
//! ## Configuration Example
//!
//! ```toml
//! [streams.InputJSON]
//! type = "source"
//! format = "json"
//! json.mapping.orderId = "$.order.id"
//! json.mapping.amount = "$.order.total"
//!
//! [streams.OutputJSON]
//! type = "sink"
//! format = "json"
//! json.template = "{\"eventType\":\"ORDER\",\"id\":\"{{orderId}}\",\"amount\":{{amount}}}"
//! ```

pub mod bytes_mapper;
pub mod csv_mapper;
pub mod factory;
pub mod json_mapper;
pub mod validation;

use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use std::fmt::Debug;

/// Trait for mapping raw bytes to EventFlux events (for source streams)
///
/// Mappers are fully configured and ready to use when created via factories.
/// The `map` method handles all parsing, extraction, and error handling.
pub trait SourceMapper: Debug + Send + Sync {
    /// Map raw input bytes to EventFlux events
    ///
    /// # Arguments
    /// * `input` - Raw bytes from external source (JSON, CSV, etc.)
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Successfully mapped events (may contain multiple events for batch processing)
    /// * `Err(EventFluxError)` - Mapping failed due to malformed input or extraction errors
    ///
    /// # Implementation Notes
    /// - Must handle malformed input gracefully
    /// - May return multiple events for array/batch inputs
    /// - Should validate data against expected schema when possible
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError>;

    /// Clone this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SourceMapper>;
}

impl Clone for Box<dyn SourceMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Trait for mapping EventFlux events to raw bytes (for sink streams)
///
/// Mappers are fully configured and ready to use when created via factories.
/// The `map_event` method handles all serialization and template rendering.
///
/// # One event = one transport message
///
/// Mappers serialize exactly ONE event into ONE payload. `SinkCallbackAdapter`
/// iterates batches and calls `map_event` + `Sink::publish` per event, matching
/// the per-record semantics of real transports (Kafka record, AMQP message,
/// WebSocket frame). Sinks that want transport-level batching (e.g. HTTP batch
/// POST) must buffer internally — batching is a transport concern, not a
/// formatting concern.
pub trait SinkMapper: Debug + Send + Sync {
    /// Map ONE EventFlux event to one transport payload
    ///
    /// # Arguments
    /// * `event` - EventFlux event to serialize
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Successfully serialized bytes ready for external sink
    /// * `Err(EventFluxError)` - Mapping failed due to serialization errors
    ///
    /// # Implementation Notes
    /// - Must handle template rendering errors gracefully
    /// - Output format must match sink expectations
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError>;

    /// Clone this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SinkMapper>;
}

impl Clone for Box<dyn SinkMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
