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

pub mod rabbitmq_source;
pub mod timer_source;
pub mod websocket_source;

use crate::core::exception::EventFluxError;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::mapper::SourceMapper;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Callback for receiving data from sources
///
/// Sources produce raw bytes from external systems and deliver them via this callback.
/// The callback is responsible for parsing bytes into Events and delivering to InputHandler.
pub trait SourceCallback: Debug + Send + Sync {
    /// Called when source has new data available
    ///
    /// # Arguments
    /// * `data` - Raw bytes from external system (JSON, CSV, binary, etc.)
    ///
    /// # Returns
    /// * `Ok(())` - Data processed successfully
    /// * `Err(EventFluxError)` - Processing failed
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError>;
}

pub trait Source: Debug + Send + Sync {
    /// Start the source with a callback for data delivery
    ///
    /// Sources read from external systems and deliver raw bytes via the callback.
    /// The callback handles parsing (via SourceMapper) and event delivery.
    ///
    /// # Architecture
    /// ```text
    /// Source::read() → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
    /// ```
    fn start(&mut self, callback: Arc<dyn SourceCallback>);
    fn stop(&mut self);
    fn clone_box(&self) -> Box<dyn Source>;

    /// Phase 2 validation: Verify connectivity and external resource availability
    ///
    /// This method is called during application initialization (Phase 2) to validate
    /// that external systems are reachable and properly configured.
    ///
    /// **Fail-Fast Principle**: Application should NOT start if transports are not ready.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default - sources that don't need external validation can use this.
    /// Sources with external dependencies (Kafka, HTTP, etc.) MUST override this method.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - External system is reachable and properly configured
    /// * `Err(EventFluxError)` - Validation failed, application should not start
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Kafka source validates broker connectivity
    /// fn validate_connectivity(&self) -> Result<(), EventFluxError> {
    ///     // 1. Validate brokers are reachable
    ///     let metadata = self.consumer.fetch_metadata(None, Duration::from_secs(10))?;
    ///
    ///     // 2. Validate topic exists
    ///     if !metadata.topics().iter().any(|t| t.name() == self.topic) {
    ///         return Err(EventFluxError::configuration(
    ///             format!("Topic '{}' does not exist", self.topic)
    ///         ));
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn validate_connectivity(&self) -> Result<(), crate::core::exception::EventFluxError> {
        Ok(()) // Default: no validation needed
    }

    /// Set the DLQ junction for error routing
    ///
    /// This method allows setting the DLQ junction after source creation,
    /// enabling DLQ strategy when the factory creates a source before the DLQ
    /// junction is available. The stream_initializer wires it after creation.
    ///
    /// Sources that support DLQ error handling should override this method.
    /// The default implementation is a no-op for sources that don't support DLQ.
    ///
    /// # Arguments
    /// * `_junction` - The InputHandler for the DLQ stream
    fn set_error_dlq_junction(&mut self, _junction: Arc<Mutex<InputHandler>>) {
        // Default: no-op for sources that don't support DLQ
    }
}

impl Clone for Box<dyn Source> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Adapter that connects the Source architecture (produces bytes) with SourceMapper and InputHandler
///
/// This adapter implements the clean architecture flow:
/// ```text
/// Source → bytes → SourceCallback::on_data() → SourceMapper → Events → InputHandler
/// ```
///
/// The adapter receives raw bytes from sources, uses the mapper to parse them into Events,
/// and delivers the Events to the InputHandler for processing.
#[derive(Debug)]
pub struct SourceCallbackAdapter {
    mapper: Arc<Mutex<Box<dyn SourceMapper>>>,
    handler: Arc<Mutex<InputHandler>>,
}

impl SourceCallbackAdapter {
    pub fn new(
        mapper: Arc<Mutex<Box<dyn SourceMapper>>>,
        handler: Arc<Mutex<InputHandler>>,
    ) -> Self {
        Self { mapper, handler }
    }
}

impl SourceCallback for SourceCallbackAdapter {
    fn on_data(&self, data: &[u8]) -> Result<(), EventFluxError> {
        // Transform bytes → Events via mapper
        // Mapper can now report parsing errors instead of silently dropping data
        let events = self.mapper.lock().unwrap().map(data)?;

        // Deliver Events to InputHandler
        for event in events.iter() {
            self.handler
                .lock()
                .unwrap()
                .send_single_event(event.clone())
                .map_err(|e| {
                    log::error!("[SourceCallbackAdapter] Failed to send event: {}", e);
                    EventFluxError::app_runtime(format!("Failed to send event: {}", e))
                })?;
        }

        Ok(())
    }
}
