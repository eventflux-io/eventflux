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

use std::fmt::Debug;

/// Sink trait for publishing formatted event data to external systems
///
/// Sinks receive formatted payloads (JSON, CSV, XML, or binary) from mappers
/// and publish them to external transports (HTTP, Kafka, files, etc.).
///
/// # Architecture
///
/// The event flow is:
/// ```text
/// StreamJunction → Events → SinkMapper → bytes → Sink::publish() → External System
/// ```
///
/// All sinks work with byte payloads, ensuring a clean separation between:
/// - **Formatting** (handled by SinkMapper)
/// - **Transport** (handled by Sink)
pub trait Sink: Debug + Send + Sync {
    /// Publish formatted payload to external system
    ///
    /// This method receives pre-formatted bytes from a mapper and publishes
    /// them to the external system (HTTP endpoint, Kafka topic, file, etc.).
    ///
    /// # Arguments
    ///
    /// * `payload` - Formatted event data (JSON, CSV, XML, or binary format)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Payload published successfully
    /// * `Err(EventFluxError)` - Publishing failed (connection issues, format errors, etc.)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // HTTP sink publishes JSON to REST API
    /// fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
    ///     let client = reqwest::blocking::Client::new();
    ///     client.post(&self.url)
    ///         .body(payload.to_vec())
    ///         .header("Content-Type", "application/json")
    ///         .send()?;
    ///     Ok(())
    /// }
    /// ```
    fn publish(&self, payload: &[u8]) -> Result<(), crate::core::exception::EventFluxError>;

    /// Start the sink (connect, initialize resources)
    fn start(&self) {}

    /// Stop the sink (disconnect, cleanup resources)
    fn stop(&self) {}

    /// Clone the sink into a boxed trait object
    fn clone_box(&self) -> Box<dyn Sink>;

    /// Phase 2 validation: Verify connectivity and external resource availability
    ///
    /// This method is called during application initialization (Phase 2) to validate
    /// that external systems are reachable and properly configured.
    ///
    /// **Fail-Fast Principle**: Application should NOT start if transports are not ready.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default - sinks that don't need external validation can use this.
    /// Sinks with external dependencies (Kafka, HTTP, databases) MUST override this method.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - External system is reachable and properly configured
    /// * `Err(EventFluxError)` - Validation failed, application should not start
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // HTTP sink validates URL reachability
    /// fn validate_connectivity(&self) -> Result<(), EventFluxError> {
    ///     let client = reqwest::blocking::Client::new();
    ///     let response = client.head(&self.url)
    ///         .timeout(Duration::from_secs(10))
    ///         .send()?;
    ///
    ///     if !response.status().is_success() && !response.status().is_client_error() {
    ///         return Err(EventFluxError::configuration(
    ///             format!("HTTP endpoint '{}' not reachable: {}", self.url, response.status())
    ///         ));
    ///     }
    ///     Ok(())
    /// }
    /// ```
    fn validate_connectivity(&self) -> Result<(), crate::core::exception::EventFluxError> {
        Ok(()) // Default: no validation needed
    }
}

impl Clone for Box<dyn Sink> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
