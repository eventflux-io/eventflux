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

/// Publishes formatted event data to an outside system.
///
/// Sinks get formatted payloads (JSON, CSV, or binary) from mappers and send
/// them over a transport (HTTP, Kafka, files, and so on).
///
/// # Architecture
///
/// The event flow is
/// ```text
/// StreamJunction → Events → SinkMapper → bytes → Sink::publish() → External System
/// ```
///
/// All sinks work with byte payloads, which keeps two jobs apart
/// - **Formatting**, done by SinkMapper
/// - **Transport**, done by Sink
pub trait Sink: Debug + Send + Sync {
    /// Publishes one formatted payload to the outside system.
    ///
    /// Gets bytes already formatted by a mapper and sends them on, to an HTTP
    /// endpoint, a Kafka topic, a file, and so on.
    ///
    /// # Arguments
    ///
    /// * `payload` - Formatted event data (JSON, CSV, or binary)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The payload was published
    /// * `Err(EventFluxError)` - Publishing failed, such as a connection problem
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

    /// Starts the sink, such as connecting and setting up resources. Does
    /// nothing by default.
    fn start(&self) {}

    /// Stops the sink, such as disconnecting and freeing resources. Does
    /// nothing by default.
    fn stop(&self) {}

    /// Clones the sink into a boxed trait object
    fn clone_box(&self) -> Box<dyn Sink>;

    /// Phase 2 check that outside systems can be reached and are set up right.
    ///
    /// Called during app startup (Phase 2). The app should NOT start when a
    /// transport is not ready, so problems show up early.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok. Sinks with no outside system to check can keep it. Sinks
    /// that depend on one (Kafka, HTTP, databases) MUST override it.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The outside system can be reached and is set up right
    /// * `Err(EventFluxError)` - The check failed and the app should not start
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
        Ok(())
    }
}

impl Clone for Box<dyn Sink> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
