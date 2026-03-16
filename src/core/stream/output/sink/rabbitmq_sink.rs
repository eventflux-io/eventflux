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

//! # RabbitMQ Sink
//!
//! Publishes formatted event data to RabbitMQ exchanges.
//!
//! ## Architecture
//!
//! ```text
//! Events → SinkMapper → bytes → RabbitMQSink::publish() → RabbitMQ Exchange
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! let mut properties = HashMap::new();
//! properties.insert("rabbitmq.host".to_string(), "localhost".to_string());
//! properties.insert("rabbitmq.exchange".to_string(), "events".to_string());
//!
//! let sink = RabbitMQSink::from_properties(&properties)?;
//! sink.start();
//! sink.publish(b"{\"event\": \"test\"}")?;
//! sink.stop();
//! ```

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Configuration for RabbitMQ sink
#[derive(Debug, Clone)]
pub struct RabbitMQSinkConfig {
    /// RabbitMQ broker hostname
    pub host: String,
    /// RabbitMQ broker port (default: 5672)
    pub port: u16,
    /// Virtual host (default: "/")
    pub vhost: String,
    /// Exchange name to publish to
    pub exchange: String,
    /// Routing key for messages (default: "")
    pub routing_key: String,
    /// Username for authentication (default: "guest")
    pub username: String,
    /// Password for authentication (default: "guest")
    pub password: String,
    /// Whether messages should be persistent (default: true)
    pub persistent: bool,
    /// Content type header for messages (default: "application/octet-stream")
    pub content_type: Option<String>,
    /// Whether to declare the exchange if it doesn't exist (default: false)
    pub declare_exchange: bool,
    /// Exchange type when declaring (default: "direct")
    pub exchange_type: String,
    /// Whether to use mandatory flag for publishing (default: false)
    /// When true, unroutable messages will trigger a return instead of being silently dropped
    pub mandatory: bool,
}

impl Default for RabbitMQSinkConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            exchange: String::new(),
            routing_key: String::new(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            persistent: true,
            content_type: None,
            declare_exchange: false,
            exchange_type: "direct".to_string(),
            mandatory: false,
        }
    }
}

impl RabbitMQSinkConfig {
    /// Build AMQP URI from configuration
    pub fn amqp_uri(&self) -> String {
        // The default vhost "/" must be URL-encoded as "%2f"
        let encoded_vhost = urlencoding::encode(&self.vhost);
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, encoded_vhost
        )
    }

    /// Parse configuration from properties HashMap
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        // Required: host
        config.host = properties
            .get("rabbitmq.host")
            .ok_or("Missing required property: rabbitmq.host")?
            .clone();

        // Required: exchange
        config.exchange = properties
            .get("rabbitmq.exchange")
            .ok_or("Missing required property: rabbitmq.exchange")?
            .clone();

        // Optional properties
        if let Some(port) = properties.get("rabbitmq.port") {
            config.port = port
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.port: {}", e))?;
        }

        if let Some(vhost) = properties.get("rabbitmq.vhost") {
            config.vhost = vhost.clone();
        }

        if let Some(username) = properties.get("rabbitmq.username") {
            config.username = username.clone();
        }

        if let Some(password) = properties.get("rabbitmq.password") {
            config.password = password.clone();
        }

        if let Some(routing_key) = properties.get("rabbitmq.routing.key") {
            config.routing_key = routing_key.clone();
        }

        if let Some(persistent) = properties.get("rabbitmq.persistent") {
            config.persistent = persistent
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.persistent: {}", e))?;
        }

        if let Some(content_type) = properties.get("rabbitmq.content.type") {
            config.content_type = Some(content_type.clone());
        }

        if let Some(declare) = properties.get("rabbitmq.declare.exchange") {
            config.declare_exchange = declare
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.declare.exchange: {}", e))?;
        }

        if let Some(exchange_type) = properties.get("rabbitmq.exchange.type") {
            config.exchange_type = exchange_type.clone();
        }

        if let Some(mandatory) = properties.get("rabbitmq.mandatory") {
            config.mandatory = mandatory
                .parse()
                .map_err(|e| format!("Invalid rabbitmq.mandatory: {}", e))?;
        }

        Ok(config)
    }

    /// Convert the exchange_type string to lapin::ExchangeKind
    pub fn exchange_kind(&self) -> lapin::ExchangeKind {
        match self.exchange_type.as_str() {
            "fanout" => lapin::ExchangeKind::Fanout,
            "topic" => lapin::ExchangeKind::Topic,
            "headers" => lapin::ExchangeKind::Headers,
            _ => lapin::ExchangeKind::Direct,
        }
    }
}

/// Internal state for RabbitMQ connection
struct RabbitMQConnectionState {
    connection: lapin::Connection,
    channel: lapin::Channel,
}

/// RabbitMQ sink that publishes messages to an exchange
///
/// This implementation:
/// - Maintains a persistent connection/channel in `start()`
/// - Publishes messages synchronously via internal tokio runtime
/// - Supports configurable persistence, routing keys, and exchange declaration
#[derive(Debug)]
pub struct RabbitMQSink {
    /// Configuration
    config: RabbitMQSinkConfig,
    /// Connection state (established in start())
    state: Arc<Mutex<Option<RabbitMQConnectionState>>>,
    /// Tokio runtime for async operations (owned runtime when created by us)
    runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    /// Runtime handle for async operations (stored when started inside existing runtime)
    runtime_handle: Arc<Mutex<Option<tokio::runtime::Handle>>>,
}

impl RabbitMQSink {
    /// Create a new RabbitMQ sink
    pub fn new(config: RabbitMQSinkConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            runtime_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a RabbitMQ sink from properties
    ///
    /// # Required Properties
    /// - `rabbitmq.host`: Broker hostname
    /// - `rabbitmq.exchange`: Exchange name to publish to
    ///
    /// # Optional Properties
    /// - `rabbitmq.port`: Broker port (default: 5672)
    /// - `rabbitmq.vhost`: Virtual host (default: "/")
    /// - `rabbitmq.username`: Username (default: "guest")
    /// - `rabbitmq.password`: Password (default: "guest")
    /// - `rabbitmq.routing.key`: Routing key (default: "")
    /// - `rabbitmq.persistent`: Message persistence (default: true)
    /// - `rabbitmq.content.type`: Content-Type header
    /// - `rabbitmq.declare.exchange`: Declare exchange if missing (default: false)
    /// - `rabbitmq.exchange.type`: Exchange type when declaring (default: "direct")
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let config = RabbitMQSinkConfig::from_properties(properties)?;
        Ok(Self::new(config))
    }

    /// Get or create tokio runtime
    fn get_runtime(&self) -> Result<(), EventFluxError> {
        let mut runtime_guard = self.runtime.lock().unwrap();
        if runtime_guard.is_none() {
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                EventFluxError::app_runtime(format!("Failed to create tokio runtime: {}", e))
            })?;
            *runtime_guard = Some(rt);
        }
        Ok(())
    }
}

// Custom Debug impl since RabbitMQConnectionState doesn't implement Debug
impl std::fmt::Debug for RabbitMQConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitMQConnectionState")
            .field("connected", &true)
            .finish()
    }
}

impl Clone for RabbitMQSink {
    fn clone(&self) -> Self {
        // Clone creates a new sink with same config but no connection
        // Connection will be established on start()
        Self {
            config: self.config.clone(),
            state: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            runtime_handle: Arc::new(Mutex::new(None)),
        }
    }
}

impl Sink for RabbitMQSink {
    fn start(&self) {
        let config = self.config.clone();
        let state = Arc::clone(&self.state);

        // Create async connection logic
        let connect_future = async move {
            let uri = config.amqp_uri();
            log::info!("[RabbitMQSink] Connecting to {}", config.host);

            // Connect to RabbitMQ
            let connection =
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
                    .await
                    .map_err(|e| EventFluxError::ConnectionUnavailable {
                        message: format!("Failed to connect to RabbitMQ: {}", e),
                        source: Some(Box::new(e)),
                    })?;

            // Create channel
            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

            // Declare exchange if configured
            if config.declare_exchange {
                channel
                    .exchange_declare(
                        &config.exchange,
                        config.exchange_kind(),
                        lapin::options::ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        lapin::types::FieldTable::default(),
                    )
                    .await
                    .map_err(|e| {
                        EventFluxError::configuration(format!(
                            "Failed to declare exchange '{}': {}",
                            config.exchange, e
                        ))
                    })?;
            }

            // Store connection state
            let mut state_guard = state.lock().unwrap();
            *state_guard = Some(RabbitMQConnectionState {
                connection,
                channel,
            });

            log::info!(
                "[RabbitMQSink] Connected, publishing to exchange '{}'",
                config.exchange
            );

            Ok::<(), EventFluxError>(())
        };

        // Check if we're already inside a tokio runtime
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Store the handle for later use in publish()/stop() from non-async threads
            {
                let mut handle_guard = self.runtime_handle.lock().unwrap();
                *handle_guard = Some(handle.clone());
            }
            // Use block_in_place to avoid nested runtime panic
            tokio::task::block_in_place(|| handle.block_on(connect_future))
        } else {
            // Not inside a runtime - create one and use block_on
            // Also initialize the stored runtime for later use
            if let Err(e) = self.get_runtime() {
                log::error!("[RabbitMQSink] Failed to initialize runtime: {}", e);
                return;
            }

            let runtime_guard = self.runtime.lock().unwrap();
            let rt = match runtime_guard.as_ref() {
                Some(rt) => rt,
                None => {
                    log::error!("[RabbitMQSink] Runtime not initialized");
                    return;
                }
            };
            rt.block_on(connect_future)
        };

        if let Err(e) = result {
            log::error!("[RabbitMQSink] Start failed: {}", e);
        }
    }

    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        // Build publish properties
        let mut properties = lapin::BasicProperties::default();

        if self.config.persistent {
            properties = properties.with_delivery_mode(2); // Persistent
        }

        if let Some(ref content_type) = self.config.content_type {
            properties = properties.with_content_type(content_type.as_str().into());
        }

        let exchange = self.config.exchange.clone();
        let routing_key = self.config.routing_key.clone();
        let payload_owned = payload.to_vec();
        let state = Arc::clone(&self.state);
        let publish_options = lapin::options::BasicPublishOptions {
            mandatory: self.config.mandatory,
            ..Default::default()
        };

        // Define the async publish operation
        let publish_future = async move {
            // Clone the channel (internally Arc-based) and drop the guard before .await
            let channel = {
                let state_guard = state
                    .lock()
                    .map_err(|_| EventFluxError::app_runtime("State lock poisoned".to_string()))?;
                let conn_state =
                    state_guard
                        .as_ref()
                        .ok_or_else(|| EventFluxError::ConnectionUnavailable {
                            message: "RabbitMQ sink not started - call start() first".to_string(),
                            source: None,
                        })?;
                conn_state.channel.clone()
            };

            channel
                .basic_publish(
                    &exchange,
                    &routing_key,
                    publish_options,
                    &payload_owned,
                    properties,
                )
                .await
                .map_err(|e| EventFluxError::app_runtime(format!("Publish failed: {}", e)))?
                .await
                .map_err(|e| {
                    EventFluxError::app_runtime(format!("Publish confirmation failed: {}", e))
                })?;

            Ok::<(), EventFluxError>(())
        };

        // Check if we're already inside a tokio runtime
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // We're inside a runtime - use block_in_place to avoid nested runtime panic
            tokio::task::block_in_place(|| handle.block_on(publish_future))
        } else {
            // Not inside a runtime - try our owned runtime first
            let runtime_guard = self
                .runtime
                .lock()
                .map_err(|_| EventFluxError::app_runtime("Runtime lock poisoned".to_string()))?;

            if let Some(rt) = runtime_guard.as_ref() {
                return rt.block_on(publish_future);
            }
            drop(runtime_guard);

            // Fall back to stored handle (set when start() was called inside a runtime)
            let handle_guard = self
                .runtime_handle
                .lock()
                .map_err(|_| EventFluxError::app_runtime("Handle lock poisoned".to_string()))?;

            if let Some(handle) = handle_guard.as_ref() {
                return handle.block_on(publish_future);
            }

            Err(EventFluxError::app_runtime(
                "Tokio runtime not initialized - call start() first".to_string(),
            ))
        }
    }

    fn stop(&self) {
        log::info!("[RabbitMQSink] Stopping...");

        // Create the shutdown future
        let state = Arc::clone(&self.state);
        let shutdown_future = async move {
            // Take the connection state and drop the guard before .await
            let conn_state = {
                let mut state_guard = state.lock().unwrap();
                state_guard.take()
            };
            if let Some(conn_state) = conn_state {
                if let Err(e) = conn_state.channel.close(200, "Normal shutdown").await {
                    log::warn!("[RabbitMQSink] Error closing channel: {}", e);
                }
                if let Err(e) = conn_state.connection.close(200, "Normal shutdown").await {
                    log::warn!("[RabbitMQSink] Error closing connection: {}", e);
                }
            }
        };

        // Try to use runtime/handle in order of preference
        let runtime_guard = self.runtime.lock().unwrap();
        if let Some(rt) = runtime_guard.as_ref() {
            // Use our owned runtime
            rt.block_on(shutdown_future);
        } else {
            drop(runtime_guard);

            // Try current runtime context
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(shutdown_future);
                });
            } else {
                // Fall back to stored handle (set when start() was called inside a runtime)
                let handle_guard = self.runtime_handle.lock().unwrap();
                if let Some(handle) = handle_guard.as_ref() {
                    handle.block_on(shutdown_future);
                } else {
                    // No runtime available - connection state will be dropped when sink is dropped
                    log::warn!("[RabbitMQSink] No runtime available for graceful shutdown");
                }
            }
        }

        log::info!("[RabbitMQSink] Stopped");
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();

        // Create the async validation logic
        let validate_future = async move {
            let uri = config.amqp_uri();

            // Try to connect
            let connection =
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
                    .await
                    .map_err(|e| EventFluxError::ConnectionUnavailable {
                        message: format!(
                            "Failed to connect to RabbitMQ at {}:{}: {}",
                            config.host, config.port, e
                        ),
                        source: Some(Box::new(e)),
                    })?;

            // Create channel and check exchange exists (if not declaring)
            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

            // Validate exchange access - behavior depends on declare_exchange setting:
            // - If declare_exchange=true: attempt actual declaration to verify permissions
            // - If declare_exchange=false: passive declare to verify exchange exists
            if config.declare_exchange {
                // Try to declare exchange (mirrors start() behavior) to fail fast on
                // permission issues, invalid type, etc. Exchange declaration is idempotent
                // so this won't cause issues if the exchange already exists with same config.
                channel
                    .exchange_declare(
                        &config.exchange,
                        config.exchange_kind(),
                        lapin::options::ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        lapin::types::FieldTable::default(),
                    )
                    .await
                    .map_err(|e| {
                        EventFluxError::configuration(format!(
                            "Failed to declare exchange '{}' (type '{}'): {}. \
                            Check permissions and that no conflicting exchange exists.",
                            config.exchange, config.exchange_type, e
                        ))
                    })?;
            } else {
                // Passive declare to check exchange exists
                // Note: Passive declare may fail with PRECONDITION_FAILED if exchange exists
                // with different properties (e.g., durable=true vs our durable=false default).
                // This is still a success case - the exchange exists and we can publish to it.
                let passive_result = channel
                    .exchange_declare(
                        &config.exchange,
                        config.exchange_kind(),
                        lapin::options::ExchangeDeclareOptions {
                            passive: true, // Just check existence
                            ..Default::default()
                        },
                        lapin::types::FieldTable::default(),
                    )
                    .await;

                match passive_result {
                    Ok(_) => {
                        // Exchange exists with matching properties - validation passes
                    }
                    Err(e) => {
                        // Check if PRECONDITION_FAILED (exchange exists with different properties)
                        let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                            || e.to_string().contains("406");

                        if is_precondition_failed {
                            // Exchange exists but with different properties (e.g., durable)
                            // This is fine - validation passes, we can still publish to it
                            log::debug!(
                                "[RabbitMQSink] Exchange '{}' exists with different properties, validation passes",
                                config.exchange
                            );
                        } else {
                            // Exchange truly doesn't exist
                            return Err(EventFluxError::configuration(format!(
                                "Exchange '{}' does not exist (expected type '{}'): {}",
                                config.exchange, config.exchange_type, e
                            )));
                        }
                    }
                }
            }

            // Clean up
            let _ = channel.close(200, "Validation complete").await;
            let _ = connection.close(200, "Validation complete").await;

            Ok::<(), EventFluxError>(())
        };

        // Check if we're already inside a tokio runtime
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Use block_in_place to avoid nested runtime panic
            tokio::task::block_in_place(|| handle.block_on(validate_future))
        } else {
            // Not inside a runtime - create a temporary one
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                EventFluxError::configuration(format!("Failed to create tokio runtime: {}", e))
            })?;
            rt.block_on(validate_future)
        }
    }
}

// ============================================================================
// RabbitMQ Sink Factory
// ============================================================================

/// Factory for creating RabbitMQ sink instances.
///
/// This factory is registered with EventFluxContext and used to create
/// RabbitMQ sinks from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct RabbitMQSinkFactory;

impl SinkFactory for RabbitMQSinkFactory {
    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["rabbitmq.host", "rabbitmq.exchange"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "rabbitmq.port",
            "rabbitmq.vhost",
            "rabbitmq.username",
            "rabbitmq.password",
            "rabbitmq.routing.key",
            "rabbitmq.persistent",
            "rabbitmq.content.type",
            "rabbitmq.declare.exchange",
            "rabbitmq.exchange.type",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        // Parse and validate configuration
        let parsed =
            RabbitMQSinkConfig::from_properties(config).map_err(EventFluxError::configuration)?;

        // Create RabbitMQ sink
        let sink = RabbitMQSink::new(parsed);

        Ok(Box::new(sink))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_properties_required_only() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "myhost".to_string());
        props.insert("rabbitmq.exchange".to_string(), "myexchange".to_string());

        let config = RabbitMQSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.host, "myhost");
        assert_eq!(config.exchange, "myexchange");
        assert_eq!(config.port, 5672);
        assert_eq!(config.routing_key, "");
        assert!(config.persistent);
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = HashMap::new();
        props.insert(
            "rabbitmq.host".to_string(),
            "broker.example.com".to_string(),
        );
        props.insert("rabbitmq.exchange".to_string(), "events".to_string());
        props.insert("rabbitmq.port".to_string(), "5673".to_string());
        props.insert("rabbitmq.vhost".to_string(), "production".to_string());
        props.insert("rabbitmq.username".to_string(), "admin".to_string());
        props.insert("rabbitmq.password".to_string(), "secret".to_string());
        props.insert("rabbitmq.routing.key".to_string(), "mykey".to_string());
        props.insert("rabbitmq.persistent".to_string(), "false".to_string());
        props.insert(
            "rabbitmq.content.type".to_string(),
            "application/json".to_string(),
        );
        props.insert("rabbitmq.declare.exchange".to_string(), "true".to_string());
        props.insert("rabbitmq.exchange.type".to_string(), "topic".to_string());

        let config = RabbitMQSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.host, "broker.example.com");
        assert_eq!(config.exchange, "events");
        assert_eq!(config.port, 5673);
        assert_eq!(config.vhost, "production");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.routing_key, "mykey");
        assert!(!config.persistent);
        assert_eq!(config.content_type, Some("application/json".to_string()));
        assert!(config.declare_exchange);
        assert_eq!(config.exchange_type, "topic");
    }

    #[test]
    fn test_config_missing_host() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.exchange".to_string(), "myexchange".to_string());

        let result = RabbitMQSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rabbitmq.host"));
    }

    #[test]
    fn test_config_missing_exchange() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "localhost".to_string());

        let result = RabbitMQSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("rabbitmq.exchange"));
    }

    #[test]
    fn test_amqp_uri_default_vhost() {
        let config = RabbitMQSinkConfig {
            host: "localhost".to_string(),
            port: 5672,
            vhost: "/".to_string(),
            exchange: "test".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            ..Default::default()
        };

        // Default vhost "/" is URL-encoded as "%2f"
        assert_eq!(config.amqp_uri(), "amqp://guest:guest@localhost:5672/%2F");
    }

    #[test]
    fn test_amqp_uri_custom_vhost() {
        let config = RabbitMQSinkConfig {
            host: "broker.example.com".to_string(),
            port: 5673,
            vhost: "my-vhost".to_string(),
            exchange: "test".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            ..Default::default()
        };

        assert_eq!(
            config.amqp_uri(),
            "amqp://admin:secret@broker.example.com:5673/my-vhost"
        );
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_exchange_kind_conversion() {
        // Test all supported exchange types
        let mut config = RabbitMQSinkConfig::default();

        config.exchange_type = "direct".to_string();
        assert!(matches!(
            config.exchange_kind(),
            lapin::ExchangeKind::Direct
        ));

        config.exchange_type = "fanout".to_string();
        assert!(matches!(
            config.exchange_kind(),
            lapin::ExchangeKind::Fanout
        ));

        config.exchange_type = "topic".to_string();
        assert!(matches!(config.exchange_kind(), lapin::ExchangeKind::Topic));

        config.exchange_type = "headers".to_string();
        assert!(matches!(
            config.exchange_kind(),
            lapin::ExchangeKind::Headers
        ));

        // Unknown types default to direct
        config.exchange_type = "unknown".to_string();
        assert!(matches!(
            config.exchange_kind(),
            lapin::ExchangeKind::Direct
        ));
    }

    #[test]
    fn test_sink_from_properties() {
        let mut props = HashMap::new();
        props.insert("rabbitmq.host".to_string(), "localhost".to_string());
        props.insert("rabbitmq.exchange".to_string(), "test-exchange".to_string());

        let sink = RabbitMQSink::from_properties(&props);
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        assert_eq!(sink.config.host, "localhost");
        assert_eq!(sink.config.exchange, "test-exchange");
    }

    #[test]
    fn test_sink_clone() {
        let config = RabbitMQSinkConfig {
            host: "localhost".to_string(),
            exchange: "test".to_string(),
            ..Default::default()
        };
        let sink = RabbitMQSink::new(config);
        let cloned = sink.clone();

        assert_eq!(cloned.config.host, "localhost");
        assert_eq!(cloned.config.exchange, "test");
        // Cloned sink should not have connection state
        assert!(cloned.state.lock().unwrap().is_none());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = RabbitMQSinkFactory;
        assert_eq!(factory.name(), "rabbitmq");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"rabbitmq.host"));
        assert!(factory.required_parameters().contains(&"rabbitmq.exchange"));
    }

    #[test]
    fn test_factory_create() {
        let factory = RabbitMQSinkFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());
        config.insert("rabbitmq.exchange".to_string(), "test-exchange".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_missing_host() {
        let factory = RabbitMQSinkFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.exchange".to_string(), "test-exchange".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_missing_exchange() {
        let factory = RabbitMQSinkFactory;
        let mut config = HashMap::new();
        config.insert("rabbitmq.host".to_string(), "localhost".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = RabbitMQSinkFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "rabbitmq");
    }
}
