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
//! Publishes formatted event data to a RabbitMQ exchange.
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
    /// RabbitMQ broker host name
    pub host: String,
    /// RabbitMQ broker port. Default 5672.
    pub port: u16,
    /// Virtual host. Default "/".
    pub vhost: String,
    /// Exchange to publish to
    pub exchange: String,
    /// Routing key for messages. Default "".
    pub routing_key: String,
    /// User name for login. Default "guest".
    pub username: String,
    /// Password for login. Default "guest". Hidden in Debug output. Call
    /// `.expose()` only where it is used.
    pub password: crate::core::stream::connector_util::Redacted,
    /// Mark messages as persistent (delivery mode 2). Default true.
    pub persistent: bool,
    /// Content type header for messages. Default none, so no header is sent.
    pub content_type: Option<String>,
    /// Declare the exchange (as durable) if it is missing. Default false.
    pub declare_exchange: bool,
    /// Exchange type used when declaring. Default "direct".
    pub exchange_type: String,
    /// Set the mandatory flag when publishing. Default false.
    /// When true, the broker returns messages it cannot route instead of
    /// silently dropping them. This sink does not read those returns.
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
            password: "guest".into(),
            persistent: true,
            content_type: None,
            declare_exchange: false,
            exchange_type: "direct".to_string(),
            mandatory: false,
        }
    }
}

impl RabbitMQSinkConfig {
    /// Builds the AMQP URI. The result holds the raw password, so never log it.
    pub fn amqp_uri(&self) -> String {
        // The default vhost "/" must be URL encoded, which gives "%2F"
        let encoded_vhost = urlencoding::encode(&self.vhost);
        format!(
            "amqp://{}:{}@{}:{}/{}",
            self.username,
            self.password.expose(),
            self.host,
            self.port,
            encoded_vhost
        )
    }

    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when `rabbitmq.host` or `rabbitmq.exchange` is missing,
    /// or a port or bool value does not parse.
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        config.host = properties
            .get("rabbitmq.host")
            .ok_or("Missing required property: rabbitmq.host")?
            .clone();

        config.exchange = properties
            .get("rabbitmq.exchange")
            .ok_or("Missing required property: rabbitmq.exchange")?
            .clone();

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
            config.password = password.clone().into();
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

    /// Turns `exchange_type` into a lapin::ExchangeKind. Any value other than
    /// "fanout", "topic", or "headers" gives Direct.
    pub fn exchange_kind(&self) -> lapin::ExchangeKind {
        match self.exchange_type.as_str() {
            "fanout" => lapin::ExchangeKind::Fanout,
            "topic" => lapin::ExchangeKind::Topic,
            "headers" => lapin::ExchangeKind::Headers,
            _ => lapin::ExchangeKind::Direct,
        }
    }
}

/// The open RabbitMQ connection and channel
struct RabbitMQConnectionState {
    connection: lapin::Connection,
    channel: lapin::Channel,
}

/// RabbitMQ sink that publishes messages to an exchange.
///
/// - Opens one connection and channel in `start()` and keeps them open
/// - `publish()` blocks until the async publish finishes, using a tokio runtime
/// - Persistence, routing key, and exchange declaration are configurable
#[derive(Debug)]
pub struct RabbitMQSink {
    config: RabbitMQSinkConfig,
    /// Connection state, set in start()
    state: Arc<Mutex<Option<RabbitMQConnectionState>>>,
    /// Tokio runtime this sink created, when start() ran outside a runtime
    runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    /// Handle to the outside runtime, when start() ran inside one
    runtime_handle: Arc<Mutex<Option<tokio::runtime::Handle>>>,
}

impl RabbitMQSink {
    /// Creates a RabbitMQ sink. `start()` connects.
    pub fn new(config: RabbitMQSinkConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            runtime_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Creates a RabbitMQ sink from properties.
    ///
    /// # Required Properties
    /// - `rabbitmq.host`: broker host name
    /// - `rabbitmq.exchange`: exchange to publish to
    ///
    /// # Optional Properties
    /// - `rabbitmq.port`: broker port (default 5672)
    /// - `rabbitmq.vhost`: virtual host (default "/")
    /// - `rabbitmq.username`: user name (default "guest")
    /// - `rabbitmq.password`: password (default "guest")
    /// - `rabbitmq.routing.key`: routing key (default "")
    /// - `rabbitmq.persistent`: persistent messages (default true)
    /// - `rabbitmq.content.type`: Content-Type header
    /// - `rabbitmq.declare.exchange`: declare the exchange if missing (default false)
    /// - `rabbitmq.exchange.type`: exchange type when declaring (default "direct")
    /// - `rabbitmq.mandatory`: set the mandatory flag (default false)
    ///
    /// # Errors
    ///
    /// Returns an error when the config is invalid.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let config = RabbitMQSinkConfig::from_properties(properties)?;
        Ok(Self::new(config))
    }

    /// Creates this sink's own tokio runtime if it does not have one yet
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

// Written by hand because the lapin types inside do not implement Debug
impl std::fmt::Debug for RabbitMQConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RabbitMQConnectionState")
            .field("connected", &true)
            .finish()
    }
}

impl Clone for RabbitMQSink {
    fn clone(&self) -> Self {
        // The clone has the same config but no connection or runtime.
        // start() connects it.
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

        let connect_future = async move {
            let uri = config.amqp_uri();
            log::info!("[RabbitMQSink] Connecting to {}", config.host);

            let connection =
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
                    .await
                    .map_err(|e| EventFluxError::ConnectionUnavailable {
                        message: format!("Failed to connect to RabbitMQ: {}", e),
                        source: Some(Box::new(e)),
                    })?;

            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

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

        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Already inside a tokio runtime. Keep the handle so publish() and
            // stop() can use it later from threads outside the runtime.
            {
                let mut handle_guard = self.runtime_handle.lock().unwrap();
                *handle_guard = Some(handle.clone());
            }
            // block_in_place avoids the panic from starting a runtime inside a
            // runtime. It needs the multi thread runtime.
            tokio::task::block_in_place(|| handle.block_on(connect_future))
        } else {
            // Not inside a runtime, so create this sink's own runtime and keep
            // it for later calls
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

        // A failed start is only logged. publish() then fails with "not started".
        if let Err(e) = result {
            log::error!("[RabbitMQSink] Start failed: {}", e);
        }
    }

    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let mut properties = lapin::BasicProperties::default();

        if self.config.persistent {
            properties = properties.with_delivery_mode(2); // persistent
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

        let publish_future = async move {
            // Clone the channel, which is cheap since it is Arc based inside,
            // and drop the guard before any .await
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

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Inside a runtime. block_in_place avoids the panic from starting a
            // runtime inside a runtime.
            tokio::task::block_in_place(|| handle.block_on(publish_future))
        } else {
            // Not inside a runtime. Try this sink's own runtime first.
            let runtime_guard = self
                .runtime
                .lock()
                .map_err(|_| EventFluxError::app_runtime("Runtime lock poisoned".to_string()))?;

            if let Some(rt) = runtime_guard.as_ref() {
                return rt.block_on(publish_future);
            }
            drop(runtime_guard);

            // Then the kept handle, set when start() ran inside a runtime
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

        let state = Arc::clone(&self.state);
        let shutdown_future = async move {
            // Take the connection state and drop the guard before any .await
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

        // Pick a runtime in this order: this sink's own runtime, the current
        // runtime, then the kept handle
        let runtime_guard = self.runtime.lock().unwrap();
        if let Some(rt) = runtime_guard.as_ref() {
            rt.block_on(shutdown_future);
        } else {
            drop(runtime_guard);

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(shutdown_future);
                });
            } else {
                // The kept handle, set when start() ran inside a runtime
                let handle_guard = self.runtime_handle.lock().unwrap();
                if let Some(handle) = handle_guard.as_ref() {
                    handle.block_on(shutdown_future);
                } else {
                    // No runtime, so the connection state is only dropped with the sink
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

        let validate_future = async move {
            let uri = config.amqp_uri();

            // Give up after 10 s so a broker that cannot be reached does not hang startup
            let connection = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                lapin::Connection::connect(&uri, lapin::ConnectionProperties::default()),
            )
            .await
            .map_err(|_| EventFluxError::ConnectionUnavailable {
                message: format!(
                    "Failed to connect to RabbitMQ at {}:{}: Connection timeout after 10s",
                    config.host, config.port
                ),
                source: None,
            })?
            .map_err(|e| EventFluxError::ConnectionUnavailable {
                message: format!(
                    "Failed to connect to RabbitMQ at {}:{}: {}",
                    config.host, config.port, e
                ),
                source: Some(Box::new(e)),
            })?;

            let channel = connection.create_channel().await.map_err(|e| {
                EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to create channel: {}", e),
                    source: Some(Box::new(e)),
                }
            })?;

            // How the exchange is checked depends on declare_exchange
            // - true: really declare it, to check permissions
            // - false: a passive declare, to check that it exists
            if config.declare_exchange {
                // Declare it the same way start() does, to fail early on
                // missing permissions, a bad type, and so on. Declaring is safe
                // to repeat when the exchange already exists with the same config.
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
                // A passive declare checks that the exchange exists. It is
                // treated as possibly failing with PRECONDITION_FAILED when the
                // exchange exists with other properties, such as durable=true
                // against our durable=false default. That still counts as
                // success, since the exchange exists and can take messages.
                let passive_result = channel
                    .exchange_declare(
                        &config.exchange,
                        config.exchange_kind(),
                        lapin::options::ExchangeDeclareOptions {
                            passive: true, // only check that it exists
                            ..Default::default()
                        },
                        lapin::types::FieldTable::default(),
                    )
                    .await;

                match passive_result {
                    Ok(_) => {
                        // The exchange exists, so the check passes
                    }
                    Err(e) => {
                        // PRECONDITION_FAILED means it exists with other properties
                        let is_precondition_failed = e.to_string().contains("PRECONDITION_FAILED")
                            || e.to_string().contains("406");

                        if is_precondition_failed {
                            // The exchange exists with other properties, such as
                            // durable. The check passes, since it can still take
                            // messages.
                            log::debug!(
                                "[RabbitMQSink] Exchange '{}' exists with different properties, validation passes",
                                config.exchange
                            );
                        } else {
                            // The exchange does not exist
                            return Err(EventFluxError::configuration(format!(
                                "Exchange '{}' does not exist (expected type '{}'): {}",
                                config.exchange, config.exchange_type, e
                            )));
                        }
                    }
                }
            }

            let _ = channel.close(200, "Validation complete").await;
            let _ = connection.close(200, "Validation complete").await;

            Ok::<(), EventFluxError>(())
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Already inside a tokio runtime. block_in_place avoids the panic
            // from starting a runtime inside a runtime. It needs the multi
            // thread runtime.
            tokio::task::block_in_place(|| handle.block_on(validate_future))
        } else {
            // Not inside a runtime, so make a short lived one
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

/// Factory for RabbitMQ sinks.
///
/// Registered with EventFluxContext and used to build RabbitMQ sinks from the
/// SQL WITH clause config.
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
        let parsed =
            RabbitMQSinkConfig::from_properties(config).map_err(EventFluxError::configuration)?;

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
        assert_eq!(config.password.expose(), "secret");
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
            password: "guest".into(),
            ..Default::default()
        };

        // The default vhost "/" is URL encoded as "%2F"
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
            password: "secret".into(),
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
        // Every supported exchange type
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
        // The cloned sink must have no connection state
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
