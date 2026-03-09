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

//! # WebSocket Sink
//!
//! Publishes formatted event data to WebSocket endpoints.
//!
//! ## Architecture
//!
//! ```text
//! Events → SinkMapper → bytes → WebSocketSink::publish() → WebSocket Server
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! let mut properties = HashMap::new();
//! properties.insert("websocket.url".to_string(), "wss://example.com/events".to_string());
//!
//! let sink = WebSocketSink::from_properties(&properties)?;
//! sink.start();
//! sink.publish(b"{\"event\": \"test\"}")?;
//! sink.stop();
//! ```

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::http::Request, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};

/// Message type for WebSocket sink
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// Send messages as text (UTF-8)
    Text,
    /// Send messages as binary
    Binary,
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::Text
    }
}

impl MessageType {
    /// Parse from string
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "text" => Ok(MessageType::Text),
            "binary" => Ok(MessageType::Binary),
            _ => Err(format!(
                "Invalid message type '{}': expected 'text' or 'binary'",
                s
            )),
        }
    }
}

/// Configuration for WebSocket sink
#[derive(Debug, Clone)]
pub struct WebSocketSinkConfig {
    /// WebSocket URL (ws:// or wss://)
    pub url: String,
    /// Enable auto-reconnect on disconnect (default: true)
    pub reconnect: bool,
    /// Initial reconnect delay in milliseconds (default: 1000)
    pub reconnect_delay_ms: u64,
    /// Maximum reconnect delay in milliseconds (default: 30000)
    pub reconnect_max_delay_ms: u64,
    /// Maximum reconnect attempts (-1 = unlimited, default: 3)
    pub reconnect_max_attempts: i32,
    /// Message type: text or binary (default: text)
    pub message_type: MessageType,
    /// Custom headers for connection (e.g., Authorization)
    pub headers: HashMap<String, String>,
    /// WebSocket subprotocol for negotiation
    pub subprotocol: Option<String>,
}

impl Default for WebSocketSinkConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            reconnect: true,
            reconnect_delay_ms: 1000,
            reconnect_max_delay_ms: 30000,
            reconnect_max_attempts: 3,
            message_type: MessageType::Text,
            headers: HashMap::new(),
            subprotocol: None,
        }
    }
}

impl WebSocketSinkConfig {
    /// Build configuration from properties HashMap
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        // Required: url
        config.url = properties
            .get("websocket.url")
            .ok_or("Missing required property: websocket.url")?
            .clone();

        // Validate URL scheme
        if !config.url.starts_with("ws://") && !config.url.starts_with("wss://") {
            return Err(format!(
                "Invalid WebSocket URL: must start with ws:// or wss://, got: {}",
                config.url
            ));
        }

        // Optional properties
        if let Some(reconnect) = properties.get("websocket.reconnect") {
            config.reconnect = reconnect
                .parse()
                .map_err(|e| format!("Invalid websocket.reconnect: {}", e))?;
        }

        if let Some(delay) = properties.get("websocket.reconnect.delay.ms") {
            config.reconnect_delay_ms = delay
                .parse()
                .map_err(|e| format!("Invalid websocket.reconnect.delay.ms: {}", e))?;
        }

        if let Some(max_delay) = properties.get("websocket.reconnect.max.delay.ms") {
            config.reconnect_max_delay_ms = max_delay
                .parse()
                .map_err(|e| format!("Invalid websocket.reconnect.max.delay.ms: {}", e))?;
        }

        if let Some(max_attempts) = properties.get("websocket.reconnect.max.attempts") {
            config.reconnect_max_attempts = max_attempts
                .parse()
                .map_err(|e| format!("Invalid websocket.reconnect.max.attempts: {}", e))?;
        }

        if let Some(msg_type) = properties.get("websocket.message.type") {
            config.message_type = MessageType::from_str(msg_type)?;
        }

        // Parse subprotocol
        if let Some(subprotocol) = properties.get("websocket.subprotocol") {
            config.subprotocol = Some(subprotocol.clone());
        }

        // Parse custom headers (websocket.headers.*)
        for (key, value) in properties {
            if let Some(header_name) = key.strip_prefix("websocket.headers.") {
                config
                    .headers
                    .insert(header_name.to_string(), value.clone());
            }
        }

        Ok(config)
    }
}

/// Internal state for WebSocket connection
struct WebSocketConnectionState {
    write: futures::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    _read: futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl Debug for WebSocketConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketConnectionState")
            .field("connected", &true)
            .finish()
    }
}

/// WebSocket sink that publishes messages to an endpoint
///
/// This implementation:
/// - Maintains a persistent WebSocket connection in `start()`
/// - Publishes messages synchronously via internal tokio runtime
/// - Supports configurable message types (text/binary)
/// - Auto-reconnects on publish failures if enabled
#[derive(Debug)]
pub struct WebSocketSink {
    /// Configuration
    config: WebSocketSinkConfig,
    /// Connection state (established in start())
    state: Arc<Mutex<Option<WebSocketConnectionState>>>,
    /// Tokio runtime for async operations (owned runtime when created by us)
    runtime: Arc<Mutex<Option<tokio::runtime::Runtime>>>,
    /// Runtime handle for async operations (stored when started inside existing runtime)
    runtime_handle: Arc<Mutex<Option<tokio::runtime::Handle>>>,
}

impl WebSocketSink {
    /// Create a new WebSocket sink
    pub fn new(config: WebSocketSinkConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            runtime_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Build an HTTP request with configured headers and subprotocol
    fn build_request(config: &WebSocketSinkConfig) -> Result<Request<()>, EventFluxError> {
        let mut request = Request::builder().uri(&config.url);

        // Add custom headers
        for (name, value) in &config.headers {
            request = request.header(name.as_str(), value.as_str());
        }

        // Add subprotocol header if configured
        if let Some(ref subprotocol) = config.subprotocol {
            request = request.header("Sec-WebSocket-Protocol", subprotocol.as_str());
        }

        request.body(()).map_err(|e| {
            EventFluxError::configuration(format!("Failed to build WebSocket request: {}", e))
        })
    }

    /// Create a WebSocket sink from properties
    ///
    /// # Required Properties
    /// - `websocket.url`: WebSocket URL (ws:// or wss://)
    ///
    /// # Optional Properties
    /// - `websocket.reconnect`: Enable auto-reconnect (default: true)
    /// - `websocket.reconnect.delay.ms`: Initial reconnect delay (default: 1000)
    /// - `websocket.reconnect.max.delay.ms`: Max reconnect delay (default: 30000)
    /// - `websocket.message.type`: Message type - "text" or "binary" (default: text)
    /// - `websocket.headers.*`: Custom headers (e.g., `websocket.headers.Authorization`)
    /// - `websocket.subprotocol`: WebSocket subprotocol for negotiation
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let config = WebSocketSinkConfig::from_properties(properties)?;
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

    /// Connect to WebSocket server
    async fn connect(
        config: &WebSocketSinkConfig,
    ) -> Result<WebSocketConnectionState, EventFluxError> {
        log::info!("[WebSocketSink] Connecting to {}", config.url);

        // Build request with headers and subprotocol
        let request = Self::build_request(config)?;

        let (ws_stream, response) =
            connect_async(request)
                .await
                .map_err(|e| EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to connect to WebSocket {}: {}", config.url, e),
                    source: Some(Box::new(e)),
                })?;

        log::info!(
            "[WebSocketSink] Connected to {} (status: {})",
            config.url,
            response.status()
        );

        let (write, read) = ws_stream.split();

        Ok(WebSocketConnectionState { write, _read: read })
    }

    /// Connect with retry logic
    async fn connect_with_retry(
        config: &WebSocketSinkConfig,
    ) -> Result<WebSocketConnectionState, EventFluxError> {
        let mut attempts = 0;
        let mut delay = config.reconnect_delay_ms;

        loop {
            match Self::connect(config).await {
                Ok(state) => return Ok(state),
                Err(e) => {
                    attempts += 1;
                    // Check if we should retry:
                    // - reconnect must be enabled
                    // - max_attempts < 0 means unlimited, otherwise check limit
                    let should_retry = config.reconnect
                        && (config.reconnect_max_attempts < 0
                            || attempts < config.reconnect_max_attempts);

                    if !should_retry {
                        return Err(e);
                    }

                    log::warn!(
                        "[WebSocketSink] Connection failed, retrying in {}ms (attempt {}): {}",
                        delay,
                        attempts,
                        e
                    );

                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    delay = (delay * 2).min(config.reconnect_max_delay_ms);
                }
            }
        }
    }
}

impl Clone for WebSocketSink {
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

impl Sink for WebSocketSink {
    fn start(&self) {
        let config = self.config.clone();
        let state = Arc::clone(&self.state);

        // Create async connection logic
        let connect_future = async move {
            let conn_state = Self::connect(&config).await?;

            // Store connection state
            let mut state_guard = state.lock().unwrap();
            *state_guard = Some(conn_state);

            log::info!("[WebSocketSink] Ready to publish to {}", config.url);

            Ok::<(), EventFluxError>(())
        };

        // Check if we're already inside a tokio runtime
        let result = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Store the handle for later use in publish()/stop()
            {
                let mut handle_guard = self.runtime_handle.lock().unwrap();
                *handle_guard = Some(handle.clone());
            }
            // Use block_in_place to avoid nested runtime panic
            tokio::task::block_in_place(|| handle.block_on(connect_future))
        } else {
            // Not inside a runtime - create one and use block_on
            if let Err(e) = self.get_runtime() {
                log::error!("[WebSocketSink] Failed to initialize runtime: {}", e);
                return;
            }

            let runtime_guard = self.runtime.lock().unwrap();
            let rt = match runtime_guard.as_ref() {
                Some(rt) => rt,
                None => {
                    log::error!("[WebSocketSink] Runtime not initialized");
                    return;
                }
            };
            rt.block_on(connect_future)
        };

        if let Err(e) = result {
            log::error!("[WebSocketSink] Start failed: {}", e);
        }
    }

    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let message = match self.config.message_type {
            MessageType::Text => {
                // Convert bytes to string for text message
                let text = String::from_utf8(payload.to_vec()).map_err(|e| {
                    EventFluxError::app_runtime(format!(
                        "Failed to convert payload to UTF-8 string: {}",
                        e
                    ))
                })?;
                Message::Text(text)
            }
            MessageType::Binary => Message::Binary(payload.to_vec()),
        };

        let state = Arc::clone(&self.state);
        let config = self.config.clone();

        // Define the async publish operation
        let publish_future =
            async move {
                // Try to send on existing connection
                {
                    let mut state_guard = state.lock().unwrap();
                    if let Some(conn_state) = state_guard.as_mut() {
                        match conn_state.write.send(message.clone()).await {
                            Ok(()) => return Ok(()),
                            Err(e) => {
                                log::warn!("[WebSocketSink] Send failed, will reconnect: {}", e);
                                // Clear the state to trigger reconnection
                                *state_guard = None;
                            }
                        }
                    }
                }

                // Reconnect if enabled
                if config.reconnect {
                    log::info!("[WebSocketSink] Reconnecting...");
                    let mut conn_state = Self::connect_with_retry(&config).await?;

                    // Send the message
                    conn_state.write.send(message).await.map_err(|e| {
                        EventFluxError::app_runtime(format!("Publish failed: {}", e))
                    })?;

                    // Store the new connection
                    let mut state_guard = state.lock().unwrap();
                    *state_guard = Some(conn_state);

                    Ok(())
                } else {
                    Err(EventFluxError::ConnectionUnavailable {
                        message: "WebSocket not connected and reconnect is disabled".to_string(),
                        source: None,
                    })
                }
            };

        // Check if we're already inside a tokio runtime
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
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

            // Fall back to stored handle
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
        log::info!("[WebSocketSink] Stopping...");

        let state = Arc::clone(&self.state);

        // Create the shutdown future
        let shutdown_future = async move {
            let mut state_guard = state.lock().unwrap();
            if let Some(mut conn_state) = state_guard.take() {
                // Send close frame
                if let Err(e) = conn_state.write.close().await {
                    log::warn!("[WebSocketSink] Error closing connection: {}", e);
                }
            }
        };

        // Try to use runtime/handle in order of preference
        let runtime_guard = self.runtime.lock().unwrap();
        if let Some(rt) = runtime_guard.as_ref() {
            rt.block_on(shutdown_future);
        } else {
            drop(runtime_guard);

            // Try current runtime context
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(shutdown_future);
                });
            } else {
                // Fall back to stored handle
                let handle_guard = self.runtime_handle.lock().unwrap();
                if let Some(handle) = handle_guard.as_ref() {
                    handle.block_on(shutdown_future);
                } else {
                    log::warn!("[WebSocketSink] No runtime available for graceful shutdown");
                }
            }
        }

        log::info!("[WebSocketSink] Stopped");
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();

        // Build request with headers and subprotocol (before async block)
        let request = Self::build_request(&config)?;
        let url = config.url.clone();

        // Create the async validation logic
        let validate_future = async move {
            log::info!("[WebSocketSink] Validating connectivity to {}", url);

            // Try to connect with a timeout
            let connect_result =
                tokio::time::timeout(Duration::from_secs(10), connect_async(request)).await;

            match connect_result {
                Ok(Ok((ws_stream, response))) => {
                    log::info!(
                        "[WebSocketSink] Validation successful (status: {})",
                        response.status()
                    );

                    // Close the connection gracefully
                    let (mut write, _read) = ws_stream.split();
                    let _ = write.close().await;

                    Ok(())
                }
                Ok(Err(e)) => Err(EventFluxError::ConnectionUnavailable {
                    message: format!("Failed to connect to WebSocket {}: {}", url, e),
                    source: Some(Box::new(e)),
                }),
                Err(_) => Err(EventFluxError::ConnectionUnavailable {
                    message: format!("Connection to WebSocket {} timed out after 10 seconds", url),
                    source: None,
                }),
            }
        };

        // Check if we're already inside a tokio runtime
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
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
// WebSocket Sink Factory
// ============================================================================

/// Factory for creating WebSocket sink instances.
///
/// This factory is registered with EventFluxContext and used to create
/// WebSocket sinks from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct WebSocketSinkFactory;

impl SinkFactory for WebSocketSinkFactory {
    fn name(&self) -> &'static str {
        "websocket"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "text", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["websocket.url"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "websocket.reconnect",
            "websocket.reconnect.delay.ms",
            "websocket.reconnect.max.delay.ms",
            "websocket.reconnect.max.attempts",
            "websocket.message.type",
            "websocket.subprotocol",
            // Headers are dynamic: websocket.headers.*
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let parsed = WebSocketSinkConfig::from_properties(config)
            .map_err(|e| EventFluxError::configuration(e))?;

        let sink = WebSocketSink::new(parsed);

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
        props.insert(
            "websocket.url".to_string(),
            "wss://example.com/events".to_string(),
        );

        let config = WebSocketSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "wss://example.com/events");
        assert!(config.reconnect);
        assert_eq!(config.reconnect_delay_ms, 1000);
        assert_eq!(config.reconnect_max_delay_ms, 30000);
        assert_eq!(config.reconnect_max_attempts, 3);
        assert_eq!(config.message_type, MessageType::Text);
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "wss://example.com/events".to_string(),
        );
        props.insert("websocket.reconnect".to_string(), "false".to_string());
        props.insert(
            "websocket.reconnect.delay.ms".to_string(),
            "2000".to_string(),
        );
        props.insert(
            "websocket.reconnect.max.delay.ms".to_string(),
            "60000".to_string(),
        );
        props.insert(
            "websocket.reconnect.max.attempts".to_string(),
            "10".to_string(),
        );
        props.insert("websocket.message.type".to_string(), "binary".to_string());
        props.insert(
            "websocket.subprotocol".to_string(),
            "graphql-ws".to_string(),
        );
        props.insert(
            "websocket.headers.Authorization".to_string(),
            "Bearer token".to_string(),
        );

        let config = WebSocketSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "wss://example.com/events");
        assert!(!config.reconnect);
        assert_eq!(config.reconnect_delay_ms, 2000);
        assert_eq!(config.reconnect_max_delay_ms, 60000);
        assert_eq!(config.reconnect_max_attempts, 10);
        assert_eq!(config.message_type, MessageType::Binary);
        assert_eq!(config.subprotocol, Some("graphql-ws".to_string()));
        assert_eq!(
            config.headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
    }

    #[test]
    fn test_config_missing_url() {
        let props = HashMap::new();

        let result = WebSocketSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("websocket.url"));
    }

    #[test]
    fn test_config_invalid_url_scheme() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "http://example.com".to_string(),
        );

        let result = WebSocketSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must start with ws://"));
    }

    #[test]
    fn test_config_ws_url() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "ws://localhost:8080/ws".to_string(),
        );

        let config = WebSocketSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "ws://localhost:8080/ws");
    }

    #[test]
    fn test_config_invalid_message_type() {
        let mut props = HashMap::new();
        props.insert("websocket.url".to_string(), "wss://example.com".to_string());
        props.insert("websocket.message.type".to_string(), "invalid".to_string());

        let result = WebSocketSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid message type"));
    }

    #[test]
    fn test_config_unlimited_max_attempts() {
        let mut props = HashMap::new();
        props.insert("websocket.url".to_string(), "wss://example.com".to_string());
        props.insert(
            "websocket.reconnect.max.attempts".to_string(),
            "-1".to_string(),
        );

        let config = WebSocketSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.reconnect_max_attempts, -1);
    }

    #[test]
    fn test_config_invalid_max_attempts() {
        let mut props = HashMap::new();
        props.insert("websocket.url".to_string(), "wss://example.com".to_string());
        props.insert(
            "websocket.reconnect.max.attempts".to_string(),
            "invalid".to_string(),
        );

        let result = WebSocketSinkConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("websocket.reconnect.max.attempts"));
    }

    #[test]
    fn test_message_type_parsing() {
        assert_eq!(MessageType::from_str("text").unwrap(), MessageType::Text);
        assert_eq!(MessageType::from_str("TEXT").unwrap(), MessageType::Text);
        assert_eq!(
            MessageType::from_str("binary").unwrap(),
            MessageType::Binary
        );
        assert_eq!(
            MessageType::from_str("BINARY").unwrap(),
            MessageType::Binary
        );
        assert!(MessageType::from_str("invalid").is_err());
    }

    #[test]
    fn test_sink_from_properties() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "wss://example.com/events".to_string(),
        );

        let sink = WebSocketSink::from_properties(&props);
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        assert_eq!(sink.config.url, "wss://example.com/events");
    }

    #[test]
    fn test_sink_clone() {
        let config = WebSocketSinkConfig {
            url: "wss://example.com/events".to_string(),
            reconnect: false,
            message_type: MessageType::Binary,
            ..Default::default()
        };
        let sink = WebSocketSink::new(config);
        let cloned = sink.clone();

        assert_eq!(cloned.config.url, "wss://example.com/events");
        assert!(!cloned.config.reconnect);
        assert_eq!(cloned.config.message_type, MessageType::Binary);
        // Cloned sink should not have connection state
        assert!(cloned.state.lock().unwrap().is_none());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = WebSocketSinkFactory;
        assert_eq!(factory.name(), "websocket");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"text"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"websocket.url"));
    }

    #[test]
    fn test_factory_create() {
        let factory = WebSocketSinkFactory;
        let mut config = HashMap::new();
        config.insert(
            "websocket.url".to_string(),
            "wss://example.com/events".to_string(),
        );

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_missing_url() {
        let factory = WebSocketSinkFactory;
        let config = HashMap::new();

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = WebSocketSinkFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "websocket");
    }
}
