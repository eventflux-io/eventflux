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

//! # WebSocket Source
//!
//! Connects to WebSocket endpoints and consumes messages for the EventFlux pipeline.
//!
//! ## Architecture
//!
//! Uses synchronous Source trait with internal tokio runtime for async WebSocket operations:
//! ```text
//! WebSocket URL → tokio-tungstenite → bytes → SourceCallback → SourceMapper → Events
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use std::collections::HashMap;
//!
//! let mut properties = HashMap::new();
//! properties.insert("websocket.url".to_string(), "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string());
//!
//! let source = WebSocketSource::from_properties(&properties, None, "TradeStream")?;
//! ```

use super::{Source, SourceCallback};
use crate::core::error::handler::ErrorAction;
use crate::core::error::source_support::{ErrorConfigBuilder, SourceErrorContext};
use crate::core::event::value::AttributeValue;
use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::input::input_handler::InputHandler;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::http::Request, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};

/// Configuration for WebSocket source
#[derive(Debug, Clone)]
pub struct WebSocketSourceConfig {
    /// WebSocket URL (ws:// or wss://)
    pub url: String,
    /// Enable auto-reconnect on disconnect (default: true)
    pub reconnect: bool,
    /// Initial reconnect delay in milliseconds (default: 1000)
    pub reconnect_delay_ms: u64,
    /// Maximum reconnect delay in milliseconds (default: 30000)
    pub reconnect_max_delay_ms: u64,
    /// Maximum reconnect attempts (-1 = unlimited, default: -1)
    pub reconnect_max_attempts: i32,
    /// Custom headers for connection (e.g., Authorization)
    pub headers: HashMap<String, String>,
    /// WebSocket subprotocol for negotiation
    pub subprotocol: Option<String>,
}

impl Default for WebSocketSourceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            reconnect: true,
            reconnect_delay_ms: 1000,
            reconnect_max_delay_ms: 30000,
            reconnect_max_attempts: -1,
            headers: HashMap::new(),
            subprotocol: None,
        }
    }
}

impl WebSocketSourceConfig {
    /// Parse configuration from properties HashMap
    #[allow(clippy::field_reassign_with_default)]
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

/// WebSocket source that connects to an endpoint and receives messages
///
/// This implementation:
/// - Uses sync Source trait with internal tokio runtime for async WebSocket operations
/// - Supports auto-reconnection with exponential backoff
/// - Integrates with M5 error handling context
#[derive(Debug)]
pub struct WebSocketSource {
    /// Configuration
    config: WebSocketSourceConfig,
    /// Running flag for graceful shutdown
    running: Arc<AtomicBool>,
    /// Optional error handling context (M5 integration)
    error_ctx: Option<SourceErrorContext>,
}

impl WebSocketSource {
    /// Create a new WebSocket source without error handling
    pub fn new(config: WebSocketSourceConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None,
        }
    }

    /// Check if we need custom headers for the request
    fn needs_custom_request(config: &WebSocketSourceConfig) -> bool {
        !config.headers.is_empty() || config.subprotocol.is_some()
    }

    /// Build an HTTP request with configured headers and subprotocol
    fn build_request(config: &WebSocketSourceConfig) -> Result<Request<()>, EventFluxError> {
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

    /// Create a WebSocket source from properties
    ///
    /// # Required Properties
    /// - `websocket.url`: WebSocket URL (ws:// or wss://)
    ///
    /// # Optional Properties
    /// - `websocket.reconnect`: Enable auto-reconnect (default: true)
    /// - `websocket.reconnect.delay.ms`: Initial reconnect delay (default: 1000)
    /// - `websocket.reconnect.max.delay.ms`: Max reconnect delay (default: 30000)
    /// - `websocket.reconnect.max.attempts`: Max attempts, -1 = unlimited (default: -1)
    /// - `websocket.headers.*`: Custom headers (e.g., `websocket.headers.Authorization`)
    /// - `websocket.subprotocol`: WebSocket subprotocol for negotiation
    /// - `error.*`: Error handling properties (see SourceErrorContext)
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = WebSocketSourceConfig::from_properties(properties)?;

        // Parse error handling configuration (optional)
        let error_config_builder = ErrorConfigBuilder::from_properties(properties);
        let error_ctx = if error_config_builder.is_configured() {
            use crate::core::config::{FlatConfig, PropertySource};
            let mut flat_config = FlatConfig::new();
            for (key, value) in properties {
                if key.starts_with("error.") {
                    flat_config.set(key.clone(), value.clone(), PropertySource::SqlWith);
                }
            }

            Some(SourceErrorContext::from_config(
                &flat_config,
                dlq_junction,
                stream_name.to_string(),
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            error_ctx,
        })
    }

    /// Connect to WebSocket with retry logic
    async fn connect_with_retry(
        config: &WebSocketSourceConfig,
        running: &Arc<AtomicBool>,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, EventFluxError> {
        let mut attempts = 0;
        let mut delay = config.reconnect_delay_ms;

        loop {
            if !running.load(Ordering::SeqCst) {
                return Err(EventFluxError::app_runtime("Source stopped".to_string()));
            }

            log::info!("[WebSocketSource] Connecting to {}", config.url);

            // Use URL directly when no custom headers, otherwise build request
            let connect_result = if Self::needs_custom_request(config) {
                let request = Self::build_request(config)?;
                connect_async(request).await
            } else {
                connect_async(&config.url).await
            };

            match connect_result {
                Ok((stream, response)) => {
                    log::info!(
                        "[WebSocketSource] Connected to {} (status: {})",
                        config.url,
                        response.status()
                    );
                    return Ok(stream);
                }
                Err(e) => {
                    attempts += 1;
                    let should_retry = config.reconnect
                        && (config.reconnect_max_attempts < 0
                            || attempts < config.reconnect_max_attempts);

                    if !should_retry {
                        return Err(EventFluxError::ConnectionUnavailable {
                            message: format!(
                                "Failed to connect to WebSocket {} after {} attempts: {}",
                                config.url, attempts, e
                            ),
                            source: Some(Box::new(e)),
                        });
                    }

                    log::warn!(
                        "[WebSocketSource] Connection failed, reconnecting in {}ms (attempt {}): {}",
                        delay,
                        attempts,
                        e
                    );

                    tokio::time::sleep(Duration::from_millis(delay)).await;

                    // Exponential backoff with cap
                    delay = (delay * 2).min(config.reconnect_max_delay_ms);
                }
            }
        }
    }

    /// Main message processing loop
    async fn message_loop(
        config: WebSocketSourceConfig,
        running: Arc<AtomicBool>,
        callback: Arc<dyn SourceCallback>,
        mut error_ctx: Option<SourceErrorContext>,
    ) {
        'outer: while running.load(Ordering::SeqCst) {
            // Connect (with retry if enabled)
            let ws_stream = match Self::connect_with_retry(&config, &running).await {
                Ok(stream) => stream,
                Err(e) => {
                    if let Some(ctx) = &mut error_ctx {
                        ctx.handle_error(None, &e);
                    } else {
                        log::error!("[WebSocketSource] {}", e);
                    }
                    break;
                }
            };

            let (mut write, mut read) = ws_stream.split();

            // Message loop
            loop {
                if !running.load(Ordering::SeqCst) {
                    break 'outer;
                }

                // Use timeout to periodically check running flag
                let msg_result =
                    tokio::time::timeout(Duration::from_millis(100), read.next()).await;

                match msg_result {
                    Ok(Some(Ok(message))) => {
                        let process_result =
                            Self::process_message(message, &callback, &mut error_ctx, &mut write)
                                .await;

                        match process_result {
                            MessageResult::Continue => {}
                            MessageResult::Reconnect => {
                                if config.reconnect {
                                    log::info!("[WebSocketSource] Reconnecting...");
                                    break; // Break inner loop to reconnect
                                } else {
                                    log::info!(
                                        "[WebSocketSource] Connection closed, reconnect disabled"
                                    );
                                    break 'outer;
                                }
                            }
                            MessageResult::Stop => {
                                break 'outer;
                            }
                        }
                    }
                    Ok(Some(Err(e))) => {
                        // WebSocket error
                        let err = EventFluxError::ConnectionUnavailable {
                            message: format!("WebSocket error: {}", e),
                            source: Some(Box::new(e)),
                        };

                        if let Some(ctx) = &mut error_ctx {
                            if !ctx.handle_error(None, &err) {
                                break 'outer;
                            }
                        } else {
                            log::error!("[WebSocketSource] {}", err);
                        }

                        if config.reconnect {
                            log::info!("[WebSocketSource] Connection lost, reconnecting...");
                            break; // Break inner loop to reconnect
                        } else {
                            break 'outer;
                        }
                    }
                    Ok(None) => {
                        // Stream ended
                        log::info!("[WebSocketSource] Connection closed by server");
                        if config.reconnect {
                            break; // Break inner loop to reconnect
                        } else {
                            break 'outer;
                        }
                    }
                    Err(_) => {
                        // Timeout - continue loop to check running flag
                        continue;
                    }
                }
            }

            // Small delay before reconnect attempt
            if running.load(Ordering::SeqCst) && config.reconnect {
                tokio::time::sleep(Duration::from_millis(config.reconnect_delay_ms)).await;
            }
        }

        log::info!("[WebSocketSource] Message loop ended");
    }

    /// Process a single WebSocket message
    async fn process_message(
        message: Message,
        callback: &Arc<dyn SourceCallback>,
        error_ctx: &mut Option<SourceErrorContext>,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> MessageResult {
        match message {
            Message::Text(text) => Self::deliver_data(text.as_bytes(), callback, error_ctx),
            Message::Binary(data) => Self::deliver_data(&data, callback, error_ctx),
            Message::Ping(data) => {
                // Respond with Pong
                if let Err(e) = write.send(Message::Pong(data)).await {
                    log::warn!("[WebSocketSource] Failed to send pong: {}", e);
                }
                MessageResult::Continue
            }
            Message::Pong(_) => {
                // Pong received - connection is alive
                log::trace!("[WebSocketSource] Pong received");
                MessageResult::Continue
            }
            Message::Close(frame) => {
                if let Some(cf) = frame {
                    log::info!(
                        "[WebSocketSource] Close frame received: {} {}",
                        cf.code,
                        cf.reason
                    );
                } else {
                    log::info!("[WebSocketSource] Close frame received");
                }
                MessageResult::Reconnect
            }
            Message::Frame(_) => {
                // Raw frame - typically not used
                MessageResult::Continue
            }
        }
    }

    /// Deliver data to callback with error handling
    fn deliver_data(
        data: &[u8],
        callback: &Arc<dyn SourceCallback>,
        error_ctx: &mut Option<SourceErrorContext>,
    ) -> MessageResult {
        loop {
            match callback.on_data(data) {
                Ok(()) => {
                    // Success - reset error counter
                    if let Some(ctx) = error_ctx {
                        ctx.reset_errors();
                    }
                    return MessageResult::Continue;
                }
                Err(e) => {
                    // Create fallback event for DLQ
                    let fallback_event = Event::new_with_data(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0),
                        vec![AttributeValue::Bytes(data.to_vec())],
                    );

                    let action = if let Some(ctx) = error_ctx {
                        ctx.handle_error_with_action(Some(&fallback_event), &e)
                    } else {
                        log::error!("[WebSocketSource] Callback error: {}", e);
                        ErrorAction::Drop
                    };

                    match action {
                        ErrorAction::Retry { .. } => {
                            // Retry - loop continues
                            continue;
                        }
                        ErrorAction::Fail => {
                            log::error!("[WebSocketSource] Unrecoverable error, stopping");
                            return MessageResult::Stop;
                        }
                        _ => {
                            // Drop, DLQ, or other actions - continue processing
                            return MessageResult::Continue;
                        }
                    }
                }
            }
        }
    }
}

/// Result of processing a message
enum MessageResult {
    Continue,
    Reconnect,
    Stop,
}

impl Clone for WebSocketSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            running: Arc::new(AtomicBool::new(false)),
            error_ctx: None, // Error context contains runtime state, not cloneable
        }
    }
}

impl Source for WebSocketSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);
        let config = self.config.clone();

        // Move error_ctx into the thread
        let error_ctx = self.error_ctx.take();

        thread::spawn(move || {
            // Create a tokio runtime for async WebSocket operations
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("[WebSocketSource] Failed to create tokio runtime: {}", e);
                    return;
                }
            };

            rt.block_on(Self::message_loop(config, running, callback, error_ctx));
        });
    }

    fn stop(&mut self) {
        log::info!("[WebSocketSource] Stopping...");
        self.running.store(false, Ordering::SeqCst);
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn set_error_dlq_junction(&mut self, junction: Arc<Mutex<InputHandler>>) {
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(junction);
        }
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();
        let needs_custom = Self::needs_custom_request(&config);

        // Build request only if custom headers are needed
        let request = if needs_custom {
            Some(Self::build_request(&config)?)
        } else {
            None
        };
        let url = config.url.clone();

        // Create the async validation logic
        let validate_future = async move {
            log::info!("[WebSocketSource] Validating connectivity to {}", url);

            // Try to connect with a timeout - use URL directly if no custom headers
            let connect_result = if let Some(req) = request {
                tokio::time::timeout(Duration::from_secs(10), connect_async(req)).await
            } else {
                tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await
            };

            match connect_result {
                Ok(Ok((ws_stream, response))) => {
                    log::info!(
                        "[WebSocketSource] Validation successful (status: {})",
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
// WebSocket Source Factory
// ============================================================================

/// Factory for creating WebSocket source instances.
///
/// This factory is registered with EventFluxContext and used to create
/// WebSocket sources from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct WebSocketSourceFactory;

impl SourceFactory for WebSocketSourceFactory {
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
            "websocket.subprotocol",
            // Headers are dynamic: websocket.headers.*
            // Error handling options
            "error.strategy",
            "error.retry.max-attempts",
            "error.retry.initial-delay-ms",
            "error.retry.max-delay-ms",
            "error.retry.backoff-multiplier",
            "error.dlq.stream",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // Use the URL as the stream identifier for error context
        let stream_name = config
            .get("websocket.url")
            .cloned()
            .unwrap_or_else(|| "websocket-source".to_string());

        let source = WebSocketSource::from_properties(config, None, &stream_name)
            .map_err(EventFluxError::configuration)?;

        Ok(Box::new(source))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
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
            "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        );

        let config = WebSocketSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "wss://stream.binance.com:9443/ws/btcusdt@trade");
        assert!(config.reconnect);
        assert_eq!(config.reconnect_delay_ms, 1000);
        assert_eq!(config.reconnect_max_delay_ms, 30000);
        assert_eq!(config.reconnect_max_attempts, -1);
    }

    #[test]
    fn test_config_from_properties_all_options() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "wss://example.com/stream".to_string(),
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
            "5".to_string(),
        );
        props.insert(
            "websocket.subprotocol".to_string(),
            "graphql-ws".to_string(),
        );
        props.insert(
            "websocket.headers.Authorization".to_string(),
            "Bearer token123".to_string(),
        );
        props.insert(
            "websocket.headers.X-Custom".to_string(),
            "custom-value".to_string(),
        );

        let config = WebSocketSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "wss://example.com/stream");
        assert!(!config.reconnect);
        assert_eq!(config.reconnect_delay_ms, 2000);
        assert_eq!(config.reconnect_max_delay_ms, 60000);
        assert_eq!(config.reconnect_max_attempts, 5);
        assert_eq!(config.subprotocol, Some("graphql-ws".to_string()));
        assert_eq!(
            config.headers.get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
        assert_eq!(
            config.headers.get("X-Custom"),
            Some(&"custom-value".to_string())
        );
    }

    #[test]
    fn test_config_missing_url() {
        let props = HashMap::new();

        let result = WebSocketSourceConfig::from_properties(&props);
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

        let result = WebSocketSourceConfig::from_properties(&props);
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

        let config = WebSocketSourceConfig::from_properties(&props).unwrap();
        assert_eq!(config.url, "ws://localhost:8080/ws");
    }

    #[test]
    fn test_config_invalid_reconnect_delay() {
        let mut props = HashMap::new();
        props.insert("websocket.url".to_string(), "wss://example.com".to_string());
        props.insert(
            "websocket.reconnect.delay.ms".to_string(),
            "invalid".to_string(),
        );

        let result = WebSocketSourceConfig::from_properties(&props);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("websocket.reconnect.delay.ms"));
    }

    #[test]
    fn test_source_from_properties() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        );

        let source = WebSocketSource::from_properties(&props, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert_eq!(
            source.config.url,
            "wss://stream.binance.com:9443/ws/btcusdt@trade"
        );
        assert!(source.error_ctx.is_none());
    }

    #[test]
    fn test_source_from_properties_with_error_handling() {
        let mut props = HashMap::new();
        props.insert(
            "websocket.url".to_string(),
            "wss://example.com/stream".to_string(),
        );
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = WebSocketSource::from_properties(&props, None, "TestStream");
        assert!(source.is_ok());

        let source = source.unwrap();
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_source_clone() {
        let config = WebSocketSourceConfig {
            url: "wss://example.com/stream".to_string(),
            reconnect: false,
            ..Default::default()
        };
        let source = WebSocketSource::new(config);
        let cloned = source.clone();

        assert_eq!(cloned.config.url, "wss://example.com/stream");
        assert!(!cloned.config.reconnect);
        assert!(cloned.error_ctx.is_none()); // Error ctx not cloned
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = WebSocketSourceFactory;
        assert_eq!(factory.name(), "websocket");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"text"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"websocket.url"));
    }

    #[test]
    fn test_factory_create() {
        let factory = WebSocketSourceFactory;
        let mut config = HashMap::new();
        config.insert(
            "websocket.url".to_string(),
            "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
        );

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_missing_url() {
        let factory = WebSocketSourceFactory;
        let config = HashMap::new();

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = WebSocketSourceFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "websocket");
    }

    #[test]
    fn test_factory_create_with_error_handling_config() {
        let factory = WebSocketSourceFactory;
        let mut config = HashMap::new();
        config.insert(
            "websocket.url".to_string(),
            "wss://example.com/stream".to_string(),
        );
        config.insert("error.strategy".to_string(), "retry".to_string());
        config.insert("error.retry.max-attempts".to_string(), "3".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok(), "Factory should accept error.* config");
    }
}
