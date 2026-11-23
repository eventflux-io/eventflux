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
//! Connects to a WebSocket endpoint and reads its messages into the EventFlux pipeline.
//!
//! ## Architecture
//!
//! The Source trait is blocking, so the worker thread runs its own tokio
//! runtime for the async WebSocket client.
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

use super::{Source, SourceCallback, SourceWorker};
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
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::http::Request, tungstenite::protocol::Message, MaybeTlsStream,
    WebSocketStream,
};

/// Configuration for WebSocket source
#[derive(Debug, Clone)]
pub struct WebSocketSourceConfig {
    /// WebSocket URL, starting with ws:// or wss://
    pub url: String,
    /// Reconnect after a disconnect or a failed connect. Default true.
    pub reconnect: bool,
    /// First reconnect delay in ms. Doubles after each failed connect. Default 1000.
    pub reconnect_delay_ms: u64,
    /// Largest reconnect delay in ms. Default 30000.
    pub reconnect_max_delay_ms: u64,
    /// Most connect attempts in a row. Any negative value means no limit.
    /// Default -1.
    pub reconnect_max_attempts: i32,
    /// Extra handshake headers, such as Authorization. Values are hidden in
    /// Debug, since they may carry tokens.
    pub headers: HashMap<String, crate::core::stream::connector_util::Redacted>,
    /// WebSocket subprotocol to ask for, sent as `Sec-WebSocket-Protocol`
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
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when `websocket.url` is missing or does not start with
    /// ws:// or wss://, or a bool or number does not parse.
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let mut config = Self::default();

        config.url = properties
            .get("websocket.url")
            .ok_or("Missing required property: websocket.url")?
            .clone();

        if !config.url.starts_with("ws://") && !config.url.starts_with("wss://") {
            return Err(format!(
                "Invalid WebSocket URL: must start with ws:// or wss://, got: {}",
                config.url
            ));
        }

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

        if let Some(subprotocol) = properties.get("websocket.subprotocol") {
            config.subprotocol = Some(subprotocol.clone());
        }

        // Extra headers come from websocket.headers.<Name> keys
        config.headers =
            crate::core::stream::connector_util::parse_prefixed(properties, "websocket.headers.")
                .into_iter()
                .map(|(name, value)| (name, value.into()))
                .collect();

        Ok(config)
    }
}

/// WebSocket source that connects to an endpoint and receives its messages.
///
/// - Runs its own tokio runtime on the worker thread for the async client
/// - Reconnects on its own, doubling the wait after each failed connect
/// - Uses the `error.*` handling strategies
#[derive(Debug)]
pub struct WebSocketSource {
    config: WebSocketSourceConfig,
    /// Runs the worker thread. Starts it, signals it, and joins it on stop.
    worker: SourceWorker,
    /// Optional error handling context
    error_ctx: Option<SourceErrorContext>,
}

impl WebSocketSource {
    /// Creates a WebSocket source with no error handling
    pub fn new(config: WebSocketSourceConfig) -> Self {
        Self {
            config,
            worker: SourceWorker::new("WebSocketSource"),
            error_ctx: None,
        }
    }

    /// Returns true when the handshake needs a built request, because there are
    /// extra headers or a subprotocol
    fn needs_custom_request(config: &WebSocketSourceConfig) -> bool {
        !config.headers.is_empty() || config.subprotocol.is_some()
    }

    /// Builds the handshake request with the configured headers and subprotocol.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the URL or a header is invalid.
    fn build_request(config: &WebSocketSourceConfig) -> Result<Request<()>, EventFluxError> {
        let mut request = Request::builder().uri(&config.url);

        for (name, value) in &config.headers {
            request = request.header(name.as_str(), value.expose());
        }

        if let Some(ref subprotocol) = config.subprotocol {
            request = request.header("Sec-WebSocket-Protocol", subprotocol.as_str());
        }

        request.body(()).map_err(|e| {
            EventFluxError::configuration(format!("Failed to build WebSocket request: {}", e))
        })
    }

    /// Creates a WebSocket source from properties.
    ///
    /// # Required Properties
    /// - `websocket.url`: WebSocket URL (ws:// or wss://)
    ///
    /// # Optional Properties
    /// - `websocket.reconnect`: reconnect on its own (default true)
    /// - `websocket.reconnect.delay.ms`: first reconnect delay in ms (default 1000)
    /// - `websocket.reconnect.max.delay.ms`: largest reconnect delay in ms (default 30000)
    /// - `websocket.reconnect.max.attempts`: most attempts, -1 for no limit (default -1)
    /// - `websocket.headers.*`: extra headers, such as `websocket.headers.Authorization`
    /// - `websocket.subprotocol`: WebSocket subprotocol to ask for
    /// - `error.*`: error handling properties (see SourceErrorContext)
    ///
    /// # Errors
    ///
    /// Returns an error when the WebSocket config or the error config is invalid.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let config = WebSocketSourceConfig::from_properties(properties)?;

        // Error handling is optional. Only `error.*` keys are passed on.
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
            worker: SourceWorker::new("WebSocketSource"),
            error_ctx,
        })
    }

    /// Connects to the WebSocket, retrying when reconnect is on.
    ///
    /// The wait starts at `reconnect_delay_ms` and doubles after each failure,
    /// up to `reconnect_max_delay_ms`. The wait does not wake early on stop.
    ///
    /// # Errors
    ///
    /// Returns an error when the source was stopped, the request cannot be
    /// built, or a connect fails and no retry is left.
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

            // Use the URL as is when there are no extra headers, else build a request
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

                    // Double the wait, up to the max
                    delay = (delay * 2).min(config.reconnect_max_delay_ms);
                }
            }
        }
    }

    /// Connects, reads messages until the connection ends, and reconnects when
    /// reconnect is on. Returns when stopped or when it gives up.
    async fn message_loop(
        config: WebSocketSourceConfig,
        running: Arc<AtomicBool>,
        callback: Arc<dyn SourceCallback>,
        mut error_ctx: Option<SourceErrorContext>,
    ) {
        'outer: while running.load(Ordering::SeqCst) {
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

            loop {
                if !running.load(Ordering::SeqCst) {
                    break 'outer;
                }

                // Wait at most 100 ms so the running flag is checked often
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
                                    break; // leave the inner loop to reconnect
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
                            break; // leave the inner loop to reconnect
                        } else {
                            break 'outer;
                        }
                    }
                    Ok(None) => {
                        log::info!("[WebSocketSource] Connection closed by server");
                        if config.reconnect {
                            break; // leave the inner loop to reconnect
                        } else {
                            break 'outer;
                        }
                    }
                    Err(_) => {
                        // Timed out, so loop to check the running flag
                        continue;
                    }
                }
            }

            // Wait reconnect_delay_ms before trying to connect again
            if running.load(Ordering::SeqCst) && config.reconnect {
                tokio::time::sleep(Duration::from_millis(config.reconnect_delay_ms)).await;
            }
        }

        log::info!("[WebSocketSource] Message loop ended");
    }

    /// Handles one WebSocket message. Text and binary payloads are delivered,
    /// a ping gets a pong, and a close frame asks for a reconnect.
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
                if let Err(e) = write.send(Message::Pong(data)).await {
                    log::warn!("[WebSocketSource] Failed to send pong: {}", e);
                }
                MessageResult::Continue
            }
            Message::Pong(_) => {
                // A pong means the connection is alive
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
                // Raw frames are not normally seen when reading, so ignore them
                MessageResult::Continue
            }
        }
    }

    /// Passes data to the callback and applies the error strategy on failure.
    ///
    /// Retries while the strategy says Retry. Returns Stop on Fail or when
    /// cancelled, and Continue after success, drop, or DLQ.
    fn deliver_data(
        data: &[u8],
        callback: &Arc<dyn SourceCallback>,
        error_ctx: &mut Option<SourceErrorContext>,
    ) -> MessageResult {
        loop {
            match callback.on_data(data) {
                Ok(()) => {
                    if let Some(ctx) = error_ctx {
                        ctx.reset_errors();
                    }
                    return MessageResult::Continue;
                }
                Err(e) => {
                    // Wrap the raw bytes in an event so the DLQ has something to carry
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
                            // handle_error_with_action has already waited
                            continue;
                        }
                        ErrorAction::Fail => {
                            log::error!("[WebSocketSource] Unrecoverable error, stopping");
                            return MessageResult::Stop;
                        }
                        ErrorAction::Cancelled => {
                            // Stop was asked for during a retry. WebSocket has no
                            // ack to undo, so just stop at once.
                            return MessageResult::Stop;
                        }
                        _ => {
                            // Drop or DLQ, so go on with the next message
                            return MessageResult::Continue;
                        }
                    }
                }
            }
        }
    }
}

/// What the read loop should do after a message
enum MessageResult {
    Continue,
    Reconnect,
    Stop,
}

impl Clone for WebSocketSource {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            worker: self.worker.clone(), // the clone is a new worker that has not started
            // New context with the same strategy and DLQ, and runtime counters reset
            error_ctx: self.error_ctx.as_ref().map(SourceErrorContext::fresh),
        }
    }
}

impl Source for WebSocketSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        let config = self.config.clone();

        // The thread gets a new copy, so self keeps its config for a later restart
        let mut error_ctx = self.error_ctx.as_ref().map(SourceErrorContext::fresh);

        self.worker.start(move |running| {
            if let Some(ctx) = &mut error_ctx {
                ctx.bind_cancellation(Arc::clone(&running));
            }
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
        // A worker stuck in a connect attempt or a reconnect wait longer than
        // the grace period is left running on its own
        self.worker.stop();
    }

    fn clone_box(&self) -> Box<dyn Source> {
        Box::new(self.clone())
    }

    fn set_error_dlq_junction(
        &mut self,
        stream_name: &str,
        junction: Arc<Mutex<InputHandler>>,
    ) -> Result<(), EventFluxError> {
        // With no error.strategy the source drops failures, so there is no DLQ
        // to connect
        if let Some(ref mut ctx) = self.error_ctx {
            ctx.set_dlq_junction(stream_name, junction);
        }
        Ok(())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        let config = self.config.clone();
        let needs_custom = Self::needs_custom_request(&config);

        // Build a request only when there are extra headers or a subprotocol
        let request = if needs_custom {
            Some(Self::build_request(&config)?)
        } else {
            None
        };
        let url = config.url.clone();

        let validate_future = async move {
            log::info!("[WebSocketSource] Validating connectivity to {}", url);

            // Give up after 10 s
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

                    // Close the test connection cleanly
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
// WebSocket Source Factory
// ============================================================================

/// Factory for WebSocket sources.
///
/// Registered with EventFluxContext and used to build WebSocket sources from
/// the SQL WITH clause config.
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
        // WebSocket keys plus the shared error handling keys owned by
        // SourceErrorContext. Joined once, on first use.
        static PARAMS: std::sync::LazyLock<Vec<&'static str>> = std::sync::LazyLock::new(|| {
            [
                &[
                    "websocket.reconnect",
                    "websocket.reconnect.delay.ms",
                    "websocket.reconnect.max.delay.ms",
                    "websocket.reconnect.max.attempts",
                    "websocket.subprotocol",
                    // websocket.headers.<Name> keys are also accepted
                ][..],
                crate::core::error::source_support::SOURCE_ERROR_PARAMETERS,
            ]
            .concat()
        });
        &PARAMS
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Source>, EventFluxError> {
        // The DLQ junction starts as None. The runtime calls
        // set_error_dlq_junction() when it attaches the source, which also sets
        // the stream name. Until then the URL names the stream in error messages.
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
            Some(&"Bearer token123".into())
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("token123"), "leaked: {debug}");
        assert!(debug.contains("Authorization"), "names stay visible");
        assert_eq!(config.headers.get("X-Custom"), Some(&"custom-value".into()));
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
        assert!(cloned.error_ctx.is_none()); // the source had none to clone
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

    // Shutdown (stop before start, double stop, join on stop) is covered by the
    // SourceWorker tests in source/mod.rs. WebSocketSource leaves start and
    // stop fully to SourceWorker.
}
