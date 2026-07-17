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

//! # HTTP Source
//!
//! Two modes, selected by `http.mode`:
//!
//! - **poll**: periodically requests a URL; each 2xx body is one payload
//! - **webhook**: runs an axum listener; each accepted POST body is one payload
//!
//! ## Architecture
//!
//! ```text
//! poll:    SourceWorker thread → ureq request every interval → bytes → SourceCallback → mapper
//! webhook: SourceWorker thread → tokio runtime → axum (concurrent handlers)
//!            → bounded mpsc → delivery thread → SourceCallback → mapper
//! ```
//!
//! The webhook listener handles requests concurrently (bounded by
//! `http.max.concurrent.requests`); payloads are funneled over a bounded
//! channel to a single delivery thread that owns the error context, so a full
//! queue backpressures handlers naturally. A delivery verdict of `Fail`
//! responds 500 and shuts the listener down gracefully.

use super::{sleep_while_running, Source, SourceCallback, SourceWorker};
use crate::core::error::source_support::{
    deliver_with_error_handling, DeliveryVerdict, SourceErrorContext,
};
use crate::core::exception::EventFluxError;
use crate::core::extension::SourceFactory;
use crate::core::stream::connector_util::parse_or;
use crate::core::stream::http_common::{
    build_agent, build_request, constant_time_eq, parse_headers, parse_method, parse_url,
    probe_reachability,
};
use crate::core::stream::input::input_handler::InputHandler;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Poll-mode configuration
#[derive(Debug, Clone)]
pub struct HttpPollConfig {
    /// URL to poll
    pub url: String,
    /// Time between polls (default 5000; interruptible)
    pub interval_ms: u64,
    /// `GET` (default) or `POST` (some APIs poll via POST)
    pub method: String,
    /// Request headers from `http.headers.<Name>`
    pub headers: Vec<(String, String)>,
    /// Per-request timeout (default 30000)
    pub timeout_ms: u64,
}

/// Webhook-mode configuration
#[derive(Debug, Clone)]
pub struct HttpWebhookConfig {
    /// Bind address (default 0.0.0.0)
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Request path to accept (default "/")
    pub path: String,
    /// Optional inbound auth: required header name + exact value
    pub auth: Option<(String, String)>,
    /// Concurrency cap (default 256)
    pub max_concurrent: usize,
    /// Payload size cap in bytes (default 1 MiB); oversized → 413
    pub max_body_bytes: usize,
}

/// Source mode + its configuration
#[derive(Debug, Clone)]
pub enum HttpSourceMode {
    Poll(HttpPollConfig),
    Webhook(HttpWebhookConfig),
}

impl HttpSourceMode {
    /// Parse configuration from properties HashMap
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        match properties
            .get("http.mode")
            .ok_or("Missing required property: http.mode ('poll' or 'webhook')")?
            .as_str()
        {
            "poll" => Ok(Self::Poll(HttpPollConfig {
                url: parse_url(properties)?,
                interval_ms: parse_or(properties, "http.poll.interval.ms", 5000)?,
                method: parse_method(properties, "http.method", "GET", &["GET", "POST"])?,
                headers: parse_headers(properties),
                timeout_ms: parse_or(properties, "http.timeout.ms", 30_000)?,
            })),
            "webhook" => {
                let port: u16 = properties
                    .get("http.port")
                    .ok_or("Missing required property: http.port (webhook mode)")?
                    .parse()
                    .map_err(|e| format!("Invalid http.port: {e}"))?;
                if port == 0 {
                    // Port 0 binds an OS-assigned ephemeral port that no
                    // webhook sender could know about
                    return Err("Invalid http.port '0': a fixed port is required".to_string());
                }

                let path = properties
                    .get("http.path")
                    .cloned()
                    .unwrap_or_else(|| "/".to_string());
                if !path.starts_with('/') {
                    return Err(format!("Invalid http.path '{path}': must start with '/'"));
                }

                let auth = match (
                    properties.get("http.auth.header"),
                    properties.get("http.auth.token"),
                ) {
                    (Some(header), Some(token)) => {
                        if header.is_empty() || token.is_empty() {
                            // An empty token would authenticate anyone who
                            // sends the bare header
                            return Err("http.auth.header and http.auth.token must be non-empty"
                                .to_string());
                        }
                        if axum::http::HeaderName::from_bytes(header.as_bytes()).is_err() {
                            // An invalid name can never match an incoming
                            // header — every request would get 401
                            return Err(format!(
                                "Invalid http.auth.header '{header}': not a valid HTTP header name"
                            ));
                        }
                        Some((header.clone(), token.clone()))
                    }
                    (None, None) => None,
                    _ => {
                        return Err(
                            "http.auth.header and http.auth.token must be set together".to_string()
                        )
                    }
                };

                let max_concurrent = parse_or(properties, "http.max.concurrent.requests", 256)?;
                if max_concurrent == 0 {
                    // Zero would panic tokio's bounded channel and deadlock
                    // the concurrency limiter
                    return Err(
                        "Invalid http.max.concurrent.requests '0': must be >= 1".to_string()
                    );
                }

                Ok(Self::Webhook(HttpWebhookConfig {
                    host: properties
                        .get("http.host")
                        .cloned()
                        .unwrap_or_else(|| "0.0.0.0".to_string()),
                    port,
                    path,
                    auth,
                    max_concurrent,
                    max_body_bytes: parse_or(properties, "http.max.body.bytes", 1_048_576)?,
                }))
            }
            other => Err(format!(
                "Invalid http.mode '{other}': expected 'poll' or 'webhook'"
            )),
        }
    }

    /// Identifier used in error-context log messages
    fn stream_tag(&self) -> String {
        match self {
            Self::Poll(c) => c.url.clone(),
            Self::Webhook(c) => format!("{}:{}{}", c.host, c.port, c.path),
        }
    }
}

/// One payload handed from a webhook handler to the delivery thread,
/// with a channel for the verdict to travel back
type DeliveryJob = (
    axum::body::Bytes,
    tokio::sync::oneshot::Sender<DeliveryVerdict>,
);

/// Shared state for concurrent webhook handlers
struct WebhookState {
    auth: Option<(String, String)>,
    /// Payload size cap — enforced while reading the body, after auth
    max_body_bytes: usize,
    /// Bounded queue to the single delivery thread — `send().await`
    /// backpressures handlers naturally when delivery is slow
    delivery_tx: tokio::sync::mpsc::Sender<DeliveryJob>,
    /// Set by a Fail verdict or a dead delivery thread — triggers
    /// graceful shutdown
    failed: AtomicBool,
}

/// Webhook request handler — handlers run concurrently; deliveries are
/// funneled to one owning thread (see [`spawn_delivery_thread`])
async fn webhook_handler(State(state): State<Arc<WebhookState>>, request: Request) -> StatusCode {
    if state.failed.load(Ordering::SeqCst) {
        // A Fail verdict already triggered shutdown — turn away new
        // requests immediately instead of attempting delivery
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    // Auth is checked on the headers BEFORE the body is read, so an
    // unauthenticated flood cannot make the server buffer payloads
    if let Some((name, token)) = &state.auth {
        let authorized = request
            .headers()
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| constant_time_eq(v.as_bytes(), token.as_bytes()));
        if !authorized {
            return StatusCode::UNAUTHORIZED;
        }
    }

    // Only authenticated requests get their body buffered, capped at
    // max_body_bytes (a broken-connection read error takes the same exit —
    // the client is gone either way)
    let Ok(body) = axum::body::to_bytes(request.into_body(), state.max_body_bytes).await else {
        return StatusCode::PAYLOAD_TOO_LARGE;
    };

    if body.is_empty() {
        return StatusCode::OK; // accepted, nothing to deliver
    }

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if state.delivery_tx.send((body, reply_tx)).await.is_err() {
        // Delivery thread is gone without a shutdown signal (it panicked) —
        // a listener that can never deliver must not keep accepting
        state.failed.store(true, Ordering::SeqCst);
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    match reply_rx.await {
        Ok(DeliveryVerdict::Delivered) => StatusCode::OK,
        // Consumed by the error strategy (drop/DLQ)
        Ok(DeliveryVerdict::Disposed) => StatusCode::OK,
        Ok(DeliveryVerdict::Fail) => {
            log::error!("[HttpSource] Unrecoverable delivery error, shutting down listener");
            state.failed.store(true, Ordering::SeqCst);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(_) => {
            // Delivery thread died mid-job — shut down, don't zombie
            state.failed.store(true, Ordering::SeqCst);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// One thread owns the error context and performs all junction publishing —
/// handlers stay non-blocking and there is no lock to contend on. Exits when
/// the server (the only sender) shuts down.
fn spawn_delivery_thread(
    mut rx: tokio::sync::mpsc::Receiver<DeliveryJob>,
    callback: Arc<dyn SourceCallback>,
    mut error_ctx: Option<SourceErrorContext>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut received: u64 = 0;
        let mut failed: u64 = 0;

        while let Some((body, reply)) = rx.blocking_recv() {
            received += 1;
            let verdict =
                deliver_with_error_handling(callback.as_ref(), &body, &mut error_ctx, "HttpSource");
            if verdict != DeliveryVerdict::Delivered {
                failed += 1;
            }
            // Handler may have timed out/disconnected — nothing to do then
            let _ = reply.send(verdict);
        }

        log::info!("[HttpSource] Stopped (received: {received}, failed: {failed})");
    })
}

/// HTTP source (poll or webhook mode)
#[derive(Debug)]
pub struct HttpSource {
    /// Mode + configuration
    mode: HttpSourceMode,
    /// Worker thread lifecycle (spawn, signal, join on stop)
    worker: SourceWorker,
    /// Optional error handling context
    error_ctx: Option<SourceErrorContext>,
}

impl HttpSource {
    /// Create a new HTTP source without error handling
    pub fn new(mode: HttpSourceMode) -> Self {
        Self {
            mode,
            worker: SourceWorker::new("HttpSource"),
            error_ctx: None,
        }
    }

    /// Create an HTTP source from properties
    ///
    /// # Required Properties
    /// - `http.mode`: `poll` or `webhook`
    /// - poll: `http.url` · webhook: `http.port`
    ///
    /// # Optional Properties
    /// - poll: `http.poll.interval.ms`, `http.method`, `http.headers.<Name>`,
    ///   `http.timeout.ms`
    /// - webhook: `http.host`, `http.path`, `http.auth.header` +
    ///   `http.auth.token`, `http.max.concurrent.requests`,
    ///   `http.max.body.bytes`
    /// - `error.*`: Error handling properties (see SourceErrorContext)
    pub fn from_properties(
        properties: &HashMap<String, String>,
        dlq_junction: Option<Arc<Mutex<InputHandler>>>,
        stream_name: &str,
    ) -> Result<Self, String> {
        let mode = HttpSourceMode::from_properties(properties)?;
        let error_ctx = SourceErrorContext::from_properties(properties, dlq_junction, stream_name)?;

        Ok(Self {
            error_ctx,
            ..Self::new(mode)
        })
    }

    fn start_poll(&mut self, config: HttpPollConfig, callback: Arc<dyn SourceCallback>) {
        let mut error_ctx = self.error_ctx.take();

        self.worker.start(move |running| {
            let agent = build_agent(config.timeout_ms);
            let interval = Duration::from_millis(config.interval_ms);
            let mut polls_delivered: u64 = 0;
            let mut polls_failed: u64 = 0;

            log::info!(
                "[HttpSource] Polling '{}' every {}ms",
                config.url,
                config.interval_ms
            );

            'poll: while running.load(Ordering::SeqCst) {
                // The poll outcome distills to at most one connection error;
                // one dispatch below decides continue-vs-stop for both the
                // status and transport failure shapes
                let mut connection_error: Option<String> = None;

                let result = build_request(&config.method, &config.url, &config.headers)
                    .body(())
                    .map_err(|e| format!("request build: {e}"))
                    .and_then(|req| agent.run(req).map_err(|e| e.to_string()));

                match result {
                    Ok(mut response) => {
                        let status = response.status().as_u16();
                        match status {
                            200..=299 => match response.body_mut().read_to_vec() {
                                Ok(body) if body.is_empty() => {
                                    log::debug!("[HttpSource] Empty response body — skipping");
                                }
                                Ok(body) => match deliver_with_error_handling(
                                    callback.as_ref(),
                                    &body,
                                    &mut error_ctx,
                                    "HttpSource",
                                ) {
                                    DeliveryVerdict::Delivered => polls_delivered += 1,
                                    DeliveryVerdict::Disposed => polls_failed += 1,
                                    DeliveryVerdict::Fail => {
                                        polls_failed += 1;
                                        log::error!("[HttpSource] Unrecoverable error, stopping");
                                        break 'poll;
                                    }
                                },
                                Err(e) => {
                                    connection_error = Some(format!("body read failed: {e}"));
                                }
                            },
                            304 => {
                                log::debug!("[HttpSource] 304 Not Modified — skipping");
                            }
                            _ => connection_error = Some(format!("returned status {status}")),
                        }
                    }
                    Err(e) => connection_error = Some(format!("failed: {e}")),
                }

                if let Some(detail) = connection_error {
                    let err = EventFluxError::ConnectionUnavailable {
                        message: format!("Poll of '{}' {detail}", config.url),
                        source: None,
                    };
                    if let Some(ctx) = &mut error_ctx {
                        if !ctx.handle_error(None, &err) {
                            break 'poll;
                        }
                    } else {
                        log::error!("[HttpSource] {err}");
                    }
                }

                // Interruptible: a long interval never delays stop()
                sleep_while_running(&running, interval);
            }

            log::info!(
                "[HttpSource] Stopped (delivered: {polls_delivered}, failed: {polls_failed})"
            );
        });
    }

    fn start_webhook(&mut self, config: HttpWebhookConfig, callback: Arc<dyn SourceCallback>) {
        let error_ctx = self.error_ctx.take();

        self.worker.start(move |running| {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("[HttpSource] Failed to create tokio runtime: {e}");
                    return;
                }
            };

            // The queue capacity matches the handler concurrency cap — a
            // full queue backpressures handlers via `send().await`
            let (delivery_tx, delivery_rx) = tokio::sync::mpsc::channel(config.max_concurrent);
            let delivery_thread = spawn_delivery_thread(delivery_rx, callback, error_ctx);

            rt.block_on(async move {
                let state = Arc::new(WebhookState {
                    auth: config.auth.clone(),
                    max_body_bytes: config.max_body_bytes,
                    delivery_tx,
                    failed: AtomicBool::new(false),
                });

                let router = Router::new()
                    .route(&config.path, post(webhook_handler))
                    .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                        config.max_concurrent,
                    ))
                    .with_state(Arc::clone(&state));

                let addr = format!("{}:{}", config.host, config.port);
                let listener = match tokio::net::TcpListener::bind(&addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        log::error!("[HttpSource] Failed to bind webhook listener {addr}: {e}");
                        return;
                    }
                };

                log::info!(
                    "[HttpSource] Webhook listening on {addr}{} (max concurrent: {})",
                    config.path,
                    config.max_concurrent
                );

                // Graceful shutdown when the worker is signalled or a Fail
                // verdict was reached; in-flight requests complete first
                let shutdown_state = Arc::clone(&state);
                let shutdown = async move {
                    loop {
                        if !running.load(Ordering::SeqCst)
                            || shutdown_state.failed.load(Ordering::SeqCst)
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };

                if let Err(e) = axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown)
                    .await
                {
                    log::error!("[HttpSource] Webhook server error: {e}");
                }
            });

            // Server (the only sender) is gone — the delivery thread drains
            // any queued jobs, sees the closed channel, and exits
            if delivery_thread.join().is_err() {
                log::error!("[HttpSource] Delivery thread panicked");
            }
        });
    }
}

impl Clone for HttpSource {
    fn clone(&self) -> Self {
        Self {
            mode: self.mode.clone(),
            worker: self.worker.clone(), // Clones as a fresh, unstarted worker
            error_ctx: None,             // Error context contains runtime state, not cloneable
        }
    }
}

impl Source for HttpSource {
    fn start(&mut self, callback: Arc<dyn SourceCallback>) {
        match self.mode.clone() {
            HttpSourceMode::Poll(config) => self.start_poll(config, callback),
            HttpSourceMode::Webhook(config) => self.start_webhook(config, callback),
        }
    }

    fn stop(&mut self) {
        log::info!("[HttpSource] Stopping...");
        self.worker.stop();
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
        match &self.mode {
            // HEAD is the least side-effectful probe; any HTTP response
            // (including 405) proves reachability
            HttpSourceMode::Poll(config) => probe_reachability(
                &build_agent(config.timeout_ms),
                "HEAD",
                &config.url,
                &config.headers,
            ),
            // Fail fast on port conflicts and permission errors
            HttpSourceMode::Webhook(config) => {
                let addr = format!("{}:{}", config.host, config.port);
                std::net::TcpListener::bind(&addr).map(|_| ()).map_err(|e| {
                    EventFluxError::ConnectionUnavailable {
                        message: format!("Cannot bind webhook listener on {addr}: {e}"),
                        source: Some(Box::new(e)),
                    }
                })
            }
        }
    }
}

// ============================================================================
// HTTP Source Factory
// ============================================================================

/// Factory for creating HTTP source instances.
///
/// This factory is registered with EventFluxContext and used to create
/// HTTP sources from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct HttpSourceFactory;

impl SourceFactory for HttpSourceFactory {
    fn name(&self) -> &'static str {
        "http"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        // Mode-specific requirements (http.url / http.port) are validated
        // in from_properties, since they depend on http.mode
        &["http.mode"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            // poll mode
            "http.url",
            "http.poll.interval.ms",
            "http.method",
            "http.timeout.ms",
            // webhook mode
            "http.host",
            "http.port",
            "http.path",
            "http.auth.header",
            "http.auth.token",
            "http.max.concurrent.requests",
            "http.max.body.bytes",
            // http.headers.<Name> passthrough is also accepted
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
        // DLQ junction is None initially — stream_initializer calls
        // set_error_dlq_junction() after creation. Parse once; the mode's
        // tag identifies the stream in error-context log messages.
        let mode =
            HttpSourceMode::from_properties(config).map_err(EventFluxError::configuration)?;
        let error_ctx = SourceErrorContext::from_properties(config, None, &mode.stream_tag())
            .map_err(EventFluxError::configuration)?;

        Ok(Box::new(HttpSource {
            error_ctx,
            ..HttpSource::new(mode)
        }))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poll_props() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("http.mode".to_string(), "poll".to_string());
        props.insert(
            "http.url".to_string(),
            "http://localhost:9999/data".to_string(),
        );
        props
    }

    fn webhook_props() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("http.mode".to_string(), "webhook".to_string());
        props.insert("http.port".to_string(), "8081".to_string());
        props
    }

    #[test]
    fn test_poll_config_defaults() {
        let mode = HttpSourceMode::from_properties(&poll_props()).unwrap();
        let HttpSourceMode::Poll(config) = mode else {
            panic!("expected poll mode");
        };
        assert_eq!(config.url, "http://localhost:9999/data");
        assert_eq!(config.interval_ms, 5000);
        assert_eq!(config.method, "GET");
        assert!(config.headers.is_empty());
        assert_eq!(config.timeout_ms, 30_000);
    }

    #[test]
    fn test_poll_config_all_options() {
        let mut props = poll_props();
        props.insert("http.poll.interval.ms".to_string(), "1000".to_string());
        props.insert("http.method".to_string(), "post".to_string());
        props.insert("http.timeout.ms".to_string(), "2000".to_string());
        props.insert("http.headers.X-Api-Key".to_string(), "k".to_string());

        let HttpSourceMode::Poll(config) = HttpSourceMode::from_properties(&props).unwrap() else {
            panic!("expected poll mode");
        };
        assert_eq!(config.interval_ms, 1000);
        assert_eq!(config.method, "POST"); // normalized
        assert_eq!(config.timeout_ms, 2000);
        assert_eq!(
            config.headers,
            vec![("X-Api-Key".to_string(), "k".to_string())]
        );
    }

    #[test]
    fn test_webhook_config_defaults() {
        let HttpSourceMode::Webhook(config) =
            HttpSourceMode::from_properties(&webhook_props()).unwrap()
        else {
            panic!("expected webhook mode");
        };
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8081);
        assert_eq!(config.path, "/");
        assert!(config.auth.is_none());
        assert_eq!(config.max_concurrent, 256);
        assert_eq!(config.max_body_bytes, 1_048_576);
    }

    #[test]
    fn test_webhook_config_all_options() {
        let mut props = webhook_props();
        props.insert("http.host".to_string(), "127.0.0.1".to_string());
        props.insert("http.path".to_string(), "/hooks/alerts".to_string());
        props.insert("http.auth.header".to_string(), "X-Token".to_string());
        props.insert("http.auth.token".to_string(), "secret".to_string());
        props.insert("http.max.concurrent.requests".to_string(), "16".to_string());
        props.insert("http.max.body.bytes".to_string(), "1024".to_string());

        let HttpSourceMode::Webhook(config) = HttpSourceMode::from_properties(&props).unwrap()
        else {
            panic!("expected webhook mode");
        };
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.path, "/hooks/alerts");
        assert_eq!(
            config.auth,
            Some(("X-Token".to_string(), "secret".to_string()))
        );
        assert_eq!(config.max_concurrent, 16);
        assert_eq!(config.max_body_bytes, 1024);
    }

    #[test]
    fn test_mode_required_and_validated() {
        assert!(HttpSourceMode::from_properties(&HashMap::new())
            .unwrap_err()
            .contains("http.mode"));

        let mut props = HashMap::new();
        props.insert("http.mode".to_string(), "push".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.mode"));
    }

    #[test]
    fn test_poll_requires_url() {
        let mut props = HashMap::new();
        props.insert("http.mode".to_string(), "poll".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.url"));
    }

    #[test]
    fn test_poll_rejects_unsupported_method() {
        let mut props = poll_props();
        props.insert("http.method".to_string(), "DELETE".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.method"));
    }

    #[test]
    fn test_webhook_requires_port() {
        let mut props = HashMap::new();
        props.insert("http.mode".to_string(), "webhook".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.port"));
    }

    #[test]
    fn test_webhook_rejects_port_zero() {
        let mut props = webhook_props();
        props.insert("http.port".to_string(), "0".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.port"));
    }

    #[test]
    fn test_webhook_rejects_zero_concurrency() {
        let mut props = webhook_props();
        props.insert("http.max.concurrent.requests".to_string(), "0".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.max.concurrent.requests"));
    }

    #[test]
    fn test_webhook_auth_must_be_paired() {
        let mut props = webhook_props();
        props.insert("http.auth.header".to_string(), "X-Token".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.auth"));
    }

    #[test]
    fn test_webhook_auth_rejects_empty_values() {
        let mut props = webhook_props();
        props.insert("http.auth.header".to_string(), "X-Token".to_string());
        props.insert("http.auth.token".to_string(), String::new());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("non-empty"));
    }

    #[test]
    fn test_webhook_auth_rejects_invalid_header_name() {
        let mut props = webhook_props();
        props.insert("http.auth.header".to_string(), "X Token".to_string());
        props.insert("http.auth.token".to_string(), "secret".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("header name"));
    }

    #[test]
    fn test_webhook_path_must_start_with_slash() {
        let mut props = webhook_props();
        props.insert("http.path".to_string(), "hooks".to_string());
        assert!(HttpSourceMode::from_properties(&props)
            .unwrap_err()
            .contains("http.path"));
    }

    #[test]
    fn test_source_from_properties_with_error_handling() {
        let mut props = poll_props();
        props.insert("error.strategy".to_string(), "drop".to_string());

        let source = HttpSource::from_properties(&props, None, "TestStream").unwrap();
        assert!(source.error_ctx.is_some());
    }

    #[test]
    fn test_source_clone_resets_runtime_state() {
        let mut props = poll_props();
        props.insert("error.strategy".to_string(), "drop".to_string());
        let source = HttpSource::from_properties(&props, None, "TestStream").unwrap();
        let cloned = source.clone();
        assert!(cloned.error_ctx.is_none());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = HttpSourceFactory;
        assert_eq!(factory.name(), "http");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"http.mode"));
        assert!(factory.optional_parameters().contains(&"http.url"));
        assert!(factory.optional_parameters().contains(&"http.port"));
    }

    #[test]
    fn test_factory_create_both_modes() {
        let factory = HttpSourceFactory;
        assert!(factory.create_initialized(&poll_props()).is_ok());
        assert!(factory.create_initialized(&webhook_props()).is_ok());
        assert!(factory.create_initialized(&HashMap::new()).is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = HttpSourceFactory;
        assert_eq!(factory.clone_box().name(), "http");
    }
}
