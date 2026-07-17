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

//! # HTTP Sink
//!
//! Sends each event's formatted payload as an HTTP request (webhook-style).
//!
//! ## Architecture
//!
//! ```text
//! Events → per event: SinkMapper → bytes → HttpSink::publish() → ureq request (+ retry) → endpoint
//! ```
//!
//! One event = one request. Retries with exponential backoff apply to
//! transport errors and retryable statuses (408/429/5xx) — other 4xx mean the
//! request itself is wrong and are returned to the pipeline immediately.
//! Retries block inside `publish()`, which is backpressure by design.

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use crate::core::stream::connector_util::{parse_or, INJECTED_FORMAT_KEY};
use crate::core::stream::http_common::{
    build_agent, build_request, is_retryable_status, parse_headers, parse_method, parse_url,
    probe_reachability, HttpRetryConfig,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration for HTTP sink
#[derive(Debug, Clone)]
pub struct HttpSinkConfig {
    /// Endpoint to send to (http/https)
    pub url: String,
    /// `POST` (default), `PUT`, or `PATCH`
    pub method: String,
    /// Request headers from `http.headers.<Name>`
    pub headers: Vec<(String, String)>,
    /// Content-Type: explicit `http.content.type`, else derived from the
    /// stream format (json/csv/bytes); `None` when a
    /// `http.headers.Content-Type` entry supplies it verbatim instead
    pub content_type: Option<String>,
    /// Per-request timeout (default 30000)
    pub timeout_ms: u64,
    /// Exponential-backoff retry policy
    pub retry: HttpRetryConfig,
    /// Startup probe: `HEAD` (default), `GET`, `OPTIONS`, or `none`
    pub validation_method: String,
}

impl HttpSinkConfig {
    /// Parse configuration from properties HashMap
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let url = parse_url(properties)?;
        let method = parse_method(properties, "http.method", "POST", &["POST", "PUT", "PATCH"])?;
        let headers = parse_headers(properties);

        // Precedence: a verbatim http.headers.Content-Type wins (setting
        // both is rejected — two sources of truth); else explicit
        // http.content.type; else derived from the stream's format (injected
        // by stream_initializer as _format); else a safe binary default.
        // `header()` appends rather than replaces, so the derived value must
        // be withheld when a header already carries one.
        let header_has_content_type = headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("content-type"));
        let content_type =
            if header_has_content_type {
                if properties.contains_key("http.content.type") {
                    return Err("Content-Type is set via both http.content.type and \
                     http.headers.Content-Type — use one"
                        .to_string());
                }
                None
            } else {
                Some(
                    properties.get("http.content.type").cloned().unwrap_or_else(
                        || match properties.get(INJECTED_FORMAT_KEY).map(String::as_str) {
                            Some("json") => "application/json".to_string(),
                            Some("csv") => "text/csv".to_string(),
                            _ => "application/octet-stream".to_string(),
                        },
                    ),
                )
            };

        let validation_method = parse_method(
            properties,
            "http.validation.method",
            "HEAD",
            &["HEAD", "GET", "OPTIONS", "NONE"],
        )?;

        Ok(Self {
            url,
            method,
            headers,
            content_type,
            timeout_ms: parse_or(properties, "http.timeout.ms", 30_000)?,
            retry: HttpRetryConfig::from_properties(properties)?,
            validation_method,
        })
    }
}

/// Counters logged when the sink stops
#[derive(Debug, Default)]
struct SinkMetrics {
    requests_sent: AtomicU64,
    requests_failed: AtomicU64,
}

/// HTTP sink that sends each event as its own request
///
/// This implementation:
/// - Uses a blocking `ureq` client (rustls) — no runtime bridging
/// - Retries transport errors and 408/429/5xx with exponential backoff
/// - Treats other 4xx as permanent request errors (no retry)
#[derive(Debug)]
pub struct HttpSink {
    /// Configuration
    config: HttpSinkConfig,
    /// Reusable blocking client (thread-safe, connection pooling)
    agent: ureq::Agent,
    /// Sent/failed counters
    metrics: Arc<SinkMetrics>,
}

impl HttpSink {
    /// Create a new HTTP sink
    pub fn new(config: HttpSinkConfig) -> Self {
        let agent = build_agent(config.timeout_ms);
        Self {
            config,
            agent,
            metrics: Arc::new(SinkMetrics::default()),
        }
    }

    /// Create an HTTP sink from properties
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        Ok(Self::new(HttpSinkConfig::from_properties(properties)?))
    }

    /// One request attempt; `Ok(true)` = delivered, `Ok(false)` = retryable
    /// failure, `Err` = permanent failure
    fn attempt(&self, payload: &[u8]) -> Result<bool, EventFluxError> {
        let mut request =
            build_request(&self.config.method, &self.config.url, &self.config.headers);
        if let Some(content_type) = &self.config.content_type {
            request = request.header("Content-Type", content_type);
        }
        // The body borrows the payload — no copy, even across retry attempts
        let request = request.body(payload).map_err(|e| {
            EventFluxError::configuration(format!("Failed to build HTTP request: {e}"))
        })?;

        match self.agent.run(request) {
            Ok(response) => {
                let status = response.status().as_u16();
                if (200..300).contains(&status) {
                    Ok(true)
                } else if is_retryable_status(status) {
                    log::warn!(
                        "[HttpSink] '{}' responded {status} — will retry",
                        self.config.url
                    );
                    Ok(false)
                } else {
                    Err(EventFluxError::app_runtime(format!(
                        "HTTP sink request to '{}' rejected with status {status}",
                        self.config.url
                    )))
                }
            }
            Err(e) => {
                // Transport-level failure (connect, timeout, TLS) — retryable
                log::warn!("[HttpSink] Request to '{}' failed: {e}", self.config.url);
                Ok(false)
            }
        }
    }
}

impl Clone for HttpSink {
    fn clone(&self) -> Self {
        // ureq::Agent is Arc-shared internally — clones reuse the same
        // connection pool; shared metrics keep the stop-log totals accurate
        Self {
            config: self.config.clone(),
            agent: self.agent.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl Sink for HttpSink {
    fn publish(&self, payload: &[u8]) -> Result<(), EventFluxError> {
        let mut attempt_no: u32 = 0;
        loop {
            match self.attempt(payload) {
                Ok(true) => {
                    self.metrics.requests_sent.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Ok(false) if attempt_no < self.config.retry.max_attempts => {
                    attempt_no += 1;
                    // Blocking backoff inside publish() = backpressure
                    std::thread::sleep(self.config.retry.delay_for(attempt_no));
                }
                Ok(false) => {
                    self.metrics.requests_failed.fetch_add(1, Ordering::Relaxed);
                    return Err(EventFluxError::ConnectionUnavailable {
                        message: format!(
                            "HTTP sink request to '{}' failed after {} attempt(s)",
                            self.config.url,
                            attempt_no + 1
                        ),
                        source: None,
                    });
                }
                Err(e) => {
                    self.metrics.requests_failed.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
    }

    fn stop(&self) {
        log::info!(
            "[HttpSink] Stopped (sent: {}, failed: {})",
            self.metrics.requests_sent.load(Ordering::Relaxed),
            self.metrics.requests_failed.load(Ordering::Relaxed)
        );
    }

    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(self.clone())
    }

    fn validate_connectivity(&self) -> Result<(), EventFluxError> {
        if self.config.validation_method == "NONE" {
            return Ok(());
        }
        probe_reachability(
            &self.agent,
            &self.config.validation_method,
            &self.config.url,
            &self.config.headers,
        )
    }
}

// ============================================================================
// HTTP Sink Factory
// ============================================================================

/// Factory for creating HTTP sink instances.
///
/// This factory is registered with EventFluxContext and used to create
/// HTTP sinks from SQL WITH clause configuration.
#[derive(Debug, Clone)]
pub struct HttpSinkFactory;

impl SinkFactory for HttpSinkFactory {
    fn name(&self) -> &'static str {
        "http"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "csv", "bytes"]
    }

    fn required_parameters(&self) -> &[&str] {
        &["http.url"]
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "http.method",
            "http.content.type",
            "http.timeout.ms",
            "http.retry.max-attempts",
            "http.retry.initial-delay-ms",
            "http.retry.max-delay-ms",
            "http.retry.backoff-multiplier",
            "http.validation.method",
            // http.headers.<Name> passthrough is also accepted
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn Sink>, EventFluxError> {
        let parsed =
            HttpSinkConfig::from_properties(config).map_err(EventFluxError::configuration)?;
        Ok(Box::new(HttpSink::new(parsed)))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_props() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            "http.url".to_string(),
            "http://localhost:9999/hook".to_string(),
        );
        props
    }

    #[test]
    fn test_config_required_only_defaults() {
        let config = HttpSinkConfig::from_properties(&base_props()).unwrap();
        assert_eq!(config.url, "http://localhost:9999/hook");
        assert_eq!(config.method, "POST");
        assert!(config.headers.is_empty());
        assert_eq!(
            config.content_type.as_deref(),
            Some("application/octet-stream") // no format
        );
        assert_eq!(config.timeout_ms, 30_000);
        assert_eq!(config.retry.max_attempts, 3);
        assert_eq!(config.validation_method, "HEAD");
    }

    #[test]
    fn test_config_all_options() {
        let mut props = base_props();
        props.insert("http.method".to_string(), "put".to_string());
        props.insert(
            "http.content.type".to_string(),
            "application/xml".to_string(),
        );
        props.insert("http.timeout.ms".to_string(), "5000".to_string());
        props.insert("http.retry.max-attempts".to_string(), "5".to_string());
        props.insert("http.validation.method".to_string(), "none".to_string());
        props.insert(
            "http.headers.Authorization".to_string(),
            "Bearer tok".to_string(),
        );

        let config = HttpSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.method, "PUT"); // normalized
        assert_eq!(config.content_type.as_deref(), Some("application/xml"));
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.retry.max_attempts, 5);
        assert_eq!(config.validation_method, "NONE");
        assert_eq!(
            config.headers,
            vec![("Authorization".to_string(), "Bearer tok".to_string())]
        );
    }

    #[test]
    fn test_content_type_derived_from_injected_format() {
        for (format, expected) in [
            ("json", "application/json"),
            ("csv", "text/csv"),
            ("bytes", "application/octet-stream"),
        ] {
            let mut props = base_props();
            props.insert(INJECTED_FORMAT_KEY.to_string(), format.to_string());
            let config = HttpSinkConfig::from_properties(&props).unwrap();
            assert_eq!(
                config.content_type.as_deref(),
                Some(expected),
                "format {format}"
            );
        }

        // Explicit override beats derivation
        let mut props = base_props();
        props.insert(INJECTED_FORMAT_KEY.to_string(), "json".to_string());
        props.insert("http.content.type".to_string(), "text/plain".to_string());
        let config = HttpSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn test_content_type_header_wins_and_conflicts_rejected() {
        // A verbatim http.headers.Content-Type suppresses the derived value
        // (header() appends — a second Content-Type would be malformed)
        let mut props = base_props();
        props.insert(INJECTED_FORMAT_KEY.to_string(), "json".to_string());
        props.insert(
            "http.headers.Content-Type".to_string(),
            "application/xml".to_string(),
        );
        let config = HttpSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.content_type, None);
        assert_eq!(
            config.headers,
            vec![("Content-Type".to_string(), "application/xml".to_string())]
        );

        // Both mechanisms at once is ambiguous — rejected
        props.insert("http.content.type".to_string(), "text/plain".to_string());
        let err = HttpSinkConfig::from_properties(&props).unwrap_err();
        assert!(err.contains("http.content.type"), "got: {err}");
    }

    #[test]
    fn test_config_invalid_method() {
        let mut props = base_props();
        props.insert("http.method".to_string(), "DELETE".to_string());
        assert!(HttpSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("http.method"));
    }

    #[test]
    fn test_config_invalid_validation_method() {
        let mut props = base_props();
        props.insert("http.validation.method".to_string(), "TRACE".to_string());
        assert!(HttpSinkConfig::from_properties(&props)
            .unwrap_err()
            .contains("http.validation.method"));
    }

    #[test]
    fn test_config_missing_url() {
        assert!(HttpSinkConfig::from_properties(&HashMap::new())
            .unwrap_err()
            .contains("http.url"));
    }

    #[test]
    fn test_validation_none_skips_probe() {
        let mut props = base_props();
        // Unreachable endpoint, but validation is disabled
        props.insert("http.validation.method".to_string(), "none".to_string());
        let sink = HttpSink::from_properties(&props).unwrap();
        assert!(sink.validate_connectivity().is_ok());
    }

    // =========================================================================
    // Factory Tests
    // =========================================================================

    #[test]
    fn test_factory_metadata() {
        let factory = HttpSinkFactory;
        assert_eq!(factory.name(), "http");
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"csv"));
        assert!(factory.supported_formats().contains(&"bytes"));
        assert!(factory.required_parameters().contains(&"http.url"));
        assert!(factory.optional_parameters().contains(&"http.method"));
    }

    #[test]
    fn test_factory_create_and_missing_required() {
        let factory = HttpSinkFactory;
        assert!(factory.create_initialized(&base_props()).is_ok());
        assert!(factory.create_initialized(&HashMap::new()).is_err());
    }

    #[test]
    fn test_factory_clone() {
        let factory = HttpSinkFactory;
        assert_eq!(factory.clone_box().name(), "http");
    }
}
