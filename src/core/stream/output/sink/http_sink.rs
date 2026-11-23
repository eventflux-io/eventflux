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
//! Sends each event's formatted payload as an HTTP request, like a webhook.
//!
//! ## Architecture
//!
//! ```text
//! Events → per event: SinkMapper → bytes → HttpSink::publish() → ureq request (+ retry) → endpoint
//! ```
//!
//! One event gives one request. Network errors and the statuses worth a retry
//! (408, 429, 5xx) are retried, with a wait that grows after each try. Other
//! 4xx mean the request itself is wrong and go back to the pipeline at once.
//! Retries block inside `publish()`. This is on purpose, so a slow endpoint
//! slows the pipeline down.

use super::sink_trait::Sink;
use crate::core::exception::EventFluxError;
use crate::core::extension::SinkFactory;
use crate::core::stream::connector_util::{parse_or, Redacted, INJECTED_FORMAT_KEY};
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
    /// Endpoint to send to (http or https)
    pub url: String,
    /// `POST` (default), `PUT`, or `PATCH`
    pub method: String,
    /// Request headers from `http.headers.<Name>`. Values are hidden in Debug.
    pub headers: Vec<(String, Redacted)>,
    /// Content-Type. `http.content.type` when set, else picked from the stream
    /// format (json, csv, or bytes). `None` when an `http.headers.Content-Type`
    /// entry sets it as is instead.
    pub content_type: Option<String>,
    /// Timeout for each request in ms. Default 30000.
    pub timeout_ms: u64,
    /// Retry policy, with a wait that grows after each try
    pub retry: HttpRetryConfig,
    /// Startup check method. `HEAD` (default), `GET`, `OPTIONS`, or `NONE` to
    /// skip it. Stored in uppercase.
    pub validation_method: String,
}

impl HttpSinkConfig {
    /// Reads the config from properties.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL, method, validation method, timeout, or
    /// retry settings are invalid, or Content-Type is set through both
    /// `http.content.type` and `http.headers.Content-Type`.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let url = parse_url(properties)?;
        let method = parse_method(properties, "http.method", "POST", &["POST", "PUT", "PATCH"])?;
        let headers = parse_headers(properties);

        // Order of choice. An http.headers.Content-Type wins as is, and also
        // setting http.content.type is rejected so it is not set in two places.
        // Next comes http.content.type. Next a value from the stream format,
        // which stream_initializer adds as _format. Last a safe binary
        // default. `header()` adds a header instead of replacing it, so the
        // picked value must be left out when a header already sets one.
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

/// HTTP sink that sends each event as its own request.
///
/// - Uses a blocking `ureq` client (rustls), so no async runtime is needed
/// - Retries network errors and 408, 429, and 5xx, waiting longer each time
/// - Treats other 4xx as request errors that a retry cannot fix
#[derive(Debug)]
pub struct HttpSink {
    config: HttpSinkConfig,
    /// Reused blocking client. Thread safe, and keeps a connection pool.
    agent: ureq::Agent,
    /// Sent and failed counters
    metrics: Arc<SinkMetrics>,
}

impl HttpSink {
    /// Creates an HTTP sink
    pub fn new(config: HttpSinkConfig) -> Self {
        let agent = build_agent(config.timeout_ms);
        Self {
            config,
            agent,
            metrics: Arc::new(SinkMetrics::default()),
        }
    }

    /// Creates an HTTP sink from properties. Fails when the config is invalid.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        Ok(Self::new(HttpSinkConfig::from_properties(properties)?))
    }

    /// Makes one request. Returns `Ok(true)` when delivered, `Ok(false)` for a
    /// failure worth a retry, and `Err` for a failure a retry cannot fix (a
    /// request that cannot be built, or a 4xx other than 408 and 429).
    fn attempt(&self, payload: &[u8]) -> Result<bool, EventFluxError> {
        let mut request =
            build_request(&self.config.method, &self.config.url, &self.config.headers);
        if let Some(content_type) = &self.config.content_type {
            request = request.header("Content-Type", content_type);
        }
        // The body borrows the payload, so it is never copied, even on retries
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
                // Network failure (connect, timeout, TLS), worth a retry
                log::warn!("[HttpSink] Request to '{}' failed: {e}", self.config.url);
                Ok(false)
            }
        }
    }
}

impl Clone for HttpSink {
    fn clone(&self) -> Self {
        // ureq::Agent shares its insides through an Arc, so clones reuse the
        // same connection pool. Shared metrics keep the stop log totals right.
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
                    // Waiting inside publish() slows the pipeline down on purpose
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

/// Factory for HTTP sinks.
///
/// Registered with EventFluxContext and used to build HTTP sinks from the SQL
/// WITH clause config.
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
            // http.headers.<Name> keys are also accepted
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
        assert_eq!(config.method, "PUT"); // uppercased
        assert_eq!(config.content_type.as_deref(), Some("application/xml"));
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.retry.max_attempts, 5);
        assert_eq!(config.validation_method, "NONE");
        assert_eq!(
            config.headers,
            vec![("Authorization".to_string(), Redacted::new("Bearer tok"))]
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("Bearer tok"), "leaked: {debug}");
        assert!(debug.contains("Authorization"), "names stay visible");
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

        // An explicit value beats the one picked from the format
        let mut props = base_props();
        props.insert(INJECTED_FORMAT_KEY.to_string(), "json".to_string());
        props.insert("http.content.type".to_string(), "text/plain".to_string());
        let config = HttpSinkConfig::from_properties(&props).unwrap();
        assert_eq!(config.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn test_content_type_header_wins_and_conflicts_rejected() {
        // An http.headers.Content-Type leaves out the picked value. header()
        // adds headers, so a second Content-Type would make a bad request.
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
            vec![("Content-Type".to_string(), Redacted::new("application/xml"))]
        );

        // Setting both at once is ambiguous, so it is rejected
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
        // The endpoint cannot be reached, but the check is turned off
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
