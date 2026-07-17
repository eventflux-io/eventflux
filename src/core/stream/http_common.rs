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

//! Configuration and client helpers shared by the HTTP source and sink.

use crate::core::exception::EventFluxError;
use crate::core::stream::connector_util::{parse_or, parse_prefixed};
use std::collections::HashMap;
use std::time::Duration;

/// Request headers from `http.headers.<Name>` properties.
///
/// Secrets belong in environment variables:
/// `"http.headers.Authorization" = 'Bearer ${TOKEN}'`.
pub fn parse_headers(properties: &HashMap<String, String>) -> Vec<(String, String)> {
    parse_prefixed(properties, "http.headers.")
}

/// Validate an `http://` or `https://` URL property.
pub fn parse_url(properties: &HashMap<String, String>) -> Result<String, String> {
    let url = properties
        .get("http.url")
        .ok_or("Missing required property: http.url")?
        .trim()
        .to_string();
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!(
            "Invalid http.url '{url}': must start with http:// or https://"
        ));
    }
    Ok(url)
}

/// Exponential-backoff retry policy for outbound requests (`http.retry.*`)
#[derive(Debug, Clone)]
pub struct HttpRetryConfig {
    /// Retries after the first attempt (0 disables retrying)
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

impl HttpRetryConfig {
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let config = Self {
            max_attempts: parse_or(properties, "http.retry.max-attempts", 3)?,
            initial_delay_ms: parse_or(properties, "http.retry.initial-delay-ms", 100)?,
            max_delay_ms: parse_or(properties, "http.retry.max-delay-ms", 10_000)?,
            backoff_multiplier: parse_or(properties, "http.retry.backoff-multiplier", 2.0)?,
        };
        // NaN fails every comparison, so it needs its own rejection
        if config.backoff_multiplier.is_nan() || config.backoff_multiplier < 1.0 {
            return Err(format!(
                "Invalid http.retry.backoff-multiplier '{}': must be >= 1.0",
                config.backoff_multiplier
            ));
        }
        Ok(config)
    }

    /// Delay before retry number `retry` (1-based), capped at the ceiling
    pub fn delay_for(&self, retry: u32) -> Duration {
        crate::core::error::retry::exponential_backoff_with_multiplier(
            retry as usize,
            Duration::from_millis(self.initial_delay_ms),
            Duration::from_millis(self.max_delay_ms),
            self.backoff_multiplier,
        )
    }
}

/// Parse an optional method property against an allowlist, normalizing to
/// uppercase.
pub fn parse_method(
    properties: &HashMap<String, String>,
    key: &str,
    default: &str,
    allowed: &[&str],
) -> Result<String, String> {
    let method = properties
        .get(key)
        .map_or_else(|| default.to_string(), |v| v.to_uppercase());
    if !allowed.contains(&method.as_str()) {
        return Err(format!(
            "Invalid {key} '{method}': expected one of {}",
            allowed.join(", ")
        ));
    }
    Ok(method)
}

/// Build a request with the configured headers applied (body added by the
/// caller). One definition instead of a copy per call site.
pub fn build_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
) -> ureq::http::request::Builder {
    // ureq re-exports the exact `http` crate version its API expects
    let mut request = ureq::http::Request::builder().method(method).uri(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    request
}

/// Compare a presented secret against the expected one without an
/// early-exit on the first differing byte, so response timing does not
/// leak how much of the token matched. (Length inequality still returns
/// fast — the token length is not the secret.)
pub fn constant_time_eq(presented: &[u8], expected: &[u8]) -> bool {
    presented.len() == expected.len()
        && presented
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// Whether a response status warrants a retry: request timeout, rate
/// limiting, and server-side errors. Other 4xx mean the request itself is
/// wrong — retrying cannot fix it.
pub fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

/// Blocking client with a global per-request timeout. Non-2xx statuses are
/// returned as responses (not errors) so callers branch on status codes.
///
/// Redirects are never followed: ureq's default would rewrite a redirected
/// POST/PUT to a body-less GET (per 301/302/303 semantics), making the sink
/// report a dropped payload as delivered. A 3xx instead surfaces to the
/// caller as a non-2xx status — point the URL at the final location.
pub fn build_agent(timeout_ms: u64) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .http_status_as_error(false)
        .max_redirects(0)
        .build()
        .into()
}

/// Startup reachability probe: ANY HTTP response — including 4xx/405 —
/// proves the endpoint is reachable; only transport failures (connect,
/// timeout, TLS) fail the probe.
pub fn probe_reachability(
    agent: &ureq::Agent,
    method: &str,
    url: &str,
    headers: &[(String, String)],
) -> Result<(), EventFluxError> {
    let request = build_request(method, url, headers).body(()).map_err(|e| {
        EventFluxError::configuration(format!("Failed to build probe request for '{url}': {e}"))
    })?;

    agent
        .run(request)
        .map(|_| ())
        .map_err(|e| EventFluxError::ConnectionUnavailable {
            message: format!("HTTP endpoint '{url}' is not reachable: {e}"),
            source: Some(Box::new(e)),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_valid_and_invalid() {
        let mut props = HashMap::new();
        assert!(parse_url(&props).unwrap_err().contains("http.url"));

        props.insert("http.url".to_string(), "ftp://x".to_string());
        assert!(parse_url(&props).unwrap_err().contains("http://"));

        props.insert(
            "http.url".to_string(),
            "https://example.com/hook".to_string(),
        );
        assert_eq!(parse_url(&props).unwrap(), "https://example.com/hook");
    }

    #[test]
    fn test_retry_config_defaults_and_backoff() {
        let config = HttpRetryConfig::from_properties(&HashMap::new()).unwrap();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.delay_for(1), Duration::from_millis(100));
        assert_eq!(config.delay_for(2), Duration::from_millis(200));
        assert_eq!(config.delay_for(3), Duration::from_millis(400));
        // Capped at the ceiling
        assert_eq!(config.delay_for(20), Duration::from_millis(10_000));
    }

    #[test]
    fn test_retry_config_rejects_sub_one_multiplier() {
        for bad in ["0.5", "NaN"] {
            let mut props = HashMap::new();
            props.insert("http.retry.backoff-multiplier".to_string(), bad.to_string());
            assert!(
                HttpRetryConfig::from_properties(&props)
                    .unwrap_err()
                    .contains("backoff-multiplier"),
                "'{bad}' must be rejected"
            );
        }
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret2"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_retryable_statuses() {
        assert!(is_retryable_status(408));
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(503));
        assert!(!is_retryable_status(200));
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(404));
    }
}
