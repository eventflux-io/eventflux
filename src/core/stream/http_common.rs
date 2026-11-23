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
use crate::core::stream::connector_util::{parse_or, parse_prefixed, Redacted};
use std::collections::HashMap;
use std::time::Duration;

/// Reads request headers from `http.headers.<Name>` properties, sorted by name.
///
/// Values are wrapped in [`Redacted`] because headers often carry credentials,
/// such as `Authorization` or API keys. A Debug print of a config shows only the
/// names. Put secrets in environment variables, for example
/// `"http.headers.Authorization" = 'Bearer ${TOKEN}'`.
pub fn parse_headers(properties: &HashMap<String, String>) -> Vec<(String, Redacted)> {
    parse_prefixed(properties, "http.headers.")
        .into_iter()
        .map(|(name, value)| (name, Redacted::new(value)))
        .collect()
}

/// Reads the `http.url` property and returns it trimmed.
///
/// # Errors
///
/// Returns an error when the property is missing or does not start with
/// `http://` or `https://`.
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

/// Retry policy for outgoing requests, read from `http.retry.*`. The wait grows
/// by `backoff_multiplier` after each retry, up to `max_delay_ms`.
#[derive(Debug, Clone)]
pub struct HttpRetryConfig {
    /// Retries after the first attempt. 0 turns retrying off. Default 3.
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

impl HttpRetryConfig {
    /// Reads the policy from properties. Defaults are 3 retries, 100 ms first
    /// delay, 10000 ms max delay, and a multiplier of 2.0.
    ///
    /// # Errors
    ///
    /// Returns an error when a value does not parse, or the multiplier is NaN or
    /// below 1.0.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let config = Self {
            max_attempts: parse_or(properties, "http.retry.max-attempts", 3)?,
            initial_delay_ms: parse_or(properties, "http.retry.initial-delay-ms", 100)?,
            max_delay_ms: parse_or(properties, "http.retry.max-delay-ms", 10_000)?,
            backoff_multiplier: parse_or(properties, "http.retry.backoff-multiplier", 2.0)?,
        };
        // NaN fails every comparison, so it needs its own check
        if config.backoff_multiplier.is_nan() || config.backoff_multiplier < 1.0 {
            return Err(format!(
                "Invalid http.retry.backoff-multiplier '{}': must be >= 1.0",
                config.backoff_multiplier
            ));
        }
        Ok(config)
    }

    /// Returns the wait before retry number `retry`, counting from 1. Never more
    /// than `max_delay_ms`.
    pub fn delay_for(&self, retry: u32) -> Duration {
        crate::core::error::retry::exponential_backoff_with_multiplier(
            retry as usize,
            Duration::from_millis(self.initial_delay_ms),
            Duration::from_millis(self.max_delay_ms),
            self.backoff_multiplier,
        )
    }
}

/// Reads an optional HTTP method property and returns it in uppercase. Uses
/// `default` as given when the key is missing.
///
/// # Errors
///
/// Returns an error when the method is not in `allowed`.
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

/// Builds a request with the configured headers set. The caller adds the body.
/// Kept in one place so each call site does not copy it.
pub fn build_request(
    method: &str,
    url: &str,
    headers: &[(String, Redacted)],
) -> ureq::http::request::Builder {
    // Use ureq's own `http` re-export so the crate version matches its API
    let mut request = ureq::http::Request::builder().method(method).uri(url);
    for (name, value) in headers {
        request = request.header(name, value.expose());
    }
    request
}

/// Compares a presented secret with the expected one. It does not stop at the
/// first byte that differs, so response timing does not reveal how much of the
/// token matched. A length mismatch still returns early, since the token length
/// is not the secret.
pub fn constant_time_eq(presented: &[u8], expected: &[u8]) -> bool {
    presented.len() == expected.len()
        && presented
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// Returns true when a response status is worth a retry. That is 408 (request
/// timeout), 429 (rate limited), and any 5xx server error. Other 4xx codes mean
/// the request itself is wrong, and a retry cannot fix that.
pub fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

/// Builds a blocking client. `timeout_ms` caps the whole of each request.
/// Non 2xx statuses come back as responses, not errors, so callers can check
/// the status code.
///
/// Redirects are never followed. By default ureq would turn a redirected
/// POST or PUT into a GET with no body, as 301, 302, and 303 allow. The sink
/// would then report a dropped payload as delivered. A 3xx instead reaches the
/// caller as a non 2xx status. Point the URL at the final location.
pub fn build_agent(timeout_ms: u64) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .http_status_as_error(false)
        .max_redirects(0)
        .build()
        .into()
}

/// Checks at startup that the endpoint can be reached. Any HTTP response,
/// even a 4xx or 405, counts as reachable.
///
/// # Errors
///
/// Returns a configuration error when the request cannot be built, and
/// `ConnectionUnavailable` on a transport failure such as a failed connect,
/// a timeout, or a TLS error.
pub fn probe_reachability(
    agent: &ureq::Agent,
    method: &str,
    url: &str,
    headers: &[(String, Redacted)],
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
        // Capped at max_delay_ms
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
