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

//! Small helpers shared by connector configuration parsers.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

/// A secret configuration value (password, token, credential-bearing header)
/// whose `Debug`/`Display` output is always `***`. The raw value must be
/// requested explicitly via [`Redacted::expose`], so a `{:?}` of a connector
/// config can never leak credentials into logs (#143).
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Redacted(String);

impl Redacted {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The actual secret — call only at the point of use (connection
    /// building, header injection), never in log/format paths.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for Redacted {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Redacted {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl std::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

impl Display for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

/// Key injected by `stream_initializer` into the properties map passed to
/// source/sink factories, carrying the stream's `format` (e.g. "json").
/// The leading underscore marks it as reserved/injected — factories must not
/// accept it from user configuration under another meaning.
pub const INJECTED_FORMAT_KEY: &str = "_format";

/// Fetch a required property, rejecting missing or blank values.
pub fn parse_required(properties: &HashMap<String, String>, key: &str) -> Result<String, String> {
    let value = properties
        .get(key)
        .ok_or_else(|| format!("Missing required property: {key}"))?;
    if value.trim().is_empty() {
        return Err(format!("{key} cannot be empty"));
    }
    Ok(value.clone())
}

/// Parse an optional property, falling back to `default` when absent.
pub fn parse_or<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> Result<T, String>
where
    T::Err: Display,
{
    match properties.get(key) {
        Some(v) => v.parse().map_err(|e| format!("Invalid {key}: {e}")),
        None => Ok(default),
    }
}

/// Collect `<prefix><Name>` properties into `(Name, value)` pairs — the
/// header-map convention used by the WebSocket and HTTP connectors
/// (e.g. `http.headers.Authorization`).
pub fn parse_prefixed(properties: &HashMap<String, String>, prefix: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = properties
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(prefix)
                .filter(|name| !name.is_empty())
                .map(|name| (name.to_string(), v.clone()))
        })
        .collect();
    // HashMap iteration order is arbitrary — sort for deterministic behavior
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_or_default_value_and_error() {
        let mut props = HashMap::new();
        assert_eq!(parse_or(&props, "k", 5u64).unwrap(), 5);

        props.insert("k".to_string(), "9".to_string());
        assert_eq!(parse_or(&props, "k", 5u64).unwrap(), 9);

        props.insert("k".to_string(), "no".to_string());
        assert!(parse_or(&props, "k", 5u64)
            .unwrap_err()
            .contains("Invalid k"));
    }

    #[test]
    fn test_redacted_never_prints_the_secret() {
        let secret = Redacted::new("hunter2");
        assert_eq!(format!("{secret}"), "***");
        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(format!("{:?}", Some(&secret)), "Some(***)");
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn test_parse_required() {
        let mut props = HashMap::new();
        assert!(parse_required(&props, "k")
            .unwrap_err()
            .contains("Missing required property: k"));

        props.insert("k".to_string(), "  ".to_string());
        assert!(parse_required(&props, "k").unwrap_err().contains("empty"));

        props.insert("k".to_string(), "v".to_string());
        assert_eq!(parse_required(&props, "k").unwrap(), "v");
    }

    #[test]
    fn test_parse_prefixed() {
        let mut props = HashMap::new();
        props.insert("http.headers.B-Second".to_string(), "2".to_string());
        props.insert("http.headers.A-First".to_string(), "1".to_string());
        props.insert("http.url".to_string(), "x".to_string());
        props.insert("http.headers.".to_string(), "empty-name".to_string());

        let headers = parse_prefixed(&props, "http.headers.");
        assert_eq!(
            headers,
            vec![
                ("A-First".to_string(), "1".to_string()),
                ("B-Second".to_string(), "2".to_string()),
            ]
        );
    }
}
