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

//! # Data Mapping System
//!
//! Provides mappers for transforming raw data to/from EventFlux events.
//!
//! ## Mapper Types
//!
//! - **SourceMapper**: Maps raw bytes (JSON, CSV, etc.) → EventFlux events
//! - **SinkMapper**: Maps EventFlux events → raw bytes for external systems
//!
//! ## Auto-Mapping Policy (All-or-Nothing)
//!
//! EventFlux enforces strict auto-mapping rules:
//! - **NO `mapping.*` properties** → Auto-map ALL top-level fields by name
//! - **ANY `mapping.*` properties** → Explicitly map ALL fields (no auto-mapping)
//!
//! ## Supported Formats
//!
//! - JSON (with JSONPath extraction)
//! - CSV (with field mapping)
//!
//! ## Configuration Example
//!
//! ```toml
//! [streams.InputJSON]
//! type = "source"
//! format = "json"
//! json.mapping.orderId = "$.order.id"
//! json.mapping.amount = "$.order.total"
//!
//! [streams.OutputJSON]
//! type = "sink"
//! format = "json"
//! json.template = "{\"eventType\":\"ORDER\",\"id\":\"{{orderId}}\",\"amount\":{{amount}}}"
//! ```

pub mod bytes_mapper;
pub mod csv_mapper;
pub mod factory;
pub mod json_mapper;
pub mod validation;

use crate::core::event::Event;
use crate::core::exception::EventFluxError;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in epoch milliseconds, used as the timestamp of
/// mapped events. Returns 0 if the system clock is set before the epoch.
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Order explicit `mapping.*` entries alphabetically by field name.
///
/// Used when no stream schema is available: HashMap iteration order is
/// non-deterministic, so a stable order is required.
pub(crate) fn alphabetical_mappings<V: Clone>(mappings: &HashMap<String, V>) -> Vec<(String, V)> {
    let mut ordered: Vec<(String, V)> = mappings
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    ordered
}

/// Match schema field names to keys (JSON object keys or `mapping.*` keys)
/// one-to-one, returning the matched key for each field in schema order.
///
/// Exact matches are resolved first. A remaining field then matches a key
/// ASCII case-insensitively only when the pairing is unambiguous in both
/// directions: the field has exactly one unclaimed candidate key, and that
/// key matches no other unresolved field. Anything else is reported as an
/// error rather than guessed, so the outcome never depends on key order and
/// one key never feeds two fields. A field with no candidate maps to `None`.
pub(crate) fn match_field_names<'k>(
    field_names: &[String],
    keys: &[&'k str],
) -> Result<Vec<Option<&'k str>>, String> {
    let mut matched: Vec<Option<&'k str>> = field_names
        .iter()
        .map(|name| keys.iter().copied().find(|key| *key == name))
        .collect();

    let unclaimed: Vec<&'k str> = keys
        .iter()
        .copied()
        .filter(|key| !matched.contains(&Some(*key)))
        .collect();
    let unresolved: Vec<usize> = (0..field_names.len())
        .filter(|&i| matched[i].is_none())
        .collect();

    for &i in &unresolved {
        let field = &field_names[i];
        let candidates: Vec<&str> = unclaimed
            .iter()
            .copied()
            .filter(|key| key.eq_ignore_ascii_case(field))
            .collect();
        match candidates.as_slice() {
            [] => {}
            [key] => {
                let rivals: Vec<&str> = unresolved
                    .iter()
                    .filter(|&&j| j != i && field_names[j].eq_ignore_ascii_case(key))
                    .map(|&j| field_names[j].as_str())
                    .collect();
                if !rivals.is_empty() {
                    return Err(format!(
                        "key '{key}' matches stream fields '{field}' and '{}' only by \
                         case; use the exact field name",
                        rivals.join("', '")
                    ));
                }
                matched[i] = Some(key);
            }
            _ => {
                return Err(format!(
                    "stream field '{field}' matches keys '{}' only by case; use the \
                     exact field name",
                    candidates.join("', '")
                ));
            }
        }
    }

    Ok(matched)
}

/// Order explicit `mapping.*` entries by stream schema, validating fail-fast.
///
/// Fields are paired with mapping keys by [`match_field_names`]. Fails if a
/// schema field has no mapping, if a pairing is ambiguous, or if a mapping
/// key matches no schema field (typo or stale entry).
/// `format` is the property prefix (e.g. "json") used in error messages.
pub(crate) fn schema_ordered_mappings<V: Clone>(
    field_names: &[String],
    mappings: &HashMap<String, V>,
    format: &str,
) -> Result<Vec<(String, V)>, EventFluxError> {
    let config_error = |message: String, key: &str| EventFluxError::Configuration {
        message,
        config_key: Some(format!("{format}.mapping.{key}")),
    };

    // Sorted so error messages are stable; the matching itself is order-independent
    let mut keys: Vec<&str> = mappings.keys().map(String::as_str).collect();
    keys.sort_unstable();

    let matched =
        match_field_names(field_names, &keys).map_err(|message| EventFluxError::Configuration {
            message: format!("Ambiguous '{format}.mapping.*' configuration: {message}"),
            config_key: None,
        })?;

    let mut ordered = Vec::with_capacity(field_names.len());
    for (field_name, key) in field_names.iter().zip(&matched) {
        let Some(key) = key else {
            return Err(config_error(
                format!(
                    "No mapping configured for stream field '{field_name}'. When any \
                     '{format}.mapping.*' property is set, every stream field must be \
                     explicitly mapped."
                ),
                field_name,
            ));
        };
        ordered.push((field_name.clone(), mappings[*key].clone()));
    }

    if let Some(stray) = keys.iter().find(|key| !matched.contains(&Some(**key))) {
        return Err(config_error(
            format!(
                "Mapping key '{stray}' does not match any stream field. \
                 Remove it or rename it to a declared field."
            ),
            stray,
        ));
    }

    Ok(ordered)
}

/// Trait for mapping raw bytes to EventFlux events (for source streams)
///
/// Mappers are fully configured and ready to use when created via factories.
/// The `map` method handles all parsing, extraction, and error handling.
pub trait SourceMapper: Debug + Send + Sync {
    /// Map raw input bytes to EventFlux events
    ///
    /// # Arguments
    /// * `input` - Raw bytes from external source (JSON, CSV, etc.)
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Successfully mapped events (may contain multiple events for batch processing)
    /// * `Err(EventFluxError)` - Mapping failed due to malformed input or extraction errors
    ///
    /// # Implementation Notes
    /// - Must handle malformed input gracefully
    /// - May return multiple events for array/batch inputs
    /// - Should validate data against expected schema when possible
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError>;

    /// Clone this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SourceMapper>;
}

impl Clone for Box<dyn SourceMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Trait for mapping EventFlux events to raw bytes (for sink streams)
///
/// Mappers are fully configured and ready to use when created via factories.
/// The `map_event` method handles all serialization and template rendering.
///
/// # One event = one transport message
///
/// Mappers serialize exactly ONE event into ONE payload. `SinkCallbackAdapter`
/// iterates batches and calls `map_event` + `Sink::publish` per event, matching
/// the per-record semantics of real transports (Kafka record, AMQP message,
/// WebSocket frame). Sinks that want transport-level batching (e.g. HTTP batch
/// POST) must buffer internally — batching is a transport concern, not a
/// formatting concern.
pub trait SinkMapper: Debug + Send + Sync {
    /// Map ONE EventFlux event to one transport payload
    ///
    /// # Arguments
    /// * `event` - EventFlux event to serialize
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Successfully serialized bytes ready for external sink
    /// * `Err(EventFluxError)` - Mapping failed due to serialization errors
    ///
    /// # Implementation Notes
    /// - Must handle template rendering errors gracefully
    /// - Output format must match sink expectations
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError>;

    /// Clone this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SinkMapper>;
}

impl Clone for Box<dyn SinkMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[cfg(test)]
mod tests {
    use super::match_field_names;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_match_exact_then_unambiguous_case_insensitive() {
        let fields = names(&["price", "symbol", "volume"]);
        let matched = match_field_names(&fields, &["SYMBOL", "price", "Volume"]).unwrap();
        assert_eq!(matched, vec![Some("price"), Some("SYMBOL"), Some("Volume")]);
    }

    #[test]
    fn test_match_unmatched_field_is_none() {
        let fields = names(&["a", "b"]);
        let matched = match_field_names(&fields, &["a"]).unwrap();
        assert_eq!(matched, vec![Some("a"), None]);
    }

    #[test]
    fn test_match_exact_key_is_never_reused_by_case_variant_field() {
        // "id" takes its exact key; "ID" must not fall back to it
        let fields = names(&["ID", "id"]);
        let matched = match_field_names(&fields, &["id"]).unwrap();
        assert_eq!(matched, vec![None, Some("id")]);
    }

    #[test]
    fn test_match_field_with_two_case_candidates_is_ambiguous() {
        let fields = names(&["id"]);
        let err = match_field_names(&fields, &["Id", "iD"]).unwrap_err();
        assert!(err.contains("'id'"), "got: {err}");
    }

    #[test]
    fn test_match_key_fitting_two_fields_by_case_is_ambiguous() {
        let fields = names(&["foo", "FOO"]);
        let err = match_field_names(&fields, &["Foo"]).unwrap_err();
        assert!(err.contains("'Foo'"), "got: {err}");
    }

    #[test]
    fn test_match_result_is_independent_of_key_order() {
        let fields = names(&["price", "symbol", "foo", "FOO"]);
        let forward = ["Price", "symbol", "foo", "FOO"];
        let mut reversed = forward;
        reversed.reverse();
        assert_eq!(
            match_field_names(&fields, &forward).unwrap(),
            match_field_names(&fields, &reversed).unwrap()
        );

        let ambiguous = ["Foo", "fOO"];
        let mut ambiguous_rev = ambiguous;
        ambiguous_rev.reverse();
        let fields = names(&["foo", "FOO"]);
        assert!(match_field_names(&fields, &ambiguous).is_err());
        assert!(match_field_names(&fields, &ambiguous_rev).is_err());
    }
}
