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
//! Mappers that turn raw data into EventFlux events and back.
//!
//! ## Mapper Types
//!
//! - **SourceMapper**: maps raw bytes (JSON, CSV, and so on) to EventFlux events
//! - **SinkMapper**: maps EventFlux events to raw bytes for outside systems
//!
//! ## Auto Mapping Rule (All or Nothing)
//!
//! - **NO `mapping.*` properties**: auto map ALL fields. JSON matches top level
//!   keys by schema field name when the schema is known. CSV maps by position.
//! - **ANY `mapping.*` property**: ALL fields must be mapped explicitly, with no
//!   auto mapping
//!
//! ## Supported Formats
//!
//! - JSON (with JSONPath)
//! - CSV (with column mapping)
//! - bytes (raw binary passed through as is)
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

/// Current wall clock time in milliseconds since the Unix epoch, used as the
/// timestamp of mapped events. Returns 0 if the system clock is set before
/// the epoch.
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Sorts explicit `mapping.*` entries by field name.
///
/// Used when there is no stream schema. HashMap order is random, so a fixed
/// order is needed.
pub(crate) fn alphabetical_mappings<V: Clone>(mappings: &HashMap<String, V>) -> Vec<(String, V)> {
    let mut ordered: Vec<(String, V)> = mappings
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    ordered
}

/// Returns the first field name declared more than once (exact match), if any.
fn first_duplicate(field_names: &[String]) -> Option<&str> {
    field_names
        .iter()
        .enumerate()
        .find(|(i, name)| field_names[..*i].contains(name))
        .map(|(_, name)| name.as_str())
}

/// Rejects a schema that declares the same field name twice (exact match).
///
/// Stream definitions only warn about duplicate attribute names, but mapping
/// by name cannot tell the two positions apart. One JSON key or `mapping.*`
/// entry would feed both, or one output key would overwrite the other. Called
/// when a mapper is set up, so the app fails when it is created.
///
/// # Errors
///
/// Returns a configuration error naming the first duplicate field.
pub(crate) fn ensure_unique_field_names(
    field_names: &[String],
    format: &str,
) -> Result<(), EventFluxError> {
    match first_duplicate(field_names) {
        Some(name) => Err(EventFluxError::Configuration {
            message: format!(
                "Stream field '{name}' is declared more than once; the '{format}' mapper \
                 maps fields by name and cannot tell the duplicates apart. Rename one of them."
            ),
            config_key: None,
        }),
        None => Ok(()),
    }
}

/// Pairs schema field names with keys (JSON object keys or `mapping.*` keys),
/// one key per field. Returns the matched key for each field, in schema order.
///
/// Field names must be unique. Exact matches are found first. A field left
/// over then matches a key ignoring ASCII case only when the pair is clear
/// both ways. The field must have exactly one key that differs only by case
/// and is not taken, and that key must match no other field left over.
/// Anything else is an error instead of a guess, so the result never depends
/// on key order and one key never feeds two fields. A field with no key gives
/// `None`.
///
/// # Errors
///
/// Returns an error message when a field name is declared twice or a match
/// ignoring case is ambiguous.
pub(crate) fn match_field_names<'k>(
    field_names: &[String],
    keys: &[&'k str],
) -> Result<Vec<Option<&'k str>>, String> {
    if let Some(name) = first_duplicate(field_names) {
        return Err(format!("stream field '{name}' is declared more than once"));
    }

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

/// Works out the output order for explicit `mapping.*` entries from scratch.
///
/// Schema order when `field_names` is not empty (checked, see
/// [`schema_ordered_mappings`]), alphabetical when there is no schema, and
/// empty when there are no explicit mappings. Mappers call this on every
/// schema change, so a cleared or replaced schema never leaves an old order
/// behind.
///
/// # Errors
///
/// Returns the error from [`schema_ordered_mappings`].
pub(crate) fn order_mappings<V: Clone>(
    field_names: &[String],
    mappings: &HashMap<String, V>,
    format: &str,
) -> Result<Vec<(String, V)>, EventFluxError> {
    if mappings.is_empty() {
        Ok(Vec::new())
    } else if field_names.is_empty() {
        Ok(alphabetical_mappings(mappings))
    } else {
        schema_ordered_mappings(field_names, mappings, format)
    }
}

/// Puts explicit `mapping.*` entries in stream schema order and checks them
/// right away.
///
/// Fields are paired with mapping keys by [`match_field_names`]. `format` is
/// the property prefix, such as "json", used in error messages.
///
/// # Errors
///
/// Returns a configuration error when a field name is declared twice, a
/// schema field has no mapping, a pair is ambiguous, or a mapping key matches
/// no schema field (a typo or an old entry).
fn schema_ordered_mappings<V: Clone>(
    field_names: &[String],
    mappings: &HashMap<String, V>,
    format: &str,
) -> Result<Vec<(String, V)>, EventFluxError> {
    ensure_unique_field_names(field_names, format)?;

    let config_error = |message: String, key: &str| EventFluxError::Configuration {
        message,
        config_key: Some(format!("{format}.mapping.{key}")),
    };

    // Sorted so error messages are always the same. The matching itself does
    // not depend on order.
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

/// Maps raw bytes to EventFlux events, for source streams.
///
/// Mappers built by the factories are fully set up and ready to use. `map`
/// does all the parsing, field lookup, and error handling.
pub trait SourceMapper: Debug + Send + Sync {
    /// Maps raw input bytes to EventFlux events.
    ///
    /// # Arguments
    /// * `input` - Raw bytes from the outside source (JSON, CSV, and so on)
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - The mapped events. May hold several events for a
    ///   batch input, or none when input is skipped.
    /// * `Err(EventFluxError)` - Mapping failed because the input is broken or a
    ///   field could not be read
    ///
    /// # Implementation Notes
    /// - Must return an error for broken input, not panic
    /// - May return several events for array or batch inputs
    /// - Should check data against the expected schema when it can
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError>;

    /// Clones this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SourceMapper>;
}

impl Clone for Box<dyn SourceMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Maps EventFlux events to raw bytes, for sink streams.
///
/// Mappers built by the factories are fully set up and ready to use.
/// `map_event` does all the encoding and template filling.
///
/// # One event gives one transport message
///
/// Mappers encode exactly ONE event into ONE payload. `SinkCallbackAdapter`
/// walks through batches and calls `map_event` and `Sink::publish` for each
/// event. This matches how real transports work one record at a time (a Kafka
/// record, an AMQP message, a WebSocket frame). Sinks that want to batch on the
/// wire, such as an HTTP batch POST, must buffer on their own. Batching belongs
/// to the transport, not to the format.
pub trait SinkMapper: Debug + Send + Sync {
    /// Maps ONE EventFlux event to one transport payload.
    ///
    /// # Arguments
    /// * `event` - EventFlux event to encode
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The encoded bytes, ready for the outside sink
    /// * `Err(EventFluxError)` - Encoding failed
    ///
    /// # Implementation Notes
    /// - Must return an error when a template cannot be filled, not panic
    /// - The output format must match what the sink expects
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError>;

    /// Clones this mapper into a boxed trait object
    fn clone_box(&self) -> Box<dyn SinkMapper>;
}

impl Clone for Box<dyn SinkMapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[cfg(test)]
mod tests {
    use super::{ensure_unique_field_names, match_field_names};

    #[test]
    fn test_match_rejects_duplicate_field_names() {
        // (id, id) must never map one key into both positions
        let fields = names(&["id", "id"]);
        let err = match_field_names(&fields, &["id"]).unwrap_err();
        assert!(err.contains("more than once"), "got: {err}");
    }

    #[test]
    fn test_ensure_unique_field_names() {
        assert!(ensure_unique_field_names(&names(&["id", "ID", "name"]), "json").is_ok());
        let err = ensure_unique_field_names(&names(&["a", "id", "b", "id"]), "json").unwrap_err();
        assert!(err.to_string().contains("'id'"), "got: {err}");
    }

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
        // "id" takes its exact key, and "ID" must not fall back to it
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
