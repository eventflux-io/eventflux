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

//! JSON Mapper Implementation
//!
//! Maps JSON to EventFlux events and back.
//!
//! ## Source Mapping (JSON → Events)
//!
//! ### Auto Mapping (no `mapping.*` properties)
//! ```json
//! {"orderId": "123", "amount": 100.0}
//! ```
//! Maps the top level fields. By schema field name when the schema is known,
//! otherwise in JSON key order.
//!
//! ### Explicit Mapping (with `mapping.*` properties)
//! ```toml
//! json.mapping.orderId = "$.order.id"
//! json.mapping.amount = "$.order.total"
//! ```
//! Reads nested fields with JSONPath.
//!
//! ## Sink Mapping (Events → JSON)
//!
//! ### Plain Output (no template)
//! Turns each event into a JSON object keyed by the schema field names.
//!
//! ### Template Output (with a template)
//! ```toml
//! json.template = "{\"eventType\":\"ORDER\",\"id\":\"{{orderId}}\",\"amount\":{{amount}}}"
//! ```
//! Note that `render_template` only fills `{{field_N}}` (by index) and
//! `{{_timestamp}}`. Named placeholders like the ones above are left as is.

use super::{
    alphabetical_mappings, ensure_unique_field_names, match_field_names, now_millis,
    order_mappings, SinkMapper, SourceMapper,
};
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Source mapper for JSON format. Each JSON payload becomes one event with the
/// current time as its timestamp.
#[derive(Debug, Clone)]
pub struct JsonSourceMapper {
    /// Field name to JSONPath. Empty means auto map the top level fields.
    mappings: HashMap<String, String>,
    /// Return no events for JSON that does not parse, instead of an error
    ignore_parse_errors: bool,
    /// Optional chrono date format for parsing timestamps
    date_format: Option<String>,
    /// Largest input in bytes. Default 10 MB.
    max_input_size: usize,
    /// Deepest JSONPath allowed. Default 32.
    max_nesting_depth: usize,
    /// Stream schema field names in schema order. Never `Some(empty)`.
    /// Turns on mapping by name in auto mapping (see `auto_map`).
    field_names: Option<Vec<String>>,
    /// Explicit mappings as (field name, JSONPath) in output order.
    /// Worked out at config time. Schema order when field names are set,
    /// otherwise alphabetical, since HashMap order is random.
    ordered_mappings: Vec<(String, String)>,
}

impl JsonSourceMapper {
    /// Creates a JSON source mapper with default settings: auto mapping,
    /// 10 MB input limit, and a JSONPath depth limit of 32
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            ignore_parse_errors: false,
            date_format: None,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_nesting_depth: 32,            // 32 levels default
            field_names: None,
            ordered_mappings: Vec::new(),
        }
    }

    /// Creates a JSON source mapper with explicit JSONPath mappings. Output is
    /// in alphabetical field order until `set_field_names` is called.
    pub fn with_mappings(mappings: HashMap<String, String>) -> Self {
        let ordered_mappings = alphabetical_mappings(&mappings);
        Self {
            mappings,
            ignore_parse_errors: false,
            date_format: None,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_nesting_depth: 32,            // 32 levels default
            field_names: None,
            ordered_mappings,
        }
    }

    /// Sets the stream schema field names, in schema order.
    ///
    /// Turns on mapping by name in auto mapping. Each schema field is found in
    /// the JSON object by exact key, then by key ignoring ASCII case. When no
    /// schema field matches any JSON key, values are taken by position. An
    /// empty list clears the schema. Auto mapping then goes back to JSON key
    /// order and explicit mappings go back to alphabetical order.
    ///
    /// With explicit `mapping.*` properties, this checks the config right away.
    /// Every schema field must have a mapping (matched exactly or ignoring
    /// case) and every mapping key must name a schema field. Output follows
    /// schema order.
    ///
    /// # Errors
    ///
    /// Returns an error when a field name appears more than once, or the
    /// fields and mappings do not match. The mapper is then left unchanged.
    pub fn set_field_names(&mut self, names: Vec<String>) -> Result<(), EventFluxError> {
        ensure_unique_field_names(&names, "json")?;
        let ordered_mappings = order_mappings(&names, &self.mappings, "json")?;

        self.ordered_mappings = ordered_mappings;
        self.field_names = (!names.is_empty()).then_some(names);
        Ok(())
    }

    /// Sets whether JSON that does not parse gives no events instead of an
    /// error. Only covers parsing, not mapping errors.
    pub fn set_ignore_parse_errors(&mut self, ignore: bool) {
        self.ignore_parse_errors = ignore;
    }

    /// Sets the date format for parsing timestamps.
    ///
    /// Accepts Java SimpleDateFormat and chrono patterns. Java patterns are
    /// turned into chrono patterns. `None` clears it.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut mapper = JsonSourceMapper::new();
    ///
    /// // Java format - automatically converted
    /// mapper.set_date_format(Some("yyyy-MM-dd'T'HH:mm:ss".to_string()));
    ///
    /// // Chrono format - passed through unchanged
    /// mapper.set_date_format(Some("%Y-%m-%dT%H:%M:%S".to_string()));
    ///
    /// // Both work identically
    /// ```
    pub fn set_date_format(&mut self, format: Option<String>) {
        self.date_format = format.map(|f| convert_java_date_format(&f));
    }

    /// Sets the largest input in bytes, to guard against huge inputs
    pub fn set_max_input_size(&mut self, max_size: usize) {
        self.max_input_size = max_size;
    }

    /// Sets the deepest JSONPath allowed, to guard against costly paths
    pub fn set_max_nesting_depth(&mut self, max_depth: usize) {
        self.max_nesting_depth = max_depth;
    }

    /// Maps the top level JSON fields to event attributes.
    ///
    /// With schema field names, each field is found by exact JSON key, then by
    /// one clear match ignoring ASCII case. Each JSON key serves at most one
    /// field (see `match_field_names`). A match that is ambiguous by case fails
    /// the event instead of guessing. When at least one field matches by name,
    /// the payload counts as named, and fields with no match become Null, so a
    /// value is never used twice. Only when NO field matches by name does it
    /// take values by position. serde_json's `preserve_order` keeps the
    /// incoming key order for that.
    ///
    /// Without schema field names, all top level fields are mapped in JSON key
    /// order, which is the older behaviour.
    ///
    /// # Errors
    ///
    /// Returns `MappingFailed` when the root is not an object, a key match is
    /// ambiguous, or a value cannot be turned into an attribute.
    fn auto_map(&self, json: &JsonValue) -> Result<Vec<Event>, EventFluxError> {
        let obj = json
            .as_object()
            .ok_or_else(|| EventFluxError::MappingFailed {
                message: "JSON root must be an object for auto-mapping".to_string(),
                source: None,
            })?;

        let to_attribute = |value: Option<&JsonValue>| match value {
            Some(v) => json_value_to_attribute(v, self.date_format.as_deref()),
            None => Ok(AttributeValue::Null),
        };

        let event_data: Result<Vec<AttributeValue>, EventFluxError> = match &self.field_names {
            Some(names) => {
                let matched = match_fields(obj, names)?;
                if matched.iter().any(Option::is_some) {
                    // Named payload, so fields with no match become Null
                    matched.into_iter().map(to_attribute).collect()
                } else {
                    // No key matched any schema field, so take values by
                    // position. Missing values become Null and extras are dropped.
                    let mut values = obj.values();
                    names.iter().map(|_| to_attribute(values.next())).collect()
                }
            }
            // No schema, so keep the JSON key order
            None => obj.values().map(|v| to_attribute(Some(v))).collect(),
        };
        Ok(vec![Event::new_with_data(now_millis(), event_data?)])
    }

    /// Reads fields with the explicit JSONPath mappings.
    ///
    /// Uses the (field, path) order worked out at config time. That is schema
    /// order when schema field names were set (checked for gaps in
    /// `set_field_names`), otherwise alphabetical.
    ///
    /// # Errors
    ///
    /// Returns the error from `extract_json_path` or from turning a value into
    /// an attribute.
    fn extract_with_mappings(&self, json: &JsonValue) -> Result<Vec<Event>, EventFluxError> {
        let mut event_data = Vec::with_capacity(self.ordered_mappings.len());

        for (_field_name, json_path) in &self.ordered_mappings {
            let value = extract_json_path(json, json_path, self.max_nesting_depth)?;
            event_data.push(json_value_to_attribute(
                &value,
                self.date_format.as_deref(),
            )?);
        }

        Ok(vec![Event::new_with_data(now_millis(), event_data)])
    }
}

/// Matches schema fields to top level JSON values with [`match_field_names`].
///
/// Shortcut: when every field matches a key exactly, which is the usual case,
/// the general matcher and its key list are skipped.
///
/// # Errors
///
/// Returns `MappingFailed` when a match ignoring case is ambiguous.
fn match_fields<'a>(
    obj: &'a serde_json::Map<String, JsonValue>,
    names: &[String],
) -> Result<Vec<Option<&'a JsonValue>>, EventFluxError> {
    let exact: Vec<Option<&'a JsonValue>> = names.iter().map(|name| obj.get(name)).collect();
    if exact.iter().all(Option::is_some) {
        return Ok(exact);
    }

    let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    let matched =
        match_field_names(names, &keys).map_err(|message| EventFluxError::MappingFailed {
            message: format!("Ambiguous JSON keys: {message}"),
            source: None,
        })?;
    Ok(matched
        .into_iter()
        .map(|key| key.and_then(|k| obj.get(k)))
        .collect())
}

impl Default for JsonSourceMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMapper for JsonSourceMapper {
    fn map(&self, input: &[u8]) -> Result<Vec<Event>, EventFluxError> {
        // Reject huge inputs so a sender cannot exhaust memory
        if input.len() > self.max_input_size {
            return Err(EventFluxError::MappingFailed {
                message: format!(
                    "Input size {} bytes exceeds maximum allowed {} bytes",
                    input.len(),
                    self.max_input_size
                ),
                source: None,
            });
        }

        let json: JsonValue = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => {
                if self.ignore_parse_errors {
                    // Skip this payload by returning no events
                    return Ok(Vec::new());
                } else {
                    return Err(EventFluxError::MappingFailed {
                        message: format!("JSON parse error: {}", e),
                        source: Some(Box::new(e)),
                    });
                }
            }
        };

        // All or nothing. With any explicit mapping, only mapped fields are read.
        if self.mappings.is_empty() {
            self.auto_map(&json)
        } else {
            self.extract_with_mappings(&json)
        }
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(self.clone())
    }
}

/// Sink mapper for JSON format
#[derive(Debug, Clone)]
pub struct JsonSinkMapper {
    /// Optional template for custom JSON output, with `{{field_N}}` and
    /// `{{_timestamp}}` placeholders (see `render_template`)
    template: Option<String>,
    /// Pretty print the JSON with indentation. Has no effect on template output.
    pretty_print: bool,
    /// Schema field names used as JSON keys. Without them the keys are
    /// field_0, field_1, and so on.
    field_names: Option<Vec<String>>,
}

impl JsonSinkMapper {
    /// Creates a JSON sink mapper with no template, which writes plain JSON objects
    pub fn new() -> Self {
        Self {
            template: None,
            pretty_print: false,
            field_names: None,
        }
    }

    /// Creates a JSON sink mapper that fills in a template
    pub fn with_template(template: String) -> Self {
        Self {
            template: Some(template),
            pretty_print: false,
            field_names: None,
        }
    }

    /// Turns pretty printing with indentation on or off. Template output is
    /// not affected.
    pub fn set_pretty_print(&mut self, pretty: bool) {
        self.pretty_print = pretty;
    }

    /// Sets the schema field names used as JSON keys. Not used by template output.
    pub fn set_field_names(&mut self, names: Vec<String>) {
        self.field_names = Some(names);
    }
}

impl Default for JsonSinkMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkMapper for JsonSinkMapper {
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError> {
        if let Some(template) = &self.template {
            let rendered = render_template(template, event)?;
            Ok(rendered.into_bytes())
        } else {
            let json_value = event_to_json_with_names(event, self.field_names.as_deref())?;
            let json_str = if self.pretty_print {
                serde_json::to_string_pretty(&json_value)
            } else {
                serde_json::to_string(&json_value)
            }
            .map_err(|e| EventFluxError::MappingFailed {
                message: format!("JSON serialization error: {}", e),
                source: Some(Box::new(e)),
            })?;
            Ok(json_str.into_bytes())
        }
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Date Format Conversion
// ============================================================================

/// Turns a Java SimpleDateFormat pattern into a chrono pattern.
///
/// EventFlux config uses Java SimpleDateFormat patterns to match Java based CEP
/// systems, but the Rust code uses chrono. This function does the conversion.
/// A pattern that already holds `%` is taken as chrono and returned as is.
///
/// # Common Pattern Conversions
///
/// | Java Pattern | Chrono Pattern | Description |
/// |--------------|----------------|-------------|
/// | `yyyy` | `%Y` | 4 digit year |
/// | `yy` | `%y` | 2 digit year |
/// | `MM` | `%m` | 2 digit month (01 to 12) |
/// | `M` | `%-m` | Month without leading zero |
/// | `dd` | `%d` | 2 digit day (01 to 31) |
/// | `d` | `%-d` | Day without leading zero |
/// | `HH` | `%H` | Hour (00 to 23) |
/// | `hh` | `%I` | Hour (01 to 12) |
/// | `mm` | `%M` | Minute (00 to 59) |
/// | `ss` | `%S` | Second (00 to 59) |
/// | `SSS` | `%3f` | Milliseconds |
/// | `a` | `%p` | AM or PM |
/// | `z` | `%Z` | Time zone name |
/// | `Z` | `%z` | Time zone offset |
/// | `'T'` | `T` | Literal text (quotes removed) |
///
/// # Examples
///
/// ```rust,ignore
/// assert_eq!(
///     convert_java_date_format("yyyy-MM-dd'T'HH:mm:ss"),
///     "%Y-%m-%dT%H:%M:%S"
/// );
///
/// assert_eq!(
///     convert_java_date_format("MM/dd/yyyy HH:mm:ss.SSS"),
///     "%m/%d/%Y %H:%M:%S.%3f"
/// );
///
/// assert_eq!(
///     convert_java_date_format("yyyy-MM-dd"),
///     "%Y-%m-%d"
/// );
/// ```
///
/// # Notes
///
/// - The conversion happens on its own, so users can write Java patterns
/// - Patterns already in chrono form pass through unchanged
/// - Quotes (`'`) around literal text are removed. A doubled quote (`''`) is
///   not turned into a literal quote.
/// - Letters not in the table are copied as is
pub fn convert_java_date_format(java_format: &str) -> String {
    // A `%` means the pattern is already in chrono form
    if java_format.contains('%') {
        return java_format.to_string();
    }

    let mut result = String::with_capacity(java_format.len());
    let mut chars = java_format.chars().peekable();
    let mut in_quotes = false;

    // Each letter arm counts how many times the letter repeats in a row and
    // picks the chrono code from that count
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                // Quotes turn literal mode on or off and are dropped
                in_quotes = !in_quotes;
            }
            _ if in_quotes => {
                result.push(ch);
            }
            'y' => {
                let mut count = 1;
                while chars.peek() == Some(&'y') {
                    chars.next();
                    count += 1;
                }
                // yyyy (4 or more) gives %Y (4 digit year), fewer give %y (2 digits)
                result.push_str(if count >= 4 { "%Y" } else { "%y" });
            }
            'M' => {
                let mut count = 1;
                while chars.peek() == Some(&'M') {
                    chars.next();
                    count += 1;
                }
                // MM gives %m (2 digit month), M gives %-m (no leading zero)
                result.push_str(if count >= 2 { "%m" } else { "%-m" });
            }
            'd' => {
                let mut count = 1;
                while chars.peek() == Some(&'d') {
                    chars.next();
                    count += 1;
                }
                // dd gives %d (2 digit day), d gives %-d (no leading zero)
                result.push_str(if count >= 2 { "%d" } else { "%-d" });
            }
            'H' => {
                let mut count = 1;
                while chars.peek() == Some(&'H') {
                    chars.next();
                    count += 1;
                }
                // HH gives %H (24 hour, zero padded), H gives %-H (not padded)
                result.push_str(if count >= 2 { "%H" } else { "%-H" });
            }
            'h' => {
                let mut count = 1;
                while chars.peek() == Some(&'h') {
                    chars.next();
                    count += 1;
                }
                // hh gives %I (12 hour, zero padded), h gives %-I (not padded)
                result.push_str(if count >= 2 { "%I" } else { "%-I" });
            }
            'm' => {
                let mut count = 1;
                while chars.peek() == Some(&'m') {
                    chars.next();
                    count += 1;
                }
                // mm gives %M (minute, zero padded), m gives %-M (not padded)
                result.push_str(if count >= 2 { "%M" } else { "%-M" });
            }
            's' => {
                let mut count = 1;
                while chars.peek() == Some(&'s') {
                    chars.next();
                    count += 1;
                }
                // ss gives %S (second, zero padded), s gives %-S (not padded)
                result.push_str(if count >= 2 { "%S" } else { "%-S" });
            }
            'S' => {
                let mut count = 1;
                while chars.peek() == Some(&'S') {
                    chars.next();
                    count += 1;
                }
                // SSS (or more) gives %3f (milliseconds), SS gives %2f, S gives %1f
                result.push_str(&format!("%{}f", count.min(3)));
            }
            'a' => {
                // AM or PM
                result.push_str("%p");
            }
            'z' => {
                // Time zone name
                result.push_str("%Z");
            }
            'Z' => {
                // Time zone offset
                result.push_str("%z");
            }
            _ => {
                // Copy everything else as is, such as separators
                result.push(ch);
            }
        }
    }

    result
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Reads a value from JSON with a simple JSONPath and returns a copy of it.
///
/// **Supported JSONPath syntax**
/// - `$.field` - top level field
/// - `$.nested.field` - nested field
/// - `$.array[0]` - array element
/// - `$.nested.array[0].field` - mixed path
///
/// Only one index per path part is supported.
///
/// # Parameters
/// - `json`: the JSON value to read from
/// - `path`: the JSONPath, such as "$.field" or "$.nested.field"
/// - `max_depth`: most path parts allowed, to guard against costly paths
///
/// # Errors
///
/// Returns `MappingFailed` when the path does not start with `$.`, has more
/// parts than `max_depth`, has a bad array index, names a field that is
/// missing, or uses an index past the end of the array.
pub fn extract_json_path(
    json: &JsonValue,
    path: &str,
    max_depth: usize,
) -> Result<JsonValue, EventFluxError> {
    if !path.starts_with("$.") {
        return Err(EventFluxError::MappingFailed {
            message: format!("Invalid JSONPath '{}'. Must start with '$.'", path),
            source: None,
        });
    }

    let path_parts: Vec<&str> = path[2..].split('.').collect();

    // Reject very deep paths so a config cannot make lookups too costly
    if path_parts.len() > max_depth {
        return Err(EventFluxError::MappingFailed {
            message: format!(
                "JSONPath nesting depth {} exceeds maximum allowed {}",
                path_parts.len(),
                max_depth
            ),
            source: None,
        });
    }

    let mut current = json;

    for part in path_parts {
        // Array index in the form field[index]. Takes the text between the
        // first `[` and the last character as the index.
        if let Some(bracket_idx) = part.find('[') {
            let field_name = &part[..bracket_idx];
            let index_str = &part[bracket_idx + 1..part.len() - 1];
            let index = index_str
                .parse::<usize>()
                .map_err(|_| EventFluxError::MappingFailed {
                    message: format!("Invalid array index in path: {}", part),
                    source: None,
                })?;

            current = current
                .get(field_name)
                .ok_or_else(|| EventFluxError::MappingFailed {
                    message: format!("Field '{}' not found in JSON", field_name),
                    source: None,
                })?;

            current = current
                .get(index)
                .ok_or_else(|| EventFluxError::MappingFailed {
                    message: format!("Array index {} out of bounds", index),
                    source: None,
                })?;
        } else {
            current = current
                .get(part)
                .ok_or_else(|| EventFluxError::MappingFailed {
                    message: format!("Field '{}' not found in JSON", part),
                    source: None,
                })?;
        }
    }

    Ok(current.clone())
}

/// Turns a JSON value into an AttributeValue, with optional date parsing.
///
/// When `date_format` is given, every string value is parsed as a date and
/// time with that format, read as UTC. On success it returns the time in
/// milliseconds as AttributeValue::Long. On failure it prints a warning to
/// stderr and keeps the value as AttributeValue::String.
///
/// Whole numbers that fit in i32 become Int, larger ones Long, and others
/// Double. Arrays and objects become their JSON text as a String.
///
/// # Errors
///
/// Returns `MappingFailed` for a number that fits none of those types, or an
/// array or object that cannot be written as JSON.
pub fn json_value_to_attribute(
    value: &JsonValue,
    date_format: Option<&str>,
) -> Result<AttributeValue, EventFluxError> {
    match value {
        JsonValue::String(s) => {
            if let Some(format) = date_format {
                match chrono::NaiveDateTime::parse_from_str(s, format) {
                    Ok(dt) => {
                        let timestamp = dt.and_utc().timestamp_millis();
                        return Ok(AttributeValue::Long(timestamp));
                    }
                    Err(e) => {
                        // Warn, since the user asked for date parsing and will
                        // get a String instead
                        eprintln!(
                            "Warning: Failed to parse date '{}' with format '{}': {}. Falling back to String.",
                            s, format, e
                        );
                        return Ok(AttributeValue::String(s.clone()));
                    }
                }
            }
            Ok(AttributeValue::String(s.clone()))
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Ok(AttributeValue::Int(i as i32))
                } else {
                    Ok(AttributeValue::Long(i))
                }
            } else if let Some(f) = n.as_f64() {
                Ok(AttributeValue::Double(f))
            } else {
                Err(EventFluxError::MappingFailed {
                    message: format!("Unsupported number format: {}", n),
                    source: None,
                })
            }
        }
        JsonValue::Bool(b) => Ok(AttributeValue::Bool(*b)),
        JsonValue::Null => Ok(AttributeValue::Null),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let json_str =
                serde_json::to_string(value).map_err(|e| EventFluxError::MappingFailed {
                    message: format!("Failed to serialize complex JSON value: {}", e),
                    source: Some(Box::new(e)),
                })?;
            Ok(AttributeValue::String(json_str))
        }
    }
}

/// Turns an event into a JSON object with keys field_0, field_1, and so on.
/// The older form, see [`event_to_json_with_names`].
pub fn event_to_json(event: &Event) -> Result<JsonValue, EventFluxError> {
    event_to_json_with_names(event, None)
}

/// Turns an event into a JSON object.
///
/// The object always has a `_timestamp` key with the event timestamp. Each
/// value is keyed by its schema field name, or `field_<index>` when no name is
/// given for that position.
///
/// # Errors
///
/// Returns `MappingFailed` when a float or double is NaN or infinite.
pub fn event_to_json_with_names(
    event: &Event,
    field_names: Option<&[String]>,
) -> Result<JsonValue, EventFluxError> {
    let mut obj = serde_json::Map::new();

    obj.insert(
        "_timestamp".to_string(),
        JsonValue::Number(event.timestamp.into()),
    );

    for (idx, attr_value) in event.data.iter().enumerate() {
        let field_name = if let Some(names) = field_names {
            names
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("field_{}", idx))
        } else {
            format!("field_{}", idx)
        };
        let json_value = attribute_to_json_value(attr_value)?;
        obj.insert(field_name, json_value);
    }

    Ok(JsonValue::Object(obj))
}

/// Turns an AttributeValue into a JSON value. Bytes become `<bytes:N>` and
/// Object becomes `<object>`.
///
/// # Errors
///
/// Returns `MappingFailed` when a float or double is NaN or infinite, since
/// JSON cannot hold those.
pub fn attribute_to_json_value(attr: &AttributeValue) -> Result<JsonValue, EventFluxError> {
    match attr {
        AttributeValue::String(s) => Ok(JsonValue::String(s.clone())),
        AttributeValue::Int(i) => Ok(JsonValue::Number((*i).into())),
        AttributeValue::Long(l) => Ok(JsonValue::Number((*l).into())),
        AttributeValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .ok_or_else(|| EventFluxError::MappingFailed {
                message: format!("Invalid float value: {}", f),
                source: None,
            }),
        AttributeValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(JsonValue::Number)
            .ok_or_else(|| EventFluxError::MappingFailed {
                message: format!("Invalid double value: {}", d),
                source: None,
            }),
        AttributeValue::Bool(b) => Ok(JsonValue::Bool(*b)),
        AttributeValue::Null => Ok(JsonValue::Null),
        AttributeValue::Bytes(bytes) => Ok(JsonValue::String(format!("<bytes:{}>", bytes.len()))),
        AttributeValue::Object(_) => Ok(JsonValue::String("<object>".to_string())),
    }
}

/// Fills in a template with event data.
///
/// **Template variables**
/// - `{{field_0}}`, `{{field_1}}`, and so on - attribute values by index
/// - `{{_timestamp}}` - the event timestamp
///
/// Plain text replacement with no other logic. Values are not quoted or
/// escaped (see `attribute_value_to_string`). Any other placeholder, such as
/// one with a field name, is left as is. Never returns an error today.
pub fn render_template(template: &str, event: &Event) -> Result<String, EventFluxError> {
    let mut result = template.to_string();

    result = result.replace("{{_timestamp}}", &event.timestamp.to_string());

    for (idx, attr_value) in event.data.iter().enumerate() {
        let field_name = format!("field_{}", idx);
        let placeholder = format!("{{{{{}}}}}", field_name);
        let value_str = attribute_value_to_string(attr_value);
        result = result.replace(&placeholder, &value_str);
    }

    Ok(result)
}

/// Turns an AttributeValue into text for a template. Null becomes `null`,
/// Bytes becomes `<bytes:N>`, and Object becomes `<object>`.
pub fn attribute_value_to_string(value: &AttributeValue) -> String {
    match value {
        AttributeValue::String(s) => s.clone(),
        AttributeValue::Int(i) => i.to_string(),
        AttributeValue::Long(l) => l.to_string(),
        AttributeValue::Double(d) => d.to_string(),
        AttributeValue::Float(f) => f.to_string(),
        AttributeValue::Bool(b) => b.to_string(),
        AttributeValue::Null => "null".to_string(),
        AttributeValue::Bytes(bytes) => format!("<bytes:{}>", bytes.len()),
        AttributeValue::Object(_) => "<object>".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_mapping() {
        let json_str = r#"{"orderId": "123", "amount": 100.0}"#;
        let mapper = JsonSourceMapper::new();

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 2);
    }

    #[test]
    fn test_explicit_mapping() {
        let json_str = r#"{"order": {"id": "123", "total": 100.0}}"#;
        let mut mappings = HashMap::new();
        mappings.insert("orderId".to_string(), "$.order.id".to_string());
        mappings.insert("amount".to_string(), "$.order.total".to_string());

        let mapper = JsonSourceMapper::with_mappings(mappings);

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 2);
    }

    #[test]
    fn test_json_path_extraction() {
        let json = serde_json::json!({
            "order": {
                "id": "123",
                "items": [
                    {"name": "item1", "price": 10.0},
                    {"name": "item2", "price": 20.0}
                ]
            }
        });

        let value = extract_json_path(&json, "$.order.id", 32).unwrap();
        assert_eq!(value, serde_json::json!("123"));

        let value = extract_json_path(&json, "$.order.items[0].name", 32).unwrap();
        assert_eq!(value, serde_json::json!("item1"));
    }

    #[test]
    fn test_template_rendering() {
        let template = r#"{"eventType":"ORDER","id":"{{field_0}}","amount":{{field_1}}}"#;
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::String("order-1".to_string()),
                AttributeValue::Double(100.0),
            ],
        );

        let rendered = render_template(template, &event).unwrap();
        assert!(rendered.contains("\"id\":\"order-1\""));
        assert!(rendered.contains("\"amount\":100"));
    }

    #[test]
    fn test_all_or_nothing_mapping_policy() {
        // No mappings, so everything is auto mapped
        let mapper1 = JsonSourceMapper::new();
        assert!(mapper1.mappings.is_empty());

        // Any mapping at all means everything is explicit
        let mut mappings = HashMap::new();
        mappings.insert("field1".to_string(), "$.field1".to_string());
        let mapper2 = JsonSourceMapper::with_mappings(mappings);
        assert!(!mapper2.mappings.is_empty());
    }

    #[test]
    fn test_json_sink_simple() {
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::String("test".to_string()),
                AttributeValue::Int(42),
            ],
        );

        let mapper = JsonSinkMapper::new();
        let result = mapper.map_event(&event).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        // Verify it's valid JSON
        let _: JsonValue = serde_json::from_str(&json_str).unwrap();
    }

    #[test]
    fn test_json_sink_template() {
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::String("test-id".to_string()),
                AttributeValue::Double(99.5),
            ],
        );

        let template = r#"{"id":"{{field_0}}","value":{{field_1}}}"#.to_string();
        let mapper = JsonSinkMapper::with_template(template);
        let result = mapper.map_event(&event).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        assert!(json_str.contains("\"id\":\"test-id\""));
        assert!(json_str.contains("\"value\":99.5"));
    }

    // Error case tests, guarding earlier bug fixes

    #[test]
    fn test_ignore_parse_errors_enabled() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_ignore_parse_errors(true);

        // Broken JSON must be skipped, not fail
        let result = mapper.map(b"invalid json{{{");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0); // no events, just skipped
    }

    #[test]
    fn test_ignore_parse_errors_disabled() {
        let mapper = JsonSourceMapper::new(); // ignore_parse_errors = false by default

        // Broken JSON must fail
        let result = mapper.map(b"invalid json{{{");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("JSON parse error"));
    }

    #[test]
    fn test_date_format_parsing() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_date_format(Some("%Y-%m-%d %H:%M:%S".to_string()));

        let json_str = r#"{"timestamp": "2025-10-19 12:34:56", "value": 100}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        // The first field (timestamp) must be parsed as Long milliseconds
        assert!(matches!(events[0].data[0], AttributeValue::Long(_)));
        // The second field (value) must stay an Int
        assert!(matches!(events[0].data[1], AttributeValue::Int(100)));
    }

    #[test]
    fn test_date_format_parsing_fallback() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_date_format(Some("%Y-%m-%d".to_string()));

        // The value does not match the format, so it must stay a String
        let json_str = r#"{"timestamp": "not a date", "value": 100}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "not a date"
        ));
    }

    #[test]
    fn test_max_input_size_limit() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_max_input_size(100); // small limit

        // Input larger than the limit
        let large_json = format!(r#"{{"data": "{}"}}"#, "x".repeat(200));
        let result = mapper.map(large_json.as_bytes());

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds maximum allowed"));
    }

    #[test]
    fn test_max_nesting_depth_limit() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_max_nesting_depth(3); // small limit

        let mut mappings = HashMap::new();
        // The path a.b.c.d.e has 5 levels, more than the limit of 3
        mappings.insert("field".to_string(), "$.a.b.c.d.e".to_string());
        mapper = JsonSourceMapper::with_mappings(mappings);
        mapper.set_max_nesting_depth(3);

        let json_str = r#"{"a": {"b": {"c": {"d": {"e": "value"}}}}}"#;
        let result = mapper.map(json_str.as_bytes());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nesting depth"));
    }

    #[test]
    fn test_large_input_within_limit() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_max_input_size(1024 * 1024); // 1 MB limit

        let json = format!(r#"{{"data": "{}"}}"#, "x".repeat(1000));
        let result = mapper.map(json.as_bytes());

        // Within the limit, so it must succeed
        assert!(result.is_ok());
    }

    // ========================================================================
    // Date Format Conversion Tests
    // ========================================================================

    #[test]
    fn test_convert_java_date_format_iso8601() {
        // Java: yyyy-MM-dd'T'HH:mm:ss
        // Chrono: %Y-%m-%dT%H:%M:%S
        assert_eq!(
            convert_java_date_format("yyyy-MM-dd'T'HH:mm:ss"),
            "%Y-%m-%dT%H:%M:%S"
        );
    }

    #[test]
    fn test_convert_java_date_format_with_milliseconds() {
        // Java: yyyy-MM-dd HH:mm:ss.SSS
        // Chrono: %Y-%m-%d %H:%M:%S.%3f
        assert_eq!(
            convert_java_date_format("yyyy-MM-dd HH:mm:ss.SSS"),
            "%Y-%m-%d %H:%M:%S.%3f"
        );
    }

    #[test]
    fn test_convert_java_date_format_us_style() {
        // Java: MM/dd/yyyy hh:mm:ss a
        // Chrono: %m/%d/%Y %I:%M:%S %p
        assert_eq!(
            convert_java_date_format("MM/dd/yyyy hh:mm:ss a"),
            "%m/%d/%Y %I:%M:%S %p"
        );
    }

    #[test]
    fn test_convert_java_date_format_simple_date() {
        // Java: yyyy-MM-dd
        // Chrono: %Y-%m-%d
        assert_eq!(convert_java_date_format("yyyy-MM-dd"), "%Y-%m-%d");
    }

    #[test]
    fn test_convert_java_date_format_two_digit_year() {
        // Java: yy-MM-dd
        // Chrono: %y-%m-%d
        assert_eq!(convert_java_date_format("yy-MM-dd"), "%y-%m-%d");
    }

    #[test]
    fn test_convert_java_date_format_without_leading_zeros() {
        // Java: M/d/yyyy
        // Chrono: %-m/%-d/%Y
        assert_eq!(convert_java_date_format("M/d/yyyy"), "%-m/%-d/%Y");
    }

    #[test]
    fn test_convert_java_date_format_single_vs_double_time() {
        // A single letter drops the leading zero, as in Java SimpleDateFormat
        assert_eq!(convert_java_date_format("H:m:s"), "%-H:%-M:%-S");
        assert_eq!(convert_java_date_format("HH:mm:ss"), "%H:%M:%S");
        // 12 hour format
        assert_eq!(convert_java_date_format("h:m"), "%-I:%-M");
        assert_eq!(convert_java_date_format("hh:mm"), "%I:%M");
    }

    #[test]
    fn test_convert_java_date_format_with_timezone() {
        // Java: yyyy-MM-dd'T'HH:mm:ssZ
        // Chrono: %Y-%m-%dT%H:%M:%S%z
        assert_eq!(
            convert_java_date_format("yyyy-MM-dd'T'HH:mm:ssZ"),
            "%Y-%m-%dT%H:%M:%S%z"
        );
    }

    #[test]
    fn test_convert_java_date_format_with_timezone_name() {
        // Java: yyyy-MM-dd HH:mm:ss z
        // Chrono: %Y-%m-%d %H:%M:%S %Z
        assert_eq!(
            convert_java_date_format("yyyy-MM-dd HH:mm:ss z"),
            "%Y-%m-%d %H:%M:%S %Z"
        );
    }

    #[test]
    fn test_convert_java_date_format_already_chrono() {
        // Already in chrono form, so it passes through unchanged
        assert_eq!(convert_java_date_format("%Y-%m-%d"), "%Y-%m-%d");
        assert_eq!(
            convert_java_date_format("%Y-%m-%dT%H:%M:%S"),
            "%Y-%m-%dT%H:%M:%S"
        );
    }

    #[test]
    fn test_convert_java_date_format_quoted_literals() {
        // Java: yyyy'年'MM'月'dd'日'
        // Chrono: %Y年%m月%d日 (quotes removed)
        assert_eq!(
            convert_java_date_format("yyyy'年'MM'月'dd'日'"),
            "%Y年%m月%d日"
        );
    }

    #[test]
    fn test_convert_java_date_format_complex_pattern() {
        // Java: 'Created at' yyyy-MM-dd 'T' HH:mm:ss.SSS
        // Chrono: Created at %Y-%m-%d T %H:%M:%S.%3f
        assert_eq!(
            convert_java_date_format("'Created at' yyyy-MM-dd 'T' HH:mm:ss.SSS"),
            "Created at %Y-%m-%d T %H:%M:%S.%3f"
        );
    }

    #[test]
    fn test_date_format_integration_with_mapper() {
        let mut mapper = JsonSourceMapper::new();

        // A Java pattern must be converted
        mapper.set_date_format(Some("yyyy-MM-dd'T'HH:mm:ss".to_string()));

        // The stored format is the chrono one
        assert_eq!(mapper.date_format, Some("%Y-%m-%dT%H:%M:%S".to_string()));
    }

    #[test]
    fn test_date_format_chrono_passthrough() {
        let mut mapper = JsonSourceMapper::new();

        // A chrono pattern must pass through unchanged
        mapper.set_date_format(Some("%Y-%m-%d %H:%M:%S".to_string()));

        // The stored format is the same
        assert_eq!(mapper.date_format, Some("%Y-%m-%d %H:%M:%S".to_string()));
    }

    #[test]
    fn test_date_parsing_with_java_format() {
        let json_str = r#"{"timestamp": "2025-10-21T15:30:00"}"#;
        let mut mapper = JsonSourceMapper::new();

        // Use Java format
        mapper.set_date_format(Some("yyyy-MM-dd'T'HH:mm:ss".to_string()));

        let mut mappings = HashMap::new();
        mappings.insert("timestamp".to_string(), "$.timestamp".to_string());
        mapper = JsonSourceMapper::with_mappings(mappings);
        mapper.set_date_format(Some("yyyy-MM-dd'T'HH:mm:ss".to_string()));

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);

        // The first field must be the timestamp parsed as Long milliseconds
        match &events[0].data[0] {
            AttributeValue::Long(ts) => {
                // Must be a sensible time, around October 2025
                assert!(*ts > 1_700_000_000_000); // After 2023
                assert!(*ts < 1_800_000_000_000); // Before 2027
            }
            _ => panic!("Expected Long timestamp, got {:?}", events[0].data[0]),
        }
    }

    // P1.9 Field Ordering Tests
    // These tests check that JSON field order is kept, not sorted alphabetically

    #[test]
    fn test_auto_mapping_preserves_json_field_order() {
        // JSON fields that are not in alphabetical order
        // Sorted alphabetically they would be amount, symbol, timestamp
        // Kept in order they are symbol, timestamp, amount
        let json_str = r#"{"symbol": "AAPL", "timestamp": 1234567890, "amount": 100.5}"#;
        let mapper = JsonSourceMapper::new();

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);

        // Fields must be in JSON key order (symbol, timestamp, amount),
        // NOT alphabetical order (amount, symbol, timestamp)
        match &events[0].data[0] {
            AttributeValue::String(s) if s == "AAPL" => {}
            _ => panic!(
                "Expected first field to be 'AAPL', got {:?}",
                events[0].data[0]
            ),
        }
        // The numeric value could be Int or Long depending on size
        match &events[0].data[1] {
            AttributeValue::Long(1234567890) | AttributeValue::Int(1234567890) => {}
            _ => panic!(
                "Expected second field to be timestamp 1234567890, got {:?}",
                events[0].data[1]
            ),
        }
        match &events[0].data[2] {
            AttributeValue::Double(amt) if (*amt - 100.5).abs() < 0.001 => {}
            _ => panic!(
                "Expected third field to be amount 100.5, got {:?}",
                events[0].data[2]
            ),
        }
    }

    #[test]
    fn test_auto_mapping_different_ordering() {
        // Another test with clearly reverse alphabetical order
        // Alphabetical: id, name, priority
        // Actual order: priority, name, id
        let json_str = r#"{"priority": 1, "name": "Task", "id": "T123"}"#;
        let mapper = JsonSourceMapper::new();

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);

        // Order must be priority, name, id (NOT alphabetical)
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "Task"
        ));
        assert!(matches!(
            events[0].data[2],
            AttributeValue::String(ref s) if s == "T123"
        ));
    }

    // ========================================================================
    // Auto Mapping Tests That Use the Schema (#201)
    // JSON fields must match stream fields by name. Exact match first, then
    // ignoring case, and by position only when nothing matches.
    // ========================================================================

    fn schema_mapper(names: &[&str]) -> JsonSourceMapper {
        let mut mapper = JsonSourceMapper::new();
        mapper
            .set_field_names(names.iter().map(|s| s.to_string()).collect())
            .unwrap();
        mapper
    }

    #[test]
    fn test_schema_auto_mapping_exact_name_match() {
        // Repro for issue #201. The schema is (price, symbol, volume) but the
        // JSON keys arrive in another order (symbol, price, volume).
        let mapper = schema_mapper(&["price", "symbol", "volume"]);
        let json_str = r#"{"symbol": "AAPL", "price": 185.5, "volume": 5000}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);

        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 185.5).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[2], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_schema_auto_mapping_case_insensitive_match() {
        // Documented behaviour. {"Symbol": ..., "PRICE": ..., "Volume": ...}
        // still maps correctly, since matching can ignore case
        let mapper = schema_mapper(&["symbol", "price", "volume"]);
        let json_str = r#"{"PRICE": 185.5, "Volume": 5000, "Symbol": "AAPL"}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Double(p) if (p - 185.5).abs() < 1e-9));
        assert!(matches!(events[0].data[2], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_schema_auto_mapping_exact_match_wins_over_case_insensitive() {
        // When both an exact key and one that differs by case exist, exact wins
        let mapper = schema_mapper(&["symbol"]);
        let json_str = r#"{"SYMBOL": "wrong", "symbol": "right"}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "right"
        ));
    }

    #[test]
    fn test_schema_auto_mapping_case_variant_fields_do_not_share_a_key() {
        // Schema (id, ID) with only "id" in the JSON. "id" matches exactly,
        // and "ID" must NOT reuse the same key by ignoring case
        let mapper = schema_mapper(&["id", "ID"]);
        let events = mapper.map(br#"{"id": 1}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Null));

        // Same with the other case declared first. The exact match still wins
        // whatever the schema order
        let mapper = schema_mapper(&["ID", "id"]);
        let events = mapper.map(br#"{"id": 1}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Null));
        assert!(matches!(events[0].data[1], AttributeValue::Int(1)));
    }

    #[test]
    fn test_schema_auto_mapping_key_matching_two_fields_by_case_is_rejected() {
        // Neither field matches exactly and "Id" fits both when case is
        // ignored. Picking one would be a guess from schema order, so the
        // event fails
        let mapper = schema_mapper(&["id", "ID"]);
        let err = mapper.map(br#"{"Id": 1}"#).unwrap_err();
        assert!(err.to_string().contains("'Id'"), "got: {}", err);

        // With both keys present, each field gets its own exact match
        let events = mapper.map(br#"{"ID": 2, "id": 1}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(2)));
    }

    #[test]
    fn test_schema_auto_mapping_field_matching_two_keys_by_case_is_rejected() {
        // Field "id" has no exact key and fits both "Id" and "iD". The result
        // must not depend on which key comes first in the payload
        let mapper = schema_mapper(&["id"]);
        for payload in [br#"{"Id": 1, "iD": 2}"#, br#"{"iD": 2, "Id": 1}"#] {
            let err = mapper.map(payload).unwrap_err();
            assert!(err.to_string().contains("Ambiguous"), "got: {}", err);
        }
    }

    #[test]
    fn test_schema_auto_mapping_ambiguity_ignored_when_exact_match_exists() {
        // An exact key resolves the field, so other case variants in the
        // payload are simply extra keys, not an ambiguity
        let mapper = schema_mapper(&["id"]);
        let events = mapper.map(br#"{"Id": 9, "id": 1, "ID": 8}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
    }

    #[test]
    fn test_schema_duplicate_field_names_rejected_at_config_time() {
        // Schema (id, id). {"id": 1} would otherwise fill both positions.
        // Rejected when the mapper is set up, for auto mapping...
        let mut mapper = JsonSourceMapper::new();
        let err = mapper
            .set_field_names(vec!["id".to_string(), "id".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("more than once"), "got: {}", err);

        // ...and for explicit mappings, where one mapping.id would feed both
        let mut mappings = HashMap::new();
        mappings.insert("id".to_string(), "$.id".to_string());
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["id".to_string(), "id".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("more than once"), "got: {}", err);
    }

    fn explicit_mapper() -> JsonSourceMapper {
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), "$.s".to_string());
        mappings.insert("amount".to_string(), "$.a".to_string());
        JsonSourceMapper::with_mappings(mappings)
    }

    fn first_two(mapper: &JsonSourceMapper) -> (AttributeValue, AttributeValue) {
        let events = mapper.map(br#"{"s": "AAPL", "a": 5}"#).unwrap();
        (events[0].data[0].clone(), events[0].data[1].clone())
    }

    #[test]
    fn test_clearing_schema_restores_alphabetical_mapping_order() {
        let mut mapper = explicit_mapper();
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();
        assert!(matches!(first_two(&mapper).0, AttributeValue::String(_)));

        // Cleared, so no old schema order is left. Alphabetical (amount, symbol)
        mapper.set_field_names(Vec::new()).unwrap();
        let (first, second) = first_two(&mapper);
        assert!(matches!(first, AttributeValue::Int(5)), "got {first:?}");
        assert!(
            matches!(second, AttributeValue::String(_)),
            "got {second:?}"
        );
    }

    #[test]
    fn test_replacing_schema_recomputes_mapping_order() {
        let mut mapper = explicit_mapper();
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();
        mapper
            .set_field_names(vec!["amount".to_string(), "symbol".to_string()])
            .unwrap();
        assert!(matches!(first_two(&mapper).0, AttributeValue::Int(5)));
    }

    #[test]
    fn test_failed_set_field_names_leaves_mapper_unchanged() {
        let mut mapper = explicit_mapper();
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();

        // Invalid schemas (missing mapping, duplicate name) are rejected...
        assert!(mapper.set_field_names(vec!["symbol".to_string()]).is_err());
        assert!(mapper
            .set_field_names(vec!["symbol".to_string(), "symbol".to_string()])
            .is_err());

        // ...and the schema order set before still applies
        assert!(matches!(first_two(&mapper).0, AttributeValue::String(_)));
        assert_eq!(
            mapper.field_names,
            Some(vec!["symbol".to_string(), "amount".to_string()])
        );
    }

    #[test]
    fn test_schema_auto_mapping_positional_fallback() {
        // No JSON key matches any schema name, so values are taken by position
        let mapper = schema_mapper(&["price", "symbol", "volume"]);
        let json_str = r#"{"p": 185.5, "s": "AAPL", "v": 5000}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 185.5).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[2], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_schema_auto_mapping_name_mode_is_all_or_nothing() {
        // Once ANY field matches by name, the payload counts as named. A field
        // with no match becomes Null instead of taking a value by position,
        // which could use a value twice
        let mapper = schema_mapper(&["price", "symbol"]);
        let json_str = r#"{"symbol": "AAPL", "cost": 185.5}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        // "price" has no name match, so it is Null. It is NOT "AAPL" by
        // position, since "symbol" already took that value
        assert!(matches!(events[0].data[0], AttributeValue::Null));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
    }

    #[test]
    fn test_schema_auto_mapping_extra_key_does_not_fill_missing_field() {
        // An extra JSON key must not leak into a schema field with no match
        // by way of position
        let mapper = schema_mapper(&["price", "symbol", "volume"]);
        let json_str = r#"{"price": 1.0, "symbol": "AAPL", "venue": "NYSE"}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 1.0).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        // "volume" is missing, so it is Null and not "NYSE"
        assert!(matches!(events[0].data[2], AttributeValue::Null));
    }

    #[test]
    fn test_schema_auto_mapping_missing_field_is_null() {
        // The schema has more fields than the JSON object gives. The last field,
        // with no match, maps to Null, not an error
        let mapper = schema_mapper(&["symbol", "price", "volume"]);
        let json_str = r#"{"symbol": "AAPL", "price": 185.5}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events[0].data.len(), 3);
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Double(_)));
        assert!(matches!(events[0].data[2], AttributeValue::Null));
    }

    #[test]
    fn test_schema_auto_mapping_ignores_extra_json_fields() {
        // JSON has extra fields the schema does not declare, and they are skipped
        let mapper = schema_mapper(&["symbol", "volume"]);
        let json_str = r#"{"price": 185.5, "symbol": "AAPL", "volume": 5000, "venue": "NYSE"}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events[0].data.len(), 2);
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_schema_explicit_mappings_follow_schema_order() {
        // Explicit JSONPath mappings must come out in schema order, not
        // alphabetical order
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), "$.trade.sym".to_string());
        mappings.insert("amount".to_string(), "$.trade.total".to_string());

        // Schema order is symbol first, amount second. Alphabetical would put
        // amount first.
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();

        let json_str = r#"{"trade": {"sym": "AAPL", "total": 100.0}}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Double(a) if (a - 100.0).abs() < 1e-9));
    }

    #[test]
    fn test_schema_explicit_mappings_missing_field_fails_at_config_time() {
        // With a schema, every stream field needs a mapping once any
        // mapping.* property is set (all or nothing). This must fail when the
        // mapper is set up, not on each event at runtime.
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), "$.trade.sym".to_string());

        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("amount"), "got: {}", err);
    }

    #[test]
    fn test_schema_explicit_mappings_match_keys_case_insensitively() {
        // A mapping key that differs from the attribute only by case is
        // accepted, the same way auto mapping can ignore case
        let mut mappings = HashMap::new();
        mappings.insert("orderid".to_string(), "$.order.id".to_string());

        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        mapper.set_field_names(vec!["orderId".to_string()]).unwrap();

        let json_str = r#"{"order": {"id": "O-1"}}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "O-1"
        ));
    }

    #[test]
    fn test_schema_explicit_mappings_case_variant_fields_do_not_share_a_key() {
        // Schema (foo, FOO) with only mapping.FOO. FOO takes its exact key,
        // and foo must not reuse it by ignoring case
        let mut mappings = HashMap::new();
        mappings.insert("FOO".to_string(), "$.a".to_string());
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("'foo'"), "got: {}", err);

        // With a mapping for each, both resolve exactly to their own path
        let mut mappings = HashMap::new();
        mappings.insert("foo".to_string(), "$.a".to_string());
        mappings.insert("FOO".to_string(), "$.b".to_string());
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        mapper
            .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
            .unwrap();
        let events = mapper.map(br#"{"a": 1, "b": 2}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(2)));
    }

    #[test]
    fn test_schema_explicit_mappings_key_matching_two_fields_by_case_is_rejected() {
        // Neither field matches exactly and "Foo" fits both when case is
        // ignored. That is ambiguous, not "first field in schema order wins"
        let mut mappings = HashMap::new();
        mappings.insert("Foo".to_string(), "$.a".to_string());
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("Ambiguous"), "got: {}", err);
        assert!(err.to_string().contains("'Foo'"), "got: {}", err);
    }

    #[test]
    fn test_schema_explicit_mappings_case_only_aliases_are_rejected_deterministically() {
        // Schema (foo, FOO) with keys "Foo" and "fOO". Neither key is exact
        // and each fits both fields. Taking the first fit would depend on
        // HashMap order, so it must be rejected every time.
        for _ in 0..32 {
            let mut mappings = HashMap::new();
            mappings.insert("Foo".to_string(), "$.a".to_string());
            mappings.insert("fOO".to_string(), "$.b".to_string());
            let mut mapper = JsonSourceMapper::with_mappings(mappings);
            let err = mapper
                .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
                .unwrap_err();
            assert!(err.to_string().contains("Ambiguous"), "got: {}", err);
        }
    }

    #[test]
    fn test_schema_explicit_mappings_unambiguous_case_match_still_works() {
        // One field and one key that differs only by case is clear, so it is accepted
        let mut mappings = HashMap::new();
        mappings.insert("ORDERID".to_string(), "$.id".to_string());
        mappings.insert("amount".to_string(), "$.amt".to_string());
        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        mapper
            .set_field_names(vec!["orderId".to_string(), "amount".to_string()])
            .unwrap();
        let events = mapper.map(br#"{"id": "O-1", "amt": 5}"#).unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "O-1"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Int(5)));
    }

    #[test]
    fn test_schema_explicit_mappings_stray_key_fails_at_config_time() {
        // A mapping key that matches no stream field (a typo or an old entry)
        // is a config error, not a key that silently does nothing
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), "$.sym".to_string());
        mappings.insert("trade_ts".to_string(), "$.T".to_string());

        let mut mapper = JsonSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["symbol".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("trade_ts"), "got: {}", err);
    }

    #[test]
    fn test_set_field_names_empty_means_no_schema() {
        // An empty schema is stored as "unset", so the older JSON key order
        // behaviour applies
        let mut mapper = JsonSourceMapper::new();
        mapper.set_field_names(Vec::new()).unwrap();
        assert!(mapper.field_names.is_none());

        let json_str = r#"{"b": 2, "a": 1}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(2)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(1)));
    }

    #[test]
    fn test_schema_auto_mapping_with_date_format() {
        // Values matched by name still go through date parsing
        let mut mapper = schema_mapper(&["value", "timestamp"]);
        mapper.set_date_format(Some("%Y-%m-%d %H:%M:%S".to_string()));

        let json_str = r#"{"timestamp": "2025-10-19 12:34:56", "value": 100}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert!(matches!(events[0].data[0], AttributeValue::Int(100)));
        assert!(matches!(events[0].data[1], AttributeValue::Long(_)));
    }
}
