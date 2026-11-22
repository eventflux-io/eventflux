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
//! Provides bidirectional mapping between JSON and EventFlux events.
//!
//! ## Source Mapping (JSON → Events)
//!
//! ### Auto-Mapping (No `mapping.*` properties)
//! ```json
//! {"orderId": "123", "amount": 100.0}
//! ```
//! Maps all top-level fields by name.
//!
//! ### Explicit Mapping (With `mapping.*` properties)
//! ```toml
//! json.mapping.orderId = "$.order.id"
//! json.mapping.amount = "$.order.total"
//! ```
//! Extracts nested fields using JSONPath.
//!
//! ## Sink Mapping (Events → JSON)
//!
//! ### Simple Serialization (No template)
//! Converts events to JSON objects with field names from schema.
//!
//! ### Template-Based (With template)
//! ```toml
//! json.template = "{\"eventType\":\"ORDER\",\"id\":\"{{orderId}}\",\"amount\":{{amount}}}"
//! ```

use super::{
    alphabetical_mappings, match_field_names, now_millis, schema_ordered_mappings, SinkMapper,
    SourceMapper,
};
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Source mapper for JSON format
#[derive(Debug, Clone)]
pub struct JsonSourceMapper {
    /// Field name → JSONPath mappings
    /// Empty = auto-map all top-level fields
    mappings: HashMap<String, String>,
    /// Whether to ignore parse errors (continue processing)
    ignore_parse_errors: bool,
    /// Optional date format for parsing timestamps
    date_format: Option<String>,
    /// Maximum input size in bytes (default: 10 MB)
    max_input_size: usize,
    /// Maximum JSONPath nesting depth (default: 32)
    max_nesting_depth: usize,
    /// Stream schema field names in schema order; never `Some(empty)`.
    /// Enables name-based auto-mapping (see `auto_map`).
    field_names: Option<Vec<String>>,
    /// Explicit mappings as (field name, JSONPath) in output order.
    /// Precomputed at configuration time: schema order when field names are
    /// set, alphabetical otherwise (HashMap iteration is non-deterministic).
    ordered_mappings: Vec<(String, String)>,
}

impl JsonSourceMapper {
    /// Create a new JSON source mapper with default settings
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

    /// Create a new JSON source mapper with explicit mappings
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

    /// Set the stream schema field names (in schema order).
    ///
    /// Enables name-based auto-mapping: each schema field is resolved from the
    /// JSON object by exact key match, then ASCII case-insensitive match; when
    /// no schema field matches any JSON key, values fall back to positional
    /// order. An empty list is treated as "no schema" (unset).
    ///
    /// With explicit `mapping.*` properties configured, this validates the
    /// configuration fail-fast: every schema field must have a mapping
    /// (matched exactly or case-insensitively) and every mapping key must
    /// correspond to a schema field. Output follows schema order.
    pub fn set_field_names(&mut self, names: Vec<String>) -> Result<(), EventFluxError> {
        if names.is_empty() {
            self.field_names = None;
            return Ok(());
        }
        if !self.mappings.is_empty() {
            self.ordered_mappings = schema_ordered_mappings(&names, &self.mappings, "json")?;
        }
        self.field_names = Some(names);
        Ok(())
    }

    /// Set whether to ignore parse errors
    pub fn set_ignore_parse_errors(&mut self, ignore: bool) {
        self.ignore_parse_errors = ignore;
    }

    /// Set date format for timestamp parsing
    ///
    /// Accepts both Java SimpleDateFormat and chrono format patterns.
    /// Java patterns are automatically converted to chrono format.
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

    /// Set maximum input size in bytes (for DoS protection)
    pub fn set_max_input_size(&mut self, max_size: usize) {
        self.max_input_size = max_size;
    }

    /// Set maximum JSONPath nesting depth (for DoS protection)
    pub fn set_max_nesting_depth(&mut self, max_depth: usize) {
        self.max_nesting_depth = max_depth;
    }

    /// Auto-map top-level JSON fields to event attributes.
    ///
    /// With schema field names available, each schema field is resolved by
    /// exact JSON key match, then an unambiguous ASCII case-insensitive match,
    /// with each JSON key used by at most one field (see `match_field_names`).
    /// An ambiguous case-only match fails the event instead of guessing.
    /// When at least one field matches by name, the payload is treated as name-based:
    /// unmatched fields map to Null (a value is never consumed twice). Only
    /// when NO field matches by name does the mapper fall back to positional
    /// order (serde_json's `preserve_order` keeps the incoming key order).
    ///
    /// Without schema field names, all top-level fields are mapped in JSON
    /// insertion order (legacy behavior).
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
                    // Name-based payload: unmatched fields become Null
                    matched.into_iter().map(to_attribute).collect()
                } else {
                    // No key matched any schema field: positional fallback
                    let mut values = obj.values();
                    names.iter().map(|_| to_attribute(values.next())).collect()
                }
            }
            // No schema available: preserve JSON key order
            None => obj.values().map(|v| to_attribute(Some(v))).collect(),
        };
        Ok(vec![Event::new_with_data(now_millis(), event_data?)])
    }

    /// Extract fields using explicit JSONPath mappings
    ///
    /// Uses the (field, path) order precomputed at configuration time:
    /// schema order when schema field names were set (validated for
    /// completeness in `set_field_names`), alphabetical otherwise.
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

/// Match schema fields to top-level JSON values using [`match_field_names`].
///
/// Fast path: when every field matches a key exactly (the common case),
/// the general matcher and its key list allocation are skipped.
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
        // Check input size to prevent DoS attacks
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

        // Parse JSON with error handling that respects ignore_parse_errors
        let json: JsonValue = match serde_json::from_slice(input) {
            Ok(v) => v,
            Err(e) => {
                if self.ignore_parse_errors {
                    // Skip this event - return empty list
                    return Ok(Vec::new());
                } else {
                    return Err(EventFluxError::MappingFailed {
                        message: format!("JSON parse error: {}", e),
                        source: Some(Box::new(e)),
                    });
                }
            }
        };

        // Apply all-or-nothing auto-mapping policy
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
    /// Optional template string for custom JSON output
    /// Uses {{fieldName}} placeholders
    template: Option<String>,
    /// Whether to pretty-print JSON (with indentation)
    pretty_print: bool,
    /// Schema field names for JSON output (uses field_0, field_1 if not set)
    field_names: Option<Vec<String>>,
}

impl JsonSinkMapper {
    /// Create a new JSON sink mapper without template (simple serialization)
    pub fn new() -> Self {
        Self {
            template: None,
            pretty_print: false,
            field_names: None,
        }
    }

    /// Create a new JSON sink mapper with a template
    pub fn with_template(template: String) -> Self {
        Self {
            template: Some(template),
            pretty_print: false,
            field_names: None,
        }
    }

    /// Enable pretty-printing (formatted JSON with indentation)
    pub fn set_pretty_print(&mut self, pretty: bool) {
        self.pretty_print = pretty;
    }

    /// Set the schema field names for JSON output
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
            // Use template rendering
            let rendered = render_template(template, event)?;
            Ok(rendered.into_bytes())
        } else {
            // Simple JSON serialization with optional field names
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

/// Convert Java SimpleDateFormat pattern to chrono format pattern
///
/// EventFlux configuration uses Java SimpleDateFormat patterns for consistency
/// with Java-based CEP systems, but the Rust implementation uses chrono.
/// This function performs automatic conversion.
///
/// # Common Pattern Conversions
///
/// | Java Pattern | Chrono Pattern | Description |
/// |--------------|----------------|-------------|
/// | `yyyy` | `%Y` | 4-digit year |
/// | `yy` | `%y` | 2-digit year |
/// | `MM` | `%m` | 2-digit month (01-12) |
/// | `M` | `%-m` | Month without leading zero |
/// | `dd` | `%d` | 2-digit day (01-31) |
/// | `d` | `%-d` | Day without leading zero |
/// | `HH` | `%H` | Hour (00-23) |
/// | `hh` | `%I` | Hour (01-12) |
/// | `mm` | `%M` | Minute (00-59) |
/// | `ss` | `%S` | Second (00-59) |
/// | `SSS` | `%3f` | Milliseconds |
/// | `a` | `%p` | AM/PM |
/// | `'T'` | `T` | Literal character (quotes removed) |
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
/// - This conversion is **automatic and transparent** - users can use Java patterns
/// - Already-converted chrono patterns pass through unchanged
/// - Quotes (`'`) around literal characters are removed
pub fn convert_java_date_format(java_format: &str) -> String {
    // If format already looks like chrono format (contains %), pass through unchanged
    if java_format.contains('%') {
        return java_format.to_string();
    }

    let mut result = String::with_capacity(java_format.len());
    let mut chars = java_format.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                // Toggle quote mode - quotes are removed, content is literal
                in_quotes = !in_quotes;
            }
            _ if in_quotes => {
                // Inside quotes - keep as literal character
                result.push(ch);
            }
            'y' => {
                // Count consecutive y's
                let mut count = 1;
                while chars.peek() == Some(&'y') {
                    chars.next();
                    count += 1;
                }
                // yyyy → %Y (4-digit year), yy → %y (2-digit year)
                result.push_str(if count >= 4 { "%Y" } else { "%y" });
            }
            'M' => {
                // Count consecutive M's
                let mut count = 1;
                while chars.peek() == Some(&'M') {
                    chars.next();
                    count += 1;
                }
                // MM → %m (2-digit month), M → %-m (month without leading zero)
                result.push_str(if count >= 2 { "%m" } else { "%-m" });
            }
            'd' => {
                // Count consecutive d's
                let mut count = 1;
                while chars.peek() == Some(&'d') {
                    chars.next();
                    count += 1;
                }
                // dd → %d (2-digit day), d → %-d (day without leading zero)
                result.push_str(if count >= 2 { "%d" } else { "%-d" });
            }
            'H' => {
                // Count consecutive H's
                let mut count = 1;
                while chars.peek() == Some(&'H') {
                    chars.next();
                    count += 1;
                }
                // HH → %H (24-hour with zero-pad), H → %-H (without)
                result.push_str(if count >= 2 { "%H" } else { "%-H" });
            }
            'h' => {
                // Count consecutive h's
                let mut count = 1;
                while chars.peek() == Some(&'h') {
                    chars.next();
                    count += 1;
                }
                // hh → %I (12-hour with zero-pad), h → %-I (without)
                result.push_str(if count >= 2 { "%I" } else { "%-I" });
            }
            'm' => {
                // Count consecutive m's
                let mut count = 1;
                while chars.peek() == Some(&'m') {
                    chars.next();
                    count += 1;
                }
                // mm → %M (minute with zero-pad), m → %-M (without)
                result.push_str(if count >= 2 { "%M" } else { "%-M" });
            }
            's' => {
                // Count consecutive s's
                let mut count = 1;
                while chars.peek() == Some(&'s') {
                    chars.next();
                    count += 1;
                }
                // ss → %S (second with zero-pad), s → %-S (without)
                result.push_str(if count >= 2 { "%S" } else { "%-S" });
            }
            'S' => {
                // Count consecutive S's for milliseconds
                let mut count = 1;
                while chars.peek() == Some(&'S') {
                    chars.next();
                    count += 1;
                }
                // SSS → %3f (milliseconds), SS → %2f, S → %1f
                result.push_str(&format!("%{}f", count.min(3)));
            }
            'a' => {
                // a → %p (AM/PM)
                result.push_str("%p");
            }
            'z' => {
                // z → %Z (timezone abbreviation)
                result.push_str("%Z");
            }
            'Z' => {
                // Z → %z (timezone offset)
                result.push_str("%z");
            }
            _ => {
                // Keep all other characters as-is (separators, etc.)
                result.push(ch);
            }
        }
    }

    result
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract value from JSON using JSONPath expression
///
/// **Supported JSONPath Syntax**:
/// - `$.field` - Top-level field
/// - `$.nested.field` - Nested field
/// - `$.array[0]` - Array element
/// - `$.nested.array[0].field` - Complex path
///
/// Extract value from JSON using JSONPath
///
/// # Parameters
/// - `json`: The JSON value to extract from
/// - `path`: The JSONPath expression (e.g., "$.field" or "$.nested.field")
/// - `max_depth`: Maximum allowed nesting depth (for DoS protection)
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

    // Check nesting depth to prevent DoS attacks
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
        // Handle array indexing: field[index]
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

/// Convert JSON value to AttributeValue
/// Convert JSON value to AttributeValue with optional date parsing
///
/// If `date_format` is provided, attempts to parse string values as dates using the format.
/// If parsing succeeds, returns the timestamp in milliseconds as AttributeValue::Long.
/// If parsing fails, keeps the value as AttributeValue::String.
pub fn json_value_to_attribute(
    value: &JsonValue,
    date_format: Option<&str>,
) -> Result<AttributeValue, EventFluxError> {
    match value {
        JsonValue::String(s) => {
            // Try to parse as date if date_format is configured
            if let Some(format) = date_format {
                match chrono::NaiveDateTime::parse_from_str(s, format) {
                    Ok(dt) => {
                        // Convert to timestamp in milliseconds
                        let timestamp = dt.and_utc().timestamp_millis();
                        return Ok(AttributeValue::Long(timestamp));
                    }
                    Err(e) => {
                        // Log warning when date parsing configured but fails
                        // This helps debugging - user expects date parsing but gets String instead
                        eprintln!(
                            "Warning: Failed to parse date '{}' with format '{}': {}. Falling back to String.",
                            s, format, e
                        );
                        // Fall back to String (keep original value)
                        return Ok(AttributeValue::String(s.clone()));
                    }
                }
            }
            Ok(AttributeValue::String(s.clone()))
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Check if it fits in i32
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
            // For complex types, serialize to JSON string
            let json_str =
                serde_json::to_string(value).map_err(|e| EventFluxError::MappingFailed {
                    message: format!("Failed to serialize complex JSON value: {}", e),
                    source: Some(Box::new(e)),
                })?;
            Ok(AttributeValue::String(json_str))
        }
    }
}

/// Convert Event to JSON value (legacy - uses generic field names)
pub fn event_to_json(event: &Event) -> Result<JsonValue, EventFluxError> {
    event_to_json_with_names(event, None)
}

/// Convert Event to JSON value with optional schema field names
pub fn event_to_json_with_names(
    event: &Event,
    field_names: Option<&[String]>,
) -> Result<JsonValue, EventFluxError> {
    let mut obj = serde_json::Map::new();

    // Add timestamp
    obj.insert(
        "_timestamp".to_string(),
        JsonValue::Number(event.timestamp.into()),
    );

    // Add event data with schema field names if available
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

/// Convert AttributeValue to JSON value
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

/// Render template with event data
///
/// **Template Variables**:
/// - `{{fieldName}}` - Any stream attribute (by index field_0, field_1, etc.)
/// - `{{_timestamp}}` - Event processing timestamp
/// - `{{_eventTime}}` - Original event time (if available)
/// - `{{_streamName}}` - Source stream name (if available)
///
/// **Implementation**: Simple text replacement (no complex logic)
pub fn render_template(template: &str, event: &Event) -> Result<String, EventFluxError> {
    let mut result = template.to_string();

    // Replace system variables
    result = result.replace("{{_timestamp}}", &event.timestamp.to_string());

    // Replace event attributes (field_0, field_1, etc.)
    for (idx, attr_value) in event.data.iter().enumerate() {
        let field_name = format!("field_{}", idx);
        let placeholder = format!("{{{{{}}}}}", field_name);
        let value_str = attribute_value_to_string(attr_value);
        result = result.replace(&placeholder, &value_str);
    }

    Ok(result)
}

/// Convert AttributeValue to string for template rendering
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
        // All auto-mapped (no mappings)
        let mapper1 = JsonSourceMapper::new();
        assert!(mapper1.mappings.is_empty());

        // All explicit (any mapping present = all explicit)
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

    // ERROR CASE TESTS - Testing bug fixes

    #[test]
    fn test_ignore_parse_errors_enabled() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_ignore_parse_errors(true);

        // Malformed JSON should be skipped, not error
        let result = mapper.map(b"invalid json{{{");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0); // No events, just skipped
    }

    #[test]
    fn test_ignore_parse_errors_disabled() {
        let mapper = JsonSourceMapper::new(); // ignore_parse_errors = false by default

        // Malformed JSON should cause error
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
        // First field (timestamp) should be parsed as Long (milliseconds)
        assert!(matches!(events[0].data[0], AttributeValue::Long(_)));
        // Second field (value) should remain as Int
        assert!(matches!(events[0].data[1], AttributeValue::Int(100)));
    }

    #[test]
    fn test_date_format_parsing_fallback() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_date_format(Some("%Y-%m-%d".to_string()));

        // Date doesn't match format, should fall back to String
        let json_str = r#"{"timestamp": "not a date", "value": 100}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        // Should keep as String since parsing failed
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "not a date"
        ));
    }

    #[test]
    fn test_max_input_size_limit() {
        let mut mapper = JsonSourceMapper::new();
        mapper.set_max_input_size(100); // Set small limit

        // Create JSON larger than limit
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
        mapper.set_max_nesting_depth(3); // Set small limit

        let mut mappings = HashMap::new();
        // This path has 5 levels: a.b.c.d.e (exceeds limit of 3)
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

        // Create JSON within limit
        let json = format!(r#"{{"data": "{}"}}"#, "x".repeat(1000));
        let result = mapper.map(json.as_bytes());

        // Should succeed since it's within limit
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
        // Single-char specifiers omit leading zero (Java SimpleDateFormat behavior)
        assert_eq!(convert_java_date_format("H:m:s"), "%-H:%-M:%-S");
        assert_eq!(convert_java_date_format("HH:mm:ss"), "%H:%M:%S");
        // 12-hour format
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
        // Already chrono format - pass through unchanged
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

        // Set Java format - should be auto-converted
        mapper.set_date_format(Some("yyyy-MM-dd'T'HH:mm:ss".to_string()));

        // Verify internal format is converted
        assert_eq!(mapper.date_format, Some("%Y-%m-%dT%H:%M:%S".to_string()));
    }

    #[test]
    fn test_date_format_chrono_passthrough() {
        let mut mapper = JsonSourceMapper::new();

        // Set chrono format - should pass through unchanged
        mapper.set_date_format(Some("%Y-%m-%d %H:%M:%S".to_string()));

        // Verify internal format is unchanged
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

        // First field should be timestamp parsed as Long (milliseconds)
        match &events[0].data[0] {
            AttributeValue::Long(ts) => {
                // Verify it's a reasonable timestamp (around October 2025)
                assert!(*ts > 1_700_000_000_000); // After 2023
                assert!(*ts < 1_800_000_000_000); // Before 2027
            }
            _ => panic!("Expected Long timestamp, got {:?}", events[0].data[0]),
        }
    }

    // P1.9 - Field Ordering Tests
    // These tests verify that JSON field order is preserved, not alphabetically sorted

    #[test]
    fn test_auto_mapping_preserves_json_field_order() {
        // Test JSON with fields in non-alphabetical order
        // If alphabetically sorted: amount, symbol, timestamp
        // If order preserved: symbol, timestamp, amount
        let json_str = r#"{"symbol": "AAPL", "timestamp": 1234567890, "amount": 100.5}"#;
        let mapper = JsonSourceMapper::new();

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);

        // Verify fields are in JSON insertion order (symbol, timestamp, amount)
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

        // Verify order is: priority, name, id (NOT alphabetical)
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
    // Schema-Aware Auto-Mapping Tests (#201)
    // JSON fields must match stream fields by name: exact match first,
    // then case-insensitive, with positional order only as a fallback.
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
        // Issue #201 repro: schema is (price, symbol, volume) but the JSON
        // keys arrive in a different order (symbol, price, volume).
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
        // Documented behavior: {"Symbol": ..., "PRICE": ..., "Volume": ...}
        // still maps correctly due to case-insensitive matching
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
        // When both an exact and a case-variant key exist, exact wins
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
        // Schema (id, ID) with only "id" in the JSON: "id" matches exactly,
        // and "ID" must NOT reuse the same key case-insensitively
        let mapper = schema_mapper(&["id", "ID"]);
        let events = mapper.map(br#"{"id": 1}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Null));

        // Same with the case-variant field declared first: the exact match
        // still wins regardless of schema order
        let mapper = schema_mapper(&["ID", "id"]);
        let events = mapper.map(br#"{"id": 1}"#).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Null));
        assert!(matches!(events[0].data[1], AttributeValue::Int(1)));
    }

    #[test]
    fn test_schema_auto_mapping_key_matching_two_fields_by_case_is_rejected() {
        // Neither field matches exactly and "Id" fits both by case: picking
        // one would be a guess based on schema order, so the event fails
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
        // Field "id" has no exact key and fits both "Id" and "iD": the
        // result must not depend on which key appears first in the payload
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
    fn test_schema_auto_mapping_positional_fallback() {
        // No JSON key matches any schema name: fall back to positional order
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
        // Once ANY field matches by name, the payload is treated as
        // name-based: an unmatched field becomes Null instead of stealing a
        // positional value (which could consume a value twice)
        let mapper = schema_mapper(&["price", "symbol"]);
        let json_str = r#"{"symbol": "AAPL", "cost": 185.5}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        // "price" has no name match → Null (NOT positional "AAPL", which is
        // already consumed by "symbol")
        assert!(matches!(events[0].data[0], AttributeValue::Null));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
    }

    #[test]
    fn test_schema_auto_mapping_extra_key_does_not_fill_missing_field() {
        // An extra JSON key must not leak into an unmatched schema field
        // through the positional fallback
        let mapper = schema_mapper(&["price", "symbol", "volume"]);
        let json_str = r#"{"price": 1.0, "symbol": "AAPL", "venue": "NYSE"}"#;

        let events = mapper.map(json_str.as_bytes()).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 1.0).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        // "volume" is absent → Null, not "NYSE"
        assert!(matches!(events[0].data[2], AttributeValue::Null));
    }

    #[test]
    fn test_schema_auto_mapping_missing_field_is_null() {
        // Schema has more fields than the JSON object provides: the
        // unmatched trailing field maps to Null, not an error
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
        // JSON carries extra fields the schema doesn't declare — they are skipped
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
        // Explicit JSONPath mappings must be emitted in schema order,
        // not alphabetical order
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), "$.trade.sym".to_string());
        mappings.insert("amount".to_string(), "$.trade.total".to_string());

        // Schema order: symbol first, amount second (alphabetical would
        // put amount first)
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
        // mapping.* property is set (all-or-nothing policy). This must fail
        // when the mapper is configured, not per event at runtime.
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
        // accepted, consistent with auto-mapping's case-insensitive matching
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
        // Schema (foo, FOO) with only mapping.FOO: FOO takes its exact key,
        // and foo must not reuse it through the case-insensitive fallback
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
        // Neither field matches exactly and "Foo" fits both by case: this is
        // ambiguous, not "first field in schema order wins"
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
        // Schema (foo, FOO) with keys "Foo" and "fOO": neither key is exact
        // and each fits both fields. Greedy assignment would depend on
        // HashMap order; it must be rejected, every time.
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
        // One field, one case-variant key: unambiguous, so it is accepted
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
        // A mapping key matching no stream field (typo, stale entry) is a
        // configuration error, not a silently dead key
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
        // Empty schema is canonicalized to "unset": legacy insertion-order
        // behavior applies
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
        // Name-matched values still go through date parsing
        let mut mapper = schema_mapper(&["value", "timestamp"]);
        mapper.set_date_format(Some("%Y-%m-%d %H:%M:%S".to_string()));

        let json_str = r#"{"timestamp": "2025-10-19 12:34:56", "value": 100}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert!(matches!(events[0].data[0], AttributeValue::Int(100)));
        assert!(matches!(events[0].data[1], AttributeValue::Long(_)));
    }
}
