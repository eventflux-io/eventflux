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

//! CSV Mapper Implementation
//!
//! Provides bidirectional mapping between CSV and EventFlux events.
//!
//! ## Source Mapping (CSV → Events)
//!
//! ### Auto-Mapping (No `mapping.*` properties)
//! Maps CSV columns by position to event fields in order.
//!
//! ### Explicit Mapping (With `mapping.*` properties)
//! ```toml
//! csv.mapping.orderId = "0"    # First column
//! csv.mapping.amount = "2"     # Third column
//! csv.mapping.customer = "1"   # Second column
//! ```
//!
//! ## Sink Mapping (Events → CSV)
//!
//! Converts events to CSV format with configurable delimiter and headers.
//!
//! ## Configuration Example
//!
//! ```toml
//! [streams.InputCSV]
//! type = "source"
//! format = "csv"
//! csv.delimiter = ","
//! csv.has-header = "true"
//! csv.mapping.id = "0"
//! csv.mapping.amount = "1"
//!
//! [streams.OutputCSV]
//! type = "sink"
//! format = "csv"
//! csv.delimiter = ","
//! csv.include-header = "true"
//! ```

use super::{SinkMapper, SourceMapper};
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;
use std::collections::HashMap;

/// Source mapper for CSV format
#[derive(Debug, Clone)]
pub struct CsvSourceMapper {
    /// Field name → column index mappings
    /// Empty = map all columns in order
    mappings: HashMap<String, usize>,
    /// CSV delimiter character
    delimiter: char,
    /// Whether the first row is a header (skip it)
    has_header: bool,
    /// Whether to ignore parse errors
    ignore_parse_errors: bool,
    /// Maximum input size in bytes (default: 10 MB)
    max_input_size: usize,
    /// Maximum number of fields per row (default: 1000)
    max_fields: usize,
}

impl CsvSourceMapper {
    /// Create a new CSV source mapper with default settings (comma delimiter, no header)
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            delimiter: ',',
            has_header: false,
            ignore_parse_errors: false,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_fields: 1000,                 // 1000 fields default
        }
    }

    /// Create a CSV source mapper with explicit column mappings
    pub fn with_mappings(mappings: HashMap<String, usize>) -> Self {
        Self {
            mappings,
            delimiter: ',',
            has_header: false,
            ignore_parse_errors: false,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_fields: 1000,                 // 1000 fields default
        }
    }

    /// Set the CSV delimiter
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter;
    }

    /// Set whether the CSV has a header row
    pub fn set_has_header(&mut self, has_header: bool) {
        self.has_header = has_header;
    }

    /// Set whether to ignore parse errors
    pub fn set_ignore_parse_errors(&mut self, ignore: bool) {
        self.ignore_parse_errors = ignore;
    }

    /// Set maximum input size in bytes (for DoS protection)
    pub fn set_max_input_size(&mut self, max_size: usize) {
        self.max_input_size = max_size;
    }

    /// Set maximum number of fields per row (for DoS protection)
    pub fn set_max_fields(&mut self, max_fields: usize) {
        self.max_fields = max_fields;
    }

    /// Parse a single CSV line
    fn parse_line(&self, line: &str) -> Result<Vec<String>, EventFluxError> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;

        for ch in line.chars() {
            match ch {
                '"' => in_quotes = !in_quotes,
                c if c == self.delimiter && !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                c => current_field.push(c),
            }
        }

        // Add the last field
        fields.push(current_field.trim().to_string());

        Ok(fields)
    }

    /// Convert CSV field to AttributeValue with type inference
    fn parse_field(&self, field: &str) -> AttributeValue {
        // Try parsing as different types
        if field.is_empty() || field.eq_ignore_ascii_case("null") {
            return AttributeValue::Null;
        }

        // Try boolean
        if field.eq_ignore_ascii_case("true") {
            return AttributeValue::Bool(true);
        }
        if field.eq_ignore_ascii_case("false") {
            return AttributeValue::Bool(false);
        }

        // Try integer
        if let Ok(i) = field.parse::<i32>() {
            return AttributeValue::Int(i);
        }

        // Try long
        if let Ok(l) = field.parse::<i64>() {
            return AttributeValue::Long(l);
        }

        // Try double
        if let Ok(d) = field.parse::<f64>() {
            return AttributeValue::Double(d);
        }

        // Default to string
        AttributeValue::String(field.to_string())
    }
}

impl Default for CsvSourceMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMapper for CsvSourceMapper {
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

        let input_str = std::str::from_utf8(input).map_err(|e| EventFluxError::MappingFailed {
            message: format!("Invalid UTF-8 in CSV input: {}", e),
            source: Some(Box::new(e)),
        })?;

        let mut events = Vec::new();
        let mut lines = input_str.lines();

        // Skip header if configured
        if self.has_header {
            lines.next();
        }

        for (line_num, line) in lines.enumerate() {
            if line.trim().is_empty() {
                continue; // Skip empty lines
            }

            let fields = match self.parse_line(line) {
                Ok(f) => f,
                Err(e) => {
                    if self.ignore_parse_errors {
                        // Skip this line - continue to next
                        continue;
                    } else {
                        return Err(EventFluxError::MappingFailed {
                            message: format!("CSV parse error at line {}: {}", line_num + 1, e),
                            source: None,
                        });
                    }
                }
            };

            // Check field count to prevent DoS attacks
            if fields.len() > self.max_fields {
                return Err(EventFluxError::MappingFailed {
                    message: format!(
                        "Field count {} exceeds maximum allowed {} at line {}",
                        fields.len(),
                        self.max_fields,
                        line_num + 1
                    ),
                    source: None,
                });
            }

            let event_data = if self.mappings.is_empty() {
                // Auto-map: use all columns in order
                fields.iter().map(|f| self.parse_field(f)).collect()
            } else {
                // Explicit mapping: extract specific columns
                let mut sorted_mappings: Vec<_> = self.mappings.iter().collect();
                sorted_mappings.sort_by_key(|(field_name, _)| *field_name);

                sorted_mappings
                    .iter()
                    .map(|(_, &col_idx)| {
                        fields
                            .get(col_idx)
                            .map(|f| self.parse_field(f))
                            .unwrap_or(AttributeValue::Null)
                    })
                    .collect()
            };

            // Use current timestamp
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            events.push(Event::new_with_data(timestamp, event_data));
        }

        Ok(events)
    }

    fn clone_box(&self) -> Box<dyn SourceMapper> {
        Box::new(self.clone())
    }
}

/// Sink mapper for CSV format
#[derive(Debug, Clone)]
pub struct CsvSinkMapper {
    /// CSV delimiter character
    delimiter: char,
    /// Whether to include header row (field names)
    include_header: bool,
    /// Optional custom header names
    header_names: Option<Vec<String>>,
}

impl CsvSinkMapper {
    /// Create a new CSV sink mapper with default settings (comma delimiter, no header)
    pub fn new() -> Self {
        Self {
            delimiter: ',',
            include_header: false,
            header_names: None,
        }
    }

    /// Set the CSV delimiter
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter;
    }

    /// Set whether to include header row
    pub fn set_include_header(&mut self, include: bool) {
        self.include_header = include;
    }

    /// Set custom header names
    pub fn set_header_names(&mut self, headers: Vec<String>) {
        self.header_names = Some(headers);
    }

    /// Format a value for CSV output (escape if needed)
    fn format_field(&self, value: &AttributeValue) -> String {
        let s = match value {
            AttributeValue::String(s) => s.clone(),
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Long(l) => l.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Double(d) => d.to_string(),
            AttributeValue::Bool(b) => b.to_string(),
            AttributeValue::Null => String::new(),
            AttributeValue::Bytes(bytes) => format!("<bytes:{}>", bytes.len()),
            AttributeValue::Object(_) => "<object>".to_string(),
        };

        // Escape if contains delimiter, quotes, or newlines
        if s.contains(self.delimiter) || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s
        }
    }
}

impl Default for CsvSinkMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkMapper for CsvSinkMapper {
    fn map(&self, events: &[Event]) -> Result<Vec<u8>, EventFluxError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut output = String::new();

        // Add header if configured
        if self.include_header {
            if let Some(headers) = &self.header_names {
                output.push_str(&headers.join(&self.delimiter.to_string()));
            } else {
                // Generate default headers (field_0, field_1, etc.)
                let field_count = events[0].data.len();
                let headers: Vec<String> =
                    (0..field_count).map(|i| format!("field_{}", i)).collect();
                output.push_str(&headers.join(&self.delimiter.to_string()));
            }
            output.push('\n');
        }

        // Add data rows
        for event in events {
            let fields: Vec<String> = event.data.iter().map(|v| self.format_field(v)).collect();
            output.push_str(&fields.join(&self.delimiter.to_string()));
            output.push('\n');
        }

        Ok(output.into_bytes())
    }

    fn clone_box(&self) -> Box<dyn SinkMapper> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_source_auto_mapping() {
        let csv_str = "123,100.0,customer1\n456,200.5,customer2";
        let mapper = CsvSourceMapper::new();

        let events = mapper.map(csv_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data.len(), 3);
        assert_eq!(events[1].data.len(), 3);
    }

    #[test]
    fn test_csv_source_with_header() {
        let csv_str = "id,amount,customer\n123,100.0,customer1\n456,200.5,customer2";
        let mut mapper = CsvSourceMapper::new();
        mapper.set_has_header(true);

        let events = mapper.map(csv_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 2); // Header row skipped
    }

    #[test]
    fn test_csv_source_explicit_mapping() {
        let csv_str = "123,customer1,100.0\n456,customer2,200.5";
        let mut mappings = HashMap::new();
        mappings.insert("id".to_string(), 0);
        mappings.insert("amount".to_string(), 2);

        let mapper = CsvSourceMapper::with_mappings(mappings);

        let events = mapper.map(csv_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data.len(), 2); // Only mapped fields
    }

    #[test]
    fn test_csv_source_type_inference() {
        let csv_str = "123,100.5,true,test";
        let mapper = CsvSourceMapper::new();

        let events = mapper.map(csv_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].data[0], AttributeValue::Int(123)));
        assert!(matches!(events[0].data[1], AttributeValue::Double(_)));
        assert!(matches!(events[0].data[2], AttributeValue::Bool(true)));
        assert!(matches!(events[0].data[3], AttributeValue::String(_)));
    }

    #[test]
    fn test_csv_sink_simple() {
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::Int(42),
                AttributeValue::String("test".to_string()),
                AttributeValue::Double(99.5),
            ],
        );

        let mapper = CsvSinkMapper::new();
        let result = mapper.map(&[event]).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        assert!(csv_str.contains("42"));
        assert!(csv_str.contains("test"));
        assert!(csv_str.contains("99.5"));
    }

    #[test]
    fn test_csv_sink_with_header() {
        let event = Event::new_with_data(
            123,
            vec![
                AttributeValue::Int(42),
                AttributeValue::String("test".to_string()),
            ],
        );

        let mut mapper = CsvSinkMapper::new();
        mapper.set_include_header(true);

        let result = mapper.map(&[event]).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        let lines: Vec<&str> = csv_str.lines().collect();
        assert_eq!(lines.len(), 2); // Header + data row
        assert!(lines[0].contains("field_0"));
        assert!(lines[0].contains("field_1"));
    }

    #[test]
    fn test_csv_sink_escaping() {
        let event = Event::new_with_data(
            123,
            vec![AttributeValue::String("test,with,commas".to_string())],
        );

        let mapper = CsvSinkMapper::new();
        let result = mapper.map(&[event]).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        // Should be quoted due to commas
        assert!(csv_str.contains('"'));
    }

    #[test]
    fn test_csv_custom_delimiter() {
        let csv_str = "123|100.0|customer1";
        let mut mapper = CsvSourceMapper::new();
        mapper.set_delimiter('|');

        let events = mapper.map(csv_str.as_bytes()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);
    }

    // ERROR CASE TESTS - Testing bug fixes

    #[test]
    fn test_csv_ignore_parse_errors_enabled() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_ignore_parse_errors(true);

        // Valid CSV - both lines should parse successfully
        // (Note: CSV format is very permissive, most strings are valid)
        let csv_str = "123,100.0,customer1\n456,200.5,customer2";
        let events = mapper.map(csv_str.as_bytes()).unwrap();

        // Both lines should be parsed
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_csv_max_input_size_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_input_size(100); // Set small limit

        // Create CSV larger than limit
        let large_csv = format!("{}\n", "x,y,z".repeat(100));
        let result = mapper.map(large_csv.as_bytes());

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds maximum allowed"));
    }

    #[test]
    fn test_csv_max_fields_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_fields(5); // Set small limit

        // Create CSV with more fields than limit
        let csv_str = "1,2,3,4,5,6,7,8,9,10";
        let result = mapper.map(csv_str.as_bytes());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Field count"));
    }

    #[test]
    fn test_csv_fields_within_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_fields(10); // Set reasonable limit

        // Create CSV within limit
        let csv_str = "1,2,3,4,5";
        let result = mapper.map(csv_str.as_bytes());

        // Should succeed since it's within limit
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0].data.len(), 5);
    }

    #[test]
    fn test_csv_large_input_within_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_input_size(1024 * 1024); // 1 MB limit

        // Create CSV within limit
        let csv_str = format!("{}\n", "a,b,c".repeat(100));
        let result = mapper.map(csv_str.as_bytes());

        // Should succeed since it's within limit
        assert!(result.is_ok());
    }
}
