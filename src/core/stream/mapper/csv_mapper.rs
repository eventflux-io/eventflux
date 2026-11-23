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
//! Maps CSV to EventFlux events and back.
//!
//! ## Source Mapping (CSV → Events)
//!
//! ### Auto Mapping (no `mapping.*` properties)
//! Maps CSV columns by position to event fields, in order.
//!
//! ### Explicit Mapping (with `mapping.*` properties)
//! ```toml
//! csv.mapping.orderId = "0"    # First column
//! csv.mapping.amount = "2"     # Third column
//! csv.mapping.customer = "1"   # Second column
//! ```
//!
//! ## Sink Mapping (Events → CSV)
//!
//! Turns each event into one CSV row, with a configurable delimiter and an
//! optional header line.
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

use super::{alphabetical_mappings, now_millis, order_mappings, SinkMapper, SourceMapper};
use crate::core::event::{AttributeValue, Event};
use crate::core::exception::EventFluxError;
use std::collections::HashMap;

/// Source mapper for CSV format.
///
/// Each non empty line becomes one event with the current time as its
/// timestamp. Fields are trimmed and their type is guessed (see `parse_field`).
#[derive(Debug, Clone)]
pub struct CsvSourceMapper {
    /// Field name to column index. Empty means map all columns in order.
    mappings: HashMap<String, usize>,
    /// CSV delimiter character
    delimiter: char,
    /// The first line is a header and is skipped
    has_header: bool,
    /// Skip lines that fail to parse instead of returning an error
    ignore_parse_errors: bool,
    /// Largest input in bytes. Default 10 MB.
    max_input_size: usize,
    /// Most fields in one row. Default 1000.
    max_fields: usize,
    /// Explicit mappings as (field name, column index) in output order.
    /// Worked out at config time. Schema order when field names are set,
    /// otherwise alphabetical, since HashMap order is random.
    ordered_mappings: Vec<(String, usize)>,
}

impl CsvSourceMapper {
    /// Creates a CSV source mapper with default settings: comma delimiter, no
    /// header, 10 MB input limit, and 1000 fields per row
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            delimiter: ',',
            has_header: false,
            ignore_parse_errors: false,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_fields: 1000,                 // 1000 fields default
            ordered_mappings: Vec::new(),
        }
    }

    /// Creates a CSV source mapper with explicit column mappings. Output is in
    /// alphabetical field order until `set_field_names` is called.
    pub fn with_mappings(mappings: HashMap<String, usize>) -> Self {
        let ordered_mappings = alphabetical_mappings(&mappings);
        Self {
            mappings,
            delimiter: ',',
            has_header: false,
            ignore_parse_errors: false,
            max_input_size: 10 * 1024 * 1024, // 10 MB default
            max_fields: 1000,                 // 1000 fields default
            ordered_mappings,
        }
    }

    /// Sets the stream schema field names, in schema order.
    ///
    /// With explicit `mapping.*` properties, this puts the output in schema
    /// order and checks right away that every stream field has a mapping
    /// (matched exactly, or ignoring ASCII case) and every mapping key names a
    /// stream field. Without explicit mappings, CSV stays positional and the
    /// names are not used. An empty list clears the schema and goes back to
    /// alphabetical order.
    ///
    /// # Errors
    ///
    /// Returns an error when a field and the mappings do not match. The mapper
    /// is then left unchanged.
    pub fn set_field_names(&mut self, names: Vec<String>) -> Result<(), EventFluxError> {
        self.ordered_mappings = order_mappings(&names, &self.mappings, "csv")?;
        Ok(())
    }

    /// Sets the CSV delimiter
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter;
    }

    /// Sets whether the first line is a header to skip
    pub fn set_has_header(&mut self, has_header: bool) {
        self.has_header = has_header;
    }

    /// Sets whether lines that fail to parse are skipped instead of failing
    pub fn set_ignore_parse_errors(&mut self, ignore: bool) {
        self.ignore_parse_errors = ignore;
    }

    /// Sets the largest input in bytes, to guard against huge inputs
    pub fn set_max_input_size(&mut self, max_size: usize) {
        self.max_input_size = max_size;
    }

    /// Sets the most fields in one row, to guard against huge inputs
    pub fn set_max_fields(&mut self, max_fields: usize) {
        self.max_fields = max_fields;
    }

    /// Splits one CSV line into trimmed fields.
    ///
    /// A `"` turns quoting on or off and is not kept, so a delimiter inside
    /// quotes stays in the field. Doubled quotes (`""`) are not turned back
    /// into one quote. It never returns an error today.
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

        fields.push(current_field.trim().to_string());

        Ok(fields)
    }

    /// Turns a CSV field into an AttributeValue by guessing its type.
    ///
    /// Tries in this order: empty or `null` (any case) gives Null, `true` or
    /// `false` (any case) gives Bool, then Int (i32), Long (i64), and Double.
    /// Anything else stays a String.
    fn parse_field(&self, field: &str) -> AttributeValue {
        if field.is_empty() || field.eq_ignore_ascii_case("null") {
            return AttributeValue::Null;
        }

        if field.eq_ignore_ascii_case("true") {
            return AttributeValue::Bool(true);
        }
        if field.eq_ignore_ascii_case("false") {
            return AttributeValue::Bool(false);
        }

        if let Ok(i) = field.parse::<i32>() {
            return AttributeValue::Int(i);
        }

        if let Ok(l) = field.parse::<i64>() {
            return AttributeValue::Long(l);
        }

        if let Ok(d) = field.parse::<f64>() {
            return AttributeValue::Double(d);
        }

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

        let input_str = std::str::from_utf8(input).map_err(|e| EventFluxError::MappingFailed {
            message: format!("Invalid UTF-8 in CSV input: {}", e),
            source: Some(Box::new(e)),
        })?;

        let mut events = Vec::new();
        let mut lines = input_str.lines();

        if self.has_header {
            lines.next();
        }

        // line_num counts from the first data line, so with a header it is one
        // less than the line number in the input
        for (line_num, line) in lines.enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            let fields = match self.parse_line(line) {
                Ok(f) => f,
                Err(e) => {
                    if self.ignore_parse_errors {
                        continue;
                    } else {
                        return Err(EventFluxError::MappingFailed {
                            message: format!("CSV parse error at line {}: {}", line_num + 1, e),
                            source: None,
                        });
                    }
                }
            };

            // Reject huge rows so a sender cannot exhaust memory. This is not
            // skipped by ignore_parse_errors.
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
                // Auto mapping uses all columns in order
                fields.iter().map(|f| self.parse_field(f)).collect()
            } else {
                // Explicit mapping takes columns in the order worked out at
                // config time (schema order when known, else alphabetical). A
                // missing column gives Null.
                self.ordered_mappings
                    .iter()
                    .map(|(_, col_idx)| {
                        fields
                            .get(*col_idx)
                            .map(|f| self.parse_field(f))
                            .unwrap_or(AttributeValue::Null)
                    })
                    .collect()
            };

            events.push(Event::new_with_data(now_millis(), event_data));
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
    /// Write a header line of field names before each row
    include_header: bool,
    /// Optional custom header names
    header_names: Option<Vec<String>>,
}

impl CsvSinkMapper {
    /// Creates a CSV sink mapper with default settings: comma delimiter, no header
    pub fn new() -> Self {
        Self {
            delimiter: ',',
            include_header: false,
            header_names: None,
        }
    }

    /// Sets the CSV delimiter
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter;
    }

    /// Sets whether to write a header line before each row
    pub fn set_include_header(&mut self, include: bool) {
        self.include_header = include;
    }

    /// Sets custom header names. Without them the header is `field_0`,
    /// `field_1`, and so on. Header names are written as is, not quoted.
    pub fn set_header_names(&mut self, headers: Vec<String>) {
        self.header_names = Some(headers);
    }

    /// Formats a value for CSV output.
    ///
    /// Null becomes an empty field, Bytes becomes `<bytes:N>`, and Object
    /// becomes `<object>`. A value that holds the delimiter, a quote, or a
    /// newline is wrapped in quotes, with inner quotes doubled.
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
    fn map_event(&self, event: &Event) -> Result<Vec<u8>, EventFluxError> {
        let mut output = String::new();

        // Each event is its own message, so the header goes before EVERY
        // message. It is off by default and only written when
        // `csv.include-header` turns it on.
        if self.include_header {
            if let Some(headers) = &self.header_names {
                output.push_str(&headers.join(&self.delimiter.to_string()));
            } else {
                // Default header names are field_0, field_1, and so on
                let field_count = event.data.len();
                let headers: Vec<String> =
                    (0..field_count).map(|i| format!("field_{}", i)).collect();
                output.push_str(&headers.join(&self.delimiter.to_string()));
            }
            output.push('\n');
        }

        let fields: Vec<String> = event.data.iter().map(|v| self.format_field(v)).collect();
        output.push_str(&fields.join(&self.delimiter.to_string()));
        output.push('\n');

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
        assert_eq!(events[0].data.len(), 2); // only the mapped fields
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
        let result = mapper.map_event(&event).unwrap();
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

        let result = mapper.map_event(&event).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        let lines: Vec<&str> = csv_str.lines().collect();
        assert_eq!(lines.len(), 2); // header and data row
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
        let result = mapper.map_event(&event).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        // Must be quoted because of the commas
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

    // Error case tests, guarding earlier bug fixes

    #[test]
    fn test_csv_ignore_parse_errors_enabled() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_ignore_parse_errors(true);

        // Valid CSV, so both lines parse. The CSV parser is very lenient and
        // accepts almost any string.
        let csv_str = "123,100.0,customer1\n456,200.5,customer2";
        let events = mapper.map(csv_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_csv_max_input_size_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_input_size(100); // small limit

        // Input larger than the limit
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
        mapper.set_max_fields(5); // small limit

        // More fields than the limit
        let csv_str = "1,2,3,4,5,6,7,8,9,10";
        let result = mapper.map(csv_str.as_bytes());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Field count"));
    }

    #[test]
    fn test_csv_fields_within_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_fields(10);

        let csv_str = "1,2,3,4,5";
        let result = mapper.map(csv_str.as_bytes());

        // Within the limit, so it must succeed
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0].data.len(), 5);
    }

    #[test]
    fn test_csv_large_input_within_limit() {
        let mut mapper = CsvSourceMapper::new();
        mapper.set_max_input_size(1024 * 1024); // 1 MB limit

        let csv_str = format!("{}\n", "a,b,c".repeat(100));
        let result = mapper.map(csv_str.as_bytes());

        // Within the limit, so it must succeed
        assert!(result.is_ok());
    }

    // ========================================================================
    // Explicit Mapping Tests That Use the Schema (#201)
    // ========================================================================

    #[test]
    fn test_csv_schema_explicit_mappings_follow_schema_order() {
        // Schema order is symbol, amount. Alphabetical order would put amount
        // first and silently swap the fields.
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), 0usize);
        mappings.insert("amount".to_string(), 1usize);

        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();

        let events = mapper.map(b"AAPL,42.5").unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Double(a) if (a - 42.5).abs() < 1e-9));
    }

    #[test]
    fn test_csv_schema_explicit_mappings_missing_field_fails_at_config_time() {
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), 0usize);

        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("amount"), "got: {}", err);
    }

    #[test]
    fn test_csv_schema_explicit_mappings_stray_key_fails_at_config_time() {
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), 0usize);
        mappings.insert("stale_field".to_string(), 1usize);

        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["symbol".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("stale_field"), "got: {}", err);
    }

    #[test]
    fn test_csv_schema_case_variant_fields_do_not_share_a_column() {
        // Schema (foo, FOO) with only mapping.FOO must be rejected, not map
        // both fields to the same column
        let mut mappings = HashMap::new();
        mappings.insert("FOO".to_string(), 0usize);
        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("'foo'"), "got: {}", err);
    }

    #[test]
    fn test_csv_schema_case_only_aliases_are_rejected() {
        // Schema (foo, FOO) with keys "Foo" and "fOO" is ambiguous. It must fail
        // instead of picking columns by HashMap order.
        let mut mappings = HashMap::new();
        mappings.insert("Foo".to_string(), 0usize);
        mappings.insert("fOO".to_string(), 1usize);
        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["foo".to_string(), "FOO".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("Ambiguous"), "got: {}", err);
    }

    #[test]
    fn test_csv_schema_duplicate_field_names() {
        // With explicit mappings, one csv.mapping.id would feed both fields
        let mut mappings = HashMap::new();
        mappings.insert("id".to_string(), 0usize);
        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let err = mapper
            .set_field_names(vec!["id".to_string(), "id".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("more than once"), "got: {}", err);

        // Without mappings CSV is positional, so duplicate names are harmless
        let mut mapper = CsvSourceMapper::new();
        mapper
            .set_field_names(vec!["id".to_string(), "id".to_string()])
            .unwrap();
        let events = mapper.map(b"1,2").unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(2)));
    }

    #[test]
    fn test_csv_schema_lifecycle_never_leaves_stale_order() {
        let mut mappings = HashMap::new();
        mappings.insert("symbol".to_string(), 0usize);
        mappings.insert("amount".to_string(), 1usize);
        let mut mapper = CsvSourceMapper::with_mappings(mappings);
        let first = |m: &CsvSourceMapper| m.map(b"AAPL,5").unwrap()[0].data[0].clone();

        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();
        assert!(matches!(first(&mapper), AttributeValue::String(_)));

        // A failed update keeps the current schema order
        assert!(mapper.set_field_names(vec!["symbol".to_string()]).is_err());
        assert!(matches!(first(&mapper), AttributeValue::String(_)));

        // A new schema gets its order worked out again
        mapper
            .set_field_names(vec!["amount".to_string(), "symbol".to_string()])
            .unwrap();
        assert!(matches!(first(&mapper), AttributeValue::Int(5)));

        // Clearing the schema goes back to alphabetical order (amount, symbol)
        mapper
            .set_field_names(vec!["symbol".to_string(), "amount".to_string()])
            .unwrap();
        mapper.set_field_names(Vec::new()).unwrap();
        assert!(matches!(first(&mapper), AttributeValue::Int(5)));
    }

    #[test]
    fn test_csv_schema_without_mappings_stays_positional() {
        // CSV auto mapping works by position, so schema names are not used
        let mut mapper = CsvSourceMapper::new();
        mapper
            .set_field_names(vec!["a".to_string(), "b".to_string()])
            .unwrap();

        let events = mapper.map(b"1,2").unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(1)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(2)));
    }
}
