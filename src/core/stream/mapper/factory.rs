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

//! Mapper Factory System
//!
//! Factory traits and the built in factories that build mappers from config.
//! `create_initialized` returns a mapper that is fully set up and ready to use.
//!
//! ## Factory Pattern
//!
//! Every mapper factory works the same way.
//! 1. Parse the config into an internal config struct
//! 2. Check the config
//! 3. Build the fully set up mapper
//! 4. Return it as a boxed trait object
//!
//! ## Configuration Format
//!
//! Config is a flat HashMap whose keys start with the format name.
//!
//! ```toml
//! json.mapping.orderId = "$.order.id"
//! json.mapping.amount = "$.order.total"
//! json.ignore-parse-errors = "true"
//! json.date-format = "yyyy-MM-dd"
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use eventflux::core::stream::mapper::factory::*;
//! use std::collections::HashMap;
//!
//! let mut config = HashMap::new();
//! config.insert("json.mapping.id".to_string(), "$.order.id".to_string());
//!
//! let factory = JsonSourceMapperFactory;
//! let mapper = factory.create_initialized(&config)?;
//! ```

use super::bytes_mapper::{BytesSinkMapper, BytesSourceMapper};
use super::csv_mapper::{CsvSinkMapper, CsvSourceMapper};
use super::json_mapper::{JsonSinkMapper, JsonSourceMapper};
use super::{ensure_unique_field_names, SinkMapper, SourceMapper};
use crate::core::exception::EventFluxError;
use std::collections::HashMap;

// ============================================================================
// Factory Traits
// ============================================================================

use std::fmt::Debug;

/// Factory that builds source mappers.
///
/// This is the main factory trait for source mappers. Implementations should
/// list their config keys and return mappers that are fully set up.
pub trait SourceMapperFactory: Debug + Send + Sync {
    /// Returns the format name this factory handles, such as "json" or "csv"
    fn name(&self) -> &'static str;

    /// Lists the required config keys. For example `&[]` for JSON, which needs
    /// none, or `&["avro.schema"]` for an Avro mapper.
    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    /// Lists the optional config keys. For example
    /// `&["json.ignore-parse-errors", "json.date-format"]`.
    fn optional_parameters(&self) -> &[&str] {
        &[]
    }

    /// Builds a fully set up source mapper from config.
    ///
    /// # Arguments
    /// * `config` - Flat config map with keys that start with the format name
    ///
    /// # Returns
    /// * `Ok(Box<dyn SourceMapper>)` - Mapper ready to use
    /// * `Err(EventFluxError)` - The config could not be parsed or is invalid
    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError>;

    /// Builds a source mapper that knows the schema field names, so it can map
    /// by name.
    ///
    /// # Arguments
    /// * `config` - Flat config map with keys that start with the format name
    /// * `field_names` - Stream schema field names in schema order, such as
    ///   ["price", "symbol", "volume"]
    fn create_with_schema(
        &self,
        config: &HashMap<String, String>,
        _field_names: &[String],
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        // The default ignores the field names
        self.create_initialized(config)
    }

    /// Clones this factory into a boxed trait object
    fn clone_box(&self) -> Box<dyn SourceMapperFactory>;
}

impl Clone for Box<dyn SourceMapperFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Factory that builds sink mappers.
///
/// This is the main factory trait for sink mappers. Implementations should
/// list their config keys and return mappers that are fully set up.
pub trait SinkMapperFactory: Debug + Send + Sync {
    /// Returns the format name this factory handles, such as "json" or "csv"
    fn name(&self) -> &'static str;

    /// Lists the required config keys. For example `&[]` for JSON, or
    /// `&["csv.delimiter"]` for a CSV mapper that needs a delimiter.
    fn required_parameters(&self) -> &[&str] {
        &[]
    }

    /// Lists the optional config keys. For example
    /// `&["json.pretty-print", "json.template"]`.
    fn optional_parameters(&self) -> &[&str] {
        &[]
    }

    /// Builds a fully set up sink mapper from config.
    ///
    /// # Arguments
    /// * `config` - Flat config map with keys that start with the format name
    ///
    /// # Returns
    /// * `Ok(Box<dyn SinkMapper>)` - Mapper ready to use
    /// * `Err(EventFluxError)` - The config could not be parsed or is invalid
    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError>;

    /// Builds a sink mapper that knows the schema field names, so outputs such
    /// as JSON can use them as keys.
    ///
    /// # Arguments
    /// * `config` - Flat config map with keys that start with the format name
    /// * `field_names` - Schema field names for output, such as ["symbol", "trade_count"]
    fn create_with_schema(
        &self,
        config: &HashMap<String, String>,
        _field_names: &[String],
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        // The default ignores the field names
        self.create_initialized(config)
    }

    /// Clones this factory into a boxed trait object
    fn clone_box(&self) -> Box<dyn SinkMapperFactory>;
}

impl Clone for Box<dyn SinkMapperFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ============================================================================
// JSON Mapper Factories
// ============================================================================

/// Factory for JSON source mappers
#[derive(Debug, Clone, Default)]
pub struct JsonSourceMapperFactory;

/// Internal config for the JSON source mapper
#[derive(Debug, Clone)]
struct JsonSourceMapperConfig {
    mappings: HashMap<String, String>,
    ignore_parse_errors: bool,
    date_format: Option<String>,
}

impl JsonSourceMapperConfig {
    /// Reads `json.mapping.<field>`, `json.ignore-parse-errors`, and
    /// `json.date-format`. A flag is true only when its value is "true" in any
    /// case. Never returns an error today.
    fn parse(config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        let mut mappings = HashMap::new();

        for (key, value) in config {
            if let Some(field_name) = key.strip_prefix("json.mapping.") {
                mappings.insert(field_name.to_string(), value.clone());
            }
        }

        let ignore_parse_errors = config
            .get("json.ignore-parse-errors")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let date_format = config.get("json.date-format").cloned();

        Ok(JsonSourceMapperConfig {
            mappings,
            ignore_parse_errors,
            date_format,
        })
    }
}

impl SourceMapperFactory for JsonSourceMapperFactory {
    fn name(&self) -> &'static str {
        "json"
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "json.mapping.*",
            "json.ignore-parse-errors",
            "json.date-format",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        self.create_with_schema(config, &[])
    }

    fn create_with_schema(
        &self,
        config: &HashMap<String, String>,
        field_names: &[String],
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        Ok(Box::new(build_json_source_mapper(config, field_names)?))
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}

/// Builds a fully set up JSON source mapper. An empty `field_names` means no
/// schema.
///
/// # Errors
///
/// Returns an error when the explicit mappings do not match the schema fields.
fn build_json_source_mapper(
    config: &HashMap<String, String>,
    field_names: &[String],
) -> Result<JsonSourceMapper, EventFluxError> {
    let mapper_config = JsonSourceMapperConfig::parse(config)?;

    let mut mapper = if mapper_config.mappings.is_empty() {
        JsonSourceMapper::new()
    } else {
        JsonSourceMapper::with_mappings(mapper_config.mappings)
    };

    mapper.set_ignore_parse_errors(mapper_config.ignore_parse_errors);
    mapper.set_date_format(mapper_config.date_format);
    // With explicit mappings this checks right away that every stream field
    // is mapped and that no mapping key is left over
    mapper.set_field_names(field_names.to_vec())?;

    Ok(mapper)
}

/// Factory for JSON sink mappers
#[derive(Debug, Clone, Default)]
pub struct JsonSinkMapperFactory;

/// Internal config for the JSON sink mapper
#[derive(Debug, Clone)]
struct JsonSinkMapperConfig {
    template: Option<String>,
    pretty_print: bool,
}

impl JsonSinkMapperConfig {
    /// Reads `json.template` and `json.pretty-print`. Pretty print is on only
    /// when the value is "true" in any case. Never returns an error today.
    fn parse(config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        let template = config.get("json.template").cloned();

        let pretty_print = config
            .get("json.pretty-print")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Ok(JsonSinkMapperConfig {
            template,
            pretty_print,
        })
    }
}

impl SinkMapperFactory for JsonSinkMapperFactory {
    fn name(&self) -> &'static str {
        "json"
    }

    fn optional_parameters(&self) -> &[&str] {
        &["json.template", "json.pretty-print"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        let mapper_config = JsonSinkMapperConfig::parse(config)?;

        let mut mapper = if let Some(template) = mapper_config.template {
            JsonSinkMapper::with_template(template)
        } else {
            JsonSinkMapper::new()
        };

        mapper.set_pretty_print(mapper_config.pretty_print);

        Ok(Box::new(mapper))
    }

    fn create_with_schema(
        &self,
        config: &HashMap<String, String>,
        field_names: &[String],
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        let mapper_config = JsonSinkMapperConfig::parse(config)?;

        let mut mapper = if let Some(template) = mapper_config.template {
            JsonSinkMapper::with_template(template)
        } else {
            // Field names become output keys, so a duplicate name would make
            // one value silently overwrite the other in the output object
            ensure_unique_field_names(field_names, "json")?;
            JsonSinkMapper::new()
        };

        mapper.set_pretty_print(mapper_config.pretty_print);
        mapper.set_field_names(field_names.to_vec());

        Ok(Box::new(mapper))
    }

    fn clone_box(&self) -> Box<dyn SinkMapperFactory> {
        Box::new(self.clone())
    }
}

// ============================================================================
// CSV Mapper Factories
// ============================================================================

/// Factory for CSV source mappers
#[derive(Debug, Clone, Default)]
pub struct CsvSourceMapperFactory;

/// Internal config for the CSV source mapper
#[derive(Debug, Clone)]
struct CsvSourceMapperConfig {
    mappings: HashMap<String, usize>,
    delimiter: char,
    has_header: bool,
    ignore_parse_errors: bool,
}

impl CsvSourceMapperConfig {
    /// Reads `csv.mapping.<field>` (a column index), `csv.delimiter`,
    /// `csv.has-header`, and `csv.ignore-parse-errors`.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when a column index is not a whole number
    /// of 0 or more.
    fn parse(config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        let mut mappings = HashMap::new();

        // Field name to column index
        for (key, value) in config {
            if let Some(field_name) = key.strip_prefix("csv.mapping.") {
                let col_idx =
                    value
                        .parse::<usize>()
                        .map_err(|e| EventFluxError::Configuration {
                            message: format!(
                                "Invalid column index for {}: '{}' ({})",
                                field_name, value, e
                            ),
                            config_key: Some(key.clone()),
                        })?;
                mappings.insert(field_name.to_string(), col_idx);
            }
        }

        // Only the first character is used. Default is a comma, also when the
        // value is empty.
        let delimiter = config
            .get("csv.delimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or(',');

        let has_header = config
            .get("csv.has-header")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let ignore_parse_errors = config
            .get("csv.ignore-parse-errors")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        Ok(CsvSourceMapperConfig {
            mappings,
            delimiter,
            has_header,
            ignore_parse_errors,
        })
    }
}

impl SourceMapperFactory for CsvSourceMapperFactory {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn optional_parameters(&self) -> &[&str] {
        &[
            "csv.mapping.*",
            "csv.delimiter",
            "csv.has-header",
            "csv.ignore-parse-errors",
        ]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        self.create_with_schema(config, &[])
    }

    fn create_with_schema(
        &self,
        config: &HashMap<String, String>,
        field_names: &[String],
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        Ok(Box::new(build_csv_source_mapper(config, field_names)?))
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}

/// Builds a fully set up CSV source mapper. An empty `field_names` means no
/// schema.
///
/// # Errors
///
/// Returns an error when a column index is invalid, or the explicit mappings
/// do not match the schema fields.
fn build_csv_source_mapper(
    config: &HashMap<String, String>,
    field_names: &[String],
) -> Result<CsvSourceMapper, EventFluxError> {
    let mapper_config = CsvSourceMapperConfig::parse(config)?;

    let mut mapper = if mapper_config.mappings.is_empty() {
        CsvSourceMapper::new()
    } else {
        CsvSourceMapper::with_mappings(mapper_config.mappings)
    };

    mapper.set_delimiter(mapper_config.delimiter);
    mapper.set_has_header(mapper_config.has_header);
    mapper.set_ignore_parse_errors(mapper_config.ignore_parse_errors);
    // With explicit mappings this puts output in schema order and checks
    // right away that every stream field is mapped with no keys left over
    mapper.set_field_names(field_names.to_vec())?;

    Ok(mapper)
}

/// Factory for CSV sink mappers
#[derive(Debug, Clone, Default)]
pub struct CsvSinkMapperFactory;

/// Internal config for the CSV sink mapper
#[derive(Debug, Clone)]
struct CsvSinkMapperConfig {
    delimiter: char,
    include_header: bool,
    header_names: Option<Vec<String>>,
}

impl CsvSinkMapperConfig {
    /// Reads `csv.delimiter`, `csv.include-header`, and `csv.header-names`.
    /// Never returns an error today.
    fn parse(config: &HashMap<String, String>) -> Result<Self, EventFluxError> {
        // Only the first character is used. Default is a comma.
        let delimiter = config
            .get("csv.delimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or(',');

        let include_header = config
            .get("csv.include-header")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // Comma separated and trimmed, whatever the delimiter is
        let header_names = config
            .get("csv.header-names")
            .map(|s| s.split(',').map(|name| name.trim().to_string()).collect());

        Ok(CsvSinkMapperConfig {
            delimiter,
            include_header,
            header_names,
        })
    }
}

impl SinkMapperFactory for CsvSinkMapperFactory {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn optional_parameters(&self) -> &[&str] {
        &["csv.delimiter", "csv.include-header", "csv.header-names"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        let mapper_config = CsvSinkMapperConfig::parse(config)?;

        let mut mapper = CsvSinkMapper::new();
        mapper.set_delimiter(mapper_config.delimiter);
        mapper.set_include_header(mapper_config.include_header);

        if let Some(headers) = mapper_config.header_names {
            mapper.set_header_names(headers);
        }

        Ok(Box::new(mapper))
    }

    fn clone_box(&self) -> Box<dyn SinkMapperFactory> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Bytes Mapper Factories
// ============================================================================

/// Factory for bytes source mappers, which pass raw binary through.
///
/// The mappers keep raw binary data exactly, with no UTF-8 conversion.
/// Payloads are stored in `AttributeValue::Bytes` and can make the full trip
/// through the system unchanged.
#[derive(Debug, Clone, Default)]
pub struct BytesSourceMapperFactory;

impl SourceMapperFactory for BytesSourceMapperFactory {
    fn name(&self) -> &'static str {
        "bytes"
    }

    fn create_initialized(
        &self,
        _config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        // BytesSourceMapper has no config. It always keeps the raw bytes exactly.
        Ok(Box::new(BytesSourceMapper::new()))
    }

    fn clone_box(&self) -> Box<dyn SourceMapperFactory> {
        Box::new(self.clone())
    }
}

/// Factory for bytes sink mappers, which output one field as raw bytes.
///
/// `bytes.field-index` picks the field. Default 0. A value that is not a whole
/// number of 0 or more is an invalid parameter error.
#[derive(Debug, Clone, Default)]
pub struct BytesSinkMapperFactory;

impl SinkMapperFactory for BytesSinkMapperFactory {
    fn name(&self) -> &'static str {
        "bytes"
    }

    fn optional_parameters(&self) -> &[&str] {
        &["bytes.field-index"]
    }

    fn create_initialized(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        let field_index = match config.get("bytes.field-index") {
            Some(value) => value.parse::<usize>().map_err(|_| {
                EventFluxError::invalid_parameter_with_details(
                    format!(
                        "Invalid bytes.field-index value: '{}' is not a valid non-negative integer",
                        value
                    ),
                    "bytes.field-index",
                    "non-negative integer (e.g., 0, 1, 2)",
                )
            })?,
            None => 0, // the first field
        };

        let mut mapper = BytesSinkMapper::new();
        mapper.set_field_index(field_index);

        Ok(Box::new(mapper))
    }

    fn clone_box(&self) -> Box<dyn SinkMapperFactory> {
        Box::new(self.clone())
    }
}

// ============================================================================
// Mapper Factory Registry
// ============================================================================

/// Registry of mapper factories, keyed by format name
pub struct MapperFactoryRegistry {
    source_factories: HashMap<String, Box<dyn SourceMapperFactory>>,
    sink_factories: HashMap<String, Box<dyn SinkMapperFactory>>,
}

impl MapperFactoryRegistry {
    /// Creates a registry with the built in json, csv, and bytes factories for
    /// both sources and sinks
    pub fn new() -> Self {
        let mut registry = Self {
            source_factories: HashMap::new(),
            sink_factories: HashMap::new(),
        };

        registry.register_source_factory(Box::new(JsonSourceMapperFactory));
        registry.register_source_factory(Box::new(CsvSourceMapperFactory));
        registry.register_source_factory(Box::new(BytesSourceMapperFactory));
        registry.register_sink_factory(Box::new(JsonSinkMapperFactory));
        registry.register_sink_factory(Box::new(CsvSinkMapperFactory));
        registry.register_sink_factory(Box::new(BytesSinkMapperFactory));

        registry
    }

    /// Registers a source mapper factory under its name. Replaces any factory
    /// already registered for that format.
    pub fn register_source_factory(&mut self, factory: Box<dyn SourceMapperFactory>) {
        let format = factory.name().to_string();
        self.source_factories.insert(format, factory);
    }

    /// Registers a sink mapper factory under its name. Replaces any factory
    /// already registered for that format.
    pub fn register_sink_factory(&mut self, factory: Box<dyn SinkMapperFactory>) {
        let format = factory.name().to_string();
        self.sink_factories.insert(format, factory);
    }

    /// Finds the source mapper factory for a format. Returns a configuration
    /// error for an unknown format.
    fn source_factory(&self, format: &str) -> Result<&dyn SourceMapperFactory, EventFluxError> {
        self.source_factories
            .get(format)
            .map(|f| f.as_ref())
            .ok_or_else(|| EventFluxError::Configuration {
                message: format!("Unknown source mapper format: {}", format),
                config_key: Some("format".to_string()),
            })
    }

    /// Finds the sink mapper factory for a format. Returns a configuration
    /// error for an unknown format.
    fn sink_factory(&self, format: &str) -> Result<&dyn SinkMapperFactory, EventFluxError> {
        self.sink_factories
            .get(format)
            .map(|f| f.as_ref())
            .ok_or_else(|| EventFluxError::Configuration {
                message: format!("Unknown sink mapper format: {}", format),
                config_key: Some("format".to_string()),
            })
    }

    /// Builds a source mapper for the format. Fails for an unknown format or
    /// bad config.
    pub fn create_source_mapper(
        &self,
        format: &str,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        self.source_factory(format)?.create_initialized(config)
    }

    /// Builds a source mapper for the format that knows the schema field names.
    /// Fails for an unknown format or bad config.
    pub fn create_source_mapper_with_schema(
        &self,
        format: &str,
        config: &HashMap<String, String>,
        field_names: &[String],
    ) -> Result<Box<dyn SourceMapper>, EventFluxError> {
        self.source_factory(format)?
            .create_with_schema(config, field_names)
    }

    /// Builds a sink mapper for the format. Fails for an unknown format or bad
    /// config.
    pub fn create_sink_mapper(
        &self,
        format: &str,
        config: &HashMap<String, String>,
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        self.sink_factory(format)?.create_initialized(config)
    }

    /// Builds a sink mapper for the format that knows the schema field names.
    /// Fails for an unknown format or bad config.
    pub fn create_sink_mapper_with_schema(
        &self,
        format: &str,
        config: &HashMap<String, String>,
        field_names: &[String],
    ) -> Result<Box<dyn SinkMapper>, EventFluxError> {
        self.sink_factory(format)?
            .create_with_schema(config, field_names)
    }
}

impl Default for MapperFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_source_factory_auto_mapping() {
        let config = HashMap::new();
        let factory = JsonSourceMapperFactory;

        let mapper = factory.create_initialized(&config).unwrap();
        let json_str = r#"{"orderId": "123", "amount": 100.0}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 2);
    }

    #[test]
    fn test_json_source_factory_explicit_mapping() {
        let mut config = HashMap::new();
        config.insert("json.mapping.orderId".to_string(), "$.order.id".to_string());
        config.insert(
            "json.mapping.amount".to_string(),
            "$.order.total".to_string(),
        );

        let factory = JsonSourceMapperFactory;
        let mapper = factory.create_initialized(&config).unwrap();

        let json_str = r#"{"order": {"id": "123", "total": 100.0}}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 2);
    }

    #[test]
    fn test_json_source_factory_create_with_schema_maps_by_name() {
        use crate::core::event::AttributeValue;

        let config = HashMap::new();
        let factory = JsonSourceMapperFactory;
        let field_names = vec![
            "price".to_string(),
            "symbol".to_string(),
            "volume".to_string(),
        ];

        let mapper = factory.create_with_schema(&config, &field_names).unwrap();

        // JSON keys in a different order than the schema
        let json_str = r#"{"symbol": "AAPL", "price": 185.5, "volume": 5000}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 185.5).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[2], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_json_source_factory_create_with_schema_incomplete_mapping_fails() {
        // A schema field with no mapping must fail when the mapper is built (at
        // app startup), not on each event at runtime
        let mut config = HashMap::new();
        config.insert("json.mapping.a".to_string(), "$.x".to_string());

        let factory = JsonSourceMapperFactory;
        let field_names = vec!["a".to_string(), "b".to_string()];
        let err = factory
            .create_with_schema(&config, &field_names)
            .unwrap_err();
        assert!(err.to_string().contains('b'), "got: {}", err);
    }

    #[test]
    fn test_csv_source_factory_create_with_schema_orders_by_schema() {
        use crate::core::event::AttributeValue;

        let mut config = HashMap::new();
        config.insert("csv.mapping.symbol".to_string(), "0".to_string());
        config.insert("csv.mapping.amount".to_string(), "1".to_string());

        let factory = CsvSourceMapperFactory;
        let field_names = vec!["symbol".to_string(), "amount".to_string()];
        let mapper = factory.create_with_schema(&config, &field_names).unwrap();

        let events = mapper.map(b"AAPL,42.5").unwrap();
        assert!(matches!(
            events[0].data[0],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[1], AttributeValue::Double(a) if (a - 42.5).abs() < 1e-9));
    }

    #[test]
    fn test_json_sink_factory_rejects_duplicate_field_names() {
        // Two "id" fields would write one output key twice, silently
        // dropping a value
        let factory = JsonSinkMapperFactory;
        let field_names = vec!["id".to_string(), "id".to_string()];
        let err = factory
            .create_with_schema(&HashMap::new(), &field_names)
            .unwrap_err();
        assert!(err.to_string().contains("more than once"), "got: {}", err);

        // Template output does not use field names, so it is unaffected
        let mut config = HashMap::new();
        config.insert(
            "json.template".to_string(),
            r#"{"a":{{field_0}},"b":{{field_1}}}"#.to_string(),
        );
        assert!(factory.create_with_schema(&config, &field_names).is_ok());
    }

    #[test]
    fn test_registry_create_source_mapper_with_schema() {
        use crate::core::event::AttributeValue;

        let registry = MapperFactoryRegistry::new();
        let config = HashMap::new();
        let field_names = vec!["b".to_string(), "a".to_string()];

        let mapper = registry
            .create_source_mapper_with_schema("json", &config, &field_names)
            .unwrap();

        let events = mapper.map(r#"{"a": 1, "b": 2}"#.as_bytes()).unwrap();
        assert!(matches!(events[0].data[0], AttributeValue::Int(2)));
        assert!(matches!(events[0].data[1], AttributeValue::Int(1)));
    }

    #[test]
    fn test_json_sink_factory_simple() {
        let config = HashMap::new();
        let factory = JsonSinkMapperFactory;

        let mapper = factory.create_initialized(&config).unwrap();
        let event = crate::core::event::Event::new_with_data(
            123,
            vec![
                crate::core::event::AttributeValue::String("test".to_string()),
                crate::core::event::AttributeValue::Int(42),
            ],
        );

        let result = mapper.map_event(&event).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_json_sink_factory_with_template() {
        let mut config = HashMap::new();
        config.insert(
            "json.template".to_string(),
            r#"{"id":"{{field_0}}","value":{{field_1}}}"#.to_string(),
        );

        let factory = JsonSinkMapperFactory;
        let mapper = factory.create_initialized(&config).unwrap();

        let event = crate::core::event::Event::new_with_data(
            123,
            vec![
                crate::core::event::AttributeValue::String("test-id".to_string()),
                crate::core::event::AttributeValue::Double(99.5),
            ],
        );

        let result = mapper.map_event(&event).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        assert!(json_str.contains("\"id\":\"test-id\""));
        assert!(json_str.contains("\"value\":99.5"));
    }

    #[test]
    fn test_csv_source_factory() {
        let mut config = HashMap::new();
        config.insert("csv.delimiter".to_string(), ",".to_string());
        config.insert("csv.has-header".to_string(), "false".to_string());

        let factory = CsvSourceMapperFactory;
        let mapper = factory.create_initialized(&config).unwrap();

        let csv_str = "123,100.0,customer1";
        let events = mapper.map(csv_str.as_bytes()).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data.len(), 3);
    }

    #[test]
    fn test_csv_sink_factory() {
        let mut config = HashMap::new();
        config.insert("csv.delimiter".to_string(), ",".to_string());
        config.insert("csv.include-header".to_string(), "true".to_string());

        let factory = CsvSinkMapperFactory;
        let mapper = factory.create_initialized(&config).unwrap();

        let event = crate::core::event::Event::new_with_data(
            123,
            vec![
                crate::core::event::AttributeValue::Int(42),
                crate::core::event::AttributeValue::String("test".to_string()),
            ],
        );

        let result = mapper.map_event(&event).unwrap();
        let csv_str = String::from_utf8(result).unwrap();

        assert!(csv_str.contains("field_0"));
        assert!(csv_str.contains("42"));
        assert!(csv_str.contains("test"));
    }

    #[test]
    fn test_bytes_sink_factory_invalid_field_index() {
        let factory = BytesSinkMapperFactory;

        // A value that is not a number must fail
        let mut config = HashMap::new();
        config.insert("bytes.field-index".to_string(), "first".to_string());
        let result = factory.create_initialized(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("bytes.field-index"),
            "Error should mention the parameter: {}",
            err
        );

        // A negative value must fail, since it does not parse as usize
        let mut config = HashMap::new();
        config.insert("bytes.field-index".to_string(), "-1".to_string());
        let result = factory.create_initialized(&config);
        assert!(result.is_err());

        // A valid number must succeed
        let mut config = HashMap::new();
        config.insert("bytes.field-index".to_string(), "2".to_string());
        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_registry() {
        let registry = MapperFactoryRegistry::new();
        let config = HashMap::new();

        let json_source = registry.create_source_mapper("json", &config).unwrap();
        assert!(json_source.map(r#"{"test": 1}"#.as_bytes()).is_ok());

        let csv_source = registry.create_source_mapper("csv", &config).unwrap();
        assert!(csv_source.map("1,2,3".as_bytes()).is_ok());

        // An unknown format must fail
        assert!(registry.create_source_mapper("unknown", &config).is_err());
    }
}
