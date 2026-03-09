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

//! WITH Clause Extraction and Validation
//!
//! This module provides functions to extract and validate SQL WITH clause options
//! for CREATE STREAM statements, converting them into FlatConfig for configuration management.

use sqlparser::ast::{CreateTableOptions, Expr, SqlOption};

use super::error::ConverterError;
use crate::core::config::stream_config::{FlatConfig, PropertySource};

/// Extract WITH clause options into FlatConfig
///
/// Parses SqlOptions from CREATE STREAM WITH(...) clause and stores them
/// with PropertySource::SqlWith priority.
///
/// # Example
///
/// ```sql
/// CREATE STREAM DataStream (id INT, value DOUBLE)
/// WITH (
///     type = 'source',
///     extension = 'kafka',
///     format = 'json',
///     kafka.bootstrap.servers = 'localhost:9092'
/// );
/// ```
///
/// # Arguments
///
/// * `table_options` - The CreateTableOptions from the parsed CREATE TABLE statement
///
/// # Returns
///
/// * `Ok(FlatConfig)` - Successfully extracted configuration
/// * `Err(ConverterError)` - If WITH clause contains invalid options
pub fn extract_with_options(
    table_options: &CreateTableOptions,
) -> Result<FlatConfig, ConverterError> {
    let mut config = FlatConfig::new();

    match table_options {
        CreateTableOptions::With(options) => {
            for option in options {
                match option {
                    SqlOption::KeyValue { key, value } => {
                        let key_str = key.value.clone();
                        let value_str = extract_value_as_string(value)?;
                        config.set(key_str, value_str, PropertySource::SqlWith);
                    }
                    SqlOption::Ident(ident) => {
                        // Boolean flags like "HEAP" or standalone identifiers
                        // Treat as key with empty value
                        config.set(ident.value.clone(), "", PropertySource::SqlWith);
                    }
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Complex WITH clause options (CLUSTERED, PARTITION) not supported for streams".to_string()
                        ));
                    }
                }
            }
        }
        CreateTableOptions::None => {
            // No WITH clause - return empty config
        }
        _ => {
            return Err(ConverterError::UnsupportedFeature(
                "Only WITH(...) clause supported for stream configuration".to_string(),
            ));
        }
    }

    Ok(config)
}

/// Extract SQL expression value as string
///
/// Converts various SQL expression types to string representation for configuration storage.
fn extract_value_as_string(expr: &Expr) -> Result<String, ConverterError> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            sqlparser::ast::Value::Number(n, _) => Ok(n.clone()),
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::Boolean(b) => Ok(b.to_string()),
            sqlparser::ast::Value::Null => Ok("null".to_string()),
            _ => Err(ConverterError::InvalidExpression(format!(
                "Unsupported value type in WITH clause: {:?}",
                value_with_span.value
            ))),
        },
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        _ => Err(ConverterError::InvalidExpression(format!(
            "Complex expressions not supported in WITH clause: {}",
            expr
        ))),
    }
}

/// Validate WITH clause configuration
///
/// Performs parse-time validation of stream configuration properties.
///
/// # Validation Rules
///
/// - **Type validation**: If type is specified, must be 'source', 'sink', or 'internal'
/// - **Source/Sink streams**: Must have both 'extension' and 'format'
/// - **Internal streams**: Must NOT have 'extension' or 'format'
/// - **Pure internal streams** (no type): Must NOT have 'extension' or 'format'
///
/// # Arguments
///
/// * `config` - The FlatConfig extracted from WITH clause
///
/// # Returns
///
/// * `Ok(())` - Configuration is valid
/// * `Err(ConverterError)` - Configuration violates validation rules
pub fn validate_with_clause(config: &FlatConfig) -> Result<(), ConverterError> {
    // Check if this is a table configuration (has 'extension' but no 'type')
    // Tables use 'extension' directly (e.g., 'cache', 'jdbc')
    // Streams use 'type' (e.g., 'source', 'sink', 'internal')
    if config.get("type").is_none() && config.get("extension").is_some() {
        // This is a TABLE with extension - skip stream validation
        // Tables are validated at runtime by the table factory
        return Ok(());
    }

    // Stream validation
    if let Some(stream_type_str) = config.get("type") {
        match stream_type_str.as_str() {
            "source" | "sink" => {
                // External streams require extension
                // Format is optional - some sources/sinks (like timer) use internal binary format
                require_property(config, "extension")?;
            }
            "internal" => {
                // Internal streams must NOT have extension or format
                if config.get("extension").is_some() {
                    return Err(ConverterError::InvalidExpression(
                        "Stream with type='internal' cannot specify 'extension'".to_string(),
                    ));
                }
                if config.get("format").is_some() {
                    return Err(ConverterError::InvalidExpression(
                        "Stream with type='internal' cannot specify 'format'".to_string(),
                    ));
                }
            }
            other => {
                return Err(ConverterError::InvalidExpression(format!(
                    "Invalid stream type '{}', must be 'source', 'sink', or 'internal'",
                    other
                )));
            }
        }
    } else {
        // No type and no extension = pure internal stream (async properties, etc.)
        // Pure internal streams cannot have extension or format
        if config.get("format").is_some() {
            return Err(ConverterError::InvalidExpression(
                "Pure internal streams (no type) cannot specify 'format'".to_string(),
            ));
        }
    }

    Ok(())
}

/// Check if a required property exists in configuration
///
/// Helper function for validation that returns a descriptive error if property is missing.
fn require_property(config: &FlatConfig, key: &str) -> Result<(), ConverterError> {
    if !config.contains(key) {
        return Err(ConverterError::InvalidExpression(format!(
            "Missing required property '{}'",
            key
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, Value};

    fn create_key_value_option(key: &str, value: &str) -> SqlOption {
        SqlOption::KeyValue {
            key: Ident::new(key),
            value: Expr::Value(Value::SingleQuotedString(value.to_string()).into()),
        }
    }

    #[test]
    fn test_extract_with_options_empty() {
        let options = CreateTableOptions::None;
        let config = extract_with_options(&options).unwrap();
        assert!(config.is_empty());
    }

    #[test]
    fn test_extract_with_options_source_stream() {
        let options = CreateTableOptions::With(vec![
            create_key_value_option("type", "source"),
            create_key_value_option("extension", "kafka"),
            create_key_value_option("format", "json"),
        ]);

        let config = extract_with_options(&options).unwrap();
        assert_eq!(config.get("type"), Some(&"source".to_string()));
        assert_eq!(config.get("extension"), Some(&"kafka".to_string()));
        assert_eq!(config.get("format"), Some(&"json".to_string()));
    }

    #[test]
    fn test_extract_with_options_with_properties() {
        let options = CreateTableOptions::With(vec![
            create_key_value_option("type", "source"),
            create_key_value_option("extension", "kafka"),
            create_key_value_option("format", "json"),
            create_key_value_option("bootstrap_servers", "localhost:9092"),
            create_key_value_option("topic", "events"),
        ]);

        let config = extract_with_options(&options).unwrap();
        assert_eq!(config.len(), 5);
        assert_eq!(
            config.get("bootstrap_servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(config.get("topic"), Some(&"events".to_string()));
    }

    #[test]
    fn test_extract_with_options_boolean() {
        let options = CreateTableOptions::With(vec![
            create_key_value_option("type", "internal"),
            SqlOption::KeyValue {
                key: Ident::new("enabled"),
                value: Expr::Value(Value::Boolean(true).into()),
            },
        ]);

        let config = extract_with_options(&options).unwrap();
        assert_eq!(config.get("enabled"), Some(&"true".to_string()));
    }

    #[test]
    fn test_extract_with_options_number() {
        let options = CreateTableOptions::With(vec![SqlOption::KeyValue {
            key: Ident::new("buffer_size"),
            value: Expr::Value(Value::Number("1024".to_string(), false).into()),
        }]);

        let config = extract_with_options(&options).unwrap();
        assert_eq!(config.get("buffer_size"), Some(&"1024".to_string()));
    }

    #[test]
    fn test_validate_with_clause_source_valid() {
        let mut config = FlatConfig::new();
        config.set("type", "source", PropertySource::SqlWith);
        config.set("extension", "kafka", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith);

        assert!(validate_with_clause(&config).is_ok());
    }

    #[test]
    fn test_validate_with_clause_sink_valid() {
        let mut config = FlatConfig::new();
        config.set("type", "sink", PropertySource::SqlWith);
        config.set("extension", "log", PropertySource::SqlWith);
        config.set("format", "text", PropertySource::SqlWith);

        assert!(validate_with_clause(&config).is_ok());
    }

    #[test]
    fn test_validate_with_clause_internal_valid() {
        let mut config = FlatConfig::new();
        config.set("type", "internal", PropertySource::SqlWith);

        assert!(validate_with_clause(&config).is_ok());
    }

    #[test]
    fn test_validate_with_clause_pure_internal_valid() {
        let config = FlatConfig::new(); // No type specified
        assert!(validate_with_clause(&config).is_ok());
    }

    #[test]
    fn test_validate_with_clause_source_missing_extension() {
        let mut config = FlatConfig::new();
        config.set("type", "source", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required property 'extension'"));
    }

    #[test]
    fn test_validate_with_clause_source_without_format() {
        // Format is optional - some sources use internal binary format
        let mut config = FlatConfig::new();
        config.set("type", "source", PropertySource::SqlWith);
        config.set("extension", "timer", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(
            result.is_ok(),
            "Sources should be allowed without format (e.g., timer uses binary passthrough)"
        );
    }

    #[test]
    fn test_validate_with_clause_internal_with_extension() {
        let mut config = FlatConfig::new();
        config.set("type", "internal", PropertySource::SqlWith);
        config.set("extension", "kafka", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("type='internal' cannot specify 'extension'"));
    }

    #[test]
    fn test_validate_with_clause_internal_with_format() {
        let mut config = FlatConfig::new();
        config.set("type", "internal", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("type='internal' cannot specify 'format'"));
    }

    #[test]
    fn test_validate_with_clause_invalid_type() {
        let mut config = FlatConfig::new();
        config.set("type", "invalid", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid stream type"));
    }

    #[test]
    fn test_validate_with_clause_table_with_extension() {
        let mut config = FlatConfig::new();
        // No type + extension = TABLE (e.g., cache, jdbc)
        config.set("extension", "cache", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_ok(), "Tables with extension should be valid");
    }

    #[test]
    fn test_validate_with_clause_pure_internal_with_format() {
        let mut config = FlatConfig::new();
        // No type specified (pure internal)
        config.set("format", "json", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Pure internal streams (no type) cannot specify 'format'"));
    }

    #[test]
    fn test_require_property_exists() {
        let mut config = FlatConfig::new();
        config.set("key1", "value1", PropertySource::SqlWith);

        assert!(require_property(&config, "key1").is_ok());
    }

    #[test]
    fn test_require_property_missing() {
        let config = FlatConfig::new();

        let result = require_property(&config, "missing_key");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required property 'missing_key'"));
    }
}
