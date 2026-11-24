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

//! WITH clause reading and checks.
//!
//! Reads the WITH (...) options of CREATE STREAM and CREATE TABLE statements into a
//! FlatConfig and checks them.

use sqlparser::ast::{CreateTableOptions, Expr, SqlOption};

use super::error::ConverterError;
use crate::core::config::stream_config::{FlatConfig, PropertySource};

/// Read the WITH (...) options into a FlatConfig.
///
/// Each option is stored with `PropertySource::SqlWith`. A bare identifier option is
/// stored as a key with an empty value. An option set to NULL is left out, so it
/// counts as not set. No WITH clause gives an empty config.
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
/// * `Ok(FlatConfig)` - The options read
/// * `Err(ConverterError)` - `UnsupportedFeature` for options other than key and value
///   pairs or bare identifiers, or for an options form other than WITH (...).
///   `InvalidExpression` for a value that is not a number, string, boolean, NULL or
///   identifier.
pub fn extract_with_options(
    table_options: &CreateTableOptions,
) -> Result<FlatConfig, ConverterError> {
    let mut config = FlatConfig::new();

    match table_options {
        CreateTableOptions::With(options) => {
            for option in options {
                match option {
                    SqlOption::KeyValue { key, value } => {
                        // NULL means not set, the same as a YAML key with no value
                        if let Some(value_str) = extract_value_as_string(value)? {
                            config.set(key.value.clone(), value_str, PropertySource::SqlWith);
                        }
                    }
                    SqlOption::Ident(ident) => {
                        // A bare flag such as HEAP. Stored as a key with an empty value.
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
            // No WITH clause. Return the empty config.
        }
        _ => {
            return Err(ConverterError::UnsupportedFeature(
                "Only WITH(...) clause supported for stream configuration".to_string(),
            ));
        }
    }

    Ok(config)
}

/// Turn a WITH option value into a string.
///
/// Numbers keep their text, quoted strings lose their quotes, booleans become "true" or
/// "false" and an identifier gives its name. NULL gives `None`, so the option is left
/// out instead of reaching a connector as the text "null". Anything else is an
/// `InvalidExpression` error.
fn extract_value_as_string(expr: &Expr) -> Result<Option<String>, ConverterError> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            sqlparser::ast::Value::Number(n, _) => Ok(Some(n.clone())),
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Some(s.clone())),
            sqlparser::ast::Value::Boolean(b) => Ok(Some(b.to_string())),
            sqlparser::ast::Value::Null => Ok(None),
            _ => Err(ConverterError::InvalidExpression(format!(
                "Unsupported value type in WITH clause: {:?}",
                value_with_span.value
            ))),
        },
        Expr::Identifier(ident) => Ok(Some(ident.value.clone())),
        _ => Err(ConverterError::InvalidExpression(format!(
            "Complex expressions not supported in WITH clause: {}",
            expr
        ))),
    }
}

/// Check a WITH clause config when the SQL is parsed.
///
/// # Validation Rules
///
/// - **No type but an extension**: This is a table. Nothing is checked here. The table
///   factory checks it at runtime.
/// - **Type**: If given, it must be 'source', 'sink' or 'internal'
/// - **Source and sink streams**: Must have 'extension'. 'format' is optional because some
///   connectors, such as timer, use their own internal format.
/// - **Internal streams**: Must not have 'extension' or 'format'
/// - **Plain internal streams** (no type, no extension): Must not have 'format'
///
/// # Arguments
///
/// * `config` - The FlatConfig read from the WITH clause
///
/// # Returns
///
/// * `Ok(())` - The config passes every rule
/// * `Err(ConverterError)` - `InvalidExpression` naming the broken rule
pub fn validate_with_clause(config: &FlatConfig) -> Result<(), ConverterError> {
    // Tables set 'extension' (such as 'cache' or 'jdbc') and no 'type'. Streams set 'type'.
    if config.get("type").is_none() && config.get("extension").is_some() {
        return Ok(());
    }

    if let Some(stream_type_str) = config.get("type") {
        match stream_type_str.as_str() {
            "source" | "sink" => {
                // 'format' is optional. Some connectors, such as timer, need none.
                require_property(config, "extension")?;
            }
            "internal" => {
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
        // No type and no extension. A plain internal stream that may still set other
        // options, such as async settings. The extension case returned above.
        if config.get("format").is_some() {
            return Err(ConverterError::InvalidExpression(
                "Pure internal streams (no type) cannot specify 'format'".to_string(),
            ));
        }
    }

    Ok(())
}

/// Return an `InvalidExpression` error naming `key` when the config lacks it.
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
    fn test_extract_with_options_null_is_left_out() {
        let options = CreateTableOptions::With(vec![
            create_key_value_option("type", "sink"),
            SqlOption::KeyValue {
                key: Ident::new("file.path"),
                value: Expr::Value(Value::Null.into()),
            },
        ]);

        let config = extract_with_options(&options).unwrap();
        assert_eq!(config.len(), 1);
        assert!(config.get("file.path").is_none());
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
        // 'format' is optional. Some sources, such as timer, need none.
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
        // No type but an extension means a table, such as cache or jdbc
        config.set("extension", "cache", PropertySource::SqlWith);

        let result = validate_with_clause(&config);
        assert!(result.is_ok(), "Tables with extension should be valid");
    }

    #[test]
    fn test_validate_with_clause_pure_internal_with_format() {
        let mut config = FlatConfig::new();
        // No type and no extension means a plain internal stream
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
