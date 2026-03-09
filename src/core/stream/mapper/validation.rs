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

//! Mapper Configuration Validation
//!
//! Provides validation functions to ensure source and sink streams
//! don't use incompatible mapper properties.
//!
//! ## Validation Rules
//!
//! ### Source Stream Restrictions
//! - **Cannot** use `{format}.template` (sink-only property)
//! - **Can** use `{format}.mapping.*` (source-only properties)
//!
//! ### Sink Stream Restrictions
//! - **Cannot** use `{format}.mapping.*` (source-only properties)
//! - **Can** use `{format}.template` (sink-only property)
//!
//! ## Example Errors
//!
//! ```text
//! Stream has type='source' but specifies 'json.template' (sink-only property)
//! Stream has type='sink' but specifies 'json.mapping.orderId' (source-only properties)
//! ```

use std::collections::HashMap;

/// Validate that a source stream configuration doesn't contain sink-only properties
///
/// # Arguments
/// * `config` - Stream configuration map
/// * `format` - Format name (e.g., "json", "csv")
///
/// # Returns
/// * `Ok(())` - Configuration is valid
/// * `Err(String)` - Validation failed with error message
///
/// # Example
/// ```rust,ignore
/// let mut config = HashMap::new();
/// config.insert("json.mapping.id".to_string(), "$.id".to_string());
///
/// // This is valid for a source stream
/// validate_source_mapper_config(&config, "json")?;
///
/// config.insert("json.template".to_string(), "...".to_string());
///
/// // This will fail - template is sink-only
/// assert!(validate_source_mapper_config(&config, "json").is_err());
/// ```
pub fn validate_source_mapper_config(
    config: &HashMap<String, String>,
    format: &str,
) -> Result<(), String> {
    // Check for sink-only properties
    let template_key = format!("{}.template", format);

    if config.contains_key(&template_key) {
        return Err(format!(
            "Stream has type='source' but specifies '{}' (sink-only property)",
            template_key
        ));
    }

    // Additional format-specific validation
    match format {
        "json" => validate_json_source_config(config),
        "csv" => validate_csv_source_config(config),
        _ => Ok(()), // Unknown formats pass validation
    }
}

/// Validate that a sink stream configuration doesn't contain source-only properties
///
/// # Arguments
/// * `config` - Stream configuration map
/// * `format` - Format name (e.g., "json", "csv")
///
/// # Returns
/// * `Ok(())` - Configuration is valid
/// * `Err(String)` - Validation failed with error message
///
/// # Example
/// ```rust,ignore
/// let mut config = HashMap::new();
/// config.insert("json.template".to_string(), "{...}".to_string());
///
/// // This is valid for a sink stream
/// validate_sink_mapper_config(&config, "json")?;
///
/// config.insert("json.mapping.id".to_string(), "$.id".to_string());
///
/// // This will fail - mapping is source-only
/// assert!(validate_sink_mapper_config(&config, "json").is_err());
/// ```
pub fn validate_sink_mapper_config(
    config: &HashMap<String, String>,
    format: &str,
) -> Result<(), String> {
    // Check for source-only properties (mapping.*)
    let mapping_prefix = format!("{}.mapping.", format);

    for key in config.keys() {
        if key.starts_with(&mapping_prefix) {
            return Err(format!(
                "Stream has type='sink' but specifies '{}' (source-only properties)",
                mapping_prefix.trim_end_matches('.')
            ));
        }
    }

    // Additional format-specific validation
    match format {
        "json" => validate_json_sink_config(config),
        "csv" => validate_csv_sink_config(config),
        _ => Ok(()), // Unknown formats pass validation
    }
}

// ============================================================================
// Format-Specific Validation
// ============================================================================

/// Validate JSON source-specific configuration
fn validate_json_source_config(config: &HashMap<String, String>) -> Result<(), String> {
    // Check for sink-only JSON properties
    let sink_only_keys = ["json.pretty-print"];

    for key in &sink_only_keys {
        if config.contains_key(*key) {
            return Err(format!(
                "Stream has type='source' but specifies '{}' (sink-only property)",
                key
            ));
        }
    }

    // Validate JSONPath syntax if mappings are present
    for (key, value) in config {
        if let Some(_field_name) = key.strip_prefix("json.mapping.") {
            if !value.starts_with("$.") {
                return Err(format!(
                    "Invalid JSONPath '{}' for {}. Must start with '$.'",
                    value, key
                ));
            }
        }
    }

    Ok(())
}

/// Validate JSON sink-specific configuration
fn validate_json_sink_config(config: &HashMap<String, String>) -> Result<(), String> {
    // Check for source-only JSON properties
    let source_only_keys = ["json.ignore-parse-errors", "json.date-format"];

    for key in &source_only_keys {
        if config.contains_key(*key) {
            return Err(format!(
                "Stream has type='sink' but specifies '{}' (source-only property)",
                key
            ));
        }
    }

    Ok(())
}

/// Validate CSV source-specific configuration
fn validate_csv_source_config(config: &HashMap<String, String>) -> Result<(), String> {
    // Check for sink-only CSV properties
    let sink_only_keys = ["csv.include-header", "csv.header-names"];

    for key in &sink_only_keys {
        if config.contains_key(*key) {
            return Err(format!(
                "Stream has type='source' but specifies '{}' (sink-only property)",
                key
            ));
        }
    }

    // Validate column indices if mappings are present
    for (key, value) in config {
        if let Some(_field_name) = key.strip_prefix("csv.mapping.") {
            if value.parse::<usize>().is_err() {
                return Err(format!(
                    "Invalid column index '{}' for {}. Must be a non-negative integer",
                    value, key
                ));
            }
        }
    }

    Ok(())
}

/// Validate CSV sink-specific configuration
fn validate_csv_sink_config(config: &HashMap<String, String>) -> Result<(), String> {
    // Check for source-only CSV properties
    let source_only_keys = ["csv.has-header", "csv.ignore-parse-errors"];

    for key in &source_only_keys {
        if config.contains_key(*key) {
            return Err(format!(
                "Stream has type='sink' but specifies '{}' (source-only property)",
                key
            ));
        }
    }

    Ok(())
}

/// Validate mapper configuration for a stream type
///
/// This is a convenience function that dispatches to the appropriate validator
/// based on stream type.
///
/// # Arguments
/// * `config` - Stream configuration map
/// * `format` - Format name (e.g., "json", "csv")
/// * `stream_type` - Either "source" or "sink"
///
/// # Returns
/// * `Ok(())` - Configuration is valid
/// * `Err(String)` - Validation failed with error message
pub fn validate_mapper_config(
    config: &HashMap<String, String>,
    format: &str,
    stream_type: &str,
) -> Result<(), String> {
    match stream_type {
        "source" => validate_source_mapper_config(config, format),
        "sink" => validate_sink_mapper_config(config, format),
        _ => Err(format!(
            "Unknown stream type '{}'. Must be 'source' or 'sink'",
            stream_type
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // JSON Source Validation Tests
    // ========================================================================

    #[test]
    fn test_json_source_valid_mapping() {
        let mut config = HashMap::new();
        config.insert("json.mapping.id".to_string(), "$.order.id".to_string());
        config.insert(
            "json.mapping.amount".to_string(),
            "$.order.total".to_string(),
        );

        assert!(validate_source_mapper_config(&config, "json").is_ok());
    }

    #[test]
    fn test_json_source_rejects_template() {
        let mut config = HashMap::new();
        config.insert("json.template".to_string(), "{...}".to_string());

        let result = validate_source_mapper_config(&config, "json");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("json.template"));
        assert!(err_msg.contains("sink-only"));
    }

    #[test]
    fn test_json_source_rejects_pretty_print() {
        let mut config = HashMap::new();
        config.insert("json.pretty-print".to_string(), "true".to_string());

        let result = validate_source_mapper_config(&config, "json");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("json.pretty-print"));
    }

    #[test]
    fn test_json_source_invalid_jsonpath() {
        let mut config = HashMap::new();
        config.insert("json.mapping.id".to_string(), "invalid.path".to_string());

        let result = validate_source_mapper_config(&config, "json");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Invalid JSONPath"));
    }

    // ========================================================================
    // JSON Sink Validation Tests
    // ========================================================================

    #[test]
    fn test_json_sink_valid_template() {
        let mut config = HashMap::new();
        config.insert("json.template".to_string(), "{...}".to_string());
        config.insert("json.pretty-print".to_string(), "true".to_string());

        assert!(validate_sink_mapper_config(&config, "json").is_ok());
    }

    #[test]
    fn test_json_sink_rejects_mapping() {
        let mut config = HashMap::new();
        config.insert("json.mapping.id".to_string(), "$.id".to_string());

        let result = validate_sink_mapper_config(&config, "json");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("json.mapping"));
        assert!(err_msg.contains("source-only"));
    }

    #[test]
    fn test_json_sink_rejects_ignore_parse_errors() {
        let mut config = HashMap::new();
        config.insert("json.ignore-parse-errors".to_string(), "true".to_string());

        let result = validate_sink_mapper_config(&config, "json");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("json.ignore-parse-errors"));
    }

    // ========================================================================
    // CSV Source Validation Tests
    // ========================================================================

    #[test]
    fn test_csv_source_valid_mapping() {
        let mut config = HashMap::new();
        config.insert("csv.mapping.id".to_string(), "0".to_string());
        config.insert("csv.mapping.amount".to_string(), "2".to_string());
        config.insert("csv.delimiter".to_string(), ",".to_string());
        config.insert("csv.has-header".to_string(), "true".to_string());

        assert!(validate_source_mapper_config(&config, "csv").is_ok());
    }

    #[test]
    fn test_csv_source_rejects_include_header() {
        let mut config = HashMap::new();
        config.insert("csv.include-header".to_string(), "true".to_string());

        let result = validate_source_mapper_config(&config, "csv");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("csv.include-header"));
        assert!(err_msg.contains("sink-only"));
    }

    #[test]
    fn test_csv_source_invalid_column_index() {
        let mut config = HashMap::new();
        config.insert("csv.mapping.id".to_string(), "invalid".to_string());

        let result = validate_source_mapper_config(&config, "csv");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Invalid column index"));
    }

    // ========================================================================
    // CSV Sink Validation Tests
    // ========================================================================

    #[test]
    fn test_csv_sink_valid_config() {
        let mut config = HashMap::new();
        config.insert("csv.delimiter".to_string(), ",".to_string());
        config.insert("csv.include-header".to_string(), "true".to_string());
        config.insert("csv.header-names".to_string(), "id,amount".to_string());

        assert!(validate_sink_mapper_config(&config, "csv").is_ok());
    }

    #[test]
    fn test_csv_sink_rejects_mapping() {
        let mut config = HashMap::new();
        config.insert("csv.mapping.id".to_string(), "0".to_string());

        let result = validate_sink_mapper_config(&config, "csv");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("csv.mapping"));
        assert!(err_msg.contains("source-only"));
    }

    #[test]
    fn test_csv_sink_rejects_has_header() {
        let mut config = HashMap::new();
        config.insert("csv.has-header".to_string(), "true".to_string());

        let result = validate_sink_mapper_config(&config, "csv");
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("csv.has-header"));
    }

    // ========================================================================
    // General Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_mapper_config_dispatcher() {
        let mut config = HashMap::new();
        config.insert("json.mapping.id".to_string(), "$.id".to_string());

        // Valid for source
        assert!(validate_mapper_config(&config, "json", "source").is_ok());

        // Invalid for sink
        assert!(validate_mapper_config(&config, "json", "sink").is_err());

        // Invalid stream type
        assert!(validate_mapper_config(&config, "json", "invalid").is_err());
    }

    #[test]
    fn test_empty_config_always_valid() {
        let config = HashMap::new();

        assert!(validate_source_mapper_config(&config, "json").is_ok());
        assert!(validate_sink_mapper_config(&config, "json").is_ok());
        assert!(validate_source_mapper_config(&config, "csv").is_ok());
        assert!(validate_sink_mapper_config(&config, "csv").is_ok());
    }
}
