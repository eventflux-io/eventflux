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

//! # TOML Configuration Module
//!
//! Loads TOML configuration for EventFlux streams and tables and merges its layers.
//!
//! ## Features
//!
//! - **TOML Loading**: Reads TOML files, including nested tables
//! - **Environment Variable Substitution**: Fills in `${VAR}` and `${VAR:default}` once,
//!   when the file is loaded
//! - **Configuration Merging**: Layers properties so higher priority sources win
//! - **Validation**: Rejects fields that belong in SQL, so SQL stays the source of truth
//!   for the shape of each stream
//!
//! ## Configuration Sources (Priority: Low to High)
//!
//! 1. **Rust defaults**. Built in defaults (PropertySource::RustDefault).
//! 2. **TOML [application]**. Shared by all streams and tables
//!    (PropertySource::TomlApplication).
//! 3. **TOML [streams.StreamName]**. One stream (PropertySource::TomlStream).
//! 4. **SQL WITH clause**. Highest priority (PropertySource::SqlWith).
//!
//! ## Example TOML Configuration
//!
//! ```toml
//! [application]
//! buffer_size = "2048"
//! timeout = "30s"
//!
//! [application.kafka]
//! brokers = "${KAFKA_BROKERS:localhost:9092}"
//!
//! [streams.Orders]
//! # Note: type, extension, format MUST be in SQL WITH clause, not here!
//! kafka.topic = "orders"
//! kafka.group_id = "order-processor"
//!
//! [tables.OrdersTable]
//! # Tables are always bidirectional - no type/format allowed
//! extension = "redis"
//! redis.host = "${REDIS_HOST}"
//! redis.port = "6379"
//! ```

use crate::core::config::stream_config::{FlatConfig, PropertySource};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;

// ============================================================================
// Data Structures
// ============================================================================

/// Whole TOML configuration file
#[derive(Deserialize, Debug, Clone)]
pub struct TomlConfig {
    /// The `[application]` section, shared by all streams and tables
    pub application: Option<ApplicationSection>,

    /// The `[streams.<name>]` sections, keyed by stream name
    pub streams: Option<HashMap<String, TomlStreamConfig>>,

    /// The `[tables.<name>]` sections, keyed by table name
    pub tables: Option<HashMap<String, TomlTableConfig>>,
}

/// The `[application]` section
///
/// Every property here is a default for all streams and tables.
#[derive(Deserialize, Debug, Clone)]
pub struct ApplicationSection {
    /// Properties of any depth, not yet flattened
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

/// One `[streams.<name>]` section
///
/// **IMPORTANT**: `type`, `extension` and `format` are **NOT allowed** in TOML. They
/// MUST be set in the SQL WITH clause, so SQL alone decides the shape of a stream.
#[derive(Deserialize, Debug, Clone)]
pub struct TomlStreamConfig {
    /// All stream properties. `type`, `extension` and `format` are rejected by `validate`.
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

/// One `[tables.<name>]` section
///
/// **IMPORTANT**: `type` and `format` are **NOT allowed** for tables. Data flows both
/// ways and tables only use the relational schema.
#[derive(Deserialize, Debug, Clone)]
pub struct TomlTableConfig {
    /// All table properties. `type` and `format` are rejected by `validate`.
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

// ============================================================================
// TOML Flattening
// ============================================================================

/// Flatten nested TOML values into dotted keys
///
/// Walks the value and writes each leaf into `config` under its dotted path. Arrays
/// become comma separated strings. Array items that are not scalars are skipped with
/// a warning on stderr. Datetimes keep their TOML text form.
///
/// # Examples
///
/// ```toml
/// [streams.Orders.kafka]
/// brokers = "localhost:9092"
/// topic = "orders"
/// ```
/// Produces:
/// - `kafka.brokers` = "localhost:9092"
/// - `kafka.topic` = "orders"
///
/// ```toml
/// [streams.Orders]
/// kafka.brokers = "localhost:9092"
/// kafka.topic = "orders"
/// ```
/// Produces the same result:
/// - `kafka.brokers` = "localhost:9092"
/// - `kafka.topic` = "orders"
///
/// # Arguments
///
/// * `prefix` - Key path so far. Empty at the root.
/// * `value` - TOML value to flatten
/// * `config` - FlatConfig to write into
/// * `source` - Source recorded for every property written
pub fn flatten_toml_value(
    prefix: &str,
    value: &toml::Value,
    config: &mut FlatConfig,
    source: PropertySource,
) {
    match value {
        toml::Value::Table(table) => {
            for (key, val) in table {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_toml_value(&new_prefix, val, config, source);
            }
        }
        toml::Value::String(s) => {
            config.set(prefix, s.clone(), source);
        }
        toml::Value::Integer(i) => {
            config.set(prefix, i.to_string(), source);
        }
        toml::Value::Float(f) => {
            config.set(prefix, f.to_string(), source);
        }
        toml::Value::Boolean(b) => {
            config.set(prefix, b.to_string(), source);
        }
        toml::Value::Array(arr) => {
            let str_values: Vec<String> = arr
                .iter()
                .filter_map(|v| match v {
                    toml::Value::String(s) => Some(s.clone()),
                    toml::Value::Integer(i) => Some(i.to_string()),
                    toml::Value::Float(f) => Some(f.to_string()),
                    toml::Value::Boolean(b) => Some(b.to_string()),
                    _ => {
                        eprintln!("Warning: Unsupported array element type in TOML, skipping");
                        None
                    }
                })
                .collect();
            config.set(prefix, str_values.join(","), source);
        }
        toml::Value::Datetime(_) => {
            config.set(prefix, value.to_string(), source);
        }
    }
}

// ============================================================================
// Environment Variable Substitution
// ============================================================================

/// Pattern for `${VAR}` and `${VAR:default}`. Compiled once, on first use.
static ENV_VAR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{([^}:]+)(?::([^}]*))?\}").unwrap());

/// Replace environment variable references in a string
///
/// # Syntax
///
/// - `${VAR_NAME}` is required and fails when the variable is unset
/// - `${VAR_NAME:default}` uses the default when the variable is unset
///
/// # Timing
///
/// Runs once, when the configuration is loaded, not each time a value is read.
///
/// # Behavior
///
/// - Unset variable with no default: returns `Err` naming every such variable
/// - Unset variable with a default: uses the default
/// - Set variable: uses its value, even when it is empty
///
/// # Examples
///
/// ```
/// use eventflux::core::config::toml_config::substitute_env_vars;
///
/// std::env::set_var("KAFKA_BROKER", "kafka:9092");
///
/// let result = substitute_env_vars("${KAFKA_BROKER}").unwrap();
/// assert_eq!(result, "kafka:9092");
///
/// let result = substitute_env_vars("${MISSING:default}").unwrap();
/// assert_eq!(result, "default");
///
/// let result = substitute_env_vars("${MISSING}");
/// assert!(result.is_err());
/// ```
pub fn substitute_env_vars(value: &str) -> Result<String, String> {
    let mut result = value.to_string();
    let mut missing_vars = Vec::new();

    for cap in ENV_VAR_REGEX.captures_iter(value) {
        let var_name = &cap[1];
        let default_value = cap.get(2).map(|m| m.as_str());

        match env::var(var_name) {
            Ok(env_value) => {
                let placeholder = &cap[0];
                result = result.replace(placeholder, &env_value);
            }
            Err(_) => {
                if let Some(default) = default_value {
                    let placeholder = &cap[0];
                    result = result.replace(placeholder, default);
                } else {
                    missing_vars.push(var_name.to_string());
                }
            }
        }
    }

    if !missing_vars.is_empty() {
        return Err(format!(
            "Missing required environment variables: {}",
            missing_vars.join(", ")
        ));
    }

    Ok(result)
}

/// Replace environment variable references in every string inside a TOML value
///
/// Walks tables and arrays and changes the strings in place with
/// [`substitute_env_vars`]. Other value types are left alone.
///
/// # Arguments
///
/// * `value` - TOML value to change in place
///
/// # Returns
///
/// `Ok(())` on success, or `Err(String)` if a required variable is unset. Strings
/// handled before the error stay changed.
pub fn substitute_toml_env_vars(value: &mut toml::Value) -> Result<(), String> {
    match value {
        toml::Value::String(s) => {
            *s = substitute_env_vars(s)?;
        }
        toml::Value::Table(table) => {
            for (_, val) in table.iter_mut() {
                substitute_toml_env_vars(val)?;
            }
        }
        toml::Value::Array(arr) => {
            for val in arr.iter_mut() {
                substitute_toml_env_vars(val)?;
            }
        }
        _ => {}
    }
    Ok(())
}

// ============================================================================
// Validation
// ============================================================================

impl TomlStreamConfig {
    /// Check that `type`, `extension` and `format` are not set in TOML
    ///
    /// These MUST be in the SQL WITH clause. They decide the shape of the stream and
    /// the parser needs them while it reads the SQL, before TOML is merged in. Only
    /// top level keys are checked.
    pub fn validate(&self) -> Result<(), String> {
        let forbidden = ["type", "extension", "format"];
        for key in &forbidden {
            if self.properties.contains_key(*key) {
                return Err(format!(
                    "Stream defines '{}' in TOML, but '{}' MUST be in SQL WITH clause. \
                    Type, extension, and format are structural properties required by the parser \
                    at Phase 1 (parse-time), so they cannot be in TOML configuration.",
                    key, key
                ));
            }
        }
        Ok(())
    }

    /// Flatten the stream properties into a [`FlatConfig`] with dotted keys, all marked
    /// with `source`
    pub fn to_flat_config(&self, source: PropertySource) -> FlatConfig {
        let mut config = FlatConfig::new();

        for (key, value) in &self.properties {
            flatten_toml_value(key, value, &mut config, source);
        }

        config
    }
}

impl TomlTableConfig {
    /// Check that `type` and `format` are not set in TOML for a table
    ///
    /// Data flows both ways in a table and it only uses the relational schema. Only
    /// top level keys are checked.
    pub fn validate(&self) -> Result<(), String> {
        let forbidden = ["type", "format"];
        for key in &forbidden {
            if self.properties.contains_key(*key) {
                return Err(format!(
                    "Table defines '{}' in TOML, but tables cannot have '{}' property. \
                    Tables are always bidirectional and use relational schema only.",
                    key, key
                ));
            }
        }
        Ok(())
    }

    /// Flatten the table properties into a [`FlatConfig`] with dotted keys, all marked
    /// with `source`
    pub fn to_flat_config(&self, source: PropertySource) -> FlatConfig {
        let mut config = FlatConfig::new();

        for (key, value) in &self.properties {
            flatten_toml_value(key, value, &mut config, source);
        }

        config
    }
}

// ============================================================================
// Configuration Container
// ============================================================================

/// Loaded stream and table configurations, kept in separate maps
///
/// Separate maps stop a stream and a table with the same name from clashing, and let
/// each be checked with its own rules.
///
/// # Why Separate Namespaces?
///
/// Streams and tables can have the same name without conflict:
/// - Stream "Data" and table "Data" can both exist
/// - Each has its own validation rules. Streams have type and format, tables do not.
/// - One cannot overwrite the other's settings by mistake
///
/// # Example
///
/// ```rust,ignore
/// let config = load_toml_config("config.toml")?;
/// let orders_stream = config.streams.get("Orders"); // Stream named "Orders"
/// let users_table = config.tables.get("Users");     // Table named "Users"
/// // No collision even if both streams and tables have name "Data"
/// ```
#[derive(Debug, Clone, Default)]
pub struct LoadedConfig {
    /// Stream configurations (from [streams.*] sections)
    pub streams: HashMap<String, FlatConfig>,
    /// Table configurations (from [tables.*] sections)
    pub tables: HashMap<String, FlatConfig>,
}

// ============================================================================
// Configuration Loading and Merging
// ============================================================================

/// Load a TOML file and build one merged [`FlatConfig`] per stream and per table
///
/// Fills in environment variables first, then starts each stream and table from the
/// `[application]` properties and lays its own section on top.
///
/// # Merge Order (lowest to highest priority)
///
/// 1. Rust defaults (PropertySource::RustDefault). Not added here.
/// 2. TOML [application] (PropertySource::TomlApplication)
/// 3. TOML [streams.StreamName] (PropertySource::TomlStream). Table sections also use
///    TomlStream.
/// 4. SQL WITH clause (PropertySource::SqlWith). The caller merges this later.
///
/// # Arguments
///
/// * `toml_path` - Path to TOML configuration file
///
/// # Returns
///
/// A [`LoadedConfig`] with the stream and table configs keyed by name. Streams and
/// tables with no section of their own are not included.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed, a required environment
/// variable is unset, or a stream or table section sets a forbidden key.
///
/// # Example
///
/// ```toml
/// [application]
/// buffer_size = "2048"
///
/// [streams.Orders]
/// kafka.topic = "orders"
/// ```
///
/// Results in:
/// - The Orders stream gets `buffer_size=2048` from `[application]` and
///   `kafka.topic=orders` from its own section
pub fn load_toml_config(toml_path: &str) -> Result<LoadedConfig, String> {
    // 1. Read the TOML file
    let toml_str = std::fs::read_to_string(toml_path)
        .map_err(|e| format!("Failed to read TOML file '{}': {}", toml_path, e))?;

    // 2. Parse TOML
    let mut toml_config: TomlConfig = toml::from_str(&toml_str)
        .map_err(|e| format!("Failed to parse TOML file '{}': {}", toml_path, e))?;

    // 3. Fill in environment variables
    if let Some(ref mut app) = toml_config.application {
        for (_, value) in app.properties.iter_mut() {
            substitute_toml_env_vars(value)?;
        }
    }
    if let Some(ref mut streams) = toml_config.streams {
        for (_, stream_config) in streams.iter_mut() {
            for (_, value) in stream_config.properties.iter_mut() {
                substitute_toml_env_vars(value)?;
            }
        }
    }
    if let Some(ref mut tables) = toml_config.tables {
        for (_, table_config) in tables.iter_mut() {
            for (_, value) in table_config.properties.iter_mut() {
                substitute_toml_env_vars(value)?;
            }
        }
    }

    // 4. Build a FlatConfig for each stream and table
    //    CRITICAL: separate maps so a stream and a table with the same name do not clash
    let mut stream_configs = HashMap::new();
    let mut table_configs = HashMap::new();

    // 4a. Flatten the [application] defaults
    let mut app_defaults = FlatConfig::new();
    if let Some(app) = toml_config.application {
        for (key, value) in &app.properties {
            flatten_toml_value(
                key,
                value,
                &mut app_defaults,
                PropertySource::TomlApplication,
            );
        }
    }

    // 4b. Lay each stream section over the defaults
    if let Some(streams) = toml_config.streams {
        for (stream_name, stream_config) in streams {
            stream_config.validate()?;

            let mut flat_config = app_defaults.clone();

            // TomlStream outranks TomlApplication, so the stream section wins
            flat_config.merge(&stream_config.to_flat_config(PropertySource::TomlStream));

            stream_configs.insert(stream_name, flat_config);
        }
    }

    // 4c. Lay each table section over the defaults, the same way as streams
    if let Some(tables) = toml_config.tables {
        for (table_name, table_config) in tables {
            table_config.validate()?;

            let mut flat_config = app_defaults.clone();

            // Tables reuse the TomlStream source so they outrank [application]
            flat_config.merge(&table_config.to_flat_config(PropertySource::TomlStream));

            table_configs.insert(table_name, flat_config);
        }
    }

    Ok(LoadedConfig {
        streams: stream_configs,
        tables: table_configs,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // TOML Flattening Tests
    // ========================================================================

    #[test]
    fn test_flatten_toml_value_nested_table() {
        let toml_str = r#"
            [kafka]
            brokers = "localhost:9092"
            topic = "orders"
        "#;

        let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
        let mut config = FlatConfig::new();
        flatten_toml_value(
            "",
            &toml_value,
            &mut config,
            PropertySource::TomlApplication,
        );

        assert_eq!(
            config.get("kafka.brokers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(config.get("kafka.topic"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_flatten_toml_value_dot_notation() {
        let toml_str = r#"
            kafka.brokers = "localhost:9092"
            kafka.topic = "orders"
        "#;

        let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
        let mut config = FlatConfig::new();
        flatten_toml_value(
            "",
            &toml_value,
            &mut config,
            PropertySource::TomlApplication,
        );

        assert_eq!(
            config.get("kafka.brokers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(config.get("kafka.topic"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_flatten_toml_value_types() {
        let toml_str = r#"
            string_val = "test"
            int_val = 42
            float_val = 3.14
            bool_val = true
            array_val = ["a", "b", "c"]
        "#;

        let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
        let mut config = FlatConfig::new();
        flatten_toml_value(
            "",
            &toml_value,
            &mut config,
            PropertySource::TomlApplication,
        );

        assert_eq!(config.get("string_val"), Some(&"test".to_string()));
        assert_eq!(config.get("int_val"), Some(&"42".to_string()));
        assert_eq!(config.get("float_val"), Some(&"3.14".to_string()));
        assert_eq!(config.get("bool_val"), Some(&"true".to_string()));
        assert_eq!(config.get("array_val"), Some(&"a,b,c".to_string()));
    }

    #[test]
    fn test_flatten_toml_value_deep_nesting() {
        let toml_str = r#"
            [level1.level2.level3]
            deep_key = "deep_value"
        "#;

        let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
        let mut config = FlatConfig::new();
        flatten_toml_value(
            "",
            &toml_value,
            &mut config,
            PropertySource::TomlApplication,
        );

        assert_eq!(
            config.get("level1.level2.level3.deep_key"),
            Some(&"deep_value".to_string())
        );
    }

    // ========================================================================
    // Environment Variable Substitution Tests
    // ========================================================================

    #[test]
    fn test_env_var_substitution_present() {
        std::env::set_var("TEST_VAR", "test-value");

        let result = substitute_env_vars("${TEST_VAR}").unwrap();
        assert_eq!(result, "test-value");

        std::env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_env_var_substitution_with_default() {
        let result = substitute_env_vars("${MISSING_VAR:default}").unwrap();
        assert_eq!(result, "default");
    }

    #[test]
    fn test_env_var_substitution_missing_no_default() {
        let result = substitute_env_vars("${MISSING_VAR_NO_DEFAULT}");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Missing required environment variables"));
    }

    #[test]
    fn test_env_var_substitution_multiple() {
        std::env::set_var("VAR1", "value1");
        std::env::set_var("VAR2", "value2");

        let result = substitute_env_vars("${VAR1}-${VAR2}").unwrap();
        assert_eq!(result, "value1-value2");

        std::env::remove_var("VAR1");
        std::env::remove_var("VAR2");
    }

    #[test]
    fn test_env_var_substitution_mixed() {
        std::env::set_var("PRESENT", "exists");

        let result = substitute_env_vars("${PRESENT}-${MISSING:fallback}").unwrap();
        assert_eq!(result, "exists-fallback");

        std::env::remove_var("PRESENT");
    }

    #[test]
    fn test_substitute_toml_env_vars_string() {
        std::env::set_var("BROKER", "kafka:9092");

        let mut value = toml::Value::String("${BROKER}".to_string());
        substitute_toml_env_vars(&mut value).unwrap();

        assert_eq!(value.as_str().unwrap(), "kafka:9092");

        std::env::remove_var("BROKER");
    }

    #[test]
    fn test_substitute_toml_env_vars_table() {
        std::env::set_var("HOST", "localhost");
        std::env::set_var("PORT", "9092");

        let toml_str = r#"
            host = "${HOST}"
            port = "${PORT}"
        "#;

        let mut value: toml::Value = toml::from_str(toml_str).unwrap();
        substitute_toml_env_vars(&mut value).unwrap();

        let table = value.as_table().unwrap();
        assert_eq!(table.get("host").unwrap().as_str().unwrap(), "localhost");
        assert_eq!(table.get("port").unwrap().as_str().unwrap(), "9092");

        std::env::remove_var("HOST");
        std::env::remove_var("PORT");
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_toml_stream_config_validation_forbidden_type() {
        let mut properties = HashMap::new();
        properties.insert(
            "type".to_string(),
            toml::Value::String("source".to_string()),
        );

        let stream_config = TomlStreamConfig { properties };
        let result = stream_config.validate();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("'type' MUST be in SQL WITH clause"));
    }

    #[test]
    fn test_toml_stream_config_validation_forbidden_extension() {
        let mut properties = HashMap::new();
        properties.insert(
            "extension".to_string(),
            toml::Value::String("kafka".to_string()),
        );

        let stream_config = TomlStreamConfig { properties };
        let result = stream_config.validate();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("'extension' MUST be in SQL WITH clause"));
    }

    #[test]
    fn test_toml_stream_config_validation_forbidden_format() {
        let mut properties = HashMap::new();
        properties.insert(
            "format".to_string(),
            toml::Value::String("json".to_string()),
        );

        let stream_config = TomlStreamConfig { properties };
        let result = stream_config.validate();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("'format' MUST be in SQL WITH clause"));
    }

    #[test]
    fn test_toml_stream_config_validation_valid() {
        let mut properties = HashMap::new();
        properties.insert(
            "kafka.topic".to_string(),
            toml::Value::String("orders".to_string()),
        );
        properties.insert("buffer_size".to_string(), toml::Value::Integer(2048));

        let stream_config = TomlStreamConfig { properties };
        let result = stream_config.validate();

        assert!(result.is_ok());
    }

    #[test]
    fn test_toml_table_config_validation_forbidden_type() {
        let mut properties = HashMap::new();
        properties.insert("type".to_string(), toml::Value::String("table".to_string()));

        let table_config = TomlTableConfig { properties };
        let result = table_config.validate();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("tables cannot have 'type' property"));
    }

    #[test]
    fn test_toml_table_config_validation_forbidden_format() {
        let mut properties = HashMap::new();
        properties.insert(
            "format".to_string(),
            toml::Value::String("json".to_string()),
        );

        let table_config = TomlTableConfig { properties };
        let result = table_config.validate();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("tables cannot have 'format' property"));
    }

    #[test]
    fn test_toml_table_config_validation_valid() {
        let mut properties = HashMap::new();
        properties.insert(
            "redis.host".to_string(),
            toml::Value::String("localhost".to_string()),
        );

        let table_config = TomlTableConfig { properties };
        let result = table_config.validate();

        assert!(result.is_ok());
    }

    // ========================================================================
    // Configuration Merging Tests
    // ========================================================================

    #[test]
    fn test_toml_config_merge_application_defaults() {
        // Application defaults
        let mut app_config = FlatConfig::new();
        app_config.set(
            "kafka.brokers",
            "app-broker",
            PropertySource::TomlApplication,
        );
        app_config.set("kafka.timeout", "30s", PropertySource::TomlApplication);

        // Stream-specific config
        let mut stream_config = FlatConfig::new();
        stream_config.set("kafka.brokers", "stream-broker", PropertySource::TomlStream);
        stream_config.set("kafka.topic", "orders", PropertySource::TomlStream);

        // Merge
        let mut final_config = app_config.clone();
        final_config.merge(&stream_config);

        // Stream-specific overrides application
        assert_eq!(
            final_config.get("kafka.brokers"),
            Some(&"stream-broker".to_string())
        );
        // Stream-specific adds new property
        assert_eq!(final_config.get("kafka.topic"), Some(&"orders".to_string()));
        // Application default inherited
        assert_eq!(final_config.get("kafka.timeout"), Some(&"30s".to_string()));
    }

    #[test]
    fn test_stream_config_to_flat_config() {
        let mut properties = HashMap::new();
        properties.insert(
            "kafka".to_string(),
            toml::Value::Table({
                let mut table = toml::map::Map::new();
                table.insert(
                    "brokers".to_string(),
                    toml::Value::String("localhost:9092".to_string()),
                );
                table.insert(
                    "topic".to_string(),
                    toml::Value::String("orders".to_string()),
                );
                table
            }),
        );

        let stream_config = TomlStreamConfig { properties };
        let flat_config = stream_config.to_flat_config(PropertySource::TomlStream);

        assert_eq!(
            flat_config.get("kafka.brokers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(flat_config.get("kafka.topic"), Some(&"orders".to_string()));
    }

    #[test]
    fn test_table_config_to_flat_config() {
        let mut properties = HashMap::new();
        properties.insert(
            "redis.host".to_string(),
            toml::Value::String("localhost".to_string()),
        );
        properties.insert("redis.port".to_string(), toml::Value::Integer(6379));

        let table_config = TomlTableConfig { properties };
        let flat_config = table_config.to_flat_config(PropertySource::TomlStream);

        assert_eq!(
            flat_config.get("redis.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(flat_config.get("redis.port"), Some(&"6379".to_string()));
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn test_load_toml_config_complete() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let toml_content = r#"
[application]
buffer_size = "2048"
timeout = "30s"

[application.kafka]
brokers = "app-kafka:9092"

[streams.Orders]
kafka.topic = "orders"
kafka.group_id = "order-processor"

[streams.Payments]
kafka.topic = "payments"

[tables.OrdersTable]
redis.host = "localhost"
redis.port = "6379"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        let path = temp_file.path().to_str().unwrap();

        let configs = load_toml_config(path).unwrap();

        // Check Orders stream
        let orders = configs.streams.get("Orders").unwrap();
        assert_eq!(orders.get("buffer_size"), Some(&"2048".to_string()));
        assert_eq!(orders.get("timeout"), Some(&"30s".to_string()));
        assert_eq!(
            orders.get("kafka.brokers"),
            Some(&"app-kafka:9092".to_string())
        );
        assert_eq!(orders.get("kafka.topic"), Some(&"orders".to_string()));
        assert_eq!(
            orders.get("kafka.group_id"),
            Some(&"order-processor".to_string())
        );

        // Check Payments stream inherits application defaults
        let payments = configs.streams.get("Payments").unwrap();
        assert_eq!(payments.get("buffer_size"), Some(&"2048".to_string()));
        assert_eq!(payments.get("kafka.topic"), Some(&"payments".to_string()));

        // Check OrdersTable is in separate namespace
        let orders_table = configs.tables.get("OrdersTable").unwrap();
        assert_eq!(
            orders_table.get("redis.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(orders_table.get("redis.port"), Some(&"6379".to_string()));
    }

    #[test]
    fn test_load_toml_config_with_env_vars() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        std::env::set_var("KAFKA_BROKER", "env-kafka:9092");
        std::env::set_var("KAFKA_TOPIC", "env-orders");

        let toml_content = r#"
[streams.Orders]
kafka.brokers = "${KAFKA_BROKER}"
kafka.topic = "${KAFKA_TOPIC}"
kafka.timeout = "${KAFKA_TIMEOUT:30s}"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        let path = temp_file.path().to_str().unwrap();

        let configs = load_toml_config(path).unwrap();
        let orders = configs.streams.get("Orders").unwrap();

        assert_eq!(
            orders.get("kafka.brokers"),
            Some(&"env-kafka:9092".to_string())
        );
        assert_eq!(orders.get("kafka.topic"), Some(&"env-orders".to_string()));
        assert_eq!(orders.get("kafka.timeout"), Some(&"30s".to_string())); // Uses default

        std::env::remove_var("KAFKA_BROKER");
        std::env::remove_var("KAFKA_TOPIC");
    }

    #[test]
    fn test_load_toml_config_missing_env_var_fails() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let toml_content = r#"
[streams.Orders]
kafka.brokers = "${MISSING_BROKER}"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        let path = temp_file.path().to_str().unwrap();

        let result = load_toml_config(path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Missing required environment variables"));
    }

    #[test]
    fn test_load_toml_config_validation_rejects_forbidden_fields() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let toml_content = r#"
[streams.Orders]
type = "source"
extension = "kafka"
format = "json"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(toml_content.as_bytes()).unwrap();
        let path = temp_file.path().to_str().unwrap();

        let result = load_toml_config(path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("MUST be in SQL WITH clause"));
    }
}
