// SPDX-License-Identifier: MIT OR Apache-2.0

//! # TOML Configuration Module
//!
//! This module provides TOML-based configuration loading and merging for EventFlux streams.
//!
//! ## Features
//!
//! - **TOML Loading**: Parse TOML configuration files with nested structure support
//! - **Environment Variable Substitution**: Eager loading with `${VAR}` or `${VAR:default}` syntax
//! - **Configuration Merging**: Multi-layer property resolution with precedence
//! - **Validation**: Ensures SQL-first principle by rejecting forbidden TOML fields
//!
//! ## Configuration Sources (Priority: Low to High)
//!
//! 1. **Rust defaults** - Built-in defaults (PropertySource::RustDefault)
//! 2. **TOML [application]** - Global application-level config (PropertySource::TomlApplication)
//! 3. **TOML [streams.StreamName]** - Stream-specific config (PropertySource::TomlStream)
//! 4. **SQL WITH clause** - Highest priority (PropertySource::SqlWith)
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

/// Top-level TOML configuration structure
#[derive(Deserialize, Debug, Clone)]
pub struct TomlConfig {
    /// Global application-level configuration
    pub application: Option<ApplicationSection>,

    /// Stream-specific configurations
    pub streams: Option<HashMap<String, TomlStreamConfig>>,

    /// Table-specific configurations
    pub tables: Option<HashMap<String, TomlTableConfig>>,
}

/// Application-level configuration section
///
/// All properties in this section apply as defaults to all streams and tables.
#[derive(Deserialize, Debug, Clone)]
pub struct ApplicationSection {
    /// Flattened properties with arbitrary nesting
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

/// Stream-specific configuration from TOML
///
/// **IMPORTANT**: The `type`, `extension`, and `format` fields are **NOT allowed** in TOML.
/// These MUST be defined in the SQL WITH clause to enforce the SQL-first principle.
#[derive(Deserialize, Debug, Clone)]
pub struct TomlStreamConfig {
    /// All stream properties (type, extension, format are forbidden here)
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

/// Table-specific configuration from TOML
///
/// **IMPORTANT**: The `type` and `format` fields are **NOT allowed** for tables.
/// Tables are always bidirectional and use relational schema only.
#[derive(Deserialize, Debug, Clone)]
pub struct TomlTableConfig {
    /// All table properties (type and format are forbidden here)
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

// ============================================================================
// TOML Flattening
// ============================================================================

/// Flatten nested TOML structures into dot-separated keys
///
/// This function recursively traverses TOML values and converts nested structures
/// into flat key-value pairs with dot notation.
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
/// Produces (identical):
/// - `kafka.brokers` = "localhost:9092"
/// - `kafka.topic` = "orders"
///
/// # Arguments
///
/// * `prefix` - Current key prefix (empty for root level)
/// * `value` - TOML value to flatten
/// * `config` - Target FlatConfig to populate
/// * `source` - Property source for tracking
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
            // Arrays converted to comma-separated strings
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
            // Convert datetime to string representation
            config.set(prefix, value.to_string(), source);
        }
    }
}

// ============================================================================
// Environment Variable Substitution
// ============================================================================

/// Compiled regex for environment variable substitution (compiled once at startup)
static ENV_VAR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{([^}:]+)(?::([^}]*))?\}").unwrap());

/// Substitute environment variables in a string value
///
/// # Syntax
///
/// - `${VAR_NAME}` - Required variable (fails if missing)
/// - `${VAR_NAME:default}` - Optional variable with default value
///
/// # Timing
///
/// Eager loading at configuration resolution time (not lazy).
///
/// # Behavior
///
/// - Missing variable WITHOUT default: Returns Err
/// - Missing variable WITH default: Uses default value
/// - Variable exists: Uses environment variable value
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

/// Recursively substitute environment variables in TOML values
///
/// This function traverses the entire TOML structure and replaces environment
/// variable placeholders in string values.
///
/// # Arguments
///
/// * `value` - Mutable TOML value to process
///
/// # Returns
///
/// `Ok(())` on success, or `Err(String)` if required variables are missing
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
    /// Validate that type/extension/format are NOT present in TOML
    ///
    /// These fields MUST be in the SQL WITH clause to enforce the SQL-first principle.
    /// They are structural properties required at parse-time (Phase 1), so they cannot
    /// be in TOML configuration.
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

    /// Convert stream config to flat configuration
    ///
    /// Flattens all nested TOML structures into dot-notation properties.
    pub fn to_flat_config(&self, source: PropertySource) -> FlatConfig {
        let mut config = FlatConfig::new();

        for (key, value) in &self.properties {
            flatten_toml_value(key, value, &mut config, source);
        }

        config
    }
}

impl TomlTableConfig {
    /// Validate that type/format are NOT present in TOML for tables
    ///
    /// Tables are always bidirectional and use relational schema only.
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

    /// Convert table config to flat configuration
    ///
    /// Flattens all nested TOML structures into dot-notation properties.
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

/// Configuration container holding separated stream and table configurations
///
/// Keeps streams and tables in separate namespaces to prevent naming collisions
/// and allow proper type-specific validation.
///
/// # Why Separate Namespaces?
///
/// Streams and tables can have the same name without conflict:
/// - Stream "Data" can coexist with Table "Data"
/// - Each has different validation rules (streams have type/format, tables don't)
/// - Prevents accidental misconfiguration
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

/// Load and merge TOML configuration into per-stream FlatConfig instances
///
/// # Merge Order (lowest to highest priority)
///
/// 1. Rust defaults (PropertySource::RustDefault)
/// 2. TOML [application] (PropertySource::TomlApplication)
/// 3. TOML [streams.StreamName] (PropertySource::TomlStream)
/// 4. SQL WITH clause (PropertySource::SqlWith) - merged later by caller
///
/// # Arguments
///
/// * `toml_path` - Path to TOML configuration file
///
/// # Returns
///
/// HashMap mapping stream names to their merged FlatConfig instances
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
/// - Orders stream gets both `buffer_size=2048` (from app) and `kafka.topic=orders` (from stream)
pub fn load_toml_config(toml_path: &str) -> Result<LoadedConfig, String> {
    // 1. Read TOML file
    let toml_str = std::fs::read_to_string(toml_path)
        .map_err(|e| format!("Failed to read TOML file '{}': {}", toml_path, e))?;

    // 2. Parse TOML
    let mut toml_config: TomlConfig = toml::from_str(&toml_str)
        .map_err(|e| format!("Failed to parse TOML file '{}': {}", toml_path, e))?;

    // 3. Substitute environment variables
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

    // 4. Build FlatConfig for each stream and table
    //    CRITICAL: Streams and tables kept in separate HashMaps to prevent namespace collision
    let mut stream_configs = HashMap::new();
    let mut table_configs = HashMap::new();

    // 4a. Extract application-level defaults
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

    // 4b. Merge stream-specific configs
    if let Some(streams) = toml_config.streams {
        for (stream_name, stream_config) in streams {
            // Validate stream config
            stream_config.validate()?;

            // Start with application defaults
            let mut flat_config = app_defaults.clone();

            // Merge stream-specific properties (higher priority)
            flat_config.merge(&stream_config.to_flat_config(PropertySource::TomlStream));

            stream_configs.insert(stream_name, flat_config);
        }
    }

    // 4c. Merge table-specific configs (using same pattern as streams)
    //     Tables go into separate HashMap to prevent namespace collision with streams
    if let Some(tables) = toml_config.tables {
        for (table_name, table_config) in tables {
            // Validate table config
            table_config.validate()?;

            // Start with application defaults
            let mut flat_config = app_defaults.clone();

            // Merge table-specific properties (higher priority)
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
