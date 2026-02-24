// SPDX-License-Identifier: MIT OR Apache-2.0

//! # Configuration Override Utilities
//!
//! This module provides utilities for overriding configuration values using
//! dot-notation paths, enabling CLI `--set` flag functionality.
//!
//! ## Example
//!
//! ```rust,ignore
//! use eventflux::core::config::{EventFluxConfig, apply_config_overrides};
//!
//! let mut config = EventFluxConfig::default();
//! let overrides = vec![
//!     "eventflux.persistence.type=sqlite".to_string(),
//!     "eventflux.persistence.path=./eventflux.db".to_string(),
//!     "eventflux.runtime.performance.thread_pool_size=8".to_string(),
//! ];
//!
//! apply_config_overrides(&mut config, &overrides)?;
//! ```

use crate::core::config::{ConfigError, ConfigResult, EventFluxConfig};
use serde_yaml::Value;

/// Apply a list of override strings to a configuration
///
/// Each override should be in the format `path=value` where path uses dot notation.
///
/// # Arguments
///
/// * `config` - The configuration to modify
/// * `overrides` - List of override strings in `key=value` format
///
/// # Example Paths
///
/// - `eventflux.persistence.type=sqlite`
/// - `eventflux.persistence.path=./data.db`
/// - `eventflux.runtime.performance.thread_pool_size=8`
/// - `eventflux.runtime.mode=distributed`
/// - `metadata.name=my-app`
pub fn apply_config_overrides(
    config: &mut EventFluxConfig,
    overrides: &[String],
) -> ConfigResult<()> {
    if overrides.is_empty() {
        return Ok(());
    }

    // Serialize config to YAML Value for manipulation
    let mut value: Value = serde_yaml::to_value(&*config).map_err(|e| {
        ConfigError::internal_error(format!("Failed to serialize config for override: {}", e))
    })?;

    // Apply each override
    for override_str in overrides {
        let (path, val) = parse_override(override_str)?;
        set_value_at_path(&mut value, &path, val)?;
    }

    // Deserialize back to EventFluxConfig
    *config = serde_yaml::from_value(value).map_err(|e| {
        ConfigError::internal_error(format!(
            "Failed to deserialize config after applying overrides: {}",
            e
        ))
    })?;

    Ok(())
}

/// Parse an override string into path components and value
///
/// Supports formats:
/// - `key=value` (simple value)
/// - `key.nested.path=value` (nested path)
fn parse_override(s: &str) -> ConfigResult<(Vec<String>, Value)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(ConfigError::internal_error(format!(
            "Invalid override format '{}': expected 'key=value'",
            s
        )));
    }

    let path_str = parts[0].trim();
    let value_str = parts[1].trim();

    if path_str.is_empty() {
        return Err(ConfigError::internal_error(
            "Override path cannot be empty".to_string(),
        ));
    }

    // Parse the path into components
    let path: Vec<String> = path_str.split('.').map(|s| s.to_string()).collect();

    // Parse the value - try to infer type
    let value = parse_value(value_str);

    Ok((path, value))
}

/// Parse a string value into an appropriate YAML value type
///
/// Attempts to parse as:
/// 1. Boolean (true/false)
/// 2. Integer
/// 3. Float
/// 4. String (fallback)
fn parse_value(s: &str) -> Value {
    // Check for boolean
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    // Check for null
    if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("~") {
        return Value::Null;
    }

    // Try integer
    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }

    // Try float - serde_yaml doesn't have from_f64, so we parse via YAML
    if s.parse::<f64>().is_ok() {
        // Use serde_yaml to parse the number correctly
        if let Ok(v) = serde_yaml::from_str::<Value>(s) {
            if v.is_number() {
                return v;
            }
        }
    }

    // Default to string
    Value::String(s.to_string())
}

/// Set a value at a dot-notation path in a YAML Value
///
/// Creates intermediate mappings as needed.
fn set_value_at_path(root: &mut Value, path: &[String], value: Value) -> ConfigResult<()> {
    if path.is_empty() {
        return Err(ConfigError::internal_error(
            "Cannot set value at empty path".to_string(),
        ));
    }

    let mut current = root;

    // Navigate to parent, creating intermediate mappings as needed
    for (i, key) in path.iter().enumerate() {
        let is_last = i == path.len() - 1;

        if is_last {
            // Set the value
            match current {
                Value::Mapping(map) => {
                    map.insert(Value::String(key.clone()), value.clone());
                    return Ok(());
                }
                _ => {
                    return Err(ConfigError::internal_error(format!(
                        "Cannot set '{}' on non-mapping value at path '{}'",
                        key,
                        path[..i].join(".")
                    )));
                }
            }
        } else {
            // Navigate or create intermediate mapping
            match current {
                Value::Mapping(map) => {
                    let key_value = Value::String(key.clone());

                    // Ensure the key exists and is a mapping
                    if !map.contains_key(&key_value) {
                        map.insert(
                            key_value.clone(),
                            Value::Mapping(serde_yaml::Mapping::new()),
                        );
                    }

                    // Navigate into the mapping
                    current = map.get_mut(&key_value).unwrap();

                    // If it's not a mapping, convert it (override primitive with object)
                    if !current.is_mapping() {
                        *current = Value::Mapping(serde_yaml::Mapping::new());
                    }
                }
                _ => {
                    return Err(ConfigError::internal_error(format!(
                        "Cannot navigate through non-mapping value at path '{}'",
                        path[..i].join(".")
                    )));
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::types::global_config::PersistenceBackendType;

    #[test]
    fn test_parse_override_simple() {
        let (path, value) = parse_override("key=value").unwrap();
        assert_eq!(path, vec!["key"]);
        assert_eq!(value, Value::String("value".to_string()));
    }

    #[test]
    fn test_parse_override_nested() {
        let (path, value) = parse_override("a.b.c=hello").unwrap();
        assert_eq!(path, vec!["a", "b", "c"]);
        assert_eq!(value, Value::String("hello".to_string()));
    }

    #[test]
    fn test_parse_override_boolean() {
        let (_, value) = parse_override("enabled=true").unwrap();
        assert_eq!(value, Value::Bool(true));

        let (_, value) = parse_override("enabled=false").unwrap();
        assert_eq!(value, Value::Bool(false));
    }

    #[test]
    fn test_parse_override_integer() {
        let (_, value) = parse_override("count=42").unwrap();
        assert_eq!(value, Value::Number(42.into()));
    }

    #[test]
    fn test_parse_override_float() {
        let (_, value) = parse_override("ratio=3.14").unwrap();
        if let Value::Number(n) = value {
            assert!((n.as_f64().unwrap() - 3.14).abs() < 0.001);
        } else {
            panic!("Expected number");
        }
    }

    #[test]
    fn test_parse_override_invalid() {
        assert!(parse_override("no_equals_sign").is_err());
        assert!(parse_override("=value_only").is_err());
    }

    #[test]
    fn test_apply_config_overrides_persistence() {
        let mut config = EventFluxConfig::default();

        let overrides = vec![
            "eventflux.persistence.type=sqlite".to_string(),
            "eventflux.persistence.path=./test.db".to_string(),
            "eventflux.persistence.enabled=true".to_string(),
        ];

        apply_config_overrides(&mut config, &overrides).unwrap();

        let persistence = config.eventflux.persistence.as_ref().unwrap();
        assert_eq!(persistence.backend_type, PersistenceBackendType::Sqlite);
        assert_eq!(persistence.path, Some("./test.db".to_string()));
        assert!(persistence.enabled);
    }

    #[test]
    fn test_apply_config_overrides_runtime() {
        let mut config = EventFluxConfig::default();

        let overrides = vec![
            "eventflux.runtime.performance.thread_pool_size=16".to_string(),
            "eventflux.runtime.performance.event_buffer_size=500000".to_string(),
        ];

        apply_config_overrides(&mut config, &overrides).unwrap();

        assert_eq!(config.eventflux.runtime.performance.thread_pool_size, 16);
        assert_eq!(
            config.eventflux.runtime.performance.event_buffer_size,
            500000
        );
    }

    #[test]
    fn test_apply_config_overrides_metadata() {
        let mut config = EventFluxConfig::default();

        let overrides = vec!["metadata.name=test-app".to_string()];

        apply_config_overrides(&mut config, &overrides).unwrap();

        assert_eq!(config.metadata.name, Some("test-app".to_string()));
    }

    #[test]
    fn test_apply_config_overrides_empty() {
        let mut config = EventFluxConfig::default();
        let original = config.clone();

        apply_config_overrides(&mut config, &[]).unwrap();

        assert_eq!(config, original);
    }
}
