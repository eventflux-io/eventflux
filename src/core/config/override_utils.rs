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

//! # Configuration Override Utilities
//!
//! Sets configuration values by dotted path, such as `eventflux.runtime.mode`. This is
//! what the CLI `--set` flag uses.
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

/// Apply a list of overrides to a configuration
///
/// Each override has the form `path=value`, where the path is a dotted list of keys.
/// Overrides apply in order, so a later one wins. The config is turned into YAML,
/// changed, and read back, so `config` is left as it was if any step fails.
///
/// # Arguments
///
/// * `config` - The configuration to modify
/// * `overrides` - Override strings in `key=value` form
///
/// # Errors
///
/// Returns `InternalError` if an override is malformed, if a value cannot be set at
/// its path, or if the changed YAML no longer reads back as an [`EventFluxConfig`],
/// for example because a value has the wrong type.
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

    // Work on a YAML copy so any path can be set without knowing the struct layout
    let mut value: Value = serde_yaml::to_value(&*config).map_err(|e| {
        ConfigError::internal_error(format!("Failed to serialize config for override: {}", e))
    })?;

    for override_str in overrides {
        let (path, val) = parse_override(override_str)?;
        set_value_at_path(&mut value, &path, val)?;
    }

    *config = serde_yaml::from_value(value).map_err(|e| {
        ConfigError::internal_error(format!(
            "Failed to deserialize config after applying overrides: {}",
            e
        ))
    })?;

    Ok(())
}

/// Split an override into its path keys and its typed value
///
/// Accepts `key=value` and `key.nested.path=value`. Splits on the first `=` only, so
/// the value may contain `=`. Both sides are trimmed. See `parse_value` for typing.
///
/// Returns `InternalError` when there is no `=` or the path is empty.
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

    let path: Vec<String> = path_str.split('.').map(|s| s.to_string()).collect();

    let value = parse_value(value_str);

    Ok((path, value))
}

/// Turn an override value into the matching YAML value type
///
/// Tries, in order:
/// 1. Boolean (`true` or `false`, any case)
/// 2. Null (`null` in any case, or `~`)
/// 3. Integer
/// 4. Float
/// 5. String, when nothing else matches
fn parse_value(s: &str) -> Value {
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("~") {
        return Value::Null;
    }

    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }

    // serde_yaml has no way to build a Number from an f64, so let the YAML parser do
    // it. Text that Rust reads as a float but YAML does not stays a string.
    if s.parse::<f64>().is_ok() {
        if let Ok(v) = serde_yaml::from_str::<Value>(s) {
            if v.is_number() {
                return v;
            }
        }
    }

    Value::String(s.to_string())
}

/// Set a value at a path of keys inside a YAML value
///
/// Creates missing mappings along the way. A key on the way that holds a scalar or a
/// list is replaced with an empty mapping, so its old value is lost.
///
/// Returns `InternalError` for an empty path, or when `root` itself is not a mapping.
fn set_value_at_path(root: &mut Value, path: &[String], value: Value) -> ConfigResult<()> {
    if path.is_empty() {
        return Err(ConfigError::internal_error(
            "Cannot set value at empty path".to_string(),
        ));
    }

    let mut current = root;

    for (i, key) in path.iter().enumerate() {
        let is_last = i == path.len() - 1;

        if is_last {
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
            match current {
                Value::Mapping(map) => {
                    let key_value = Value::String(key.clone());

                    if !map.contains_key(&key_value) {
                        map.insert(
                            key_value.clone(),
                            Value::Mapping(serde_yaml::Mapping::new()),
                        );
                    }

                    current = map.get_mut(&key_value).unwrap();

                    // Replace a scalar or list with a mapping so the path can continue
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
    #[allow(clippy::approx_constant)]
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
