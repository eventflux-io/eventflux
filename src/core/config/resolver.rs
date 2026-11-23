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

//! Configuration Resolvers
//!
//! Fills in `${VAR}` references in configuration from custom variables and
//! environment variables. Also holds a CLI argument resolver that is not built yet.

use crate::core::config::{ConfigError, ConfigResult, EventFluxConfig};
use std::collections::HashMap;
use std::env;

/// Replaces `${VAR}` references in configuration values
#[derive(Debug)]
pub struct VariableResolver {
    /// Custom variables. These win over environment variables.
    variables: HashMap<String, String>,

    /// Whether environment variables are looked up. Defaults to true.
    env_enabled: bool,

    /// Prefix tried before the plain variable name. None by default.
    env_prefix: Option<String>,
}

impl VariableResolver {
    /// Create a resolver with no custom variables and environment lookup on
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
            env_enabled: true,
            env_prefix: None,
        }
    }

    /// Add a custom variable mapping
    pub fn set_variable(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.variables.insert(key.into(), value.into());
    }

    /// Enable or disable environment variable resolution
    pub fn set_env_enabled(&mut self, enabled: bool) {
        self.env_enabled = enabled;
    }

    /// Set the prefix to try first, as in `<prefix><NAME IN UPPER CASE>`
    pub fn set_env_prefix(&mut self, prefix: Option<String>) {
        self.env_prefix = prefix;
    }

    /// Replace every `${VAR}` reference anywhere in the configuration.
    ///
    /// Works on the config as JSON text. Values are inserted as is, without JSON
    /// escaping, so a value holding a quote or backslash breaks the JSON.
    ///
    /// # Errors
    ///
    /// Returns `EnvironmentVariableNotFound` for an unknown variable with no default,
    /// and `InternalError` if the config cannot be turned into JSON and back.
    pub fn resolve_all(&self, config: EventFluxConfig) -> ConfigResult<EventFluxConfig> {
        let mut resolved_config = config;

        let json_str = serde_json::to_string(&resolved_config).map_err(|e| {
            ConfigError::internal_error(format!(
                "Failed to serialize config for variable resolution: {}",
                e
            ))
        })?;

        let resolved_json = self.resolve_variables_in_string(&json_str)?;

        resolved_config = serde_json::from_str(&resolved_json).map_err(|e| {
            ConfigError::internal_error(format!("Failed to deserialize resolved config: {}", e))
        })?;

        Ok(resolved_config)
    }

    /// Replace `${VAR}` and `${VAR:default}` references in a string.
    ///
    /// The default is used only when the variable cannot be found. It may be empty.
    /// Values that contain references are resolved again, up to 10 passes.
    ///
    /// # Errors
    ///
    /// Returns `EnvironmentVariableNotFound` for an unknown variable with no default,
    /// and `InternalError` after 10 passes, which usually means a reference loop.
    pub fn resolve_variables_in_string(&self, input: &str) -> ConfigResult<String> {
        let mut result = input.to_string();
        let var_pattern = regex::Regex::new(r"\$\{([^}:]+)(?::([^}]*))?\}").map_err(|e| {
            ConfigError::internal_error(format!("Invalid variable pattern regex: {}", e))
        })?;

        // Repeat so references inside resolved values get filled in too
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10; // Stops reference loops

        loop {
            let mut changed = false;
            iterations += 1;

            if iterations > MAX_ITERATIONS {
                return Err(ConfigError::internal_error(
                    "Too many variable resolution iterations - possible circular reference"
                        .to_string(),
                ));
            }

            let mut replacements = Vec::new();

            for cap in var_pattern.captures_iter(&result) {
                let full_match = cap.get(0).unwrap().as_str().to_string();
                let var_name = cap.get(1).unwrap().as_str();
                let default_value = cap.get(2).map(|m| m.as_str());

                let replacement = match self.resolve_variable(var_name) {
                    Ok(value) => {
                        changed = true;
                        value
                    }
                    Err(_) => {
                        if let Some(default) = default_value {
                            changed = true;
                            default.to_string()
                        } else {
                            return Err(ConfigError::env_var_not_found(var_name));
                        }
                    }
                };
                replacements.push((full_match, replacement));
            }

            if replacements.is_empty() {
                break;
            }

            for (from, to) in replacements {
                result = result.replace(&from, &to);
            }

            if !changed {
                break;
            }
        }

        Ok(result)
    }

    /// Look up one variable by name.
    ///
    /// Tries the custom variables, then `<prefix><NAME IN UPPER CASE>` when a prefix is
    /// set, then the plain environment variable. The environment is skipped when
    /// environment lookup is off.
    ///
    /// Returns `EnvironmentVariableNotFound` when nothing matches.
    pub fn resolve_variable(&self, reference: &str) -> ConfigResult<String> {
        if let Some(value) = self.variables.get(reference) {
            return Ok(value.clone());
        }

        if self.env_enabled {
            if let Some(ref prefix) = self.env_prefix {
                let env_key = format!("{}{}", prefix, reference.to_uppercase());
                if let Ok(value) = env::var(&env_key) {
                    return Ok(value);
                }
            }

            if let Ok(value) = env::var(reference) {
                return Ok(value);
            }
        }

        Err(ConfigError::env_var_not_found(reference))
    }
}

impl Default for VariableResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// CLI argument resolver. Not built yet. It collects arguments but `resolve` always
/// returns `None`.
#[derive(Debug)]
pub struct CliArgumentResolver {
    /// Arguments keyed by config path
    args: HashMap<String, String>,
}

impl CliArgumentResolver {
    /// Create a resolver with no arguments
    pub fn new() -> Self {
        Self {
            args: HashMap::new(),
        }
    }

    /// Create a resolver from the process arguments. Parsing is not built yet, so the
    /// result has no arguments.
    pub fn from_args() -> Self {
        let mut resolver = Self::new();
        resolver.parse_env_args();
        resolver
    }

    /// Add a CLI argument override
    pub fn set_arg(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.args.insert(key.into(), value.into());
    }

    /// Parse arguments from std::env::args(). Does nothing yet.
    ///
    /// The plan is to read `--config-key=value` arguments and map them to config paths
    /// as overrides. The CLI `--set` flag already does this through
    /// `apply_config_overrides`.
    fn parse_env_args(&mut self) {
        // Not built yet
    }

    /// Turn the arguments into a config to merge on top. Always returns `None` for now.
    pub fn resolve(&self) -> ConfigResult<Option<EventFluxConfig>> {
        if self.args.is_empty() {
            return Ok(None);
        }

        // Not built yet. The arguments are not turned into a config.

        Ok(None)
    }
}

impl Default for CliArgumentResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_resolver_custom() {
        let mut resolver = VariableResolver::new();
        resolver.set_variable("TEST_VAR", "test_value");

        let result = resolver.resolve_variable("TEST_VAR");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_value");
    }

    #[test]
    #[serial_test::serial]
    fn test_variable_resolver_env() {
        env::set_var("RESOLVER_TEST_VAR", "env_value");

        let resolver = VariableResolver::new();
        let result = resolver.resolve_variable("RESOLVER_TEST_VAR");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "env_value");

        env::remove_var("RESOLVER_TEST_VAR");
    }

    #[test]
    fn test_variable_resolver_missing() {
        let resolver = VariableResolver::new();
        let result = resolver.resolve_variable("NONEXISTENT_VAR");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::EnvironmentVariableNotFound { .. }
        ));
    }

    #[test]
    fn test_cli_argument_resolver() {
        let mut resolver = CliArgumentResolver::new();
        resolver.set_arg("runtime.mode", "distributed");

        // Only checks that the argument is stored, since resolve() is not built yet
        assert!(!resolver.args.is_empty());
        assert_eq!(
            resolver.args.get("runtime.mode"),
            Some(&"distributed".to_string())
        );
    }

    #[test]
    fn test_variable_substitution_basic() {
        let mut resolver = VariableResolver::new();
        resolver.set_variable("TEST_VAR", "test_value");

        let input = "This is a ${TEST_VAR} substitution";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "This is a test_value substitution");
    }

    #[test]
    fn test_variable_substitution_with_default() {
        let resolver = VariableResolver::new();

        let input = "Value: ${MISSING_VAR:default_value}";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "Value: default_value");
    }

    #[test]
    #[serial_test::serial]
    fn test_variable_substitution_env_var() {
        env::set_var("TEST_ENV_VAR", "env_test_value");

        let resolver = VariableResolver::new();
        let input = "Environment: ${TEST_ENV_VAR}";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "Environment: env_test_value");

        env::remove_var("TEST_ENV_VAR");
    }

    #[test]
    fn test_variable_substitution_nested() {
        let mut resolver = VariableResolver::new();
        resolver.set_variable("INNER_VAR", "inner");
        resolver.set_variable("OUTER_VAR", "Value: ${INNER_VAR}");

        let input = "Result: ${OUTER_VAR}";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "Result: Value: inner");
    }

    #[test]
    fn test_variable_substitution_missing_no_default() {
        let resolver = VariableResolver::new();

        let input = "Missing: ${MISSING_VAR}";
        let result = resolver.resolve_variables_in_string(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_variable_substitution_empty_default() {
        let resolver = VariableResolver::new();

        let input = "Empty: ${MISSING_VAR:}";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "Empty: ");
    }

    #[test]
    fn test_variable_substitution_multiple() {
        let mut resolver = VariableResolver::new();
        resolver.set_variable("VAR1", "value1");
        resolver.set_variable("VAR2", "value2");

        let input = "${VAR1} and ${VAR2} together";
        let result = resolver.resolve_variables_in_string(input).unwrap();
        assert_eq!(result, "value1 and value2 together");
    }

    #[test]
    #[serial_test::serial]
    fn test_resolve_all_config() {
        let mut resolver = VariableResolver::new();
        resolver.set_variable("THREAD_POOL_SIZE", "16");
        resolver.set_variable("BUFFER_SIZE", "2048");

        env::set_var("EVENTFLUX_RUNTIME_MODE", "distributed");

        let config = EventFluxConfig::default();
        // The default config has no references, so this only checks that the round
        // trip through JSON works
        let resolved = resolver.resolve_all(config).unwrap();
        assert_eq!(resolved.api_version, "eventflux.io/v1");

        env::remove_var("EVENTFLUX_RUNTIME_MODE");
    }
}
