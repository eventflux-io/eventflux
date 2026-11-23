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

//! Processor Configuration Reader
//!
//! One place for processors to read configuration values. Falls back to built in
//! defaults and caches what it finds.

use crate::core::config::{ApplicationConfig, EventFluxConfig};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Reads configuration values for processors and caches them.
///
/// Only found values are cached, so a missing key is looked up again on every call.
/// Clones share the same cache.
#[derive(Debug, Clone)]
pub struct ProcessorConfigReader {
    /// Found values keyed by `"<section>.<type>.<key>"` or `"performance.<key>"`
    config_cache: Arc<RwLock<HashMap<String, ConfigValue>>>,
    /// Application configuration
    app_config: Option<ApplicationConfig>,
    /// Global configuration
    global_config: Option<EventFluxConfig>,
}

/// A typed configuration value returned to a processor
#[derive(Debug, Clone)]
pub enum ConfigValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    ByteSize(u64),
}

impl ProcessorConfigReader {
    /// Create a reader with an empty cache
    pub fn new(
        app_config: Option<ApplicationConfig>,
        global_config: Option<EventFluxConfig>,
    ) -> Self {
        Self {
            config_cache: Arc::new(RwLock::new(HashMap::new())),
            app_config,
            global_config,
        }
    }

    /// Get a window setting. See `lookup_window_config` for where it comes from.
    pub fn get_window_config(&self, window_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("window.{}.{}", window_type, key);

        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        let value = self.lookup_window_config(window_type, key);

        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get a sink setting. See `lookup_sink_config` for where it comes from.
    pub fn get_sink_config(&self, sink_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("sink.{}.{}", sink_type, key);

        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        let value = self.lookup_sink_config(sink_type, key);

        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get a stream processor setting. Only built in defaults are returned today.
    pub fn get_stream_config(&self, processor_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("stream.{}.{}", processor_type, key);

        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        let value = self.lookup_stream_config(processor_type, key);

        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get a runtime performance setting. See `lookup_performance_config`.
    pub fn get_performance_config(&self, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("performance.{}", key);

        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        let value = self.lookup_performance_config(key);

        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Look up a window setting.
    ///
    /// If the app config has a window definition named `window_type`, the key is read
    /// from its parameters and the built in defaults are not used, even when the key
    /// is missing. Otherwise the built in default for the window type is returned.
    fn lookup_window_config(&self, window_type: &str, key: &str) -> Option<ConfigValue> {
        if let Some(ref app_config) = self.app_config {
            if let Some(crate::core::config::DefinitionConfig::Window(window_config)) =
                app_config.definitions.get(window_type)
            {
                return self.extract_config_value_from_yaml(&window_config.parameters, key);
            }
        }

        // Built in defaults
        match (window_type, key) {
            ("length", "initial_capacity_multiplier") => Some(ConfigValue::Float(1.2)),
            ("length", "max_size_multiplier") => Some(ConfigValue::Float(2.0)),
            ("time", "cleanup_interval_ms") => Some(ConfigValue::Integer(1000)),
            ("session", "timeout_ms") => Some(ConfigValue::Integer(30000)),
            ("sort", "initial_capacity_multiplier") => Some(ConfigValue::Float(1.2)),
            ("sort", "distributed_size_factor") => Some(ConfigValue::Float(0.8)),
            _ => None,
        }
    }

    /// Look up a sink setting.
    ///
    /// First reads the key from the sink `connection` block of the app config stream
    /// whose name equals `sink_type`. Falls back to the built in defaults for the
    /// `log` and `console` sinks.
    fn lookup_sink_config(&self, sink_type: &str, key: &str) -> Option<ConfigValue> {
        if let Some(ref app_config) = self.app_config {
            if let Some(stream_config) = app_config.streams.get(sink_type) {
                if let Some(ref sink) = stream_config.sink {
                    if let Some(parameters) = self.extract_parameters_from_sink_config(sink, key) {
                        return Some(parameters);
                    }
                }
            }
        }

        // Built in defaults
        match (sink_type, key) {
            ("log", "prefix") => Some(ConfigValue::String("[LOG]".to_string())),
            ("log", "level") => Some(ConfigValue::String("INFO".to_string())),
            ("console", "format") => Some(ConfigValue::String("json".to_string())),
            _ => None,
        }
    }

    /// Look up a stream processor setting. Returns the built in default only.
    fn lookup_stream_config(&self, processor_type: &str, key: &str) -> Option<ConfigValue> {
        if let Some(ref app_config) = self.app_config {
            if let Some(crate::core::config::DefinitionConfig::Stream(_stream_config)) =
                app_config.definitions.get(processor_type)
            {
                // TODO: map the @source and @sink properties of the ApplicationConfig to
                // processor properties. Until then YAML stream config is ignored here.
            }
        }

        // Built in defaults
        match (processor_type, key) {
            ("filter", "batch_size") => Some(ConfigValue::Integer(1000)),
            ("select", "buffer_size") => Some(ConfigValue::Integer(10000)),
            ("join", "cache_size") => Some(ConfigValue::Integer(50000)),
            _ => None,
        }
    }

    /// Look up a runtime performance setting.
    ///
    /// Reads `eventflux.runtime.performance` from the global config when there is one.
    /// `batch_size` is `None` there if unset. Without a global config, fixed defaults
    /// are returned. `enable_statistics` is always false.
    fn lookup_performance_config(&self, key: &str) -> Option<ConfigValue> {
        if let Some(ref global_config) = self.global_config {
            let performance = &global_config.eventflux.runtime.performance;
            match key {
                "event_buffer_size" => {
                    Some(ConfigValue::Integer(performance.event_buffer_size as i64))
                }
                "batch_size" => performance
                    .batch_size
                    .map(|v| ConfigValue::Integer(v as i64)),
                "thread_pool_size" => {
                    Some(ConfigValue::Integer(performance.thread_pool_size as i64))
                }
                "batch_processing" => Some(ConfigValue::Boolean(performance.batch_processing)),
                "async_processing" => Some(ConfigValue::Boolean(performance.async_processing)),
                "enable_statistics" => Some(ConfigValue::Boolean(false)), // Not in PerformanceConfig, default to false
                _ => None,
            }
        } else {
            // No global config, so use fixed defaults
            match key {
                "event_buffer_size" => Some(ConfigValue::Integer(1024)),
                "batch_size" => Some(ConfigValue::Integer(1000)),
                "thread_pool_size" => Some(ConfigValue::Integer(4)),
                "batch_processing" => Some(ConfigValue::Boolean(true)),
                "async_processing" => Some(ConfigValue::Boolean(false)),
                "enable_statistics" => Some(ConfigValue::Boolean(false)),
                _ => None,
            }
        }
    }

    /// Read `key` from a string map and type it as integer, float, boolean or string,
    /// trying them in that order
    #[allow(dead_code)]
    fn extract_config_value(
        &self,
        parameters: &Option<std::collections::HashMap<String, String>>,
        key: &str,
    ) -> Option<ConfigValue> {
        if let Some(ref params) = parameters {
            if let Some(value_str) = params.get(key) {
                if let Ok(int_val) = value_str.parse::<i64>() {
                    return Some(ConfigValue::Integer(int_val));
                }
                if let Ok(float_val) = value_str.parse::<f64>() {
                    return Some(ConfigValue::Float(float_val));
                }
                if let Ok(bool_val) = value_str.parse::<bool>() {
                    return Some(ConfigValue::Boolean(bool_val));
                }
                return Some(ConfigValue::String(value_str.clone()));
            }
        }
        None
    }

    /// Read `key` from a YAML mapping, such as WindowConfig parameters.
    ///
    /// Numbers, booleans and strings are returned typed. A string is typed as integer,
    /// float or boolean when it parses as one. Returns `None` if `yaml_value` is not a
    /// mapping, the key is missing, or the value is another YAML type.
    fn extract_config_value_from_yaml(
        &self,
        yaml_value: &serde_yaml::Value,
        key: &str,
    ) -> Option<ConfigValue> {
        if let serde_yaml::Value::Mapping(map) = yaml_value {
            if let Some(value) = map.get(serde_yaml::Value::String(key.to_string())) {
                match value {
                    serde_yaml::Value::Number(num) => {
                        if let Some(int_val) = num.as_i64() {
                            return Some(ConfigValue::Integer(int_val));
                        }
                        if let Some(float_val) = num.as_f64() {
                            return Some(ConfigValue::Float(float_val));
                        }
                    }
                    serde_yaml::Value::String(str_val) => {
                        if let Ok(int_val) = str_val.parse::<i64>() {
                            return Some(ConfigValue::Integer(int_val));
                        }
                        if let Ok(float_val) = str_val.parse::<f64>() {
                            return Some(ConfigValue::Float(float_val));
                        }
                        if let Ok(bool_val) = str_val.parse::<bool>() {
                            return Some(ConfigValue::Boolean(bool_val));
                        }
                        return Some(ConfigValue::String(str_val.clone()));
                    }
                    serde_yaml::Value::Bool(bool_val) => {
                        return Some(ConfigValue::Boolean(*bool_val));
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// Clear the configuration cache
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.config_cache.write() {
            cache.clear();
        }
    }

    /// Number of cached values. Returns 0 if the cache lock is poisoned.
    pub fn get_cache_size(&self) -> usize {
        self.config_cache
            .read()
            .map(|cache| cache.len())
            .unwrap_or(0)
    }

    /// Read `key` from the sink `connection` block
    fn extract_parameters_from_sink_config(
        &self,
        sink: &crate::core::config::types::application_config::SinkConfig,
        key: &str,
    ) -> Option<ConfigValue> {
        self.extract_config_value_from_yaml(&sink.connection, key)
    }
}

/// Typed accessors. Each returns `None` when the variant does not match, except that
/// `as_float` also accepts an integer and `as_usize` accepts a non negative integer.
impl ConfigValue {
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            ConfigValue::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            ConfigValue::Float(v) => Some(*v),
            ConfigValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            ConfigValue::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            ConfigValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            ConfigValue::Integer(v) if *v >= 0 => Some(*v as usize),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_reader_creation() {
        let reader = ProcessorConfigReader::new(None, None);
        assert_eq!(reader.get_cache_size(), 0);
    }

    #[test]
    fn test_window_config_defaults() {
        let reader = ProcessorConfigReader::new(None, None);

        let multiplier = reader.get_window_config("length", "initial_capacity_multiplier");
        assert!(matches!(multiplier, Some(ConfigValue::Float(1.2))));

        let timeout = reader.get_window_config("session", "timeout_ms");
        assert!(matches!(timeout, Some(ConfigValue::Integer(30000))));
    }

    #[test]
    fn test_performance_config_defaults() {
        let reader = ProcessorConfigReader::new(None, None);

        let buffer_size = reader.get_performance_config("event_buffer_size");
        assert!(matches!(buffer_size, Some(ConfigValue::Integer(1024))));

        let stats_enabled = reader.get_performance_config("enable_statistics");
        assert!(matches!(stats_enabled, Some(ConfigValue::Boolean(false))));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_config_value_conversions() {
        let int_val = ConfigValue::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.as_float(), Some(42.0));
        assert_eq!(int_val.as_usize(), Some(42));

        let float_val = ConfigValue::Float(3.14);
        assert_eq!(float_val.as_float(), Some(3.14));
        assert_eq!(float_val.as_integer(), None);

        let bool_val = ConfigValue::Boolean(true);
        assert_eq!(bool_val.as_boolean(), Some(true));
    }

    #[test]
    fn test_cache_functionality() {
        let reader = ProcessorConfigReader::new(None, None);

        // First access should cache the value
        let _val1 = reader.get_window_config("length", "initial_capacity_multiplier");
        assert_eq!(reader.get_cache_size(), 1);

        // Second access should use cached value
        let _val2 = reader.get_window_config("length", "initial_capacity_multiplier");
        assert_eq!(reader.get_cache_size(), 1);

        // Different key should add to cache
        let _val3 = reader.get_window_config("time", "cleanup_interval_ms");
        assert_eq!(reader.get_cache_size(), 2);

        // Clear cache
        reader.clear_cache();
        assert_eq!(reader.get_cache_size(), 0);
    }

    #[test]
    fn test_sink_config_defaults() {
        let reader = ProcessorConfigReader::new(None, None);

        let prefix = reader.get_sink_config("log", "prefix");
        assert!(matches!(prefix, Some(ConfigValue::String(p)) if p == "[LOG]"));

        let level = reader.get_sink_config("log", "level");
        assert!(matches!(level, Some(ConfigValue::String(l)) if l == "INFO"));

        let format = reader.get_sink_config("console", "format");
        assert!(matches!(format, Some(ConfigValue::String(f)) if f == "json"));

        let unknown = reader.get_sink_config("unknown", "param");
        assert!(unknown.is_none());
    }
}
