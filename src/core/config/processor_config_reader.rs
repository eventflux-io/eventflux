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
//! Provides a centralized way for processors to read configuration values
//! with fallback to defaults and caching for performance.

use crate::core::config::{ApplicationConfig, EventFluxConfig};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Configuration reader for processors with performance-optimized caching
#[derive(Debug, Clone)]
pub struct ProcessorConfigReader {
    /// Cached configuration values for quick access
    config_cache: Arc<RwLock<HashMap<String, ConfigValue>>>,
    /// Application configuration
    app_config: Option<ApplicationConfig>,
    /// Global configuration
    global_config: Option<EventFluxConfig>,
}

/// Configuration value types that processors can request
#[derive(Debug, Clone)]
pub enum ConfigValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    ByteSize(u64),
}

impl ProcessorConfigReader {
    /// Create a new configuration reader
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

    /// Get window configuration value with caching
    pub fn get_window_config(&self, window_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("window.{}.{}", window_type, key);

        // Check cache first
        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        // Look up from configuration
        let value = self.lookup_window_config(window_type, key);

        // Cache the result
        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get sink configuration value with caching
    pub fn get_sink_config(&self, sink_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("sink.{}.{}", sink_type, key);

        // Check cache first
        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        // Look up from configuration
        let value = self.lookup_sink_config(sink_type, key);

        // Cache the result
        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get stream processor configuration
    pub fn get_stream_config(&self, processor_type: &str, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("stream.{}.{}", processor_type, key);

        // Check cache first
        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        // Look up from configuration
        let value = self.lookup_stream_config(processor_type, key);

        // Cache the result
        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Get performance configuration
    pub fn get_performance_config(&self, key: &str) -> Option<ConfigValue> {
        let cache_key = format!("performance.{}", key);

        // Check cache first
        if let Ok(cache) = self.config_cache.read() {
            if let Some(cached_value) = cache.get(&cache_key) {
                return Some(cached_value.clone());
            }
        }

        // Look up from configuration
        let value = self.lookup_performance_config(key);

        // Cache the result
        if let Some(ref val) = value {
            if let Ok(mut cache) = self.config_cache.write() {
                cache.insert(cache_key, val.clone());
            }
        }

        value
    }

    /// Lookup window configuration from app/global config
    fn lookup_window_config(&self, window_type: &str, key: &str) -> Option<ConfigValue> {
        // First check application-specific window configuration
        if let Some(ref app_config) = self.app_config {
            if let Some(definition_config) = app_config.definitions.get(window_type) {
                if let crate::core::config::DefinitionConfig::Window(window_config) =
                    definition_config
                {
                    return self.extract_config_value_from_yaml(&window_config.parameters, key);
                }
            }
        }

        // Fallback to global defaults
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

    /// Lookup sink configuration from app/global config
    fn lookup_sink_config(&self, sink_type: &str, key: &str) -> Option<ConfigValue> {
        // Check application-specific sink configuration in streams
        if let Some(ref app_config) = self.app_config {
            if let Some(stream_config) = app_config.streams.get(sink_type) {
                if let Some(ref sink) = stream_config.sink {
                    if let Some(parameters) = self.extract_parameters_from_sink_config(sink, key) {
                        return Some(parameters);
                    }
                }
            }
        }

        // Default configurations for sinks
        match (sink_type, key) {
            ("log", "prefix") => Some(ConfigValue::String("[LOG]".to_string())),
            ("log", "level") => Some(ConfigValue::String("INFO".to_string())),
            ("console", "format") => Some(ConfigValue::String("json".to_string())),
            _ => None,
        }
    }

    /// Lookup stream processor configuration
    fn lookup_stream_config(&self, processor_type: &str, key: &str) -> Option<ConfigValue> {
        // First check application-specific stream configuration
        if let Some(ref app_config) = self.app_config {
            if let Some(definition_config) = app_config.definitions.get(processor_type) {
                if let crate::core::config::DefinitionConfig::Stream(_stream_config) =
                    definition_config
                {
                    // For now, we'll implement simple stream config extraction later
                    // TODO: Implement stream-specific configuration extraction
                }
            }
        }

        // Default configurations for stream processors
        match (processor_type, key) {
            ("filter", "batch_size") => Some(ConfigValue::Integer(1000)),
            ("select", "buffer_size") => Some(ConfigValue::Integer(10000)),
            ("join", "cache_size") => Some(ConfigValue::Integer(50000)),
            _ => None,
        }
    }

    /// Lookup performance configuration
    fn lookup_performance_config(&self, key: &str) -> Option<ConfigValue> {
        // Check global configuration for performance settings
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
            // Fallback defaults when no global config
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

    /// Extract configuration value from parameter map
    fn extract_config_value(
        &self,
        parameters: &Option<std::collections::HashMap<String, String>>,
        key: &str,
    ) -> Option<ConfigValue> {
        if let Some(ref params) = parameters {
            if let Some(value_str) = params.get(key) {
                // Try to parse as different types
                if let Ok(int_val) = value_str.parse::<i64>() {
                    return Some(ConfigValue::Integer(int_val));
                }
                if let Ok(float_val) = value_str.parse::<f64>() {
                    return Some(ConfigValue::Float(float_val));
                }
                if let Ok(bool_val) = value_str.parse::<bool>() {
                    return Some(ConfigValue::Boolean(bool_val));
                }
                // Default to string
                return Some(ConfigValue::String(value_str.clone()));
            }
        }
        None
    }

    /// Extract configuration value from YAML value (for WindowConfig parameters)
    fn extract_config_value_from_yaml(
        &self,
        yaml_value: &serde_yaml::Value,
        key: &str,
    ) -> Option<ConfigValue> {
        if let serde_yaml::Value::Mapping(map) = yaml_value {
            if let Some(value) = map.get(&serde_yaml::Value::String(key.to_string())) {
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
                        // Try to parse string as number or boolean first
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

    /// Get cache size for monitoring
    pub fn get_cache_size(&self) -> usize {
        self.config_cache
            .read()
            .map(|cache| cache.len())
            .unwrap_or(0)
    }

    /// Extract configuration value from sink configuration
    fn extract_parameters_from_sink_config(
        &self,
        sink: &crate::core::config::types::application_config::SinkConfig,
        key: &str,
    ) -> Option<ConfigValue> {
        // Extract parameters from the sink's connection YAML configuration
        self.extract_config_value_from_yaml(&sink.connection, key)
    }
}

/// Helper methods for extracting typed values from ConfigValue
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
