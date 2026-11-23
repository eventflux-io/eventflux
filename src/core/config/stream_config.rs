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

//! # Stream Configuration Module
//!
//! Configuration for EventFlux streams and tables, built from several layers. Each
//! property remembers where it came from, and a value from a higher priority source
//! replaces one from a lower priority source.
//!
//! ## Configuration Sources (Priority: Low to High)
//!
//! 1. **RustDefault**. Built in Rust defaults.
//! 2. **TomlApplication**. The TOML `[application]` section, shared by all streams.
//! 3. **TomlStream**. The TOML section for one stream.
//! 4. **SqlWith**. The SQL WITH clause. Highest priority.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use eventflux::core::config::stream_config::*;
//!
//! // Create configuration from SQL WITH clause
//! let mut config = FlatConfig::new();
//! config.set("type", "source", PropertySource::SqlWith);
//! config.set("extension", "kafka", PropertySource::SqlWith);
//! config.set("format", "json", PropertySource::SqlWith);
//!
//! // Convert to typed configuration
//! let stream_config = StreamTypeConfig::from_flat_config(&config)?;
//! assert_eq!(stream_config.stream_type, StreamType::Source);
//! assert_eq!(stream_config.extension()?, "kafka");
//! assert_eq!(stream_config.format()?, "json");
//! ```

use std::collections::HashMap;
use std::str::FromStr;

/// Where a property came from, ordered by priority
///
/// A value from a higher priority source replaces one from a lower priority source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertySource {
    /// Rust code defaults (priority: 0)
    RustDefault,
    /// TOML [application] section (priority: 1)
    TomlApplication,
    /// TOML [streams.StreamName] section (priority: 2)
    TomlStream,
    /// SQL WITH clause (priority: 3)
    SqlWith,
}

impl PropertySource {
    /// Numeric priority from 0 to 3. A higher number wins.
    #[inline]
    pub const fn priority(&self) -> u8 {
        match self {
            PropertySource::RustDefault => 0,
            PropertySource::TomlApplication => 1,
            PropertySource::TomlStream => 2,
            PropertySource::SqlWith => 3,
        }
    }

    /// Short description of the source for people to read
    #[inline]
    pub const fn description(&self) -> &'static str {
        match self {
            PropertySource::RustDefault => "Rust default",
            PropertySource::TomlApplication => "TOML [application]",
            PropertySource::TomlStream => "TOML [streams.StreamName]",
            PropertySource::SqlWith => "SQL WITH clause",
        }
    }
}

/// Flat map of string properties that remembers the source of each one
///
/// A value from a higher priority source replaces one from a lower priority source.
/// The layered stream and table configuration is built on this.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatConfig {
    properties: HashMap<String, String>,
    sources: HashMap<String, PropertySource>,
}

impl FlatConfig {
    /// Create an empty configuration
    #[inline]
    pub fn new() -> Self {
        Self {
            properties: HashMap::new(),
            sources: HashMap::new(),
        }
    }

    /// Create an empty configuration with room for `capacity` properties
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            properties: HashMap::with_capacity(capacity),
            sources: HashMap::with_capacity(capacity),
        }
    }

    /// Set a property and record its source
    ///
    /// Does nothing if the key is already set from a higher priority source. A source
    /// of equal priority replaces the value, so the last write wins.
    pub fn set(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        source: PropertySource,
    ) {
        let key = key.into();
        let value = value.into();

        if let Some(existing_source) = self.sources.get(&key) {
            if existing_source.priority() > source.priority() {
                return;
            }
        }

        self.properties.insert(key.clone(), value);
        self.sources.insert(key, source);
    }

    /// Get a property value by key
    #[inline]
    pub fn get(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    /// Get a property value with its source
    #[inline]
    pub fn get_with_source(&self, key: &str) -> Option<(&String, PropertySource)> {
        self.properties
            .get(key)
            .and_then(|value| self.sources.get(key).map(|source| (value, *source)))
    }

    /// Check if a property exists
    #[inline]
    pub fn contains(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Get an iterator over all property keys
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.properties.keys()
    }

    /// Get all properties, without their sources
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Merge `other` into this config through [`FlatConfig::set`]
    ///
    /// A property from `other` replaces an existing one only when its source has equal
    /// or higher priority.
    pub fn merge(&mut self, other: &FlatConfig) {
        for (key, value) in &other.properties {
            if let Some(source) = other.sources.get(key) {
                self.set(key.clone(), value.clone(), *source);
            }
        }
    }

    /// Get all properties whose key starts with `prefix`
    ///
    /// Handy for pulling out the settings of one extension, such as "kafka.*". Keys keep
    /// their prefix.
    pub fn get_properties_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a String)> + 'a {
        self.properties
            .iter()
            .filter(move |(key, _)| key.starts_with(prefix))
    }

    /// Get the number of properties
    #[inline]
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Check if configuration is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }
}

impl Default for FlatConfig {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Kind of stream
///
/// Decides which validation rules apply and whether an extension is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamType {
    /// Reads from an external system
    Source,
    /// Writes to an external system
    Sink,
    /// Lives only in memory
    Internal,
}

impl std::str::FromStr for StreamType {
    type Err = String;

    /// Parse "source", "sink" or "internal", ignoring case
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "source" => Ok(StreamType::Source),
            "sink" => Ok(StreamType::Sink),
            "internal" => Ok(StreamType::Internal),
            _ => Err(format!(
                "Invalid stream type '{}'. Valid values: 'source', 'sink', 'internal'",
                s
            )),
        }
    }
}

impl StreamType {
    /// Lower case name of the stream type
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            StreamType::Source => "source",
            StreamType::Sink => "sink",
            StreamType::Internal => "internal",
        }
    }

    /// Check if the stream type needs an extension. True for source and sink.
    #[inline]
    pub const fn requires_extension(&self) -> bool {
        matches!(self, StreamType::Source | StreamType::Sink)
    }

    /// True for source and sink. [`StreamTypeConfig::validate`] still lets them leave
    /// out the format.
    #[inline]
    pub const fn requires_format(&self) -> bool {
        matches!(self, StreamType::Source | StreamType::Sink)
    }
}

/// Typed stream configuration that has passed validation
///
/// Rules for each stream type:
/// - Source and sink streams **must** have an extension. The format is optional.
/// - Internal streams **must not** have an extension or a format.
#[derive(Debug, Clone)]
pub struct StreamTypeConfig {
    pub stream_type: StreamType,
    pub extension: Option<String>,
    pub format: Option<String>,
    pub properties: HashMap<String, String>,
}

impl StreamTypeConfig {
    /// Build a stream configuration and validate it. Returns the validation error.
    pub fn new(
        stream_type: StreamType,
        extension: Option<String>,
        format: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Self, String> {
        let config = Self {
            stream_type,
            extension,
            format,
            properties,
        };

        config.validate()?;
        Ok(config)
    }

    /// Build a stream configuration from a [`FlatConfig`] and validate it
    ///
    /// Reads `type`, `extension` and `format`. A missing `type` means an internal, in
    /// memory stream, so simple streams need no config at all. All properties,
    /// including those three, are copied into `properties`.
    ///
    /// Returns an error for an unknown `type` or a config that fails validation.
    pub fn from_flat_config(flat_config: &FlatConfig) -> Result<Self, String> {
        // A missing type means internal (spec lines 178-197)
        let stream_type = flat_config
            .get("type")
            .map(|s| StreamType::from_str(s))
            .transpose()?
            .unwrap_or(StreamType::Internal);

        let extension = flat_config.get("extension").cloned();
        let format = flat_config.get("format").cloned();
        let properties = flat_config.properties().clone();

        Self::new(stream_type, extension, format, properties)
    }

    /// Check the rules for the stream type. See [`StreamTypeConfig`].
    pub fn validate(&self) -> Result<(), String> {
        match self.stream_type {
            StreamType::Source | StreamType::Sink => {
                if self.extension.is_none() {
                    return Err(format!(
                        "Stream has type='{}' but missing required 'extension' property",
                        self.stream_type.as_str()
                    ));
                }

                // Format is optional. Some sources and sinks pass events in their own
                // binary form, and PassthroughMapper is used when no format is set.
            }
            StreamType::Internal => {
                if self.extension.is_some() {
                    return Err(
                        "Stream has type='internal' but specifies 'extension' (not allowed)"
                            .to_string(),
                    );
                }

                if self.format.is_some() {
                    return Err(
                        "Stream has type='internal' but specifies 'format' (not allowed)"
                            .to_string(),
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the extension. Returns an error when none is set, as for an internal stream.
    #[inline]
    pub fn extension(&self) -> Result<&str, String> {
        self.extension.as_deref().ok_or_else(|| {
            format!(
                "Stream type '{}' requires extension",
                self.stream_type.as_str()
            )
        })
    }

    /// Get the format, or `None` when none is set
    ///
    /// Some sources and sinks, such as timer, pass events in their own binary form and
    /// need no format.
    #[inline]
    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }

    /// Copy out the properties whose key starts with `prefix`, such as "kafka.*".
    /// Keys keep their prefix.
    pub fn get_properties_with_prefix(&self, prefix: &str) -> HashMap<String, String> {
        self.properties
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// Typed table configuration that has passed validation
///
/// Tables hold data that queries both read and write, for lookups and joins. Unlike
/// streams, tables:
/// - Do NOT have a 'type' property, because data flows both ways
/// - Do NOT have a 'format' property, because they only use the relational schema
/// - MUST have an 'extension' property naming the backing store, such as mysql,
///   postgres or redis
///
/// # Validation Rules
///
/// - `extension` is **REQUIRED** and must not be empty
/// - `type` is **FORBIDDEN**
/// - `format` is **FORBIDDEN**
///
/// # Example
///
/// ```rust,ignore
/// use eventflux::core::config::stream_config::*;
///
/// // Create table configuration from TOML/SQL
/// let mut config = FlatConfig::new();
/// config.set("extension", "mysql", PropertySource::SqlWith);
/// config.set("mysql.host", "localhost", PropertySource::TomlApplication);
/// config.set("mysql.table", "users", PropertySource::SqlWith);
///
/// // Convert to typed configuration
/// let table_config = TableTypeConfig::from_flat_config(&config)?;
/// assert_eq!(table_config.extension(), "mysql");
/// ```
#[derive(Debug, Clone)]
pub struct TableTypeConfig {
    pub extension: String,
    pub properties: HashMap<String, String>,
}

impl TableTypeConfig {
    /// Build a table configuration and validate it. Returns the validation error.
    pub fn new(extension: String, properties: HashMap<String, String>) -> Result<Self, String> {
        let config = Self {
            extension,
            properties,
        };

        config.validate()?;
        Ok(config)
    }

    /// Build a table configuration from a [`FlatConfig`] and validate it
    ///
    /// Returns an error if `type` or `format` is present or `extension` is missing.
    /// All properties, including `extension`, are copied into `properties`.
    pub fn from_flat_config(flat_config: &FlatConfig) -> Result<Self, String> {
        if flat_config.contains("type") {
            return Err(
                "Table defines 'type' property, but tables cannot have 'type'. \
                Tables are always bidirectional."
                    .to_string(),
            );
        }

        if flat_config.contains("format") {
            return Err(
                "Table defines 'format' property, but tables cannot have 'format'. \
                Tables use relational schema only."
                    .to_string(),
            );
        }

        let extension = flat_config
            .get("extension")
            .ok_or_else(|| {
                "Table configuration missing required 'extension' property. \
                Tables must specify backing store (e.g., 'mysql', 'postgres', 'redis')."
                    .to_string()
            })?
            .clone();

        let properties = flat_config.properties().clone();

        Self::new(extension, properties)
    }

    /// Check that the extension is not empty and that `properties` has no `type` or
    /// `format` key
    pub fn validate(&self) -> Result<(), String> {
        if self.extension.is_empty() {
            return Err("Table extension cannot be empty".to_string());
        }

        // from_flat_config already rejects these. This catches tables built with new().
        if self.properties.contains_key("type") {
            return Err(
                "Table properties contain 'type', but tables cannot have 'type' property"
                    .to_string(),
            );
        }

        if self.properties.contains_key("format") {
            return Err(
                "Table properties contain 'format', but tables cannot have 'format' property"
                    .to_string(),
            );
        }

        Ok(())
    }

    /// Get extension name
    #[inline]
    pub fn extension(&self) -> &str {
        &self.extension
    }

    /// Copy out the properties whose key starts with `prefix`, such as "mysql.*".
    /// Keys keep their prefix.
    pub fn get_properties_with_prefix(&self, prefix: &str) -> HashMap<String, String> {
        self.properties
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // PropertySource Tests
    // ========================================================================

    #[test]
    fn test_property_source_priority_ordering() {
        assert!(PropertySource::SqlWith.priority() > PropertySource::TomlStream.priority());
        assert!(PropertySource::TomlStream.priority() > PropertySource::TomlApplication.priority());
        assert!(
            PropertySource::TomlApplication.priority() > PropertySource::RustDefault.priority()
        );
    }

    #[test]
    fn test_property_source_priority_values() {
        assert_eq!(PropertySource::RustDefault.priority(), 0);
        assert_eq!(PropertySource::TomlApplication.priority(), 1);
        assert_eq!(PropertySource::TomlStream.priority(), 2);
        assert_eq!(PropertySource::SqlWith.priority(), 3);
    }

    #[test]
    fn test_property_source_descriptions() {
        assert_eq!(PropertySource::RustDefault.description(), "Rust default");
        assert_eq!(
            PropertySource::TomlApplication.description(),
            "TOML [application]"
        );
        assert_eq!(
            PropertySource::TomlStream.description(),
            "TOML [streams.StreamName]"
        );
        assert_eq!(PropertySource::SqlWith.description(), "SQL WITH clause");
    }

    // ========================================================================
    // FlatConfig Tests
    // ========================================================================

    #[test]
    fn test_flat_config_creation() {
        let config = FlatConfig::new();
        assert!(config.is_empty());
        assert_eq!(config.len(), 0);
    }

    #[test]
    fn test_flat_config_set_and_get() {
        let mut config = FlatConfig::new();
        config.set("key1", "value1", PropertySource::RustDefault);

        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert!(config.contains("key1"));
        assert_eq!(config.len(), 1);
    }

    #[test]
    fn test_flat_config_priority_override() {
        let mut config = FlatConfig::new();

        // Set with low priority
        config.set("buffer_size", "1024", PropertySource::RustDefault);
        assert_eq!(config.get("buffer_size"), Some(&"1024".to_string()));

        // Override with higher priority
        config.set("buffer_size", "2048", PropertySource::TomlApplication);
        assert_eq!(config.get("buffer_size"), Some(&"2048".to_string()));

        // Override with even higher priority
        config.set("buffer_size", "4096", PropertySource::SqlWith);
        assert_eq!(config.get("buffer_size"), Some(&"4096".to_string()));
    }

    #[test]
    fn test_flat_config_priority_no_override() {
        let mut config = FlatConfig::new();

        // Set with high priority
        config.set("buffer_size", "4096", PropertySource::SqlWith);
        assert_eq!(config.get("buffer_size"), Some(&"4096".to_string()));

        // Try to override with lower priority (should fail)
        config.set("buffer_size", "1024", PropertySource::RustDefault);
        assert_eq!(config.get("buffer_size"), Some(&"4096".to_string()));

        // Same priority does override. The last write wins.
        config.set("buffer_size", "2048", PropertySource::SqlWith);
        assert_eq!(config.get("buffer_size"), Some(&"2048".to_string()));
    }

    #[test]
    fn test_flat_config_get_with_source() {
        let mut config = FlatConfig::new();
        config.set("key1", "value1", PropertySource::TomlStream);

        let result = config.get_with_source("key1");
        assert!(result.is_some());
        let (value, source) = result.unwrap();
        assert_eq!(value, &"value1".to_string());
        assert_eq!(source, PropertySource::TomlStream);
    }

    #[test]
    fn test_flat_config_merge() {
        let mut config1 = FlatConfig::new();
        config1.set("key1", "value1", PropertySource::RustDefault);
        config1.set("key2", "value2", PropertySource::TomlApplication);

        let mut config2 = FlatConfig::new();
        config2.set("key2", "new_value2", PropertySource::SqlWith); // Higher priority
        config2.set("key3", "value3", PropertySource::TomlStream);

        config1.merge(&config2);

        assert_eq!(config1.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config1.get("key2"), Some(&"new_value2".to_string())); // Overridden
        assert_eq!(config1.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_flat_config_prefix_filtering() {
        let mut config = FlatConfig::new();
        config.set(
            "kafka_bootstrap_servers",
            "localhost:9092",
            PropertySource::SqlWith,
        );
        config.set("kafka_topic", "events", PropertySource::SqlWith);
        config.set("type", "source", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith);

        let kafka_props: Vec<_> = config
            .get_properties_with_prefix("kafka_")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        assert_eq!(kafka_props.len(), 2);
        assert!(kafka_props
            .iter()
            .any(|(k, _)| k == "kafka_bootstrap_servers"));
        assert!(kafka_props.iter().any(|(k, _)| k == "kafka_topic"));
    }

    #[test]
    fn test_flat_config_keys_iterator() {
        let mut config = FlatConfig::new();
        config.set("key1", "value1", PropertySource::RustDefault);
        config.set("key2", "value2", PropertySource::RustDefault);
        config.set("key3", "value3", PropertySource::RustDefault);

        let keys: Vec<_> = config.keys().collect();
        assert_eq!(keys.len(), 3);
    }

    // ========================================================================
    // StreamType Tests
    // ========================================================================

    #[test]
    fn test_stream_type_from_str() {
        assert_eq!(StreamType::from_str("source").unwrap(), StreamType::Source);
        assert_eq!(StreamType::from_str("Source").unwrap(), StreamType::Source);
        assert_eq!(StreamType::from_str("SOURCE").unwrap(), StreamType::Source);

        assert_eq!(StreamType::from_str("sink").unwrap(), StreamType::Sink);
        assert_eq!(StreamType::from_str("Sink").unwrap(), StreamType::Sink);

        assert_eq!(
            StreamType::from_str("internal").unwrap(),
            StreamType::Internal
        );
        assert_eq!(
            StreamType::from_str("Internal").unwrap(),
            StreamType::Internal
        );
    }

    #[test]
    fn test_stream_type_from_str_invalid() {
        assert!(StreamType::from_str("invalid").is_err());
        assert!(StreamType::from_str("").is_err());
        assert!(StreamType::from_str("stream").is_err());
    }

    #[test]
    fn test_stream_type_as_str() {
        assert_eq!(StreamType::Source.as_str(), "source");
        assert_eq!(StreamType::Sink.as_str(), "sink");
        assert_eq!(StreamType::Internal.as_str(), "internal");
    }

    #[test]
    fn test_stream_type_requires_extension() {
        assert!(StreamType::Source.requires_extension());
        assert!(StreamType::Sink.requires_extension());
        assert!(!StreamType::Internal.requires_extension());
    }

    #[test]
    fn test_stream_type_requires_format() {
        assert!(StreamType::Source.requires_format());
        assert!(StreamType::Sink.requires_format());
        assert!(!StreamType::Internal.requires_format());
    }

    // ========================================================================
    // StreamTypeConfig Tests
    // ========================================================================

    #[test]
    fn test_stream_type_config_source_valid() {
        let mut props = HashMap::new();
        props.insert("type".to_string(), "source".to_string());
        props.insert("extension".to_string(), "kafka".to_string());
        props.insert("format".to_string(), "json".to_string());

        let config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("json".to_string()),
            props,
        );

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.stream_type, StreamType::Source);
        assert_eq!(config.extension().unwrap(), "kafka");
        assert_eq!(config.format(), Some("json"));
    }

    #[test]
    fn test_stream_type_config_sink_valid() {
        let mut props = HashMap::new();
        props.insert("type".to_string(), "sink".to_string());
        props.insert("extension".to_string(), "log".to_string());
        props.insert("format".to_string(), "text".to_string());

        let config = StreamTypeConfig::new(
            StreamType::Sink,
            Some("log".to_string()),
            Some("text".to_string()),
            props,
        );

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.stream_type, StreamType::Sink);
    }

    #[test]
    fn test_stream_type_config_internal_valid() {
        let mut props = HashMap::new();
        props.insert("type".to_string(), "internal".to_string());

        let config = StreamTypeConfig::new(StreamType::Internal, None, None, props);

        assert!(config.is_ok());
    }

    #[test]
    fn test_stream_type_config_source_missing_extension() {
        let props = HashMap::new();
        let config = StreamTypeConfig::new(
            StreamType::Source,
            None, // Missing extension
            Some("json".to_string()),
            props,
        );

        assert!(config.is_err());
        assert!(config.unwrap_err().contains("missing required 'extension'"));
    }

    #[test]
    fn test_stream_type_config_source_without_format() {
        // Format is optional. Some sources pass events in their own binary form.
        let props = HashMap::new();
        let config = StreamTypeConfig::new(
            StreamType::Source,
            Some("timer".to_string()),
            None, // No format, so PassthroughMapper is used
            props,
        );

        assert!(
            config.is_ok(),
            "Sources should be allowed without format (e.g., timer uses binary passthrough)"
        );
    }

    #[test]
    fn test_stream_type_config_internal_with_extension() {
        let props = HashMap::new();
        let config = StreamTypeConfig::new(
            StreamType::Internal,
            Some("kafka".to_string()), // Not allowed for internal
            None,
            props,
        );

        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("type='internal' but specifies 'extension'"));
    }

    #[test]
    fn test_stream_type_config_internal_with_format() {
        let props = HashMap::new();
        let config = StreamTypeConfig::new(
            StreamType::Internal,
            None,
            Some("json".to_string()), // Not allowed for internal
            props,
        );

        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("type='internal' but specifies 'format'"));
    }

    #[test]
    fn test_stream_type_config_from_flat_config() {
        let mut flat_config = FlatConfig::new();
        flat_config.set("type", "source", PropertySource::SqlWith);
        flat_config.set("extension", "kafka", PropertySource::SqlWith);
        flat_config.set("format", "json", PropertySource::SqlWith);
        flat_config.set(
            "bootstrap_servers",
            "localhost:9092",
            PropertySource::SqlWith,
        );

        let config = StreamTypeConfig::from_flat_config(&flat_config);
        assert!(config.is_ok());

        let config = config.unwrap();
        assert_eq!(config.stream_type, StreamType::Source);
        assert_eq!(config.extension().unwrap(), "kafka");
        assert_eq!(config.format(), Some("json"));
        assert_eq!(
            config.properties.get("bootstrap_servers"),
            Some(&"localhost:9092".to_string())
        );
    }

    #[test]
    fn test_stream_type_config_from_flat_config_no_type_defaults_internal() {
        // A missing type means an internal, in memory stream
        let flat_config = FlatConfig::new();
        let config = StreamTypeConfig::from_flat_config(&flat_config);

        assert!(
            config.is_ok(),
            "Config without type should default to Internal"
        );
        let config = config.unwrap();
        assert_eq!(
            config.stream_type,
            StreamType::Internal,
            "Missing type should default to StreamType::Internal"
        );
        assert!(
            config.extension.is_none(),
            "Internal stream should have no extension"
        );
        assert!(
            config.format.is_none(),
            "Internal stream should have no format"
        );
    }

    #[test]
    fn test_stream_type_config_pure_internal_stream() {
        // Pure internal stream: no type, no extension, no format
        // Just in-memory event processing
        let mut flat_config = FlatConfig::new();
        flat_config.set("some.property", "value", PropertySource::SqlWith);

        let config = StreamTypeConfig::from_flat_config(&flat_config).unwrap();

        assert_eq!(config.stream_type, StreamType::Internal);
        assert!(config.extension.is_none());
        assert!(config.format.is_none());
        assert_eq!(
            config.properties.get("some.property"),
            Some(&"value".to_string())
        );

        // Should pass validation
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_stream_type_config_get_properties_with_prefix() {
        let mut props = HashMap::new();
        props.insert("type".to_string(), "source".to_string());
        props.insert("extension".to_string(), "kafka".to_string());
        props.insert("format".to_string(), "json".to_string());
        props.insert(
            "kafka_bootstrap_servers".to_string(),
            "localhost:9092".to_string(),
        );
        props.insert("kafka_topic".to_string(), "events".to_string());
        props.insert("redis_host".to_string(), "localhost".to_string());

        let config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("json".to_string()),
            props,
        )
        .unwrap();

        let kafka_props = config.get_properties_with_prefix("kafka_");
        assert_eq!(kafka_props.len(), 2);
        assert_eq!(
            kafka_props.get("kafka_bootstrap_servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(kafka_props.get("kafka_topic"), Some(&"events".to_string()));

        let redis_props = config.get_properties_with_prefix("redis_");
        assert_eq!(redis_props.len(), 1);
    }

    // ========================================================================
    // Integration Tests
    // ========================================================================

    #[test]
    fn test_complete_configuration_flow() {
        // Simulate multi-layer configuration merge
        let mut config = FlatConfig::new();

        // Layer 1: Rust defaults
        config.set("buffer_size", "1024", PropertySource::RustDefault);
        config.set("timeout", "30000", PropertySource::RustDefault);

        // Layer 2: TOML application config
        config.set("buffer_size", "2048", PropertySource::TomlApplication);
        config.set("retry_count", "3", PropertySource::TomlApplication);

        // Layer 3: TOML stream config
        config.set("type", "source", PropertySource::TomlStream);
        config.set("extension", "kafka", PropertySource::TomlStream);

        // Layer 4: SQL WITH clause
        config.set("extension", "http", PropertySource::SqlWith); // Override extension
        config.set("format", "json", PropertySource::SqlWith);

        // Verify final configuration
        assert_eq!(config.get("buffer_size"), Some(&"2048".to_string())); // From TOML app
        assert_eq!(config.get("timeout"), Some(&"30000".to_string())); // From Rust default
        assert_eq!(config.get("retry_count"), Some(&"3".to_string())); // From TOML app
        assert_eq!(config.get("type"), Some(&"source".to_string())); // From TOML stream
        assert_eq!(config.get("extension"), Some(&"http".to_string())); // Overridden by SQL WITH
        assert_eq!(config.get("format"), Some(&"json".to_string())); // From SQL WITH
    }

    #[test]
    fn test_with_capacity_optimization() {
        let config = FlatConfig::with_capacity(10);
        assert_eq!(config.len(), 0);
        assert!(config.is_empty());
    }

    // ========================================================================
    // TableTypeConfig Tests
    // ========================================================================

    #[test]
    fn test_table_config_from_flat_config_success() {
        let mut config = FlatConfig::new();
        config.set("extension", "mysql", PropertySource::SqlWith);
        config.set("mysql.host", "localhost", PropertySource::TomlApplication);
        config.set("mysql.table", "users", PropertySource::SqlWith);

        let table_config = TableTypeConfig::from_flat_config(&config);
        assert!(table_config.is_ok());

        let table_config = table_config.unwrap();
        assert_eq!(table_config.extension(), "mysql");
        assert_eq!(
            table_config.properties.get("mysql.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(
            table_config.properties.get("mysql.table"),
            Some(&"users".to_string())
        );
    }

    #[test]
    fn test_table_config_missing_extension() {
        let mut config = FlatConfig::new();
        config.set("mysql.host", "localhost", PropertySource::TomlApplication);

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("missing required 'extension' property"));
    }

    #[test]
    fn test_table_config_forbids_type_property() {
        let mut config = FlatConfig::new();
        config.set("extension", "mysql", PropertySource::SqlWith);
        config.set("type", "source", PropertySource::SqlWith);

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("cannot have 'type'"));
        assert!(error_msg.contains("bidirectional"));
    }

    #[test]
    fn test_table_config_forbids_format_property() {
        let mut config = FlatConfig::new();
        config.set("extension", "mysql", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith);

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(error_msg.contains("cannot have 'format'"));
        assert!(error_msg.contains("relational schema only"));
    }

    #[test]
    fn test_table_config_new_with_validation() {
        let mut properties = HashMap::new();
        properties.insert("mysql.host".to_string(), "localhost".to_string());
        properties.insert("mysql.port".to_string(), "3306".to_string());
        properties.insert("extension".to_string(), "mysql".to_string());

        let result = TableTypeConfig::new("mysql".to_string(), properties);
        assert!(result.is_ok());

        let config = result.unwrap();
        assert_eq!(config.extension(), "mysql");
    }

    #[test]
    fn test_table_config_empty_extension_fails() {
        let properties = HashMap::new();
        let result = TableTypeConfig::new("".to_string(), properties);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("extension cannot be empty"));
    }

    #[test]
    fn test_table_config_validate_defensive_check() {
        // This tests the defensive validation in validate()
        let mut properties = HashMap::new();
        properties.insert("type".to_string(), "source".to_string()); // Forbidden

        let result = TableTypeConfig::new("mysql".to_string(), properties);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot have 'type' property"));
    }

    #[test]
    fn test_table_config_get_properties_with_prefix() {
        let mut config = FlatConfig::new();
        config.set("extension", "mysql", PropertySource::SqlWith);
        config.set("mysql.host", "localhost", PropertySource::TomlApplication);
        config.set("mysql.port", "3306", PropertySource::TomlApplication);
        config.set("mysql.database", "eventflux", PropertySource::TomlStream);
        config.set("redis.host", "cache", PropertySource::TomlApplication);

        let table_config = TableTypeConfig::from_flat_config(&config).unwrap();

        let mysql_props = table_config.get_properties_with_prefix("mysql");
        assert_eq!(mysql_props.len(), 3);
        assert_eq!(
            mysql_props.get("mysql.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(mysql_props.get("mysql.port"), Some(&"3306".to_string()));
        assert_eq!(
            mysql_props.get("mysql.database"),
            Some(&"eventflux".to_string())
        );

        let redis_props = table_config.get_properties_with_prefix("redis");
        assert_eq!(redis_props.len(), 1);
    }

    #[test]
    fn test_table_config_complete_configuration_flow() {
        // Simulate multi-layer table configuration merge
        let mut config = FlatConfig::new();

        // Layer 1: Rust defaults
        config.set("cache.ttl", "3600", PropertySource::RustDefault);

        // Layer 2: TOML application config (common database settings)
        config.set(
            "mysql.host",
            "prod-db.company.com",
            PropertySource::TomlApplication,
        );
        config.set("mysql.port", "3306", PropertySource::TomlApplication);
        config.set(
            "mysql.user",
            "eventflux_app",
            PropertySource::TomlApplication,
        );

        // Layer 3: TOML table config (table-specific settings)
        config.set("extension", "mysql", PropertySource::TomlStream);
        config.set("mysql.table", "users", PropertySource::TomlStream);

        // Layer 4: SQL WITH clause (runtime overrides)
        config.set("mysql.database", "production", PropertySource::SqlWith);
        config.set("cache.ttl", "7200", PropertySource::SqlWith); // Override cache TTL

        // Verify final configuration
        let table_config = TableTypeConfig::from_flat_config(&config).unwrap();
        assert_eq!(table_config.extension(), "mysql");
        assert_eq!(
            table_config.properties.get("mysql.host"),
            Some(&"prod-db.company.com".to_string())
        ); // From TOML app
        assert_eq!(
            table_config.properties.get("mysql.port"),
            Some(&"3306".to_string())
        ); // From TOML app
        assert_eq!(
            table_config.properties.get("mysql.table"),
            Some(&"users".to_string())
        ); // From TOML table
        assert_eq!(
            table_config.properties.get("mysql.database"),
            Some(&"production".to_string())
        ); // Overridden by SQL WITH
        assert_eq!(
            table_config.properties.get("cache.ttl"),
            Some(&"7200".to_string())
        ); // Overridden by SQL WITH
    }

    #[test]
    fn test_table_config_redis_backend() {
        let mut config = FlatConfig::new();
        config.set("extension", "redis", PropertySource::SqlWith);
        config.set("redis.host", "localhost", PropertySource::TomlApplication);
        config.set("redis.port", "6379", PropertySource::TomlApplication);
        config.set("redis.key_prefix", "users:", PropertySource::TomlStream);

        let table_config = TableTypeConfig::from_flat_config(&config).unwrap();
        assert_eq!(table_config.extension(), "redis");
        assert_eq!(
            table_config.properties.get("redis.host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(
            table_config.properties.get("redis.port"),
            Some(&"6379".to_string())
        );
        assert_eq!(
            table_config.properties.get("redis.key_prefix"),
            Some(&"users:".to_string())
        );
    }
}
