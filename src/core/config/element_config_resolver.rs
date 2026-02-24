// SPDX-License-Identifier: MIT OR Apache-2.0

//! # Element Configuration Resolver
//!
//! Centralizes configuration resolution for all EventFlux elements (streams, tables,
//! triggers, etc.). This module implements pre-resolution of configurations by merging
//! YAML base configs with SQL WITH clause overrides before any element attachment.
//!
//! ## Configuration Precedence (highest to lowest)
//!
//! 1. SQL WITH clause - per-element inline configuration
//! 2. CLI `--set` overrides - already applied to YAML config
//! 3. YAML configuration file - environment-specific settings
//! 4. Code defaults - built-in sensible defaults
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    Pre-Resolution Flow                               │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                      │
//! │   EventFluxAppRuntime::new()                                        │
//! │      │                                                               │
//! │      └──► ElementConfigResolver::resolve_all()                      │
//! │              │                                                       │
//! │              ├── For each StreamDefinition:                         │
//! │              │      merge(YAML.streams[name], SQL.with_config)      │
//! │              │      → resolved_configs["stream:name"]               │
//! │              │                                                       │
//! │              ├── For each TableDefinition:                          │
//! │              │      merge(YAML.definitions[name], SQL.with_config)  │
//! │              │      → resolved_configs["table:name"]                │
//! │              │                                                       │
//! │              └── [Future: Triggers, Aggregations, Windows, etc.]   │
//! │                                                                      │
//! │   start()                                                           │
//! │      │                                                               │
//! │      └──► Attachment methods just use resolved_configs[key]         │
//! │                                                                      │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use eventflux::core::config::element_config_resolver::ElementConfigResolver;
//!
//! let resolver = ElementConfigResolver::new(yaml_config.as_ref());
//! let resolved = resolver.resolve_all(
//!     &app.stream_definition_map,
//!     &app.table_definition_map,
//! );
//!
//! // Later, in attachment code:
//! if let Some(config) = resolved.get("stream:EventInput") {
//!     // Use pre-merged config directly
//! }
//! ```

use crate::core::config::stream_config::FlatConfig;
use crate::core::config::types::application_config::{ApplicationConfig, SinkConfig, SourceConfig};
use crate::query_api::definition::stream_definition::StreamDefinition;
use crate::query_api::definition::table_definition::TableDefinition;
use std::collections::HashMap;
use std::sync::Arc;

/// Element types that support configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementType {
    /// Source stream (reads from external system)
    Source,
    /// Sink stream (writes to external system)
    Sink,
    /// Internal stream (in-memory only)
    InternalStream,
    /// Table (queryable state store)
    Table,
    /// Trigger (time-based event generation)
    Trigger,
    /// Aggregation (incremental computation)
    Aggregation,
    /// Window (time/length-based grouping)
    Window,
    /// Function (user-defined computation)
    Function,
}

impl std::fmt::Display for ElementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElementType::Source => write!(f, "source"),
            ElementType::Sink => write!(f, "sink"),
            ElementType::InternalStream => write!(f, "internal"),
            ElementType::Table => write!(f, "table"),
            ElementType::Trigger => write!(f, "trigger"),
            ElementType::Aggregation => write!(f, "aggregation"),
            ElementType::Window => write!(f, "window"),
            ElementType::Function => write!(f, "function"),
        }
    }
}

/// Tracks where a configuration property originated from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Property came from code defaults
    Default,
    /// Property came from YAML configuration file
    Yaml,
    /// Property came from CLI --set override
    CliOverride,
    /// Property came from SQL WITH clause
    SqlWith,
}

/// Fully resolved configuration for an element
///
/// This struct holds the merged configuration from all sources,
/// ready for use by element factories and handlers.
#[derive(Debug, Clone)]
pub struct ResolvedElementConfig {
    /// Type of element this config is for
    pub element_type: ElementType,

    /// Extension/connector name (e.g., "rabbitmq", "kafka", "mysql")
    pub extension: Option<String>,

    /// Data format (e.g., "json", "avro", "protobuf")
    pub format: Option<String>,

    /// All merged properties (connection details, options, etc.)
    pub properties: HashMap<String, String>,

    /// Track where the extension value came from (for debugging)
    pub extension_source: ConfigSource,

    /// Track where the format value came from (for debugging)
    pub format_source: ConfigSource,

    /// Track where each property came from (for debugging/tracing)
    pub property_sources: HashMap<String, ConfigSource>,
}

impl ResolvedElementConfig {
    /// Create a new empty resolved config
    pub fn new(element_type: ElementType) -> Self {
        Self {
            element_type,
            extension: None,
            format: None,
            properties: HashMap::new(),
            extension_source: ConfigSource::Default,
            format_source: ConfigSource::Default,
            property_sources: HashMap::new(),
        }
    }

    /// Get a property value
    pub fn get(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    /// Check if this config has the minimum required fields for its type
    pub fn is_valid(&self) -> bool {
        match self.element_type {
            ElementType::Source | ElementType::Sink => self.extension.is_some(),
            ElementType::Table => self.extension.is_some(),
            ElementType::InternalStream => true, // No extension required
            _ => true,                           // Future types may have different requirements
        }
    }

    /// Get a summary of where properties came from (for debugging)
    pub fn get_source_summary(&self) -> String {
        let mut yaml_count = 0;
        let mut sql_count = 0;

        for source in self.property_sources.values() {
            match source {
                ConfigSource::Yaml | ConfigSource::CliOverride => yaml_count += 1,
                ConfigSource::SqlWith => sql_count += 1,
                ConfigSource::Default => {}
            }
        }

        if yaml_count > 0 && sql_count > 0 {
            format!(
                "merged ({} from YAML, {} from SQL WITH)",
                yaml_count, sql_count
            )
        } else if sql_count > 0 {
            "SQL WITH only".to_string()
        } else if yaml_count > 0 {
            "YAML only".to_string()
        } else {
            "defaults only".to_string()
        }
    }
}

/// Resolves element configurations by merging YAML and SQL WITH sources
///
/// The resolver is created with an optional YAML ApplicationConfig reference,
/// then `resolve_all()` is called to produce a map of fully resolved configs.
pub struct ElementConfigResolver<'a> {
    /// YAML application configuration (already includes CLI --set overrides)
    yaml_config: Option<&'a ApplicationConfig>,
}

impl<'a> ElementConfigResolver<'a> {
    /// Create a new resolver with optional YAML config
    pub fn new(yaml_config: Option<&'a ApplicationConfig>) -> Self {
        Self { yaml_config }
    }

    /// Resolve all element configurations
    ///
    /// Iterates through all stream and table definitions, merging their
    /// SQL WITH configs with YAML base configs to produce fully resolved
    /// configurations ready for use.
    ///
    /// # Arguments
    ///
    /// * `stream_definitions` - Map of stream definitions from parsed SQL
    /// * `table_definitions` - Map of table definitions from parsed SQL
    ///
    /// # Returns
    ///
    /// HashMap where keys are in format "type:name" (e.g., "source:EventInput")
    /// and values are the fully resolved configurations.
    pub fn resolve_all(
        &self,
        stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
        table_definitions: &HashMap<String, Arc<TableDefinition>>,
    ) -> HashMap<String, ResolvedElementConfig> {
        let mut resolved = HashMap::new();

        // Resolve all stream definitions
        for (name, stream_def) in stream_definitions {
            if let Some(config) = self.resolve_stream(name, stream_def) {
                let key = format!("{}:{}", config.element_type, name);
                log::debug!(
                    "[ElementConfigResolver] Resolved {} '{}': {}",
                    config.element_type,
                    name,
                    config.get_source_summary()
                );
                resolved.insert(key, config);
            }
        }

        // Resolve all table definitions
        for (name, table_def) in table_definitions {
            if let Some(config) = self.resolve_table(name, table_def) {
                let key = format!("table:{}", name);
                log::debug!(
                    "[ElementConfigResolver] Resolved table '{}': {}",
                    name,
                    config.get_source_summary()
                );
                resolved.insert(key, config);
            }
        }

        log::info!(
            "[ElementConfigResolver] Resolved {} element configurations",
            resolved.len()
        );

        resolved
    }

    /// Resolve a single stream definition
    ///
    /// Determines stream type (source/sink/internal) and merges configs accordingly.
    fn resolve_stream(
        &self,
        name: &str,
        stream_def: &StreamDefinition,
    ) -> Option<ResolvedElementConfig> {
        // Get SQL WITH config if present
        let sql_with = stream_def.with_config.as_ref();

        // Determine element type from SQL WITH or YAML
        let element_type = self.determine_stream_type(name, sql_with);

        // If no type can be determined and no configs exist, skip
        if element_type.is_none() && sql_with.is_none() && !self.has_yaml_stream_config(name) {
            return None;
        }

        let element_type = element_type.unwrap_or(ElementType::InternalStream);

        // Create resolved config
        let mut config = ResolvedElementConfig::new(element_type);

        // First, apply YAML base properties
        self.apply_yaml_stream_config(name, element_type, &mut config);

        // Then, overlay SQL WITH properties (SQL wins for conflicts)
        if let Some(with_config) = sql_with {
            self.apply_sql_with_config(with_config, &mut config);
        }

        Some(config)
    }

    /// Resolve a single table definition
    fn resolve_table(
        &self,
        name: &str,
        table_def: &TableDefinition,
    ) -> Option<ResolvedElementConfig> {
        let sql_with = table_def.with_config.as_ref();

        // Tables without WITH clause and without YAML config are in-memory tables
        if sql_with.is_none() && !self.has_yaml_table_config(name) {
            return None;
        }

        let mut config = ResolvedElementConfig::new(ElementType::Table);

        // Apply YAML base properties for table
        self.apply_yaml_table_config(name, &mut config);

        // Overlay SQL WITH properties
        if let Some(with_config) = sql_with {
            self.apply_sql_with_config(with_config, &mut config);
        }

        Some(config)
    }

    /// Determine stream type from SQL WITH or YAML config
    fn determine_stream_type(
        &self,
        name: &str,
        sql_with: Option<&FlatConfig>,
    ) -> Option<ElementType> {
        // SQL WITH takes precedence
        if let Some(with_config) = sql_with {
            if let Some(type_str) = with_config.get("type") {
                return match type_str.as_str() {
                    "source" => Some(ElementType::Source),
                    "sink" => Some(ElementType::Sink),
                    "internal" => Some(ElementType::InternalStream),
                    _ => None,
                };
            }
        }

        // Fall back to YAML config
        if let Some(app_config) = self.yaml_config {
            if let Some(stream_config) = app_config.streams.get(name) {
                if stream_config.source.is_some() {
                    return Some(ElementType::Source);
                }
                if stream_config.sink.is_some() {
                    return Some(ElementType::Sink);
                }
            }
        }

        None
    }

    /// Check if YAML has config for this stream
    fn has_yaml_stream_config(&self, name: &str) -> bool {
        self.yaml_config
            .and_then(|c| c.streams.get(name))
            .map(|s| s.source.is_some() || s.sink.is_some())
            .unwrap_or(false)
    }

    /// Check if YAML has config for this table
    fn has_yaml_table_config(&self, name: &str) -> bool {
        self.yaml_config
            .and_then(|c| c.definitions.get(name))
            .is_some()
    }

    /// Apply YAML stream configuration as base
    fn apply_yaml_stream_config(
        &self,
        name: &str,
        element_type: ElementType,
        config: &mut ResolvedElementConfig,
    ) {
        let Some(app_config) = self.yaml_config else {
            return;
        };
        let Some(stream_config) = app_config.streams.get(name) else {
            return;
        };

        match element_type {
            ElementType::Source => {
                if let Some(ref source_config) = stream_config.source {
                    self.apply_source_config(source_config, config);
                }
            }
            ElementType::Sink => {
                if let Some(ref sink_config) = stream_config.sink {
                    self.apply_sink_config(sink_config, config);
                }
            }
            _ => {}
        }
    }

    /// Apply YAML source config properties
    ///
    /// Extracts ALL fields from SourceConfig including connection, security,
    /// error_handling, and rate_limit. This matches the behavior of
    /// merge_source_config_into_properties() in the YAML-only attachment path.
    fn apply_source_config(
        &self,
        source_config: &SourceConfig,
        config: &mut ResolvedElementConfig,
    ) {
        // Set extension (source_type)
        config.extension = Some(source_config.source_type.clone());
        config.extension_source = ConfigSource::Yaml;

        // Set format
        if let Some(ref fmt) = source_config.format {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::Yaml;
        }

        // Extract connection properties
        if let Some(props) = self.extract_yaml_connection(&source_config.connection) {
            for (key, value) in props {
                config.properties.insert(key.clone(), value);
                config.property_sources.insert(key, ConfigSource::Yaml);
            }
        }

        // Extract ALL remaining fields (security, error_handling, rate_limit) via serialization
        // This matches the behavior of merge_source_config_into_properties()
        if let Ok(serde_yaml::Value::Mapping(mut mapping)) = serde_yaml::to_value(source_config) {
            // Remove fields already extracted to avoid duplication
            mapping.remove(serde_yaml::Value::String("type".to_string()));
            mapping.remove(serde_yaml::Value::String("format".to_string()));
            mapping.remove(serde_yaml::Value::String("connection".to_string()));

            // Flatten remaining fields into properties
            self.flatten_yaml_into_properties(&serde_yaml::Value::Mapping(mapping), "", config);
        }
    }

    /// Apply YAML sink config properties
    ///
    /// Extracts ALL fields from SinkConfig including connection, security,
    /// delivery_guarantee, retry, and batching. This matches the behavior of
    /// merge_sink_config_into_properties() in the YAML-only attachment path.
    fn apply_sink_config(&self, sink_config: &SinkConfig, config: &mut ResolvedElementConfig) {
        // Set extension (sink_type)
        config.extension = Some(sink_config.sink_type.clone());
        config.extension_source = ConfigSource::Yaml;

        // Set format
        if let Some(ref fmt) = sink_config.format {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::Yaml;
        }

        // Extract connection properties
        if let Some(props) = self.extract_yaml_connection(&sink_config.connection) {
            for (key, value) in props {
                config.properties.insert(key.clone(), value);
                config.property_sources.insert(key, ConfigSource::Yaml);
            }
        }

        // Extract ALL remaining fields (security, delivery_guarantee, retry, batching) via serialization
        // This matches the behavior of merge_sink_config_into_properties()
        if let Ok(serde_yaml::Value::Mapping(mut mapping)) = serde_yaml::to_value(sink_config) {
            // Remove fields already extracted to avoid duplication
            mapping.remove(serde_yaml::Value::String("type".to_string()));
            mapping.remove(serde_yaml::Value::String("format".to_string()));
            mapping.remove(serde_yaml::Value::String("connection".to_string()));

            // Flatten remaining fields into properties
            self.flatten_yaml_into_properties(&serde_yaml::Value::Mapping(mapping), "", config);
        }
    }

    /// Apply YAML table configuration as base
    fn apply_yaml_table_config(&self, name: &str, _config: &mut ResolvedElementConfig) {
        let Some(app_config) = self.yaml_config else {
            return;
        };
        let Some(_def_config) = app_config.definitions.get(name) else {
            return;
        };

        // TODO: Extract table-specific YAML config when DefinitionConfig is enhanced
        // Currently tables are mainly configured via SQL WITH
    }

    /// Apply SQL WITH config properties (overrides YAML)
    fn apply_sql_with_config(&self, with_config: &FlatConfig, config: &mut ResolvedElementConfig) {
        // Override extension if specified in SQL
        if let Some(ext) = with_config.get("extension") {
            config.extension = Some(ext.clone());
            config.extension_source = ConfigSource::SqlWith;
        }

        // Override format if specified in SQL
        if let Some(fmt) = with_config.get("format") {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::SqlWith;
        }

        // Apply all properties (SQL wins for conflicts)
        for (key, value) in with_config.properties() {
            // Skip meta-properties that are handled separately
            if key == "type" || key == "extension" || key == "format" {
                continue;
            }
            config.properties.insert(key.clone(), value.clone());
            config
                .property_sources
                .insert(key.clone(), ConfigSource::SqlWith);
        }
    }

    /// Extract connection properties from YAML Value
    ///
    /// Handles all value types including sequences (arrays), which are serialized
    /// as JSON strings to match the behavior of extract_connection_config() in
    /// EventFluxAppRuntime.
    fn extract_yaml_connection(
        &self,
        connection: &serde_yaml::Value,
    ) -> Option<HashMap<String, String>> {
        let map = match connection {
            serde_yaml::Value::Mapping(m) => m,
            serde_yaml::Value::Null => return Some(HashMap::new()),
            _ => return None,
        };

        let mut props = HashMap::new();

        for (key, value) in map {
            let key_str = key.as_str()?;
            let value_str = match value {
                serde_yaml::Value::String(s) => s.clone(),
                serde_yaml::Value::Number(n) => n.to_string(),
                serde_yaml::Value::Bool(b) => b.to_string(),
                serde_yaml::Value::Null => String::new(),
                serde_yaml::Value::Sequence(_) => {
                    // Serialize arrays as JSON string for factory consumption
                    // This matches extract_connection_config() behavior
                    match serde_json::to_string(value) {
                        Ok(json) => json,
                        Err(e) => {
                            log::warn!(
                                "Failed to serialize array value for key '{}': {}",
                                key_str,
                                e
                            );
                            continue;
                        }
                    }
                }
                serde_yaml::Value::Mapping(_) => {
                    // Serialize nested mappings as JSON string
                    match serde_json::to_string(value) {
                        Ok(json) => json,
                        Err(e) => {
                            log::warn!(
                                "Failed to serialize mapping value for key '{}': {}",
                                key_str,
                                e
                            );
                            continue;
                        }
                    }
                }
                serde_yaml::Value::Tagged(tagged) => {
                    // Handle tagged values by extracting the inner value
                    match &tagged.value {
                        serde_yaml::Value::String(s) => s.clone(),
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::Bool(b) => b.to_string(),
                        _ => continue,
                    }
                }
            };
            props.insert(key_str.to_string(), value_str);
        }

        Some(props)
    }

    /// Flatten a YAML value into properties with source tracking
    ///
    /// Recursively flattens nested YAML structures into dot-notation keys.
    /// This matches the behavior of flatten_yaml_value() in EventFluxAppRuntime.
    #[allow(clippy::only_used_in_recursion)]
    fn flatten_yaml_into_properties(
        &self,
        value: &serde_yaml::Value,
        prefix: &str,
        config: &mut ResolvedElementConfig,
    ) {
        match value {
            serde_yaml::Value::Mapping(map) => {
                for (key, val) in map {
                    if let Some(key_str) = key.as_str() {
                        let new_prefix = if prefix.is_empty() {
                            key_str.to_string()
                        } else {
                            format!("{}.{}", prefix, key_str)
                        };
                        self.flatten_yaml_into_properties(val, &new_prefix, config);
                    }
                }
            }
            serde_yaml::Value::String(s) => {
                if !prefix.is_empty() {
                    config.properties.insert(prefix.to_string(), s.clone());
                    config
                        .property_sources
                        .insert(prefix.to_string(), ConfigSource::Yaml);
                }
            }
            serde_yaml::Value::Number(n) => {
                if !prefix.is_empty() {
                    config.properties.insert(prefix.to_string(), n.to_string());
                    config
                        .property_sources
                        .insert(prefix.to_string(), ConfigSource::Yaml);
                }
            }
            serde_yaml::Value::Bool(b) => {
                if !prefix.is_empty() {
                    config.properties.insert(prefix.to_string(), b.to_string());
                    config
                        .property_sources
                        .insert(prefix.to_string(), ConfigSource::Yaml);
                }
            }
            serde_yaml::Value::Sequence(seq) => {
                // For sequences, join elements with comma
                if !prefix.is_empty() {
                    let values: Vec<String> = seq
                        .iter()
                        .filter_map(|v| match v {
                            serde_yaml::Value::String(s) => Some(s.clone()),
                            serde_yaml::Value::Number(n) => Some(n.to_string()),
                            serde_yaml::Value::Bool(b) => Some(b.to_string()),
                            _ => None,
                        })
                        .collect();
                    if !values.is_empty() {
                        config
                            .properties
                            .insert(prefix.to_string(), values.join(","));
                        config
                            .property_sources
                            .insert(prefix.to_string(), ConfigSource::Yaml);
                    }
                }
            }
            serde_yaml::Value::Null => {
                // Skip null values
            }
            serde_yaml::Value::Tagged(_) => {
                // Skip tagged values
            }
        }
    }
}

/// Builder for creating ResolvedElementConfig instances
#[derive(Debug, Default)]
pub struct ResolvedElementConfigBuilder {
    element_type: Option<ElementType>,
    extension: Option<String>,
    format: Option<String>,
    properties: HashMap<String, String>,
}

impl ResolvedElementConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn element_type(mut self, t: ElementType) -> Self {
        self.element_type = Some(t);
        self
    }

    pub fn extension(mut self, ext: impl Into<String>) -> Self {
        self.extension = Some(ext.into());
        self
    }

    pub fn format(mut self, fmt: impl Into<String>) -> Self {
        self.format = Some(fmt.into());
        self
    }

    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn properties(mut self, props: HashMap<String, String>) -> Self {
        self.properties.extend(props);
        self
    }

    pub fn build(self) -> Result<ResolvedElementConfig, String> {
        let element_type = self
            .element_type
            .ok_or_else(|| "element_type is required".to_string())?;

        Ok(ResolvedElementConfig {
            element_type,
            extension: self.extension,
            format: self.format,
            properties: self.properties,
            extension_source: ConfigSource::Default,
            format_source: ConfigSource::Default,
            property_sources: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::stream_config::FlatConfig;

    #[test]
    fn test_resolved_element_config_new() {
        let config = ResolvedElementConfig::new(ElementType::Source);
        assert_eq!(config.element_type, ElementType::Source);
        assert!(config.extension.is_none());
        assert!(config.properties.is_empty());
    }

    #[test]
    fn test_resolved_element_config_validity() {
        let mut config = ResolvedElementConfig::new(ElementType::Source);
        assert!(!config.is_valid()); // No extension

        config.extension = Some("rabbitmq".to_string());
        assert!(config.is_valid());

        let internal = ResolvedElementConfig::new(ElementType::InternalStream);
        assert!(internal.is_valid()); // Internal streams don't need extension
    }

    #[test]
    fn test_element_config_resolver_no_configs() {
        let resolver = ElementConfigResolver::new(None);
        let streams: HashMap<String, Arc<StreamDefinition>> = HashMap::new();
        let tables: HashMap<String, Arc<TableDefinition>> = HashMap::new();

        let resolved = resolver.resolve_all(&streams, &tables);
        assert!(resolved.is_empty());
    }

    #[test]
    fn test_element_config_resolver_sql_only() {
        use crate::core::config::PropertySource;

        let resolver = ElementConfigResolver::new(None);

        // Create stream with SQL WITH config
        let mut stream_def = StreamDefinition::new("EventInput".to_string());
        let mut with_config = FlatConfig::new();
        with_config.set("type", "source", PropertySource::SqlWith);
        with_config.set("extension", "rabbitmq", PropertySource::SqlWith);
        with_config.set("format", "json", PropertySource::SqlWith);
        with_config.set("rabbitmq.host", "localhost", PropertySource::SqlWith);
        with_config.set("rabbitmq.port", "5672", PropertySource::SqlWith);
        stream_def.with_config = Some(with_config);

        let mut streams = HashMap::new();
        streams.insert("EventInput".to_string(), Arc::new(stream_def));
        let tables = HashMap::new();

        let resolved = resolver.resolve_all(&streams, &tables);

        assert_eq!(resolved.len(), 1);
        let config = resolved.get("source:EventInput").unwrap();
        assert_eq!(config.element_type, ElementType::Source);
        assert_eq!(config.extension, Some("rabbitmq".to_string()));
        assert_eq!(config.format, Some("json".to_string()));
        assert_eq!(config.get("rabbitmq.host"), Some(&"localhost".to_string()));
        assert_eq!(config.get("rabbitmq.port"), Some(&"5672".to_string()));
    }

    #[test]
    fn test_source_summary() {
        let mut config = ResolvedElementConfig::new(ElementType::Source);
        assert_eq!(config.get_source_summary(), "defaults only");

        config
            .property_sources
            .insert("host".to_string(), ConfigSource::Yaml);
        assert_eq!(config.get_source_summary(), "YAML only");

        config
            .property_sources
            .insert("queue".to_string(), ConfigSource::SqlWith);
        assert!(config.get_source_summary().contains("merged"));
    }

    #[test]
    fn test_element_type_display() {
        assert_eq!(format!("{}", ElementType::Source), "source");
        assert_eq!(format!("{}", ElementType::Sink), "sink");
        assert_eq!(format!("{}", ElementType::Table), "table");
    }

    #[test]
    fn test_builder() {
        let config = ResolvedElementConfigBuilder::new()
            .element_type(ElementType::Source)
            .extension("kafka")
            .format("avro")
            .property("bootstrap.servers", "localhost:9092")
            .build()
            .unwrap();

        assert_eq!(config.element_type, ElementType::Source);
        assert_eq!(config.extension, Some("kafka".to_string()));
        assert_eq!(config.format, Some("avro".to_string()));
        assert_eq!(
            config.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
    }

    #[test]
    fn test_builder_missing_type() {
        let result = ResolvedElementConfigBuilder::new()
            .extension("kafka")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_flatten_yaml_into_properties() {
        let resolver = ElementConfigResolver::new(None);
        let mut config = ResolvedElementConfig::new(ElementType::Source);

        // Create a nested YAML structure similar to security/retry config
        let yaml_value = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
            security:
              tls:
                enabled: true
                cert_path: /path/to/cert
            retry:
              max_retries: 3
              delay_ms: 1000
            "#,
        )
        .unwrap();

        resolver.flatten_yaml_into_properties(&yaml_value, "", &mut config);

        // Verify nested properties are flattened with dot notation
        assert_eq!(
            config.get("security.tls.enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.get("security.tls.cert_path"),
            Some(&"/path/to/cert".to_string())
        );
        assert_eq!(config.get("retry.max_retries"), Some(&"3".to_string()));
        assert_eq!(config.get("retry.delay_ms"), Some(&"1000".to_string()));

        // Verify source tracking
        assert_eq!(
            config.property_sources.get("security.tls.enabled"),
            Some(&ConfigSource::Yaml)
        );
    }

    #[test]
    fn test_extract_yaml_connection_with_arrays() {
        let resolver = ElementConfigResolver::new(None);

        // Create a connection config with array values (like bootstrap servers)
        let connection = serde_yaml::from_str::<serde_yaml::Value>(
            r#"
            host: localhost
            port: 9092
            bootstrap_servers:
              - kafka1:9092
              - kafka2:9092
              - kafka3:9092
            topics:
              - events
              - logs
            "#,
        )
        .unwrap();

        let props = resolver.extract_yaml_connection(&connection).unwrap();

        // Verify scalar values
        assert_eq!(props.get("host"), Some(&"localhost".to_string()));
        assert_eq!(props.get("port"), Some(&"9092".to_string()));

        // Verify arrays are serialized as JSON strings
        let bootstrap_servers = props.get("bootstrap_servers").unwrap();
        assert!(bootstrap_servers.contains("kafka1:9092"));
        assert!(bootstrap_servers.contains("kafka2:9092"));
        assert!(bootstrap_servers.contains("kafka3:9092"));

        let topics = props.get("topics").unwrap();
        assert!(topics.contains("events"));
        assert!(topics.contains("logs"));
    }
}
