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

//! # Element Configuration Resolver
//!
//! Works out the final configuration of each EventFlux element in one place. Today that
//! covers streams and tables. It merges the YAML base config with the SQL WITH clause
//! once, before any element is attached, so attachment code only looks up the result.
//!
//! ## Configuration Precedence (highest to lowest)
//!
//! 1. SQL WITH clause. Inline configuration on one element.
//! 2. CLI `--set` overrides. These are already applied to the YAML config.
//! 3. YAML configuration file. Settings for one environment.
//! 4. Code defaults. Built in values.
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

/// Kinds of element that can carry configuration.
///
/// The resolver only produces `Source`, `Sink`, `InternalStream` and `Table` today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementType {
    /// Stream that reads from an external system.
    Source,
    /// Stream that writes to an external system.
    Sink,
    /// Stream that lives only in memory.
    InternalStream,
    /// Table that queries can read and update.
    Table,
    /// Trigger that emits events on a schedule.
    Trigger,
    /// Aggregation that is computed step by step as events arrive.
    Aggregation,
    /// Window that groups events by time or count.
    Window,
    /// User defined function.
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

impl ElementType {
    /// Key of an element's entry in the map that
    /// [`ElementConfigResolver::resolve_all`] returns. The form is `"type:name"`,
    /// for example `"source:Orders"`.
    pub fn config_key(self, name: &str) -> String {
        format!("{self}:{name}")
    }
}

/// Where a configuration property came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Code defaults.
    Default,
    /// YAML configuration file.
    Yaml,
    /// CLI `--set` override.
    CliOverride,
    /// SQL WITH clause.
    SqlWith,
}

/// Final configuration for one element after all sources are merged.
///
/// Element factories and handlers read this directly.
#[derive(Debug, Clone)]
pub struct ResolvedElementConfig {
    /// Kind of element this config belongs to.
    pub element_type: ElementType,

    /// Extension or connector name, for example "rabbitmq", "kafka" or "mysql".
    pub extension: Option<String>,

    /// Data format, for example "json", "avro" or "protobuf".
    pub format: Option<String>,

    /// All merged properties, such as connection details and options.
    pub properties: HashMap<String, String>,

    /// Where the extension value came from. Used for debugging.
    pub extension_source: ConfigSource,

    /// Where the format value came from. Used for debugging.
    pub format_source: ConfigSource,

    /// Where each property came from. Used for debugging and tracing.
    pub property_sources: HashMap<String, ConfigSource>,
}

impl ResolvedElementConfig {
    /// Create an empty config. Both sources start as [`ConfigSource::Default`].
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

    /// Get a property value.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.properties.get(key)
    }

    /// Check that the config has the fields its kind needs.
    ///
    /// Sources, sinks and tables need an extension. Every other kind is always valid.
    pub fn is_valid(&self) -> bool {
        match self.element_type {
            ElementType::Source | ElementType::Sink => self.extension.is_some(),
            ElementType::Table => self.extension.is_some(),
            ElementType::InternalStream => true, // No extension required
            _ => true,                           // Other kinds have no rules yet
        }
    }

    /// Describe where the properties came from, for debug logs.
    ///
    /// CLI overrides count as YAML. Returns "merged (N from YAML, M from SQL WITH)",
    /// "SQL WITH only", "YAML only" or "defaults only".
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

/// Merges the YAML config and SQL WITH clauses into one config per element.
///
/// Build it with an optional YAML [`ApplicationConfig`], then call `resolve_all()`
/// to get the map of final configs.
pub struct ElementConfigResolver<'a> {
    /// YAML application config. CLI `--set` overrides are already applied.
    yaml_config: Option<&'a ApplicationConfig>,
}

impl<'a> ElementConfigResolver<'a> {
    /// Create a resolver. `yaml_config` may be `None` when there is no YAML file.
    pub fn new(yaml_config: Option<&'a ApplicationConfig>) -> Self {
        Self { yaml_config }
    }

    /// Resolve the config of every stream and table.
    ///
    /// For each definition, starts from the YAML config and lays the SQL WITH config
    /// on top. Streams and tables with no config in either place are left out.
    ///
    /// # Arguments
    ///
    /// * `stream_definitions` - Stream definitions from the parsed SQL
    /// * `table_definitions` - Table definitions from the parsed SQL
    ///
    /// # Returns
    ///
    /// A map keyed by "type:name", for example "source:EventInput", holding the final
    /// config of each element.
    pub fn resolve_all(
        &self,
        stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
        table_definitions: &HashMap<String, Arc<TableDefinition>>,
    ) -> HashMap<String, ResolvedElementConfig> {
        let mut resolved = HashMap::new();

        // Resolve all stream definitions
        for (name, stream_def) in stream_definitions {
            if let Some(config) = self.resolve_stream(name, stream_def) {
                let key = config.element_type.config_key(name);
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
                let key = ElementType::Table.config_key(name);
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

    /// Resolve one stream definition.
    ///
    /// Works out whether the stream is a source, sink or internal stream, then merges
    /// its configs. A stream with a WITH clause but no known type becomes an internal
    /// stream. Returns `None` when the stream has no config at all.
    fn resolve_stream(
        &self,
        name: &str,
        stream_def: &StreamDefinition,
    ) -> Option<ResolvedElementConfig> {
        let sql_with = stream_def.with_config.as_ref();

        let element_type = self.determine_stream_type(name, sql_with);

        if element_type.is_none() && sql_with.is_none() && !self.has_yaml_stream_config(name) {
            return None;
        }

        let element_type = element_type.unwrap_or(ElementType::InternalStream);

        let mut config = ResolvedElementConfig::new(element_type);

        // YAML first, then SQL WITH on top so SQL wins on conflicts
        self.apply_yaml_stream_config(name, element_type, &mut config);

        if let Some(with_config) = sql_with {
            self.apply_sql_with_config(with_config, &mut config);
        }

        Some(config)
    }

    /// Resolve one table definition. Returns `None` for an in memory table.
    fn resolve_table(
        &self,
        name: &str,
        table_def: &TableDefinition,
    ) -> Option<ResolvedElementConfig> {
        let sql_with = table_def.with_config.as_ref();

        // No WITH clause and no YAML config means a plain in memory table
        if sql_with.is_none() && !self.has_yaml_table_config(name) {
            return None;
        }

        let mut config = ResolvedElementConfig::new(ElementType::Table);

        self.apply_yaml_table_config(name, &mut config);

        if let Some(with_config) = sql_with {
            self.apply_sql_with_config(with_config, &mut config);
        }

        Some(config)
    }

    /// Work out the stream kind from the SQL WITH `type` key, else from the YAML config.
    ///
    /// An unknown `type` value in SQL returns `None` without looking at YAML. In YAML a
    /// `source` block wins over a `sink` block.
    fn determine_stream_type(
        &self,
        name: &str,
        sql_with: Option<&FlatConfig>,
    ) -> Option<ElementType> {
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

    /// Check if the YAML config has a source or sink block for this stream.
    fn has_yaml_stream_config(&self, name: &str) -> bool {
        self.yaml_config
            .and_then(|c| c.streams.get(name))
            .map(|s| s.source.is_some() || s.sink.is_some())
            .unwrap_or(false)
    }

    /// Check if the YAML `definitions` section has an entry for this table.
    fn has_yaml_table_config(&self, name: &str) -> bool {
        self.yaml_config
            .and_then(|c| c.definitions.get(name))
            .is_some()
    }

    /// Copy the YAML source or sink config of the stream into `config` as the base.
    ///
    /// Does nothing for other element kinds.
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

    /// Copy a YAML source config into `config`.
    ///
    /// Takes every field of [`SourceConfig`], including connection, security,
    /// error_handling and rate_limit. This matches
    /// `merge_source_config_into_properties()` in the YAML only attachment path.
    fn apply_source_config(
        &self,
        source_config: &SourceConfig,
        config: &mut ResolvedElementConfig,
    ) {
        config.extension = Some(source_config.source_type.clone());
        config.extension_source = ConfigSource::Yaml;

        if let Some(ref fmt) = source_config.format {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::Yaml;
        }

        if let Some(props) = self.extract_yaml_connection(&source_config.connection) {
            for (key, value) in props {
                config.properties.insert(key.clone(), value);
                config.property_sources.insert(key, ConfigSource::Yaml);
            }
        }

        // Serialize the struct to pick up every other field (security, error_handling,
        // rate_limit), the same way merge_source_config_into_properties() does
        if let Ok(serde_yaml::Value::Mapping(mut mapping)) = serde_yaml::to_value(source_config) {
            // Already handled above
            mapping.remove(serde_yaml::Value::String("type".to_string()));
            mapping.remove(serde_yaml::Value::String("format".to_string()));
            mapping.remove(serde_yaml::Value::String("connection".to_string()));

            self.flatten_yaml_into_properties(&serde_yaml::Value::Mapping(mapping), "", config);
        }
    }

    /// Copy a YAML sink config into `config`.
    ///
    /// Takes every field of [`SinkConfig`], including connection, security,
    /// delivery_guarantee, retry and batching. This matches
    /// `merge_sink_config_into_properties()` in the YAML only attachment path.
    fn apply_sink_config(&self, sink_config: &SinkConfig, config: &mut ResolvedElementConfig) {
        config.extension = Some(sink_config.sink_type.clone());
        config.extension_source = ConfigSource::Yaml;

        if let Some(ref fmt) = sink_config.format {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::Yaml;
        }

        if let Some(props) = self.extract_yaml_connection(&sink_config.connection) {
            for (key, value) in props {
                config.properties.insert(key.clone(), value);
                config.property_sources.insert(key, ConfigSource::Yaml);
            }
        }

        // Serialize the struct to pick up every other field (security,
        // delivery_guarantee, retry, batching), the same way
        // merge_sink_config_into_properties() does
        if let Ok(serde_yaml::Value::Mapping(mut mapping)) = serde_yaml::to_value(sink_config) {
            // Already handled above
            mapping.remove(serde_yaml::Value::String("type".to_string()));
            mapping.remove(serde_yaml::Value::String("format".to_string()));
            mapping.remove(serde_yaml::Value::String("connection".to_string()));

            self.flatten_yaml_into_properties(&serde_yaml::Value::Mapping(mapping), "", config);
        }
    }

    /// Copy the YAML table config into `config`. Does nothing yet.
    fn apply_yaml_table_config(&self, name: &str, _config: &mut ResolvedElementConfig) {
        let Some(app_config) = self.yaml_config else {
            return;
        };
        let Some(_def_config) = app_config.definitions.get(name) else {
            return;
        };

        // TODO: copy table settings from YAML once DefinitionConfig carries them.
        // Tables are configured through SQL WITH for now.
    }

    /// Lay the SQL WITH config over `config`. SQL values replace YAML values.
    ///
    /// `extension` and `format` set the matching fields and are not copied into
    /// `properties`. `type` is skipped because it only picks the element kind.
    fn apply_sql_with_config(&self, with_config: &FlatConfig, config: &mut ResolvedElementConfig) {
        if let Some(ext) = with_config.get("extension") {
            config.extension = Some(ext.clone());
            config.extension_source = ConfigSource::SqlWith;
        }

        if let Some(fmt) = with_config.get("format") {
            config.format = Some(fmt.clone());
            config.format_source = ConfigSource::SqlWith;
        }

        for (key, value) in with_config.properties() {
            if key == "type" || key == "extension" || key == "format" {
                continue;
            }
            config.properties.insert(key.clone(), value.clone());
            config
                .property_sources
                .insert(key.clone(), ConfigSource::SqlWith);
        }
    }

    /// Turn a YAML `connection` block into string properties.
    ///
    /// Sequences and nested mappings become JSON strings, matching
    /// `extract_connection_config()` in EventFluxAppRuntime. Null values become empty
    /// strings. A tagged value keeps its inner scalar and is skipped otherwise.
    ///
    /// Returns an empty map for a null block. Returns `None` if the block is not a
    /// mapping or has a key that is not a string.
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
                    // Factories expect arrays as JSON text, same as
                    // extract_connection_config()
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
                serde_yaml::Value::Mapping(_) => match serde_json::to_string(value) {
                    Ok(json) => json,
                    Err(e) => {
                        log::warn!(
                            "Failed to serialize mapping value for key '{}': {}",
                            key_str,
                            e
                        );
                        continue;
                    }
                },
                serde_yaml::Value::Tagged(tagged) => match &tagged.value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    _ => continue,
                },
            };
            props.insert(key_str.to_string(), value_str);
        }

        Some(props)
    }

    /// Flatten a YAML value into `config.properties`, marking each key as YAML sourced.
    ///
    /// Nested mappings become dotted keys such as `security.username`. This matches
    /// `flatten_yaml_value()` in EventFluxAppRuntime. Sequences of scalars are joined
    /// with commas. Nulls, tagged values and scalars with no key are skipped.
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
                // Nested items inside the sequence are dropped
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

/// Builder for [`ResolvedElementConfig`].
///
/// `build()` fails if no element type was set. Every source field is set to
/// [`ConfigSource::Default`].
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

        // Nested YAML shaped like the security and retry config
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
