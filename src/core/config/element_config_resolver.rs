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
//! │              │      → resolved_configs["source:name"],              │
//! │              │        ["sink:name"] or ["internal:name"]            │
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
//! )?;
//!
//! // Later, in attachment code:
//! if let Some(config) = resolved.get("source:EventInput") {
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
    /// config of each element. Errors with every problem found, joined by "; ", if
    /// a stream's config is invalid. See [`Self::resolve_stream`].
    pub fn resolve_all(
        &self,
        stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
        table_definitions: &HashMap<String, Arc<TableDefinition>>,
    ) -> Result<HashMap<String, ResolvedElementConfig>, String> {
        let mut resolved = HashMap::new();
        let mut errors = Vec::new();

        for (name, stream_def) in stream_definitions {
            let configs = match self.resolve_stream(name, stream_def) {
                Ok(configs) => configs,
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            };
            for config in configs {
                log::debug!(
                    "[ElementConfigResolver] Resolved {} '{}': {}",
                    config.element_type,
                    name,
                    config.get_source_summary()
                );
                resolved.insert(config.element_type.config_key(name), config);
            }
        }

        if !errors.is_empty() {
            return Err(errors.join("; "));
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

        Ok(resolved)
    }

    /// Resolve the configs of one stream.
    ///
    /// When SQL WITH sets `type`, it decides the kind. `source` and `sink` take the
    /// matching YAML block as the base, and any other value gives an internal stream
    /// with no YAML base. Attaching reports an unknown type later. Without a SQL
    /// type, each YAML `source` and `sink` block gives its own config, so one stream
    /// can have both. A stream with only a WITH clause becomes an internal stream.
    /// The SQL WITH values are laid over the YAML base in every case, so SQL wins.
    ///
    /// Returns no configs when the stream has no config at all. Errors if a YAML
    /// block cannot be flattened, or if YAML gives the stream a source or sink that
    /// its SQL type rules out, since one of the two would otherwise be ignored.
    fn resolve_stream(
        &self,
        name: &str,
        stream_def: &StreamDefinition,
    ) -> Result<Vec<ResolvedElementConfig>, String> {
        let sql_with = stream_def.with_config.as_ref();
        let yaml = self.yaml_config.and_then(|c| c.streams.get(name));
        let yaml_source = yaml.and_then(|s| s.source.as_ref());
        let yaml_sink = yaml.and_then(|s| s.sink.as_ref());

        let source_base = || {
            yaml_source
                .map(|source| Self::source_base(name, source))
                .transpose()
        };
        let sink_base = || {
            yaml_sink
                .map(|sink| Self::sink_base(name, sink))
                .transpose()
        };

        let mut configs = Vec::new();
        match sql_with.and_then(|w| w.get("type")).map(String::as_str) {
            Some(sql_type) => {
                let element_type = match sql_type {
                    "source" => ElementType::Source,
                    "sink" => ElementType::Sink,
                    _ => ElementType::InternalStream,
                };
                let yaml_blocks = [
                    (ElementType::Source, yaml_source.is_some()),
                    (ElementType::Sink, yaml_sink.is_some()),
                ];
                for (kind, present) in yaml_blocks {
                    if present && kind != element_type {
                        return Err(format!(
                            "Stream '{}' is declared type = '{}' in SQL WITH, but the YAML \
                             config gives it a {}",
                            name, sql_type, kind
                        ));
                    }
                }
                let base = match element_type {
                    ElementType::Source => source_base()?,
                    ElementType::Sink => sink_base()?,
                    _ => None,
                };
                configs.push(base.unwrap_or_else(|| ResolvedElementConfig::new(element_type)));
            }
            None => {
                configs.extend(source_base()?);
                configs.extend(sink_base()?);
                if configs.is_empty() && sql_with.is_some() {
                    configs.push(ResolvedElementConfig::new(ElementType::InternalStream));
                }
            }
        }

        if let Some(with_config) = sql_with {
            for config in &mut configs {
                Self::apply_sql_with_config(with_config, config);
            }
        }

        for config in &configs {
            if config.element_type == ElementType::Source {
                Self::check_source_error_config(name, config)?;
            }
        }

        Ok(configs)
    }

    /// Parse a source's `error.*` keys with the runtime parser, once YAML and SQL
    /// `WITH` are merged. A bad value then fails app creation instead of failing
    /// later, when the source is built at start. Keys the settings never read are
    /// logged as a warning (see [`Self::unused_error_keys_warning`]).
    ///
    /// Errors, naming the stream, if the keys fail to parse or do not work
    /// together. Nothing is parsed when `error.strategy` is not set, because the
    /// source then has no error handling either.
    fn check_source_error_config(name: &str, config: &ResolvedElementConfig) -> Result<(), String> {
        let error_config =
            crate::core::error::ErrorConfigBuilder::from_properties(&config.properties)
                .build()
                .map_err(|e| {
                    format!("Invalid error handling for source stream '{}': {}", name, e)
                })?;
        if let Some(warning) =
            Self::unused_error_keys_warning(name, error_config.as_ref(), &config.properties)
        {
            log::warn!("{}", warning);
        }
        Ok(())
    }

    /// Returns a warning naming the `error.*` keys of source stream `name` that
    /// the parsed `error_config` never reads, or `None` when every key is read.
    ///
    /// These keys are not an error. A SQL `WITH` override can turn off a part
    /// that the YAML sets, such as `error.strategy = 'drop'` over a YAML `dlq`
    /// block. With no `error_config` (no `error.strategy`), every `error.*` key
    /// is unused, since the source then has no error handling.
    fn unused_error_keys_warning(
        name: &str,
        error_config: Option<&crate::core::error::ErrorConfig>,
        properties: &HashMap<String, String>,
    ) -> Option<String> {
        let (unused, strategy) = match error_config {
            Some(error_config) => (
                error_config.unused_keys(properties.keys()),
                format!("is '{}'", error_config.strategy.as_str()),
            ),
            None => {
                let mut keys: Vec<String> = properties
                    .keys()
                    .filter(|key| key.starts_with("error."))
                    .cloned()
                    .collect();
                keys.sort();
                (keys, "is not set".to_string())
            }
        };
        if unused.is_empty() {
            return None;
        }
        Some(format!(
            "Source stream '{}' sets error handling keys that are not used, because they \
             are unknown or error.strategy {}: {}",
            name,
            strategy,
            unused.join(", ")
        ))
    }

    /// Build a source config from its YAML block. Errors, naming the stream, if the
    /// block cannot be flattened.
    fn source_base(name: &str, source: &SourceConfig) -> Result<ResolvedElementConfig, String> {
        let properties = source
            .to_properties()
            .map_err(|e| format!("Invalid YAML source config for stream '{}': {}", name, e))?;
        Ok(Self::yaml_base(
            ElementType::Source,
            &source.source_type,
            source.format.as_ref(),
            properties,
        ))
    }

    /// Build a sink config from its YAML block. Errors, naming the stream, if the
    /// block cannot be flattened.
    fn sink_base(name: &str, sink: &SinkConfig) -> Result<ResolvedElementConfig, String> {
        let properties = sink
            .to_properties()
            .map_err(|e| format!("Invalid YAML sink config for stream '{}': {}", name, e))?;
        Ok(Self::yaml_base(
            ElementType::Sink,
            &sink.sink_type,
            sink.format.as_ref(),
            properties,
        ))
    }

    /// Build a config whose extension, format and properties all come from YAML.
    fn yaml_base(
        element_type: ElementType,
        extension: &str,
        format: Option<&String>,
        properties: HashMap<String, String>,
    ) -> ResolvedElementConfig {
        let mut config = ResolvedElementConfig::new(element_type);
        config.extension = Some(extension.to_string());
        config.extension_source = ConfigSource::Yaml;
        if let Some(format) = format {
            config.format = Some(format.clone());
            config.format_source = ConfigSource::Yaml;
        }
        for (key, value) in properties {
            config
                .property_sources
                .insert(key.clone(), ConfigSource::Yaml);
            config.properties.insert(key, value);
        }
        config
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
            Self::apply_sql_with_config(with_config, &mut config);
        }

        Some(config)
    }

    /// Check if the YAML `definitions` section has an entry for this table.
    fn has_yaml_table_config(&self, name: &str) -> bool {
        self.yaml_config
            .and_then(|c| c.definitions.get(name))
            .is_some()
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
    fn apply_sql_with_config(with_config: &FlatConfig, config: &mut ResolvedElementConfig) {
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

        let resolved = resolver.resolve_all(&streams, &tables).unwrap();
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

        let resolved = resolver.resolve_all(&streams, &tables).unwrap();

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
    fn test_sink_base_flattens_structured_fields() {
        let sink: SinkConfig = serde_yaml::from_str(
            r#"
            type: kafka
            format: json
            security:
              authentication:
                tls:
                  enabled: true
                  cert_path: /path/to/cert
            retry:
              max_attempts: 3
              initial_delay: 1s
              max_delay: 30s
              backoff_multiplier: 2.0
              jitter: false
            "#,
        )
        .unwrap();

        let config = ElementConfigResolver::sink_base("Out", &sink).unwrap();

        assert_eq!(config.element_type, ElementType::Sink);
        assert_eq!(config.extension.as_deref(), Some("kafka"));
        assert_eq!(config.extension_source, ConfigSource::Yaml);
        assert_eq!(config.format.as_deref(), Some("json"));
        assert_eq!(
            config.get("security.authentication.tls.enabled"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.get("security.authentication.tls.cert_path"),
            Some(&"/path/to/cert".to_string())
        );
        assert_eq!(config.get("retry.max_attempts"), Some(&"3".to_string()));
        assert_eq!(config.get("retry.jitter"), Some(&"false".to_string()));
        assert_eq!(
            config
                .property_sources
                .get("security.authentication.tls.enabled"),
            Some(&ConfigSource::Yaml)
        );
        // type and format have their own fields and are not properties
        assert!(config.get("type").is_none());
        assert!(config.get("format").is_none());
    }

    #[test]
    fn test_yaml_null_value_is_absent() {
        let sink: SinkConfig =
            serde_yaml::from_str("type: file\nformat: json\nfile.path:\n").unwrap();
        let sink_config = ElementConfigResolver::sink_base("Out", &sink).unwrap();
        assert!(sink_config.get("file.path").is_none());
        assert!(!sink_config.property_sources.contains_key("file.path"));

        let source: SourceConfig =
            serde_yaml::from_str("type: kafka\nkafka.topic:\nkafka.brokers: b1:9092\n").unwrap();
        let source_config = ElementConfigResolver::source_base("In", &source).unwrap();
        assert!(source_config.get("kafka.topic").is_none());
        assert_eq!(
            source_config.get("kafka.brokers"),
            Some(&"b1:9092".to_string())
        );
    }

    #[test]
    fn test_source_base_joins_arrays_with_commas() {
        let source: SourceConfig = serde_yaml::from_str(
            r#"
            type: kafka
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

        let config = ElementConfigResolver::source_base("In", &source).unwrap();

        assert_eq!(config.get("host"), Some(&"localhost".to_string()));
        assert_eq!(config.get("port"), Some(&"9092".to_string()));

        // Kafka splits these on commas
        assert_eq!(
            config.get("bootstrap_servers"),
            Some(&"kafka1:9092,kafka2:9092,kafka3:9092".to_string())
        );
        assert_eq!(config.get("topics"), Some(&"events,logs".to_string()));
        assert_eq!(
            config.property_sources.get("topics"),
            Some(&ConfigSource::Yaml)
        );
    }

    /// Resolves one stream named `Out` against the given YAML app config and SQL
    /// WITH options.
    fn resolve_out(
        app_config_yaml: &str,
        with_options: &[(&str, &str)],
    ) -> Result<HashMap<String, ResolvedElementConfig>, String> {
        use crate::core::config::PropertySource;

        let app_config: ApplicationConfig = serde_yaml::from_str(app_config_yaml).unwrap();
        let mut stream_def = StreamDefinition::new("Out".to_string());
        if !with_options.is_empty() {
            let mut with_config = FlatConfig::new();
            for (key, value) in with_options {
                with_config.set(*key, *value, PropertySource::SqlWith);
            }
            stream_def.with_config = Some(with_config);
        }
        let streams = HashMap::from([("Out".to_string(), Arc::new(stream_def))]);

        ElementConfigResolver::new(Some(&app_config)).resolve_all(&streams, &HashMap::new())
    }

    const YAML_FILE_SINK: &str =
        "streams:\n  Out:\n    sink:\n      type: file\n      file.path: /yaml/out.json\n";

    #[test]
    fn test_sql_with_without_type_overrides_yaml() {
        let resolved = resolve_out(YAML_FILE_SINK, &[("file.path", "/sql/out.json")]).unwrap();

        let sink = resolved.get("sink:Out").unwrap();
        assert_eq!(sink.extension.as_deref(), Some("file"));
        assert_eq!(sink.get("file.path"), Some(&"/sql/out.json".to_string()));
        assert_eq!(
            sink.property_sources.get("file.path"),
            Some(&ConfigSource::SqlWith)
        );
    }

    #[test]
    fn test_yaml_source_and_sink_both_resolve() {
        let yaml = "streams:\n  Out:\n    source:\n      type: timer\n    sink:\n      type: log\n";
        let resolved = resolve_out(yaml, &[]).unwrap();

        assert_eq!(
            resolved.get("source:Out").unwrap().extension.as_deref(),
            Some("timer")
        );
        assert_eq!(
            resolved.get("sink:Out").unwrap().extension.as_deref(),
            Some("log")
        );
    }

    #[test]
    fn test_yaml_block_that_sql_type_rules_out_is_an_error() {
        let err = resolve_out(YAML_FILE_SINK, &[("type", "internal")]).unwrap_err();
        assert!(
            err.contains("Stream 'Out' is declared type = 'internal'") && err.contains("a sink"),
            "{err}"
        );

        let err = resolve_out(
            YAML_FILE_SINK,
            &[("type", "source"), ("extension", "timer")],
        )
        .unwrap_err();
        assert!(err.contains("type = 'source'"), "{err}");
    }

    #[test]
    fn test_invalid_yaml_block_is_an_error_naming_the_stream() {
        let yaml = "streams:\n  Out:\n    sink:\n      type: file\n      labels:\n        1: one\n";
        let err = resolve_out(
            yaml,
            &[("type", "sink"), ("extension", "file"), ("file.path", "/x")],
        )
        .unwrap_err();
        assert!(
            err.contains("Invalid YAML sink config for stream 'Out'")
                && err.contains("Non-string key"),
            "{err}"
        );
    }

    const YAML_SOURCE_WITH_DLQ: &str = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n        dlq:\n          stream: OutErrors\n";

    /// Regression test for https://github.com/eventflux-io/eventflux/issues/211.
    /// YAML `error_handling` must resolve to the `error.*` keys sources read.
    #[test]
    fn test_yaml_source_error_handling_resolves_to_error_keys() {
        let resolved = resolve_out(YAML_SOURCE_WITH_DLQ, &[]).unwrap();

        let source = resolved.get("source:Out").unwrap();
        assert_eq!(source.get("error.strategy"), Some(&"dlq".to_string()));
        assert_eq!(
            source.get("error.dlq.stream"),
            Some(&"OutErrors".to_string())
        );
        assert_eq!(
            source.property_sources.get("error.strategy"),
            Some(&ConfigSource::Yaml)
        );
        assert!(!source
            .properties
            .keys()
            .any(|k| k.starts_with("error_handling")));
    }

    #[test]
    fn test_sql_with_error_key_overrides_yaml_error_handling() {
        let resolved = resolve_out(YAML_SOURCE_WITH_DLQ, &[("error.strategy", "retry")]).unwrap();

        let source = resolved.get("source:Out").unwrap();
        assert_eq!(source.get("error.strategy"), Some(&"retry".to_string()));
        assert_eq!(
            source.property_sources.get("error.strategy"),
            Some(&ConfigSource::SqlWith)
        );
        assert_eq!(
            source.get("error.dlq.stream"),
            Some(&"OutErrors".to_string())
        );
    }

    /// The merged keys are checked, not the YAML block alone. A SQL `WITH` key can
    /// fill in what the YAML leaves out, or change a value the YAML check would
    /// have judged against a default.
    #[test]
    fn test_source_error_config_is_checked_after_sql_with_merge() {
        let yaml_dlq_without_stream = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n";
        let resolved = resolve_out(
            yaml_dlq_without_stream,
            &[("error.dlq.stream", "OutErrors")],
        )
        .unwrap();
        assert_eq!(
            resolved.get("source:Out").unwrap().get("error.dlq.stream"),
            Some(&"OutErrors".to_string())
        );

        // Checked alone, the YAML max of 50ms is below the 100ms default initial
        // delay and fails. The 10ms initial delay from WITH makes it valid.
        let yaml_short_max = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: retry\n        retry:\n          max_delay: 50ms\n";
        resolve_out(yaml_short_max, &[("error.retry.initial-delay", "10ms")]).unwrap();

        // YAML sets the fallback retry, WITH sets the stream and turns it on
        let yaml_fallback_only = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n        dlq:\n          fallback_retry:\n            max_attempts: 4\n";
        let resolved = resolve_out(
            yaml_fallback_only,
            &[
                ("error.dlq.stream", "OutErrors"),
                ("error.dlq.fallback-strategy", "retry"),
            ],
        )
        .unwrap();
        let source = resolved.get("source:Out").unwrap();
        let error_config =
            crate::core::error::ErrorConfigBuilder::from_properties(&source.properties)
                .build()
                .unwrap()
                .unwrap();
        let dlq = error_config.dlq.as_ref().unwrap();
        assert_eq!(dlq.stream, "OutErrors");
        assert_eq!(dlq.fallback_retry.as_ref().unwrap().max_attempts, 4);
        assert!(error_config
            .unused_keys(source.properties.keys())
            .is_empty());

        // WITH turns on the DLQ block that the YAML strategy does not read
        let yaml_drop_with_dlq = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: drop\n        dlq:\n          stream: OutErrors\n";
        resolve_out(yaml_drop_with_dlq, &[("error.strategy", "dlq")]).unwrap();
    }

    #[test]
    fn test_invalid_merged_source_error_config_is_an_error_naming_the_stream() {
        let yaml_dlq_without_stream = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n";
        let yaml_long_initial = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: retry\n        retry:\n          initial_delay: 5s\n";
        let yaml_dlq_with_fallback = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n        dlq:\n          stream: E\n          fallback_strategy: retry\n          fallback_retry:\n            max_attempts: 0\n";
        /// YAML app config, SQL WITH options, and the text the error must hold
        type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str);
        let cases: [Case; 4] = [
            (yaml_dlq_without_stream, &[], "requires 'error.dlq.stream'"),
            (
                yaml_long_initial,
                &[("error.retry.max-delay", "1s")],
                "error.retry.initial-delay cannot be greater than error.retry.max-delay",
            ),
            (
                yaml_dlq_with_fallback,
                &[],
                "error.dlq.fallback-retry.max-attempts must be greater than 0",
            ),
            // A source defined only in SQL WITH is checked the same way
            (
                "streams: {}\n",
                &[
                    ("type", "source"),
                    ("extension", "timer"),
                    ("error.strategy", "retry"),
                    ("error.retry.max-attempts", "0"),
                ],
                "error.retry.max-attempts must be greater than 0",
            ),
        ];
        for (yaml, with_options, expected) in cases {
            let err = resolve_out(yaml, with_options).unwrap_err();
            assert!(
                err.contains("Invalid error handling for source stream 'Out'")
                    && err.contains(expected),
                "expected '{expected}', got: {err}"
            );
        }
    }

    /// A part the effective strategy does not read is not an error, since a WITH
    /// override may turn it off on purpose. It is named in a warning instead,
    /// along with unknown keys and keys set without any strategy.
    #[test]
    fn test_unused_source_error_keys_are_named_in_a_warning() {
        let yaml_dlq = "streams:\n  Out:\n    source:\n      type: timer\n      error_handling:\n        strategy: dlq\n        dlq:\n          stream: OutErrors\n";
        let warning = |yaml: &str, with_options: &[(&str, &str)]| {
            let resolved = resolve_out(yaml, with_options).unwrap();
            let properties = &resolved.get("source:Out").unwrap().properties;
            let error_config = crate::core::error::ErrorConfigBuilder::from_properties(properties)
                .build()
                .unwrap();
            ElementConfigResolver::unused_error_keys_warning(
                "Out",
                error_config.as_ref(),
                properties,
            )
        };

        assert_eq!(warning(yaml_dlq, &[]), None);
        assert_eq!(
            warning(
                yaml_dlq,
                &[
                    ("error.strategy", "drop"),
                    ("error.retry.max_attempts", "5")
                ]
            )
            .unwrap(),
            "Source stream 'Out' sets error handling keys that are not used, because they \
             are unknown or error.strategy is 'drop': error.dlq.stream, error.retry.max_attempts"
        );
        assert_eq!(
            warning(
                "streams: {}\n",
                &[
                    ("type", "source"),
                    ("extension", "timer"),
                    ("error.retry.max-attempts", "5"),
                ]
            )
            .unwrap(),
            "Source stream 'Out' sets error handling keys that are not used, because they \
             are unknown or error.strategy is not set: error.retry.max-attempts"
        );
    }
}
