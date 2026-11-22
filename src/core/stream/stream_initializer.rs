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

//! Stream Initialization Module
//!
//! Bridges the factory registries in [`EventFluxContext`] and the runtime:
//! turns a validated stream/table configuration into ready-to-use connector
//! instances.
//!
//! - [`initialize_source_stream`] → [`InitializedSource`] (source + mapper)
//! - [`initialize_sink_stream`] → [`InitializedSink`] (sink + mapper)
//! - [`initialize_table`] → [`InitializedTable`] (backing store)
//!
//! Each function looks up the factories, validates format and mapper
//! configuration, creates the instances, and checks external connectivity
//! (fail-fast). Wiring the result into junctions and handlers is the
//! runtime's job (`EventFluxAppRuntime`).

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::config::stream_config::{StreamType, StreamTypeConfig, TableTypeConfig};
use crate::core::exception::EventFluxError;
use crate::core::stream::input::mapper::SourceMapper;
use crate::core::stream::input::source::Source;
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::stream::output::sink::Sink;
use crate::core::table::Table;

/// Fully initialized stream with source and mapper components
pub struct InitializedSource {
    pub source: Box<dyn Source>,
    pub mapper: Option<Box<dyn SourceMapper>>, // Optional - None = use PassthroughMapper
    pub extension: String,
    pub format: Option<String>, // Optional - None = no format (binary passthrough)
}

/// Fully initialized sink stream with sink and mapper components
pub struct InitializedSink {
    pub sink: Box<dyn Sink>,
    pub mapper: Option<Box<dyn SinkMapper>>, // Optional - None = use PassthroughMapper
    pub extension: String,
    pub format: Option<String>, // Optional - None = no format (binary passthrough)
}

/// Fully initialized table with backing store
///
/// Tables are bidirectional data structures for persistent lookups and joins.
/// Unlike streams, tables:
/// - Have no type property (always bidirectional)
/// - Have no format property (use relational schema only)
/// - Require an extension property (specifies backing store: mysql, postgres, redis, etc.)
///
/// Note: Tables use Arc for shared ownership since they can be accessed concurrently
/// by multiple queries, unlike sources/sinks which are owned by a single stream.
pub struct InitializedTable {
    pub table: Arc<dyn Table>,
    pub extension: String,
}

impl std::fmt::Debug for InitializedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedSource")
            .field("extension", &self.extension)
            .field("format", &self.format)
            .finish()
    }
}

impl std::fmt::Debug for InitializedSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedSink")
            .field("extension", &self.extension)
            .field("format", &self.format)
            .finish()
    }
}

impl std::fmt::Debug for InitializedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedTable")
            .field("extension", &self.extension)
            .finish()
    }
}

/// Connectors that exist in the codebase but were excluded from this build
/// by cargo features. The feature is always named after the extension (see
/// the `[features]` section in Cargo.toml), so one entry per gated connector.
const GATED_OUT_EXTENSIONS: &[&str] = &[
    #[cfg(not(feature = "file"))]
    "file",
    #[cfg(not(feature = "http"))]
    "http",
    #[cfg(not(feature = "kafka"))]
    "kafka",
    #[cfg(not(feature = "rabbitmq"))]
    "rabbitmq",
    #[cfg(not(feature = "websocket"))]
    "websocket",
];

/// Error for a failed connector factory lookup.
///
/// If the extension is known but compiled out, the error tells the user how
/// to rebuild instead of reporting a bare "extension not found".
fn connector_lookup_error(kind: &str, extension: &str) -> EventFluxError {
    if GATED_OUT_EXTENSIONS.contains(&extension) {
        EventFluxError::configuration(format!(
            "Extension '{extension}' is supported by EventFlux but was not included in this \
             build. Rebuild with `--features {extension}` (or `--features connectors-all`)."
        ))
    } else {
        EventFluxError::extension_not_found(kind, extension)
    }
}

/// Reject a stream configuration whose type isn't the one being initialized
/// (e.g. a sink config passed to source initialization).
fn ensure_stream_type(
    stream_config: &StreamTypeConfig,
    expected: StreamType,
) -> Result<(), EventFluxError> {
    if stream_config.stream_type == expected {
        return Ok(());
    }
    Err(EventFluxError::configuration(format!(
        "Expected a {} stream configuration, got '{}'",
        expected.as_str(),
        stream_config.stream_type.as_str()
    )))
}

/// Properties handed to source/sink factories: the user's map plus
/// engine-injected reserved keys (leading underscore). Currently `_format`
/// carries the stream's declared format so connectors can derive
/// format-dependent settings (e.g. the HTTP sink's Content-Type).
fn factory_properties(stream_config: &StreamTypeConfig) -> HashMap<String, String> {
    let mut properties = stream_config.properties.clone();
    if let Some(format) = stream_config.format() {
        properties.insert(
            crate::core::stream::connector_util::INJECTED_FORMAT_KEY.to_string(),
            format.to_string(),
        );
    }
    properties
}

/// Initialize a source stream with factory lookup and fail-fast validation
///
/// This is the main integration point between the factory registry and the
/// stream runtime for sources:
///
/// 1. Look up the appropriate source factory by extension name
/// 2. Validate that the factory supports the requested format
/// 3. Look up the mapper factory by format name
/// 4. Create fully initialized instances with fail-fast validation
/// 5. Return wired components guaranteed to be in valid state
///
/// `field_names` is the stream's schema in declaration order; it lets format
/// mappers (e.g. JSON) match incoming fields to stream attributes by name
/// instead of only by position (#201). Pass an empty slice when no schema is
/// available — mapping then falls back to positional order.
pub fn initialize_source_stream(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
    field_names: &[String],
) -> Result<InitializedSource, EventFluxError> {
    ensure_stream_type(stream_config, StreamType::Source)?;

    // 1. Look up source factory by extension
    let extension = stream_config
        .extension()
        .map_err(|e| EventFluxError::configuration(format!("Stream configuration error: {}", e)))?;

    let source_factory = context
        .get_source_factory(extension)
        .ok_or_else(|| connector_lookup_error("source", extension))?;

    // 2. Validate format support (if format is specified)
    let mapper_factory = if let Some(format) = stream_config.format() {
        // Format specified - validate and look up mapper factory
        if !source_factory.supported_formats().contains(&format) {
            return Err(EventFluxError::unsupported_format(format, extension));
        }

        // 3. Look up mapper factory by format
        Some(
            context
                .get_source_mapper_factory(format)
                .ok_or_else(|| EventFluxError::extension_not_found("source mapper", format))?,
        )
    } else {
        // No format specified - validate that source supports binary passthrough
        let supported_formats = source_factory.supported_formats();
        if !supported_formats.is_empty() {
            // Source requires explicit format but none was specified
            // This prevents silent data loss for external sources (Kafka, HTTP, etc.)
            return Err(EventFluxError::configuration(format!(
                "Source extension '{}' requires a format specification. \
                Supported formats: {}. \
                Without a format, events will be incorrectly decoded as binary, causing data loss.",
                extension,
                supported_formats.join(", ")
            )));
        }
        // PassthroughMapper will be used for binary-only sources
        None
    };

    // 4. Phase 1 Validation: Verify mapper configuration (if format is specified)
    // This ensures source streams don't use sink-only properties (template, pretty-print, etc.)
    if let Some(format) = stream_config.format() {
        crate::core::stream::mapper::validation::validate_source_mapper_config(
            &stream_config.properties,
            format,
        )
        .map_err(|e| {
            EventFluxError::configuration_with_key(
                format!(
                    "Source stream mapper configuration validation failed: {}. \
                    Source streams can use 'mapping.*' properties but not 'template' property.",
                    e
                ),
                format!("{}.mapping", format),
            )
        })?;
    }

    // 5. Create fully initialized instances (fail-fast validation)
    let factory_properties = factory_properties(stream_config);
    let source = source_factory.create_initialized(&factory_properties)?;
    let mapper = mapper_factory
        .map(|factory| factory.create_with_schema(&stream_config.properties, field_names))
        .transpose()?;

    // 6. Phase 2 Validation: Verify external connectivity (FAIL-FAST)
    // This ensures "What's the point of deploying if transports aren't ready?"
    source.validate_connectivity().map_err(|e| {
        EventFluxError::app_creation(format!(
            "Source '{}' connectivity validation failed: {}. \
            Application cannot start with unreachable external systems.",
            extension, e
        ))
    })?;

    // 7. Return wired components - instances are guaranteed valid
    Ok(InitializedSource {
        source,
        mapper,
        extension: extension.to_string(),
        format: stream_config.format().map(|s| s.to_string()),
    })
}

/// Initialize a sink stream with factory lookup and fail-fast validation
///
/// `field_names` is the stream's schema in declaration order; format mappers
/// use it for output field names (e.g. JSON keys `"symbol"` instead of
/// `"field_0"`). Pass an empty slice when no schema is available.
pub fn initialize_sink_stream(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
    field_names: &[String],
) -> Result<InitializedSink, EventFluxError> {
    ensure_stream_type(stream_config, StreamType::Sink)?;

    // 1. Look up sink factory by extension
    let extension = stream_config
        .extension()
        .map_err(|e| EventFluxError::configuration(format!("Stream configuration error: {}", e)))?;

    let sink_factory = context
        .get_sink_factory(extension)
        .ok_or_else(|| connector_lookup_error("sink", extension))?;

    // 2. Validate format support (if format is specified)
    let mapper_factory = if let Some(format) = stream_config.format() {
        // Format specified - validate and look up mapper factory
        if !sink_factory.supported_formats().contains(&format) {
            return Err(EventFluxError::unsupported_format(format, extension));
        }

        // 3. Look up mapper factory by format
        Some(
            context
                .get_sink_mapper_factory(format)
                .ok_or_else(|| EventFluxError::extension_not_found("sink mapper", format))?,
        )
    } else {
        // No format specified - validate that sink supports binary passthrough
        let supported_formats = sink_factory.supported_formats();
        if !supported_formats.is_empty() {
            // Sink requires explicit format but none was specified
            // This prevents silent data loss for external sinks (HTTP, Kafka, etc.)
            return Err(EventFluxError::configuration(format!(
                "Sink extension '{}' requires a format specification. \
                Supported formats: {}. \
                Without a format, events will be incorrectly encoded as binary, causing data loss.",
                extension,
                supported_formats.join(", ")
            )));
        }
        // PassthroughMapper will be used for binary-only sinks
        None
    };

    // 4. Phase 1 Validation: Verify mapper configuration (if format is specified)
    // This ensures sink streams don't use source-only properties (mapping.*, ignore-parse-errors, etc.)
    if let Some(format) = stream_config.format() {
        crate::core::stream::mapper::validation::validate_sink_mapper_config(
            &stream_config.properties,
            format,
        )
        .map_err(|e| {
            EventFluxError::configuration_with_key(
                format!(
                    "Sink stream mapper configuration validation failed: {}. \
                    Sink streams can use 'template' property but not 'mapping.*' properties.",
                    e
                ),
                format!("{}.template", format),
            )
        })?;
    }

    // 5. Create fully initialized instances (fail-fast validation)
    let factory_properties = factory_properties(stream_config);
    let sink = sink_factory.create_initialized(&factory_properties)?;

    let mapper = mapper_factory
        .map(|factory| factory.create_with_schema(&stream_config.properties, field_names))
        .transpose()?;

    // 6. Phase 2 Validation: Verify external connectivity (FAIL-FAST)
    // This ensures "What's the point of deploying if transports aren't ready?"
    sink.validate_connectivity().map_err(|e| {
        EventFluxError::app_creation(format!(
            "Sink '{}' connectivity validation failed: {}. \
            Application cannot start with unreachable external systems.",
            extension, e
        ))
    })?;

    // 7. Return wired components - instances are guaranteed valid
    Ok(InitializedSink {
        sink,
        mapper,
        extension: extension.to_string(),
        format: stream_config.format().map(|s| s.to_string()),
    })
}

/// Initialize a table with factory lookup and validation
///
/// Tables are bidirectional data structures for persistent lookups and joins.
/// Unlike streams, tables:
/// - Do NOT have a 'type' property (always bidirectional)
/// - Do NOT have a 'format' property (use relational schema only)
/// - MUST have an 'extension' property (specifies backing store)
///
/// # Arguments
///
/// * `context` - EventFlux context containing table factory registries
/// * `table_config` - Typed table configuration with validation
/// * `table_name` - Name of the table being initialized
///
/// # Returns
///
/// * `Ok(InitializedTable)` - Fully initialized table instance
/// * `Err(EventFluxError)` - If factory not found or initialization fails
///
/// # Example
///
/// ```rust,ignore
/// use std::collections::HashMap;
/// use eventflux::core::config::eventflux_context::EventFluxContext;
/// use eventflux::core::config::stream_config::TableTypeConfig;
/// use eventflux::core::stream::initialize_table;
///
/// let context = EventFluxContext::new();
/// let table_config = TableTypeConfig::new("inMemory".to_string(), HashMap::new())?;
///
/// let initialized = initialize_table(&context, &table_config, "Users")?;
/// // Table is ready for queries
/// initialized.table.insert(&values)?;
/// ```
pub fn initialize_table(
    context: &EventFluxContext,
    table_config: &TableTypeConfig,
    table_name: &str,
) -> Result<InitializedTable, EventFluxError> {
    // 1. Look up table factory by extension
    let extension = table_config.extension();

    let table_factory = context
        .get_table_factory(extension)
        .ok_or_else(|| EventFluxError::extension_not_found("table", extension))?;

    // 2. Create fully initialized table instance (fail-fast validation)
    // Note: TableFactory::create() returns Arc<dyn Table> for shared ownership
    let table = table_factory
        .create(
            table_name.to_string(),
            table_config.properties.clone(),
            Arc::new(context.clone()),
        )
        .map_err(|e| {
            EventFluxError::configuration(format!("Failed to create table '{}': {}", table_name, e))
        })?;

    // 3. Phase 2 Validation: Verify external backing store connectivity (FAIL-FAST)
    // This ensures "What's the point of deploying if backing stores aren't ready?"
    table.validate_connectivity().map_err(|e| {
        EventFluxError::app_creation(format!(
            "Table '{}' (extension '{}') connectivity validation failed: {}. \
            Application cannot start with unreachable backing stores.",
            table_name, extension, e
        ))
    })?;

    // 4. Return initialized table - instance is guaranteed valid
    Ok(InitializedTable {
        table,
        extension: extension.to_string(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::stream_config::{FlatConfig, PropertySource};
    use crate::core::extension::example_factories::{ExampleSinkFactory, ExampleSourceFactory};
    use crate::core::extension::JsonSourceMapperFactory;

    #[test]
    fn test_connector_lookup_error_unknown_extension() {
        // Names not in the gated-out table keep the plain not-found error
        let err = connector_lookup_error("source", "no-such-connector");
        let msg = err.to_string();
        assert!(msg.contains("no-such-connector"), "got: {msg}");
        assert!(!msg.contains("--features"), "got: {msg}");
    }

    // Only meaningful in builds where the connector is compiled out
    // (the default minimal build) — with the feature on, the extension is
    // registered and the lookup never fails.
    #[cfg(not(feature = "rabbitmq"))]
    #[test]
    fn test_connector_lookup_error_gated_out_extension_hints_feature() {
        let err = connector_lookup_error("source", "rabbitmq");
        let msg = err.to_string();
        assert!(msg.contains("--features rabbitmq"), "got: {msg}");
        assert!(msg.contains("connectors-all"), "got: {msg}");
    }

    #[test]
    fn test_initialize_source_stream_success() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));
        context.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));

        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test-topic".to_string());

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let source = initialize_source_stream(&context, &stream_config, &[])
            .expect("source initialization should succeed");
        assert_eq!(source.extension, "example");
        assert_eq!(source.format.as_deref(), Some("json"));
        assert!(source.mapper.is_some(), "json format must build a mapper");
    }

    #[test]
    fn test_initialize_source_stream_with_schema_maps_by_name() {
        use crate::core::event::AttributeValue;

        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));
        context.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));

        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test-topic".to_string());

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        // Schema order: price, symbol, volume
        let field_names = vec![
            "price".to_string(),
            "symbol".to_string(),
            "volume".to_string(),
        ];
        let result = initialize_source_stream(&context, &stream_config, &field_names).unwrap();

        let mapper = result.mapper.expect("mapper");

        // JSON keys arrive in a different order than the schema (#201)
        let json_str = r#"{"symbol": "AAPL", "price": 185.5, "volume": 5000}"#;
        let events = mapper.map(json_str.as_bytes()).unwrap();

        assert!(matches!(events[0].data[0], AttributeValue::Double(p) if (p - 185.5).abs() < 1e-9));
        assert!(matches!(
            events[0].data[1],
            AttributeValue::String(ref s) if s == "AAPL"
        ));
        assert!(matches!(events[0].data[2], AttributeValue::Int(5000)));
    }

    #[test]
    fn test_initialize_source_stream_extension_not_found() {
        let context = EventFluxContext::new();

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("nonexistent".to_string()),
            Some("json".to_string()),
            HashMap::new(),
        )
        .unwrap();

        let result = initialize_source_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        match result.unwrap_err() {
            EventFluxError::ExtensionNotFound {
                extension_type,
                name,
            } => {
                assert_eq!(extension_type, "source");
                assert_eq!(name, "nonexistent");
            }
            e => panic!("Expected ExtensionNotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_initialize_source_stream_unsupported_format() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("xml".to_string()), // The example stub doesn't support XML
            HashMap::new(),
        )
        .unwrap();

        let result = initialize_source_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("xml"));
                assert!(message.contains("example"));
            }
            e => panic!(
                "Expected Configuration error for unsupported format, got {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_initialize_source_stream_mapper_not_found() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));
        // Note: json/csv mappers are registered by default, use a non-existent format

        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test".to_string());

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("avro".to_string()), // avro mapper not registered
            config,
        )
        .unwrap();

        let result = initialize_source_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        match result.unwrap_err() {
            EventFluxError::ExtensionNotFound {
                extension_type,
                name,
            } => {
                assert_eq!(extension_type, "source mapper");
                assert_eq!(name, "avro");
            }
            e => panic!("Expected ExtensionNotFound for mapper, got {:?}", e),
        }
    }

    #[test]
    fn test_initialize_sink_stream_success() {
        let context = EventFluxContext::new();
        context.add_sink_factory("example-sink".to_string(), Box::new(ExampleSinkFactory));
        // The JSON sink mapper is registered by default

        let mut config = HashMap::new();
        config.insert(
            "example.url".to_string(),
            "http://localhost:8080/events".to_string(),
        );

        let stream_config = StreamTypeConfig::new(
            StreamType::Sink,
            Some("example-sink".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let sink = initialize_sink_stream(&context, &stream_config, &[])
            .expect("sink initialization should succeed");
        assert_eq!(sink.extension, "example-sink");
        assert_eq!(sink.format.as_deref(), Some("json"));
        assert!(sink.mapper.is_some(), "json format must build a mapper");
    }

    #[test]
    fn test_initialize_source_stream_rejects_non_source_config() {
        let context = EventFluxContext::new();

        let internal =
            StreamTypeConfig::new(StreamType::Internal, None, None, HashMap::new()).unwrap();
        let err = initialize_source_stream(&context, &internal, &[]).unwrap_err();
        assert!(
            err.to_string().contains("Expected a source stream"),
            "got: {err}"
        );
    }

    #[test]
    fn test_initialize_sink_stream_rejects_non_sink_config() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));

        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test-topic".to_string());
        let source = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        // A source config must not be initialized as a sink
        let err = initialize_sink_stream(&context, &source, &[]).unwrap_err();
        assert!(
            err.to_string().contains("Expected a sink stream"),
            "got: {err}"
        );
    }

    #[test]
    fn test_initialize_source_stream_invalid_config() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));
        context.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));

        // Missing required example.servers
        let config = HashMap::new();

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let result = initialize_source_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        // Should get InvalidParameter error from factory (for missing parameter)
        match result.unwrap_err() {
            EventFluxError::InvalidParameter { parameter, .. } => {
                if let Some(param) = parameter {
                    assert!(param.contains("example.servers"));
                }
            }
            e => panic!(
                "Expected InvalidParameter error for missing parameter, got {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_initialize_source_stream_missing_required_format() {
        let context = EventFluxContext::new();
        context.add_source_factory("example".to_string(), Box::new(ExampleSourceFactory));
        // Don't add mapper factory - we're testing the format requirement validation

        let mut config = HashMap::new();
        config.insert("example.servers".to_string(), "localhost:9092".to_string());
        config.insert("example.topic".to_string(), "test-topic".to_string());

        // Example source WITHOUT format - should be rejected
        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("example".to_string()),
            None,
            config,
        )
        .unwrap();

        let result = initialize_source_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        // Should get Configuration error requiring format
        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("requires a format specification"));
                assert!(message.contains("example"));
                assert!(message.contains("json"));
                assert!(message.contains("data loss"));
            }
            e => panic!(
                "Expected Configuration error for missing format, got {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_initialize_sink_stream_missing_required_format() {
        let context = EventFluxContext::new();
        context.add_sink_factory("example-sink".to_string(), Box::new(ExampleSinkFactory));

        let mut config = HashMap::new();
        config.insert(
            "example.url".to_string(),
            "http://localhost:8080/events".to_string(),
        );

        // Example sink WITHOUT format - should be rejected
        let stream_config = StreamTypeConfig::new(
            StreamType::Sink,
            Some("example-sink".to_string()),
            None,
            config,
        )
        .unwrap();

        let result = initialize_sink_stream(&context, &stream_config, &[]);
        assert!(result.is_err());

        // Should get Configuration error requiring format
        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("requires a format specification"));
                assert!(message.contains("example-sink"));
                assert!(message.contains("json"));
                assert!(message.contains("data loss"));
            }
            e => panic!(
                "Expected Configuration error for missing format, got {:?}",
                e
            ),
        }
    }

    // ========================================================================
    // Table Initialization Tests
    // ========================================================================

    #[test]
    fn test_initialize_table_success() {
        let context = EventFluxContext::new();

        let mut properties = HashMap::new();
        properties.insert("extension".to_string(), "inMemory".to_string());

        let table_config = TableTypeConfig::new("inMemory".to_string(), properties).unwrap();

        let table = initialize_table(&context, &table_config, "TestTable")
            .expect("table initialization should succeed");
        assert_eq!(table.extension, "inMemory");
        // Table is Arc<dyn Table> for shared ownership
    }

    #[test]
    fn test_initialize_table_extension_not_found() {
        let context = EventFluxContext::new();

        let mut properties = HashMap::new();
        properties.insert("extension".to_string(), "nonexistent".to_string());

        let table_config = TableTypeConfig::new("nonexistent".to_string(), properties).unwrap();

        let result = initialize_table(&context, &table_config, "TestTable");
        assert!(result.is_err());

        match result.unwrap_err() {
            EventFluxError::ExtensionNotFound {
                extension_type,
                name,
            } => {
                assert_eq!(extension_type, "table");
                assert_eq!(name, "nonexistent");
            }
            e => panic!("Expected ExtensionNotFound error, got {:?}", e),
        }
    }

    #[test]
    fn test_initialize_table_with_multiple_properties() {
        // Test table initialization with multiple properties to verify property passing
        let context = EventFluxContext::new();

        let mut properties = HashMap::new();
        properties.insert("extension".to_string(), "inMemory".to_string());
        properties.insert("custom.property1".to_string(), "value1".to_string());
        properties.insert("custom.property2".to_string(), "value2".to_string());

        let table_config = TableTypeConfig::new("inMemory".to_string(), properties).unwrap();

        let table = initialize_table(&context, &table_config, "PropertiesTable")
            .expect("table initialization should succeed");
        assert_eq!(table.extension, "inMemory");
    }

    #[test]
    fn test_table_config_validation_flow() {
        // Test that TableTypeConfig properly validates before initialization
        let context = EventFluxContext::new();

        // Test 1: Valid config with inMemory backend
        let mut properties1 = HashMap::new();
        properties1.insert("extension".to_string(), "inMemory".to_string());
        let config1 = TableTypeConfig::new("inMemory".to_string(), properties1).unwrap();
        let result1 = initialize_table(&context, &config1, "Table1");
        assert!(result1.is_ok());

        // Test 2: Try to create table config with forbidden 'type' property
        let mut flat_config = FlatConfig::new();
        flat_config.set("extension", "inMemory", PropertySource::SqlWith);
        flat_config.set("type", "source", PropertySource::SqlWith);
        let config2 = TableTypeConfig::from_flat_config(&flat_config);
        assert!(config2.is_err());

        // Test 3: Try to create table config with forbidden 'format' property
        let mut flat_config2 = FlatConfig::new();
        flat_config2.set("extension", "inMemory", PropertySource::SqlWith);
        flat_config2.set("format", "json", PropertySource::SqlWith);
        let config3 = TableTypeConfig::from_flat_config(&flat_config2);
        assert!(config3.is_err());
    }

    #[test]
    fn test_initialize_table_from_flat_config() {
        let context = EventFluxContext::new();

        // Simulate multi-layer configuration merge
        let mut config = FlatConfig::new();
        config.set("extension", "inMemory", PropertySource::SqlWith);
        config.set("cache.ttl", "3600", PropertySource::TomlApplication);

        let table_config = TableTypeConfig::from_flat_config(&config).unwrap();
        let table = initialize_table(&context, &table_config, "ConfigTable")
            .expect("table initialization should succeed");
        assert_eq!(table.extension, "inMemory");
    }

    #[test]
    fn test_table_forbids_type_property() {
        let mut config = FlatConfig::new();
        config.set("extension", "inMemory", PropertySource::SqlWith);
        config.set("type", "source", PropertySource::SqlWith); // Forbidden for tables

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot have 'type'"));
    }

    #[test]
    fn test_table_forbids_format_property() {
        let mut config = FlatConfig::new();
        config.set("extension", "inMemory", PropertySource::SqlWith);
        config.set("format", "json", PropertySource::SqlWith); // Forbidden for tables

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot have 'format'"));
    }

    #[test]
    fn test_table_requires_extension() {
        let mut config = FlatConfig::new();
        config.set("some.property", "value", PropertySource::SqlWith);

        let result = TableTypeConfig::from_flat_config(&config);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("missing required 'extension' property"));
    }
}
