// SPDX-License-Identifier: MIT OR Apache-2.0

//! Stream Initialization Module
//!
//! Provides comprehensive stream initialization with topological sorting for dependency
//! management. This module implements:
//!
//! 1. Dependency graph construction (query + DLQ dependencies)
//! 2. Topological sort for initialization ordering
//! 3. Stream handler creation and lifecycle management
//! 4. Integration with factory system for source/sink creation
//!
//! ## Initialization Flow
//!
//! 1. Build dependency graph from queries and DLQ configurations
//! 2. Perform topological sort to determine initialization order
//! 3. Initialize streams in dependency order (dependencies first)
//! 4. Start all source handlers
//!
//! ## Thread Safety
//!
//! All stream handlers are created behind Arc for shared ownership across
//! multiple threads during query processing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::config::stream_config::{
    FlatConfig, StreamType, StreamTypeConfig, TableTypeConfig,
};
use crate::core::exception::EventFluxError;
use crate::core::stream::handler::{SinkStreamHandler, SourceStreamHandler};
use crate::core::stream::input::mapper::SourceMapper;
use crate::core::stream::input::source::Source;
use crate::core::stream::output::mapper::SinkMapper;
use crate::core::stream::output::sink::Sink;
use crate::core::table::Table;
use crate::query_api::definition::stream_definition::StreamDefinition;
use crate::query_api::execution::query::Query;

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

/// Result of stream/table initialization
pub enum InitializedStream {
    Source(InitializedSource),
    Sink(InitializedSink),
    Internal,
    Table(InitializedTable),
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

impl std::fmt::Debug for InitializedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitializedStream::Source(s) => f.debug_tuple("Source").field(s).finish(),
            InitializedStream::Sink(s) => f.debug_tuple("Sink").field(s).finish(),
            InitializedStream::Internal => write!(f, "Internal"),
            InitializedStream::Table(t) => f.debug_tuple("Table").field(t).finish(),
        }
    }
}

/// Initialize a stream from configuration using the factory system
///
/// This is the main integration point between the factory registry and stream runtime.
/// It performs the following steps:
///
/// 1. Look up the appropriate factory by extension name
/// 2. Validate that the factory supports the requested format
/// 3. Look up the mapper factory by format name
/// 4. Create fully initialized instances with fail-fast validation
/// 5. Return wired components guaranteed to be in valid state
///
/// # Arguments
///
/// * `context` - EventFlux context containing factory registries
/// * `stream_config` - Typed stream configuration with validation
///
/// # Returns
///
/// * `Ok(InitializedStream)` - Fully initialized and validated stream components
/// * `Err(EventFluxError)` - If factory not found, format unsupported, or initialization fails
///
/// # Example
///
/// ```rust,ignore
/// use eventflux::core::stream::stream_initializer::initialize_stream;
/// use eventflux::core::config::stream_config::StreamTypeConfig;
///
/// let context = EventFluxContext::new();
/// let stream_config = StreamTypeConfig::new(
///     StreamType::Source,
///     Some("kafka".to_string()),
///     Some("json".to_string()),
///     config_map,
/// )?;
///
/// let initialized = initialize_stream(&context, &stream_config)?;
/// match initialized {
///     InitializedStream::Source(source) => {
///         // Source and mapper are ready to use
///         source.source.start(handler);
///     }
///     _ => {}
/// }
/// ```
pub fn initialize_stream(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
) -> Result<InitializedStream, EventFluxError> {
    match stream_config.stream_type {
        StreamType::Source => initialize_source_stream(context, stream_config),
        StreamType::Sink => initialize_sink_stream(context, stream_config),
        StreamType::Internal => Ok(InitializedStream::Internal),
    }
}

/// Initialize a source stream with factory lookup and validation
fn initialize_source_stream(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
) -> Result<InitializedStream, EventFluxError> {
    // 1. Look up source factory by extension
    let extension = stream_config
        .extension()
        .map_err(|e| EventFluxError::configuration(format!("Stream configuration error: {}", e)))?;

    let source_factory = context
        .get_source_factory(extension)
        .ok_or_else(|| EventFluxError::extension_not_found("source", extension))?;

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
    let source = source_factory.create_initialized(&stream_config.properties)?;
    let mapper = mapper_factory
        .map(|factory| factory.create_initialized(&stream_config.properties))
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
    Ok(InitializedStream::Source(InitializedSource {
        source,
        mapper,
        extension: extension.to_string(),
        format: stream_config.format().map(|s| s.to_string()),
    }))
}

/// Initialize a sink stream with factory lookup and validation
fn initialize_sink_stream(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
) -> Result<InitializedStream, EventFluxError> {
    initialize_sink_stream_internal(context, stream_config, None)
}

/// Initialize a sink stream with schema field names for proper JSON output
pub fn initialize_sink_stream_with_schema(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
    field_names: &[String],
) -> Result<InitializedStream, EventFluxError> {
    initialize_sink_stream_internal(context, stream_config, Some(field_names))
}

/// Internal sink stream initialization with optional field names
fn initialize_sink_stream_internal(
    context: &EventFluxContext,
    stream_config: &StreamTypeConfig,
    field_names: Option<&[String]>,
) -> Result<InitializedStream, EventFluxError> {
    // 1. Look up sink factory by extension
    let extension = stream_config
        .extension()
        .map_err(|e| EventFluxError::configuration(format!("Stream configuration error: {}", e)))?;

    let sink_factory = context
        .get_sink_factory(extension)
        .ok_or_else(|| EventFluxError::extension_not_found("sink", extension))?;

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
    let sink = sink_factory.create_initialized(&stream_config.properties)?;

    // Create mapper with field names if provided, otherwise use standard initialization
    let mapper = mapper_factory
        .map(|factory| {
            if let Some(names) = field_names {
                factory.create_with_schema(&stream_config.properties, names)
            } else {
                factory.create_initialized(&stream_config.properties)
            }
        })
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
    Ok(InitializedStream::Sink(InitializedSink {
        sink,
        mapper,
        extension: extension.to_string(),
        format: stream_config.format().map(|s| s.to_string()),
    }))
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
/// * `Ok(InitializedStream::Table)` - Fully initialized table instance
/// * `Err(EventFluxError)` - If factory not found or initialization fails
///
/// # Example
///
/// ```rust,ignore
/// use eventflux::core::stream::stream_initializer::initialize_table;
/// use eventflux::core::config::stream_config::TableTypeConfig;
///
/// let context = EventFluxContext::new();
/// let table_config = TableTypeConfig::new(
///     "mysql".to_string(),
///     config_map,
/// )?;
///
/// let initialized = initialize_table(&context, &table_config, "Users")?;
/// match initialized {
///     InitializedStream::Table(table) => {
///         // Table is ready for queries
///         table.table.insert(&values);
///     }
///     _ => {}
/// }
/// ```
pub fn initialize_table(
    context: &EventFluxContext,
    table_config: &TableTypeConfig,
    table_name: &str,
) -> Result<InitializedStream, EventFluxError> {
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
    Ok(InitializedStream::Table(InitializedTable {
        table,
        extension: extension.to_string(),
    }))
}

// ============================================================================
// Dependency Graph & Topological Sort
// ============================================================================

/// Build dependency graph for stream initialization order
///
/// Constructs dependencies from two sources:
/// 1. **Query dependencies**: INSERT INTO target FROM source
/// 2. **DLQ dependencies**: Stream depends on its DLQ stream
///
/// # Arguments
///
/// * `parsed_streams` - All defined streams with their configurations
/// * `queries` - All queries defining data flow
///
/// # Returns
///
/// HashMap mapping stream_name → set of streams it depends on
fn build_dependency_graph(
    parsed_streams: &HashMap<String, (StreamDefinition, FlatConfig)>,
    queries: &[Query],
) -> HashMap<String, HashSet<String>> {
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    // 1. Add query dependencies
    for query in queries {
        if let Some(target) = query.get_target_stream() {
            let sources = query.get_source_streams();

            dependencies
                .entry(target)
                .or_insert_with(HashSet::new)
                .extend(sources);
        }
    }

    // 2. Add DLQ dependencies
    // Stream depends on its DLQ stream (DLQ must be initialized first)
    for (stream_name, (_stream_def, config)) in parsed_streams {
        if let Some(dlq_stream) = config.get("error.dlq.stream") {
            dependencies
                .entry(stream_name.clone())
                .or_insert_with(HashSet::new)
                .insert(dlq_stream.clone());
        }
    }

    dependencies
}

/// Perform topological sort on dependency graph using DFS
///
/// Returns initialization order where dependencies appear before dependents.
/// Detects cycles which should not occur due to Phase 1 validation.
///
/// # Arguments
///
/// * `dependencies` - Dependency graph (target → [sources])
///
/// # Returns
///
/// * `Ok(Vec<String>)` - Initialization order (dependencies first)
/// * `Err(EventFluxError)` - If cycle detected during initialization
fn topological_sort(
    dependencies: &HashMap<String, HashSet<String>>,
) -> Result<Vec<String>, EventFluxError> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut visiting = HashSet::new();

    // Collect all nodes (both sources and targets)
    let mut all_nodes = HashSet::new();
    for (target, sources) in dependencies {
        all_nodes.insert(target.clone());
        all_nodes.extend(sources.iter().cloned());
    }

    for node in &all_nodes {
        if !visited.contains(node) {
            dfs_topo_sort(node, dependencies, &mut visited, &mut visiting, &mut result)?;
        }
    }

    // No need to reverse - DFS adds nodes after visiting dependencies,
    // so they're already in correct topological order (dependencies first)
    Ok(result)
}

/// DFS helper for topological sort
///
/// Recursively visits nodes in depth-first order, detecting cycles.
fn dfs_topo_sort(
    node: &String,
    dependencies: &HashMap<String, HashSet<String>>,
    visited: &mut HashSet<String>,
    visiting: &mut HashSet<String>,
    result: &mut Vec<String>,
) -> Result<(), EventFluxError> {
    if visiting.contains(node) {
        // Cycle detected (should not happen - Phase 1 prevents this)
        return Err(EventFluxError::app_creation(format!(
            "Unexpected cycle detected during initialization: {}",
            node
        )));
    }

    if visited.contains(node) {
        return Ok(());
    }

    visiting.insert(node.clone());

    // Visit all dependencies first
    if let Some(deps) = dependencies.get(node) {
        for dep in deps {
            dfs_topo_sort(dep, dependencies, visited, visiting, result)?;
        }
    }

    visiting.remove(node);
    visited.insert(node.clone());
    result.push(node.clone());

    Ok(())
}

// ============================================================================
// Stream Initialization
// ============================================================================

/// Initialize a single source stream with handler creation
///
/// Creates a fully initialized source with:
/// 1. Source instance from factory
/// 2. Mapper instance from factory
/// 3. Input handler for event processing (from InputManager)
/// 4. DLQ junction wiring (if error.dlq.stream is configured)
/// 5. SourceStreamHandler for lifecycle management
fn initialize_source_stream_with_handler(
    stream_def: &StreamDefinition,
    stream_config: &StreamTypeConfig,
    context: &EventFluxContext,
    input_manager: &crate::core::stream::input::InputManager,
    stream_name: &str,
) -> Result<Arc<SourceStreamHandler>, EventFluxError> {
    // Use existing initialization logic
    let initialized = initialize_source_stream(context, stream_config)?;

    match initialized {
        InitializedStream::Source(mut source) => {
            // Get or create InputHandler for this stream using InputManager
            // This properly integrates with the junction system
            let input_handler =
                input_manager
                    .construct_input_handler(stream_name)
                    .map_err(|e| {
                        EventFluxError::app_creation(format!(
                            "Failed to construct input handler for stream '{}': {}",
                            stream_name, e
                        ))
                    })?;

            // Wire DLQ junction if error.dlq.stream is configured
            // The DLQ stream is already initialized (topological sort ensures dependencies first)
            if let Some(dlq_stream_name) = stream_config.properties.get("error.dlq.stream") {
                // Get the DLQ stream's InputHandler from InputManager
                let dlq_junction = input_manager
                    .construct_input_handler(dlq_stream_name)
                    .map_err(|e| {
                        EventFluxError::app_creation(format!(
                            "Failed to get DLQ stream '{}' input handler for stream '{}': {}",
                            dlq_stream_name, stream_name, e
                        ))
                    })?;

                // Wire the DLQ junction to the source for error handling
                source.source.set_error_dlq_junction(dlq_junction);
            }

            // Create SourceStreamHandler
            let handler = Arc::new(SourceStreamHandler::new(
                source.source,
                source.mapper, // Already Option<Box<dyn SourceMapper>>
                input_handler,
                stream_name.to_string(),
            ));

            Ok(handler)
        }
        _ => Err(EventFluxError::app_creation(
            "Expected source stream initialization",
        )),
    }
}

/// Initialize a single sink stream with handler creation
///
/// Creates a fully initialized sink with:
/// 1. Sink instance from factory
/// 2. Mapper instance from factory (with schema field names for proper JSON output)
/// 3. SinkStreamHandler for lifecycle management
fn initialize_sink_stream_with_handler(
    stream_def: &StreamDefinition,
    stream_config: &StreamTypeConfig,
    context: &EventFluxContext,
    stream_name: &str,
) -> Result<Arc<SinkStreamHandler>, EventFluxError> {
    // Extract field names from stream definition for proper JSON output
    // (e.g., "symbol", "trade_count" instead of "field_0", "field_1")
    let field_names: Vec<String> = stream_def
        .abstract_definition
        .attribute_list
        .iter()
        .map(|attr| attr.name.clone())
        .collect();

    // Use initialization with schema field names
    let initialized = initialize_sink_stream_with_schema(context, stream_config, &field_names)?;

    match initialized {
        InitializedStream::Sink(sink) => {
            // Create SinkStreamHandler
            let handler = Arc::new(SinkStreamHandler::new(
                sink.sink,
                sink.mapper, // Already Option<Box<dyn SinkMapper>>
                stream_name.to_string(),
            ));

            Ok(handler)
        }
        _ => Err(EventFluxError::app_creation(
            "Expected sink stream initialization",
        )),
    }
}

/// Initialize a single stream based on its type
///
/// Delegates to type-specific initialization functions.
/// Returns handlers for registration in the runtime.
fn initialize_single_stream(
    stream_def: &StreamDefinition,
    stream_config: &StreamTypeConfig,
    context: &EventFluxContext,
    input_manager: &crate::core::stream::input::InputManager,
    stream_name: &str,
) -> Result<InitializedStreamHandler, EventFluxError> {
    match stream_config.stream_type {
        StreamType::Source => {
            let handler = initialize_source_stream_with_handler(
                stream_def,
                stream_config,
                context,
                input_manager,
                stream_name,
            )?;

            Ok(InitializedStreamHandler::Source(handler))
        }
        StreamType::Sink => {
            let handler = initialize_sink_stream_with_handler(
                stream_def,
                stream_config,
                context,
                stream_name,
            )?;

            Ok(InitializedStreamHandler::Sink(handler))
        }
        StreamType::Internal => {
            // Internal streams only need junction (no external I/O)
            // Junction creation happens automatically in runtime
            Ok(InitializedStreamHandler::Internal)
        }
    }
}

/// Result of stream handler initialization
pub enum InitializedStreamHandler {
    Source(Arc<SourceStreamHandler>),
    Sink(Arc<SinkStreamHandler>),
    Internal,
}

// ============================================================================
// Full Initialization Flow
// ============================================================================

/// Initialize all streams in topological order
///
/// This is the main entry point for stream initialization during application startup.
///
/// # Flow
///
/// 1. Build dependency graph (query + DLQ dependencies)
/// 2. Validate DLQ stream references (Phase 1 validation)
/// 3. Perform topological sort (dependencies first)
/// 4. Initialize each stream in dependency order
/// 5. Return initialized handlers for runtime registration
///
/// # Arguments
///
/// * `parsed_streams` - Map of stream_name → (StreamDefinition, FlatConfig)
/// * `queries` - All queries defining data flow
/// * `context` - EventFlux context with factory registries
/// * `input_manager` - InputManager for creating input handlers
///
/// # Returns
///
/// * `Ok(StreamHandlers)` - All initialized handlers
/// * `Err(EventFluxError)` - Initialization failed
///
/// # Example
///
/// ```rust,ignore
/// let mut streams = HashMap::new();
/// streams.insert("InputStream".to_string(), (stream_def, config));
///
/// let queries = vec![query];
///
/// let handlers = initialize_streams(streams, queries, &context, &input_manager)?;
/// ```
pub fn initialize_streams(
    parsed_streams: HashMap<String, (StreamDefinition, FlatConfig)>,
    queries: Vec<Query>,
    context: &EventFluxContext,
    input_manager: &crate::core::stream::input::InputManager,
) -> Result<StreamHandlers, EventFluxError> {
    // 1. Build dependency graph
    let dependencies = build_dependency_graph(&parsed_streams, &queries);

    // 2. Validate DLQ stream names exist (Phase 1 validation)
    for (stream_name, (_stream_def, config)) in &parsed_streams {
        if let Some(dlq_stream) = config.get("error.dlq.stream") {
            if !parsed_streams.contains_key(dlq_stream) {
                return Err(EventFluxError::configuration(format!(
                    "DLQ stream '{}' referenced by stream '{}' does not exist",
                    dlq_stream, stream_name
                )));
            }
        }
    }

    // 3. Topological sort
    let init_order = topological_sort(&dependencies)?;

    // 4. Initialize in dependency order
    let mut source_handlers = HashMap::new();
    let mut sink_handlers = HashMap::new();

    for stream_name in &init_order {
        if let Some((stream_def, flat_config)) = parsed_streams.get(stream_name) {
            let stream_config = StreamTypeConfig::from_flat_config(flat_config).map_err(|e| {
                EventFluxError::configuration(format!(
                    "Invalid stream configuration for '{}': {}",
                    stream_name, e
                ))
            })?;

            // Validate DLQ schema if configured (Phase 2 validation)
            if let Some(dlq_stream) = flat_config.get("error.dlq.stream") {
                validate_dlq_schema(stream_name, dlq_stream, &parsed_streams)?;
            }

            match initialize_single_stream(
                stream_def,
                &stream_config,
                context,
                input_manager,
                stream_name,
            )? {
                InitializedStreamHandler::Source(handler) => {
                    source_handlers.insert(stream_name.clone(), handler);
                }
                InitializedStreamHandler::Sink(handler) => {
                    sink_handlers.insert(stream_name.clone(), handler);
                }
                InitializedStreamHandler::Internal => {
                    // Internal streams don't need handlers
                }
            }
        }
    }

    Ok(StreamHandlers {
        source_handlers,
        sink_handlers,
    })
}

/// Collection of initialized stream handlers
pub struct StreamHandlers {
    pub source_handlers: HashMap<String, Arc<SourceStreamHandler>>,
    pub sink_handlers: HashMap<String, Arc<SinkStreamHandler>>,
}

/// Validate DLQ schema compatibility
///
/// **Phase 2 Validation** (Application Initialization)
///
/// Ensures the DLQ stream has the exact required schema for error events.
///
/// # Required DLQ Schema (Exact Match)
///
/// - `originalEvent` STRING
/// - `errorMessage` STRING
/// - `errorType` STRING
/// - `timestamp` LONG
/// - `attemptCount` INT
/// - `streamName` STRING
///
/// # Validation Rules
///
/// - Exact field count (no more, no less)
/// - All required fields present with correct types
/// - No extra fields beyond requirements
fn validate_dlq_schema(
    stream_name: &str,
    dlq_stream: &str,
    parsed_streams: &HashMap<String, (StreamDefinition, FlatConfig)>,
) -> Result<(), EventFluxError> {
    use crate::query_api::definition::attribute::Type as AttributeType;
    use std::collections::HashMap as StdHashMap;

    // Required DLQ fields (same as validation::dlq_validation::REQUIRED_DLQ_FIELDS)
    const REQUIRED_DLQ_FIELDS: &[(&str, AttributeType)] = &[
        ("originalEvent", AttributeType::STRING),
        ("errorMessage", AttributeType::STRING),
        ("errorType", AttributeType::STRING),
        ("timestamp", AttributeType::LONG),
        ("attemptCount", AttributeType::INT),
        ("streamName", AttributeType::STRING),
    ];

    // 1. Look up DLQ stream schema from parsed CREATE STREAM definition
    let (dlq_stream_def, _) = parsed_streams.get(dlq_stream).ok_or_else(|| {
        EventFluxError::configuration(format!(
            "DLQ stream '{}' for stream '{}' does not exist",
            dlq_stream, stream_name
        ))
    })?;

    // 2. Extract actual schema from DLQ stream definition
    let actual_fields: StdHashMap<String, AttributeType> = dlq_stream_def
        .abstract_definition
        .attribute_list
        .iter()
        .map(|attr| (attr.name.clone(), attr.attribute_type))
        .collect();

    // 3. Validate exact field count (no more, no less)
    if actual_fields.len() != REQUIRED_DLQ_FIELDS.len() {
        return Err(EventFluxError::configuration(format!(
            "DLQ stream '{}' has {} fields, but exactly {} required (originalEvent, errorMessage, errorType, timestamp, attemptCount, streamName)",
            dlq_stream,
            actual_fields.len(),
            REQUIRED_DLQ_FIELDS.len()
        )));
    }

    // 4. Validate each required field present with correct type
    for (required_name, required_type) in REQUIRED_DLQ_FIELDS {
        match actual_fields.get(*required_name) {
            Some(actual_type) if actual_type == required_type => {
                // ✅ Field present with correct type
            }
            Some(actual_type) => {
                return Err(EventFluxError::configuration(format!(
                    "DLQ stream '{}' field '{}' has type {:?}, expected {:?}",
                    dlq_stream, required_name, actual_type, required_type
                )));
            }
            None => {
                return Err(EventFluxError::configuration(format!(
                    "DLQ stream '{}' missing required field '{} {:?}'",
                    dlq_stream, required_name, required_type
                )));
            }
        }
    }

    // 5. Check for extra fields not in required schema
    for (actual_name, _) in &actual_fields {
        if !REQUIRED_DLQ_FIELDS
            .iter()
            .any(|(req_name, _)| req_name == actual_name)
        {
            return Err(EventFluxError::configuration(format!(
                "DLQ stream '{}' has extra field '{}' not in required schema",
                dlq_stream, actual_name
            )));
        }
    }

    // ✅ Schema validation passed
    Ok(())
}

// ============================================================================
// Helper Trait Extensions
// ============================================================================

/// Trait for extracting query dependencies
trait QuerySourceExtractor {
    fn get_source_streams(&self) -> Vec<String>;
    fn get_target_stream(&self) -> Option<String>;
}

impl QuerySourceExtractor for Query {
    fn get_source_streams(&self) -> Vec<String> {
        crate::core::validation::query_helpers::QuerySourceExtractor::get_source_streams(self)
    }

    fn get_target_stream(&self) -> Option<String> {
        crate::core::validation::query_helpers::QuerySourceExtractor::get_target_stream(self)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::stream_config::{FlatConfig, PropertySource};
    use crate::core::extension::example_factories::{HttpSinkFactory, KafkaSourceFactory};
    use crate::core::extension::{
        CsvSinkMapperFactory, JsonSinkMapperFactory, JsonSourceMapperFactory,
    };

    #[test]
    fn test_initialize_source_stream_success() {
        let context = EventFluxContext::new();
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));
        context.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));

        let mut config = HashMap::new();
        config.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        config.insert("kafka.topic".to_string(), "test-topic".to_string());

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_ok());

        match result.unwrap() {
            InitializedStream::Source(source) => {
                assert_eq!(source.extension, "kafka");
                assert_eq!(source.format.as_deref(), Some("json"));
            }
            _ => panic!("Expected Source stream"),
        }
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

        let result = initialize_stream(&context, &stream_config);
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
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("xml".to_string()), // Kafka doesn't support XML
            HashMap::new(),
        )
        .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_err());

        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("xml"));
                assert!(message.contains("kafka"));
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
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));
        // Note: json/csv mappers are registered by default, use a non-existent format

        let mut config = HashMap::new();
        config.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        config.insert("kafka.topic".to_string(), "test".to_string());

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("avro".to_string()), // avro mapper not registered
            config,
        )
        .unwrap();

        let result = initialize_stream(&context, &stream_config);
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
        context.add_sink_factory("http".to_string(), Box::new(HttpSinkFactory));
        context.add_sink_mapper_factory("json".to_string(), Box::new(CsvSinkMapperFactory)); // Using CSV as placeholder

        let mut config = HashMap::new();
        config.insert(
            "http.url".to_string(),
            "http://localhost:8080/events".to_string(),
        );

        let stream_config = StreamTypeConfig::new(
            StreamType::Sink,
            Some("http".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_ok());

        match result.unwrap() {
            InitializedStream::Sink(sink) => {
                assert_eq!(sink.extension, "http");
                assert_eq!(sink.format.as_deref(), Some("json"));
            }
            _ => panic!("Expected Sink stream"),
        }
    }

    #[test]
    fn test_initialize_internal_stream() {
        let context = EventFluxContext::new();

        let stream_config =
            StreamTypeConfig::new(StreamType::Internal, None, None, HashMap::new()).unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_ok());

        match result.unwrap() {
            InitializedStream::Internal => {}
            _ => panic!("Expected Internal stream"),
        }
    }

    #[test]
    fn test_initialize_source_stream_invalid_config() {
        let context = EventFluxContext::new();
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));
        context.add_source_mapper_factory("json".to_string(), Box::new(JsonSourceMapperFactory));

        // Missing required kafka.bootstrap.servers
        let config = HashMap::new();

        let stream_config = StreamTypeConfig::new(
            StreamType::Source,
            Some("kafka".to_string()),
            Some("json".to_string()),
            config,
        )
        .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_err());

        // Should get InvalidParameter error from factory (for missing parameter)
        match result.unwrap_err() {
            EventFluxError::InvalidParameter { parameter, .. } => {
                if let Some(param) = parameter {
                    assert!(param.contains("kafka.bootstrap.servers"));
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
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));
        // Don't add mapper factory - we're testing the format requirement validation

        let mut config = HashMap::new();
        config.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        config.insert("kafka.topic".to_string(), "test-topic".to_string());

        // Kafka source WITHOUT format - should be rejected
        let stream_config =
            StreamTypeConfig::new(StreamType::Source, Some("kafka".to_string()), None, config)
                .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_err());

        // Should get Configuration error requiring format
        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("requires a format specification"));
                assert!(message.contains("kafka"));
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
        context.add_sink_factory("http".to_string(), Box::new(HttpSinkFactory));

        let mut config = HashMap::new();
        config.insert(
            "http.url".to_string(),
            "http://localhost:8080/events".to_string(),
        );

        // HTTP sink WITHOUT format - should be rejected
        let stream_config =
            StreamTypeConfig::new(StreamType::Sink, Some("http".to_string()), None, config)
                .unwrap();

        let result = initialize_stream(&context, &stream_config);
        assert!(result.is_err());

        // Should get Configuration error requiring format
        match result.unwrap_err() {
            EventFluxError::Configuration { message, .. } => {
                assert!(message.contains("requires a format specification"));
                assert!(message.contains("http"));
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
    // Topological Sort Tests
    // ========================================================================

    #[test]
    fn test_topological_sort_linear() {
        let mut deps = HashMap::new();
        deps.insert("B".to_string(), HashSet::from(["A".to_string()]));
        deps.insert("C".to_string(), HashSet::from(["B".to_string()]));
        deps.insert("D".to_string(), HashSet::from(["C".to_string()]));

        let order = topological_sort(&deps).unwrap();

        // A should come before B, B before C, C before D
        let a_pos = order.iter().position(|x| x == "A").unwrap();
        let b_pos = order.iter().position(|x| x == "B").unwrap();
        let c_pos = order.iter().position(|x| x == "C").unwrap();
        let d_pos = order.iter().position(|x| x == "D").unwrap();

        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);
        assert!(c_pos < d_pos);
    }

    #[test]
    fn test_topological_sort_with_dlq() {
        let mut deps = HashMap::new();
        deps.insert(
            "Orders".to_string(),
            HashSet::from(["OrderErrors".to_string()]),
        );
        deps.insert(
            "Processed".to_string(),
            HashSet::from(["Orders".to_string()]),
        );

        let order = topological_sort(&deps).unwrap();

        // OrderErrors must be before Orders, Orders before Processed
        let errors_pos = order.iter().position(|x| x == "OrderErrors").unwrap();
        let orders_pos = order.iter().position(|x| x == "Orders").unwrap();
        let processed_pos = order.iter().position(|x| x == "Processed").unwrap();

        assert!(errors_pos < orders_pos);
        assert!(orders_pos < processed_pos);
    }

    #[test]
    fn test_topological_sort_multiple_dependencies() {
        let mut deps = HashMap::new();
        // C depends on both A and B
        deps.insert(
            "C".to_string(),
            HashSet::from(["A".to_string(), "B".to_string()]),
        );

        let order = topological_sort(&deps).unwrap();

        // Both A and B must come before C
        let a_pos = order.iter().position(|x| x == "A").unwrap();
        let b_pos = order.iter().position(|x| x == "B").unwrap();
        let c_pos = order.iter().position(|x| x == "C").unwrap();

        assert!(a_pos < c_pos);
        assert!(b_pos < c_pos);
    }

    #[test]
    fn test_topological_sort_diamond_dependency() {
        let mut deps = HashMap::new();
        // Diamond: D depends on B and C, B and C both depend on A
        deps.insert("B".to_string(), HashSet::from(["A".to_string()]));
        deps.insert("C".to_string(), HashSet::from(["A".to_string()]));
        deps.insert(
            "D".to_string(),
            HashSet::from(["B".to_string(), "C".to_string()]),
        );

        let order = topological_sort(&deps).unwrap();

        // A must be before B and C, B and C must be before D
        let a_pos = order.iter().position(|x| x == "A").unwrap();
        let b_pos = order.iter().position(|x| x == "B").unwrap();
        let c_pos = order.iter().position(|x| x == "C").unwrap();
        let d_pos = order.iter().position(|x| x == "D").unwrap();

        assert!(a_pos < b_pos);
        assert!(a_pos < c_pos);
        assert!(b_pos < d_pos);
        assert!(c_pos < d_pos);
    }

    #[test]
    fn test_topological_sort_cycle_detection() {
        let mut deps = HashMap::new();
        // Create a cycle: A -> B -> C -> A
        deps.insert("A".to_string(), HashSet::from(["C".to_string()]));
        deps.insert("B".to_string(), HashSet::from(["A".to_string()]));
        deps.insert("C".to_string(), HashSet::from(["B".to_string()]));

        let result = topological_sort(&deps);
        assert!(result.is_err());

        // Should get an error about cycle detection
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("cycle") || err_msg.contains("Cycle"));
    }

    #[test]
    fn test_topological_sort_empty_graph() {
        let deps = HashMap::new();
        let order = topological_sort(&deps).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_topological_sort_independent_nodes() {
        let mut deps = HashMap::new();
        deps.insert("A".to_string(), HashSet::new());
        deps.insert("B".to_string(), HashSet::new());
        deps.insert("C".to_string(), HashSet::new());

        let order = topological_sort(&deps).unwrap();
        assert_eq!(order.len(), 3);
        assert!(order.contains(&"A".to_string()));
        assert!(order.contains(&"B".to_string()));
        assert!(order.contains(&"C".to_string()));
    }

    // ========================================================================
    // Dependency Graph Tests
    // ========================================================================

    #[test]
    fn test_build_dependency_graph_query_only() {
        use crate::query_api::execution::query::input::stream::{InputStream, SingleInputStream};
        use crate::query_api::execution::query::output::output_stream::{
            InsertIntoStreamAction, OutputStream, OutputStreamAction,
        };
        use crate::query_api::execution::query::selection::Selector;

        let mut parsed_streams = HashMap::new();
        let stream_a = StreamDefinition::new("A".to_string());
        let stream_b = StreamDefinition::new("B".to_string());

        parsed_streams.insert("A".to_string(), (stream_a, FlatConfig::new()));
        parsed_streams.insert("B".to_string(), (stream_b, FlatConfig::new()));

        // Create query: FROM A INSERT INTO B
        let input_stream = InputStream::Single(SingleInputStream::new_basic(
            "A".to_string(),
            false,
            false,
            None,
            Vec::new(),
        ));

        let output_stream = OutputStream {
            eventflux_element: Default::default(),
            action: OutputStreamAction::InsertInto(InsertIntoStreamAction {
                target_id: "B".to_string(),
                is_inner_stream: false,
                is_fault_stream: false,
            }),
            output_event_type: None,
        };

        let query = Query {
            eventflux_element: Default::default(),
            input_stream: Some(input_stream),
            selector: Selector::new(),
            output_stream,
            output_rate: None,
            annotations: Vec::new(),
        };

        let queries = vec![query];
        let deps = build_dependency_graph(&parsed_streams, &queries);

        // B depends on A
        assert_eq!(deps.len(), 1);
        assert!(deps.get("B").unwrap().contains("A"));
    }

    #[test]
    fn test_build_dependency_graph_with_dlq() {
        let mut parsed_streams = HashMap::new();

        let stream_orders = StreamDefinition::new("Orders".to_string());
        let stream_errors = StreamDefinition::new("OrderErrors".to_string());

        let mut config_orders = FlatConfig::new();
        config_orders.set(
            "error.dlq.stream",
            "OrderErrors",
            PropertySource::TomlStream,
        );

        parsed_streams.insert("Orders".to_string(), (stream_orders, config_orders));
        parsed_streams.insert(
            "OrderErrors".to_string(),
            (stream_errors, FlatConfig::new()),
        );

        let queries = vec![];
        let deps = build_dependency_graph(&parsed_streams, &queries);

        // Orders depends on OrderErrors (DLQ dependency)
        assert_eq!(deps.len(), 1);
        assert!(deps.get("Orders").unwrap().contains("OrderErrors"));
    }

    #[test]
    fn test_build_dependency_graph_combined() {
        use crate::query_api::execution::query::input::stream::{InputStream, SingleInputStream};
        use crate::query_api::execution::query::output::output_stream::{
            InsertIntoStreamAction, OutputStream, OutputStreamAction,
        };
        use crate::query_api::execution::query::selection::Selector;

        let mut parsed_streams = HashMap::new();

        let stream_a = StreamDefinition::new("A".to_string());
        let stream_b = StreamDefinition::new("B".to_string());
        let stream_errors = StreamDefinition::new("Errors".to_string());

        let mut config_b = FlatConfig::new();
        config_b.set("error.dlq.stream", "Errors", PropertySource::TomlStream);

        parsed_streams.insert("A".to_string(), (stream_a, FlatConfig::new()));
        parsed_streams.insert("B".to_string(), (stream_b, config_b));
        parsed_streams.insert("Errors".to_string(), (stream_errors, FlatConfig::new()));

        // Create query: FROM A INSERT INTO B
        let input_stream = InputStream::Single(SingleInputStream::new_basic(
            "A".to_string(),
            false,
            false,
            None,
            Vec::new(),
        ));

        let output_stream = OutputStream {
            eventflux_element: Default::default(),
            action: OutputStreamAction::InsertInto(InsertIntoStreamAction {
                target_id: "B".to_string(),
                is_inner_stream: false,
                is_fault_stream: false,
            }),
            output_event_type: None,
        };

        let query = Query {
            eventflux_element: Default::default(),
            input_stream: Some(input_stream),
            selector: Selector::new(),
            output_stream,
            output_rate: None,
            annotations: Vec::new(),
        };

        let queries = vec![query];
        let deps = build_dependency_graph(&parsed_streams, &queries);

        // B depends on both A (query) and Errors (DLQ)
        assert_eq!(deps.len(), 1);
        let b_deps = deps.get("B").unwrap();
        assert_eq!(b_deps.len(), 2);
        assert!(b_deps.contains("A"));
        assert!(b_deps.contains("Errors"));
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

        let result = initialize_table(&context, &table_config, "TestTable");
        assert!(result.is_ok());

        match result.unwrap() {
            InitializedStream::Table(table) => {
                assert_eq!(table.extension, "inMemory");
                // Table is Arc<dyn Table> for shared ownership
            }
            _ => panic!("Expected Table initialization"),
        }
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

        let result = initialize_table(&context, &table_config, "PropertiesTable");
        assert!(result.is_ok(), "Table initialization failed: {:?}", result);

        match result.unwrap() {
            InitializedStream::Table(table) => {
                assert_eq!(table.extension, "inMemory");
            }
            _ => panic!("Expected Table initialization"),
        }
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
        let result = initialize_table(&context, &table_config, "ConfigTable");
        assert!(result.is_ok());

        match result.unwrap() {
            InitializedStream::Table(table) => {
                assert_eq!(table.extension, "inMemory");
            }
            _ => panic!("Expected Table initialization"),
        }
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
