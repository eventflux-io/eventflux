// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/parser/eventflux_app_parser.rs
// Corresponds to io.eventflux.core.util.parser.EventFluxAppParser
use std::collections::HashMap;
use std::sync::{Arc, Mutex}; // Added Mutex // If QueryParser needs table_map etc. from builder

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext; // QueryParser will need this
use crate::core::config::ApplicationConfig;
use crate::core::eventflux_app_runtime_builder::EventFluxAppRuntimeBuilder;
use crate::core::stream::junction_factory::{JunctionConfig, StreamJunctionFactory}; // For creating junctions
use crate::core::window::WindowRuntime;
use crate::query_api::{
    definition::{Attribute as ApiAttribute, StreamDefinition as ApiStreamDefinition}, // For fault stream creation
    // Other API definitions will be needed by specific parsers (Table, Window etc.)
    execution::ExecutionElement as ApiExecutionElement,
    EventFluxApp as ApiEventFluxApp,
};
// use super::query_parser::QueryParser; // To be created or defined in this file for now
use crate::core::partition::parser::PartitionParser;
// use super::definition_parser_helpers::*; // For defineStreamDefinitions, defineTableDefinitions etc.
use super::query_parser::QueryParser; // Use the real QueryParser implementation
use super::trigger_parser::TriggerParser;
// Core constants, if any, vs query_api constants

pub struct EventFluxAppParser;

impl EventFluxAppParser {
    // Corresponds to EventFluxAppParser.parse(EventFluxApp eventfluxApp, String eventfluxAppString, EventFluxContext eventfluxContext)
    // The eventfluxAppString is already in EventFluxAppContext. EventFluxContext is also in EventFluxAppContext.
    /// Parse EventFlux application and build runtime components
    ///
    /// This method orchestrates the parsing of all application components in the correct order:
    /// 1. Stream definitions and junctions
    /// 2. Table definitions
    /// 3. Window definitions
    /// 4. Aggregation definitions
    /// 5. Execution elements (queries, partitions, triggers)
    pub fn parse_eventflux_app_runtime_builder(
        api_eventflux_app: &ApiEventFluxApp,
        eventflux_app_context: Arc<EventFluxAppContext>,
        application_config: Option<ApplicationConfig>,
    ) -> Result<EventFluxAppRuntimeBuilder, String> {
        let mut builder = EventFluxAppRuntimeBuilder::new(
            eventflux_app_context.clone(),
            application_config.clone(),
        );

        // Get default async mode from configuration (YAML/TOML)
        let default_stream_async = eventflux_app_context
            .get_eventflux_context()
            .get_default_async_mode();

        // Process components in dependency order
        Self::process_stream_definitions(
            api_eventflux_app,
            &mut builder,
            &eventflux_app_context,
            default_stream_async,
        )?;

        Self::process_table_definitions(api_eventflux_app, &mut builder, &eventflux_app_context)?;

        Self::process_window_definitions(api_eventflux_app, &mut builder, &eventflux_app_context)?;

        Self::process_aggregation_definitions(
            api_eventflux_app,
            &mut builder,
            &eventflux_app_context,
        )?;

        Self::process_execution_elements(api_eventflux_app, &mut builder, &eventflux_app_context)?;

        Ok(builder)
    }

    /// Process stream definitions and create StreamJunctions
    fn process_stream_definitions(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
        default_stream_async: bool,
    ) -> Result<(), String> {
        for (stream_id, stream_def_arc) in &api_eventflux_app.stream_definition_map {
            builder.add_stream_definition(Arc::clone(stream_def_arc));

            let mut config = JunctionConfig::new(stream_id.clone())
                .with_buffer_size(eventflux_app_context.buffer_size as usize)
                .with_async(default_stream_async);

            // Check for SQL WITH async properties
            if let Some(with_config) = &stream_def_arc.with_config {
                // async.buffer_size property
                if let Some(buffer_size_str) = with_config.get("async.buffer_size") {
                    if let Ok(sz) = buffer_size_str.parse::<usize>() {
                        config = config.with_buffer_size(sz);
                    }
                }

                // async.workers property
                if let Some(workers_str) = with_config.get("async.workers") {
                    if let Ok(workers) = workers_str.parse::<u64>() {
                        let estimated_throughput = workers * 10000; // 10K events/worker estimate
                        config = config.with_expected_throughput(estimated_throughput);
                    }
                }

                // async.enabled property
                if let Some(async_str) = with_config.get("async.enabled") {
                    if async_str.eq_ignore_ascii_case("true") {
                        config = config.with_async(true);
                    }
                }
            }

            // Create fault stream if requested via SQL WITH fault.stream='true'
            let create_fault_stream = stream_def_arc
                .with_config
                .as_ref()
                .and_then(|cfg| cfg.get("fault.stream"))
                .map(|val| val.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

            if create_fault_stream {
                let mut fault_def = ApiStreamDefinition::new(format!(
                    "{}{}",
                    crate::query_api::constants::FAULT_STREAM_FLAG,
                    stream_id
                ));
                for attr in &stream_def_arc.abstract_definition.attribute_list {
                    fault_def
                        .abstract_definition
                        .attribute_list
                        .push(attr.clone());
                }
                fault_def
                    .abstract_definition
                    .attribute_list
                    .push(ApiAttribute::new(
                        "_error".to_string(),
                        crate::query_api::definition::attribute::Type::OBJECT,
                    ));
                builder.add_stream_definition(Arc::new(fault_def));
            }

            // Create StreamJunction with async configuration from SQL WITH or YAML
            // Preserve all tuning hints collected from WITH clause
            let mut junction_config = JunctionConfig::new(stream_id.clone())
                .with_buffer_size(config.buffer_size)
                .with_async(config.is_async);

            // Preserve expected_throughput hint if set (from async.workers)
            if let Some(throughput) = config.expected_throughput {
                junction_config = junction_config.with_expected_throughput(throughput);
            }

            // Preserve subscriber_count hint if set
            if let Some(count) = config.subscriber_count {
                junction_config = junction_config.with_subscriber_count(count);
            }

            let stream_junction = StreamJunctionFactory::create(
                junction_config,
                Arc::clone(stream_def_arc),
                eventflux_app_context.clone(),
                None,
            )
            .map_err(|e| {
                format!(
                    "Failed to create stream junction for '{}': {}",
                    stream_id, e
                )
            })?;

            builder.add_stream_junction(stream_id.clone(), stream_junction);
        }

        Ok(())
    }

    /// Process table definitions and create table instances
    fn process_table_definitions(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        for (table_id, table_def) in &api_eventflux_app.table_definition_map {
            builder.add_table_definition(Arc::clone(table_def));

            // Extract table type and properties from SQL WITH clause
            let mut props = HashMap::new();
            let table_type: Option<String>;

            if let Some(with_config) = &table_def.with_config {
                // Extract extension (table type) from WITH clause
                table_type = with_config.get("extension").cloned();

                // Copy all WITH properties
                for (key, value) in with_config.properties() {
                    props.insert(key.clone(), value.clone());
                }
            } else {
                // No SQL WITH configuration - use default InMemoryTable
                table_type = None;
            }

            // Create table based on type
            let table: Arc<dyn crate::core::table::Table> = if let Some(t_type) = table_type {
                // Try registered factory first
                if let Some(factory) = eventflux_app_context
                    .get_eventflux_context()
                    .get_table_factory(&t_type)
                {
                    factory.create(
                        table_id.clone(),
                        props.clone(),
                        eventflux_app_context.get_eventflux_context(),
                    )?
                }
                // Built-in JDBC table
                else if t_type == "jdbc" {
                    let ds = props.get("data_source").cloned().unwrap_or_default();
                    Arc::new(crate::core::table::JdbcTable::new(
                        table_id.clone(),
                        ds,
                        eventflux_app_context.get_eventflux_context(),
                    )?)
                }
                // Built-in cache table
                else if t_type == "cache" || t_type == "inMemory" {
                    Arc::new(crate::core::table::InMemoryTable::new())
                } else {
                    // Unknown extension type - error instead of silent fallback
                    return Err(format!(
                        "Unknown table extension '{}' for table '{}'. Available extensions: registered factories, 'jdbc', 'cache', 'inMemory'. \
                         Check for typos or register the extension factory.",
                        t_type, table_id
                    ));
                }
            } else {
                // No extension specified - error instead of silent fallback
                return Err(format!(
                    "Table '{}' requires explicit extension specification for durability safety. \
                     Use SQL WITH (extension='<type>') or register in configuration. \
                     Available types: 'jdbc', 'cache', 'inMemory', or custom registered extensions.",
                    table_id
                ));
            };

            eventflux_app_context
                .get_eventflux_context()
                .add_table(table_id.clone(), table);

            builder.add_table(
                table_id.clone(),
                Arc::new(Mutex::new(
                    crate::core::eventflux_app_runtime_builder::TableRuntimePlaceholder::default(),
                )),
            );
        }

        Ok(())
    }

    /// Process window definitions and create window runtimes
    fn process_window_definitions(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        for (window_id, window_def) in &api_eventflux_app.window_definition_map {
            builder.add_window_definition(Arc::clone(window_def));
            let mut runtime = WindowRuntime::new(Arc::clone(window_def));
            if let Some(handler) = &window_def.window_handler {
                let qctx = Arc::new(EventFluxQueryContext::new(
                    Arc::clone(eventflux_app_context),
                    format!("__window_{window_id}"),
                    None,
                ));
                // Create minimal parse context for legacy WindowDefinition path
                let empty_parse_ctx =
                    crate::core::util::parser::expression_parser::ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&qctx),
                        stream_meta_map: std::collections::HashMap::new(),
                        table_meta_map: std::collections::HashMap::new(),
                        window_meta_map: std::collections::HashMap::new(),
                        aggregation_meta_map: std::collections::HashMap::new(),
                        state_meta_map: std::collections::HashMap::new(),
                        stream_positions: std::collections::HashMap::new(),
                        default_source: String::new(),
                        query_name: &format!("__window_{window_id}"),
                        is_mutation_context: false,
                    };
                if let Ok(proc) =
                    crate::core::query::processor::stream::window::create_window_processor(
                        handler,
                        Arc::clone(eventflux_app_context),
                        Arc::clone(&qctx),
                        &empty_parse_ctx,
                    )
                {
                    runtime.set_processor(proc);
                }
            }
            builder.add_window(window_id.clone(), Arc::new(Mutex::new(runtime)));
        }

        // Initialize Windows after all are created
        for win_rt in builder.window_map.values() {
            win_rt.lock().unwrap().initialize();
        }

        Ok(())
    }

    /// Process aggregation definitions and wire up input processors
    fn process_aggregation_definitions(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        for (agg_id, agg_def) in &api_eventflux_app.aggregation_definition_map {
            builder.add_aggregation_definition(Arc::clone(agg_def));
            let runtime = Arc::new(Mutex::new(
                crate::core::aggregation::AggregationRuntime::new(agg_id.clone(), HashMap::new()),
            ));
            builder.add_aggregation_runtime(agg_id.clone(), Arc::clone(&runtime));

            if let Some(stream) = &agg_def.basic_single_input_stream {
                let input_id = stream.get_stream_id_str().to_string();
                if let Some(junction) = builder.stream_junction_map.get(&input_id) {
                    let qctx = Arc::new(EventFluxQueryContext::new(
                        Arc::clone(eventflux_app_context),
                        format!("__aggregation_{agg_id}"),
                        None,
                    ));
                    let proc = Arc::new(Mutex::new(
                        crate::core::aggregation::AggregationInputProcessor::new(
                            Arc::clone(&runtime),
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&qctx),
                        ),
                    ));
                    junction.lock().unwrap().subscribe(proc);
                }
            }
        }

        Ok(())
    }

    /// Process execution elements (queries and partitions) and triggers
    fn process_execution_elements(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        // Process triggers FIRST so their stream junctions exist when queries reference them
        // This enables queries like: SELECT ... FROM TriggerName
        for trig_def in api_eventflux_app.trigger_definition_map.values() {
            let runtime = TriggerParser::parse(builder, trig_def, eventflux_app_context)?;
            builder.add_trigger_runtime(Arc::new(runtime));
        }

        // Parse Execution Elements (Queries, Partitions)
        // Use counters to provide deterministic indices for stable component IDs (needed for state recovery)
        let mut query_index = 0;
        let mut partition_index = 0;
        for exec_element in &api_eventflux_app.execution_element_list {
            match exec_element {
                ApiExecutionElement::Query(api_query) => {
                    // The QueryParser needs access to various maps (stream_junctions, tables, windows, aggregations)
                    // from the builder to resolve references.
                    let query_runtime = QueryParser::parse_query(
                        api_query,
                        eventflux_app_context,
                        &builder.stream_junction_map,
                        &builder.table_definition_map,
                        &builder.aggregation_map,
                        None,
                        query_index,
                    )?;
                    builder.add_query_runtime(Arc::new(query_runtime));
                    query_index += 1; // Increment for next query
                                      // TODO: eventflux_app_context.addEternalReferencedHolder(queryRuntime);
                }
                ApiExecutionElement::Partition(api_partition) => {
                    let part_rt = PartitionParser::parse(
                        builder,
                        api_partition,
                        eventflux_app_context,
                        partition_index,
                    )?;
                    builder.add_partition_runtime(Arc::new(part_rt));
                    partition_index += 1; // Increment for next partition
                }
            }
        }

        Ok(())
    }
}
