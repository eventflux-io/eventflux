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

// Corresponds to io.eventflux.core.util.parser.EventFluxAppParser
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::config::ApplicationConfig;
use crate::core::eventflux_app_runtime_builder::EventFluxAppRuntimeBuilder;
use crate::core::stream::junction_factory::{JunctionConfig, StreamJunctionFactory};
use crate::core::window::WindowRuntime;
use crate::query_api::{
    definition::{Attribute as ApiAttribute, StreamDefinition as ApiStreamDefinition}, // For fault streams
    execution::ExecutionElement as ApiExecutionElement,
    EventFluxApp as ApiEventFluxApp,
};
// use super::query_parser::QueryParser; // To be created or defined in this file for now
use crate::core::partition::parser::PartitionParser;
// use super::definition_parser_helpers::*; // For defineStreamDefinitions, defineTableDefinitions etc.
use super::query_parser::QueryParser;
use super::trigger_parser::TriggerParser;

/// Turns a parsed EventFlux application into a runtime builder.
pub struct EventFluxAppParser;

impl EventFluxAppParser {
    // Corresponds to Java EventFluxAppParser.parse(EventFluxApp eventfluxApp, String
    // eventfluxAppString, EventFluxContext eventfluxContext). The app string and the
    // EventFluxContext both come from the EventFluxAppContext here.
    /// Parses an EventFlux application and builds its runtime components
    ///
    /// Builds everything in dependency order:
    /// 1. Stream definitions and junctions
    /// 2. Table definitions
    /// 3. Window definitions
    /// 4. Aggregation definitions
    /// 5. Triggers, then execution elements (queries and partitions)
    ///
    /// # Errors
    ///
    /// Returns an error if a stream junction, table, trigger, query or partition cannot
    /// be built. A table must name its `extension` in SQL WITH.
    pub fn parse_eventflux_app_runtime_builder(
        api_eventflux_app: &ApiEventFluxApp,
        eventflux_app_context: Arc<EventFluxAppContext>,
        application_config: Option<ApplicationConfig>,
    ) -> Result<EventFluxAppRuntimeBuilder, String> {
        let mut builder = EventFluxAppRuntimeBuilder::new(
            eventflux_app_context.clone(),
            application_config.clone(),
        );

        // Default async mode from the configuration (YAML/TOML)
        let default_stream_async = eventflux_app_context
            .get_eventflux_context()
            .get_default_async_mode();

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

    /// Registers each stream definition and creates its StreamJunction.
    ///
    /// Reads these SQL WITH properties: `async.buffer_size`, `async.workers` (turned into
    /// a throughput hint of 10K events per worker), `async.enabled` (only "true" has an
    /// effect, it cannot turn off the default) and `fault.stream`. Values that do not
    /// parse are ignored.
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

            if let Some(with_config) = &stream_def_arc.with_config {
                if let Some(buffer_size_str) = with_config.get("async.buffer_size") {
                    if let Ok(sz) = buffer_size_str.parse::<usize>() {
                        config = config.with_buffer_size(sz);
                    }
                }

                if let Some(workers_str) = with_config.get("async.workers") {
                    if let Ok(workers) = workers_str.parse::<u64>() {
                        let estimated_throughput = workers * 10000; // Rough 10K events per worker
                        config = config.with_expected_throughput(estimated_throughput);
                    }
                }

                if let Some(async_str) = with_config.get("async.enabled") {
                    if async_str.eq_ignore_ascii_case("true") {
                        config = config.with_async(true);
                    }
                }
            }

            // With fault.stream='true', also define a fault stream named
            // FAULT_STREAM_FLAG + stream id. It has the same attributes plus an `_error`
            // OBJECT attribute.
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

            // Build the junction config from the settings above, keeping every tuning hint
            // taken from the WITH clause
            let mut junction_config = JunctionConfig::new(stream_id.clone())
                .with_buffer_size(config.buffer_size)
                .with_async(config.is_async);

            // Set from async.workers
            if let Some(throughput) = config.expected_throughput {
                junction_config = junction_config.with_expected_throughput(throughput);
            }

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

    /// Creates each table and registers it in the EventFluxContext.
    ///
    /// The table type comes from `extension` in SQL WITH. A registered table factory wins,
    /// then the built in "jdbc" (reads `data_source`), "cache" and "inMemory" types.
    ///
    /// # Errors
    ///
    /// Returns an error if `extension` is missing or unknown, or if the table cannot be
    /// created. There is no silent fallback to an in memory table.
    fn process_table_definitions(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        for (table_id, table_def) in &api_eventflux_app.table_definition_map {
            builder.add_table_definition(Arc::clone(table_def));

            // Table type and properties from the SQL WITH clause
            let mut props = HashMap::new();
            let table_type: Option<String>;

            if let Some(with_config) = &table_def.with_config {
                table_type = with_config.get("extension").cloned();

                // Pass every WITH property to the table
                for (key, value) in with_config.properties() {
                    props.insert(key.clone(), value.clone());
                }
            } else {
                // No SQL WITH clause, so no type. This is rejected below.
                table_type = None;
            }

            let table: Arc<dyn crate::core::table::Table> = if let Some(t_type) = table_type {
                // A registered factory wins over the built in types
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
                // Built in JDBC table
                else if t_type == "jdbc" {
                    let ds = props.get("data_source").cloned().unwrap_or_default();
                    Arc::new(crate::core::table::JdbcTable::new(
                        table_id.clone(),
                        ds,
                        eventflux_app_context.get_eventflux_context(),
                    )?)
                }
                // Built in cache table. "cache" and "inMemory" both give an InMemoryTable.
                else if t_type == "cache" || t_type == "inMemory" {
                    Arc::new(crate::core::table::InMemoryTable::new())
                } else {
                    // Unknown type. Fail instead of silently falling back.
                    return Err(format!(
                        "Unknown table extension '{}' for table '{}'. Available extensions: registered factories, 'jdbc', 'cache', 'inMemory'. \
                         Check for typos or register the extension factory.",
                        t_type, table_id
                    ));
                }
            } else {
                // No type given. Fail instead of silently falling back.
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

    /// Creates a WindowRuntime for each window definition, then initializes them all.
    ///
    /// If the window processor cannot be created, the error is dropped and the runtime
    /// is kept without a processor.
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
                // Empty parse context for the older WindowDefinition path
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

        // Initialize only after every window exists
        for win_rt in builder.window_map.values() {
            win_rt.lock().unwrap().initialize();
        }

        Ok(())
    }

    /// Creates an AggregationRuntime for each aggregation and subscribes an input
    /// processor to its input stream junction. If that junction does not exist, the
    /// runtime is created but gets no input.
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

    /// Builds triggers, then the execution elements (queries and partitions)
    fn process_execution_elements(
        api_eventflux_app: &ApiEventFluxApp,
        builder: &mut EventFluxAppRuntimeBuilder,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String> {
        // Triggers go FIRST so their stream junctions exist when queries refer to them,
        // as in SELECT ... FROM TriggerName
        for trig_def in api_eventflux_app.trigger_definition_map.values() {
            let runtime = TriggerParser::parse(builder, trig_def, eventflux_app_context)?;
            builder.add_trigger_runtime(Arc::new(runtime));
        }

        // Count queries and partitions separately so each gets the same index on every
        // run. Component ids are built from it and must stay stable for state recovery.
        let mut query_index = 0;
        let mut partition_index = 0;
        for exec_element in &api_eventflux_app.execution_element_list {
            match exec_element {
                ApiExecutionElement::Query(api_query) => {
                    // QueryParser resolves references through the builder maps of stream
                    // junctions, table definitions and aggregations
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
                    query_index += 1;
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
                    partition_index += 1;
                }
            }
        }

        Ok(())
    }
}
