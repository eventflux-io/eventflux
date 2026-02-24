// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/parser/query_parser.rs
use super::expression_parser::{parse_expression, ExpressionParserContext};
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
use crate::core::query::input::stream::join::{JoinProcessor, JoinSide, TableJoinProcessor};
use crate::core::query::output::insert_into_stream_processor::InsertIntoStreamProcessor;
use crate::core::query::processor::stream::filter::FilterProcessor;
use crate::core::query::processor::stream::window::create_window_processor;
use crate::core::query::processor::Processor; // Trait
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::query::selector::attribute::OutputAttributeProcessor; // OAP
use crate::core::query::selector::select_processor::{OutputRateLimiter, SelectProcessor};
use crate::core::query::selector::{GroupByKeyGenerator, OrderByEventComparator};
use crate::core::stream::stream_junction::StreamJunction;
use crate::query_api::{
    definition::Attribute as ApiAttribute, // For constructing output attributes
    definition::StreamDefinition as ApiStreamDefinition,
    execution::query::input::InputStream as ApiInputStream,
    execution::query::Query as ApiQuery,
    expression::Expression as ApiExpression, // Added this import
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use rand; // For generating random names, if not using query name from annotation

use crate::core::event::complex_event::ComplexEvent;
use crate::core::query::input::stream::state::PostStateProcessor;
use crate::core::query::input::stream::state::PreStateProcessor;

/// TerminalPostStateProcessor - bridges PostStateProcessor chain to Processor chain
///
/// This processor sits at the end of a pattern chain and:
/// 1. Receives completed StateEvent from the last pattern PostStateProcessor
/// 2. Flattens all StreamEvents into a single StreamEvent
/// 3. Forwards the flattened event to the Processor chain (SelectProcessor, etc.)
#[derive(Debug)]
struct TerminalPostStateProcessor {
    state_id: usize,
    next_processor: Option<Arc<Mutex<dyn PostStateProcessor>>>,
    output_processor: Option<Arc<Mutex<dyn Processor>>>,
    is_event_returned: bool,
    total_attr_count: usize,
}

impl TerminalPostStateProcessor {
    fn new(state_id: usize, total_attr_count: usize) -> Self {
        Self {
            state_id,
            next_processor: None,
            output_processor: None,
            is_event_returned: false,
            total_attr_count,
        }
    }

    fn set_output_processor(&mut self, processor: Arc<Mutex<dyn Processor>>) {
        self.output_processor = Some(processor);
    }

    /// Flatten StateEvent into a single StreamEvent
    /// Copies all attributes from each position's StreamEvent
    fn flatten_state_event(
        &self,
        state_event: &crate::core::event::state::state_event::StateEvent,
    ) -> crate::core::event::stream::stream_event::StreamEvent {
        use crate::core::event::stream::stream_event::StreamEvent;
        use crate::core::event::value::AttributeValue;

        // Create flattened before_window_data by concatenating all positions
        let mut flattened_data: Vec<AttributeValue> = Vec::with_capacity(self.total_attr_count);
        let mut timestamp = state_event.timestamp;

        for i in 0..state_event.stream_event_count() {
            if let Some(stream_event) = state_event.get_stream_event(i) {
                // Copy attributes from this position's StreamEvent
                for attr in &stream_event.before_window_data {
                    flattened_data.push(attr.clone());
                }
                // Use first valid timestamp if not set
                if timestamp < 0 && stream_event.timestamp >= 0 {
                    timestamp = stream_event.timestamp;
                }
            }
        }

        // Create the flattened StreamEvent
        let mut result = StreamEvent::new_with_data(timestamp, flattened_data);
        result.event_type = state_event.event_type;
        result
    }
}

impl PostStateProcessor for TerminalPostStateProcessor {
    fn process(&mut self, chunk: Option<Box<dyn ComplexEvent>>) -> Option<Box<dyn ComplexEvent>> {
        if let Some(event) = chunk {
            if let Some(state_event) = event
                .as_any()
                .downcast_ref::<crate::core::event::state::state_event::StateEvent>(
            ) {
                // Flatten the StateEvent to a StreamEvent
                let flattened = self.flatten_state_event(state_event);

                // Forward to the output processor (Processor chain)
                if let Some(ref output) = self.output_processor {
                    self.is_event_returned = true;
                    output.lock().unwrap().process(Some(Box::new(flattened)));
                }
            }
        }
        None // Terminal - doesn't return events via PostStateProcessor chain
    }

    fn set_next_processor(&mut self, processor: Arc<Mutex<dyn PostStateProcessor>>) {
        self.next_processor = Some(processor);
    }

    fn get_next_processor(&self) -> Option<Arc<Mutex<dyn PostStateProcessor>>> {
        self.next_processor.clone()
    }

    fn state_id(&self) -> usize {
        self.state_id
    }

    fn set_next_state_pre_processor(&mut self, _next: Arc<Mutex<dyn PreStateProcessor>>) {
        // Terminal processor - no next state
    }

    fn set_next_every_state_pre_processor(&mut self, _next: Arc<Mutex<dyn PreStateProcessor>>) {
        // Terminal processor - doesn't loop back
    }

    fn set_callback_pre_state_processor(&mut self, _callback: Arc<Mutex<dyn PreStateProcessor>>) {
        // Terminal processor - no callback
    }

    fn get_next_every_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        None
    }

    fn is_event_returned(&self) -> bool {
        self.is_event_returned
    }

    fn clear_processed_event(&mut self) {
        self.is_event_returned = false;
    }

    fn this_state_pre_processor(&self) -> Option<Arc<Mutex<dyn PreStateProcessor>>> {
        None
    }
}

pub struct QueryParser;

impl QueryParser {
    /// Test helper that calls parse_query with query_index = 0
    ///
    /// This is available in both library and integration tests.
    #[doc(hidden)]
    pub fn parse_query_test(
        api_query: &ApiQuery,
        eventflux_app_context: &Arc<EventFluxAppContext>,
        stream_junction_map: &HashMap<String, Arc<Mutex<StreamJunction>>>,
        table_def_map: &HashMap<String, Arc<crate::query_api::definition::TableDefinition>>,
        aggregation_map: &HashMap<String, Arc<Mutex<crate::core::aggregation::AggregationRuntime>>>,
        partition_id: Option<String>,
    ) -> Result<QueryRuntime, String> {
        Self::parse_query(
            api_query,
            eventflux_app_context,
            stream_junction_map,
            table_def_map,
            aggregation_map,
            partition_id,
            0,
        )
    }

    pub fn parse_query(
        api_query: &ApiQuery,
        eventflux_app_context: &Arc<EventFluxAppContext>,
        stream_junction_map: &HashMap<String, Arc<Mutex<StreamJunction>>>,
        table_def_map: &HashMap<String, Arc<crate::query_api::definition::TableDefinition>>,
        aggregation_map: &HashMap<String, Arc<Mutex<crate::core::aggregation::AggregationRuntime>>>,
        partition_id: Option<String>,
        query_index: usize,
    ) -> Result<QueryRuntime, String> {
        // 1. Determine Query Name (from @info(name='foo') or generate)
        // Use deterministic index-based naming for state recovery compatibility
        // Include partition_id in default name to prevent collisions between
        // top-level queries (query_0) and partition queries (partition_0_query_0, partition_1_query_0)
        // Each partition receives a unique partition_id (e.g., "partition_0", "partition_1", or custom @info name)
        let query_name = api_query
            .annotations
            .iter()
            .find(|ann| ann.name == "info")
            .and_then(|ann| ann.elements.iter().find(|el| el.key == "name"))
            .map(|el| el.value.clone())
            .unwrap_or_else(|| {
                if let Some(ref pid) = partition_id {
                    format!("{}_query_{}", pid, query_index)
                } else {
                    format!("query_{}", query_index)
                }
            });

        let mut eventflux_query_context = EventFluxQueryContext::new(
            Arc::clone(eventflux_app_context),
            query_name.clone(),
            partition_id.clone(),
        );
        if partition_id.is_some() {
            eventflux_query_context.set_partitioned(true);
        }
        let eventflux_query_context = Arc::new(eventflux_query_context);

        // 2. Identify input stream & get its junction
        let input_stream_api = api_query
            .input_stream
            .as_ref()
            .ok_or_else(|| format!("Query '{query_name}' has no input stream defined."))?;

        let mut processor_chain_head: Option<Arc<Mutex<dyn Processor>>> = None;
        let mut last_processor_in_chain: Option<Arc<Mutex<dyn Processor>>> = None;

        // For N-element patterns, we need to connect the terminal to the selector chain later
        // This is set in the N-element pattern block and used after selector chain is built
        let mut n_element_terminal: Option<Arc<Mutex<TerminalPostStateProcessor>>> = None;

        // Flag to track if we're handling a logical pattern (AND/OR)
        // Logical patterns subscribe their processors directly to junctions, so we skip final subscription
        let mut is_logical_pattern = false;

        // Helper closure to link processors
        let mut link_processor = |new_processor_arc: Arc<Mutex<dyn Processor>>| {
            if processor_chain_head.is_none() {
                processor_chain_head = Some(new_processor_arc.clone());
            }
            if let Some(ref last_p_arc) = last_processor_in_chain {
                last_p_arc
                    .lock()
                    .expect("Processor Mutex poisoned")
                    .set_next_processor(Some(new_processor_arc.clone()));
            }
            last_processor_in_chain = Some(new_processor_arc);
        };

        // Build metadata and input processors depending on stream type
        let expr_parser_context: ExpressionParserContext = match input_stream_api {
            ApiInputStream::Single(single_in_stream) => {
                let input_stream_id = single_in_stream.get_stream_id_str().to_string();
                let input_junction = stream_junction_map
                    .get(&input_stream_id)
                    .ok_or_else(|| {
                        format!(
                            "Input stream '{input_stream_id}' not found for query '{query_name}'"
                        )
                    })?
                    .clone();
                let input_stream_def_from_junction = input_junction
                    .lock()
                    .expect("Input junction Mutex poisoned")
                    .get_stream_definition();
                let meta_input_event = Arc::new(MetaStreamEvent::new_for_single_input(
                    input_stream_def_from_junction,
                ));
                let mut stream_meta_map = HashMap::new();
                stream_meta_map.insert(input_stream_id.clone(), Arc::clone(&meta_input_event));
                // Table metadata is only required when a table participates as an
                // input source. Since table queries are not yet supported, avoid
                // registering tables here to prevent variable lookup ambiguity.
                let table_meta_map = HashMap::new();
                let ctx = ExpressionParserContext {
                    eventflux_app_context: Arc::clone(eventflux_app_context),
                    eventflux_query_context: Arc::clone(&eventflux_query_context),
                    stream_meta_map,
                    table_meta_map,
                    window_meta_map: HashMap::new(),
                    aggregation_meta_map: HashMap::new(),
                    state_meta_map: HashMap::new(),
                    stream_positions: {
                        let mut m = HashMap::new();
                        m.insert(input_stream_id.clone(), 0);
                        m
                    },
                    default_source: input_stream_id.clone(),
                    query_name: &query_name,
                    is_mutation_context: false,
                };

                for handler in single_in_stream.get_stream_handlers() {
                    match handler {
                        crate::query_api::execution::query::input::handler::StreamHandler::Window(w) => {
                            let win_proc = create_window_processor(
                                w.as_ref(),
                                Arc::clone(eventflux_app_context),
                                Arc::clone(&eventflux_query_context),
                                &ctx,
                            )?;
                            link_processor(win_proc);
                        }
                        crate::query_api::execution::query::input::handler::StreamHandler::Filter(f) => {
                            let condition_executor =
                                parse_expression(&f.filter_expression, &ctx).map_err(|e| e.to_string())?;
                            let filter_processor = Arc::new(Mutex::new(FilterProcessor::new(
                                condition_executor,
                                Arc::clone(eventflux_app_context),
                                Arc::clone(&eventflux_query_context),
                            )?));
                            link_processor(filter_processor);
                        }
                        _ => {}
                    }
                }

                ctx
            }
            ApiInputStream::Join(join_stream) => {
                let left_id = join_stream
                    .left_input_stream
                    .get_stream_id_str()
                    .to_string();
                let right_id = join_stream
                    .right_input_stream
                    .get_stream_id_str()
                    .to_string();
                // Extract aliases for JOIN expressions (e.g., "FROM Stream1 AS a JOIN Stream2 AS b")
                let left_alias = join_stream
                    .left_input_stream
                    .get_stream_reference_id_str()
                    .map(|s| s.to_string());
                let right_alias = join_stream
                    .right_input_stream
                    .get_stream_reference_id_str()
                    .map(|s| s.to_string());
                let left_is_table = table_def_map.contains_key(&left_id);
                let right_is_table = table_def_map.contains_key(&right_id);

                if left_is_table ^ right_is_table {
                    // stream-table join
                    let (stream_id, table_id, stream_on_left) = if left_is_table {
                        (right_id.clone(), left_id.clone(), false)
                    } else {
                        (left_id.clone(), right_id.clone(), true)
                    };
                    let stream_junction = stream_junction_map
                        .get(&stream_id)
                        .ok_or_else(|| format!("Input stream '{stream_id}' not found"))?
                        .clone();
                    let stream_def = stream_junction.lock().unwrap().get_stream_definition();
                    let table_def = table_def_map
                        .get(&table_id)
                        .ok_or_else(|| format!("Table definition '{table_id}' not found"))?
                        .clone();
                    let table = eventflux_app_context
                        .get_eventflux_context()
                        .get_table(&table_id)
                        .ok_or_else(|| format!("Table '{table_id}' not found"))?;

                    let stream_len = stream_def.abstract_definition.attribute_list.len();
                    let table_len = table_def.abstract_definition.attribute_list.len();

                    let mut stream_meta = MetaStreamEvent::new_for_single_input(stream_def.clone());
                    let table_stream_def = Arc::new(
                        crate::query_api::definition::stream_definition::StreamDefinition {
                            abstract_definition: table_def.abstract_definition.clone(),
                            with_config: None, // Tables don't use SQL WITH config
                        },
                    );
                    let mut table_meta = MetaStreamEvent::new_for_single_input(table_stream_def);
                    if stream_on_left {
                        table_meta.apply_attribute_offset(stream_len);
                    } else {
                        stream_meta.apply_attribute_offset(table_len);
                    }

                    let mut stream_meta_map = HashMap::new();
                    let mut table_meta_map = HashMap::new();
                    stream_meta_map.insert(stream_id.clone(), Arc::new(stream_meta));
                    table_meta_map.insert(table_id.clone(), Arc::new(table_meta));

                    let cond_exec = if let Some(expr) = &join_stream.on_compare {
                        Some(
                            parse_expression(
                                expr,
                                &ExpressionParserContext {
                                    eventflux_app_context: Arc::clone(eventflux_app_context),
                                    eventflux_query_context: Arc::clone(&eventflux_query_context),
                                    stream_meta_map: stream_meta_map.clone(),
                                    table_meta_map: table_meta_map.clone(),
                                    window_meta_map: HashMap::new(),
                                    aggregation_meta_map: HashMap::new(),
                                    state_meta_map: HashMap::new(),
                                    stream_positions: {
                                        let mut m = HashMap::new();
                                        if stream_on_left {
                                            m.insert(stream_id.clone(), 0);
                                            m.insert(table_id.clone(), 1);
                                        } else {
                                            m.insert(table_id.clone(), 0);
                                            m.insert(stream_id.clone(), 1);
                                        }
                                        m
                                    },
                                    default_source: stream_id.clone(),
                                    query_name: &query_name,
                                    is_mutation_context: false,
                                },
                            )
                            .map_err(|e| e.to_string())?,
                        )
                    } else {
                        None
                    };

                    let comp_cond = if let Some(expr) = &join_stream.on_compare {
                        table.compile_join_condition(expr.clone(), &stream_id, &stream_def)
                    } else {
                        None
                    };
                    let join_proc = Arc::new(Mutex::new(TableJoinProcessor::new(
                        join_stream.join_type,
                        comp_cond.map(Arc::from),
                        cond_exec,
                        stream_len,
                        table_len,
                        table,
                        Arc::clone(eventflux_app_context),
                        Arc::clone(&eventflux_query_context),
                    )));
                    stream_junction.lock().unwrap().subscribe(join_proc.clone());
                    link_processor(join_proc.clone());

                    ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&eventflux_query_context),
                        stream_meta_map,
                        table_meta_map,
                        window_meta_map: HashMap::new(),
                        aggregation_meta_map: HashMap::new(),
                        state_meta_map: HashMap::new(),
                        stream_positions: {
                            let mut m = HashMap::new();
                            if stream_on_left {
                                m.insert(stream_id.clone(), 0);
                                m.insert(table_id.clone(), 1);
                            } else {
                                m.insert(table_id.clone(), 0);
                                m.insert(stream_id.clone(), 1);
                            }
                            m
                        },
                        default_source: stream_id.clone(),
                        query_name: &query_name,
                        is_mutation_context: false,
                    }
                } else {
                    let left_junction = stream_junction_map
                        .get(&left_id)
                        .ok_or_else(|| format!("Input stream '{left_id}' not found"))?
                        .clone();
                    let right_junction = stream_junction_map
                        .get(&right_id)
                        .ok_or_else(|| format!("Input stream '{right_id}' not found"))?
                        .clone();

                    let left_def = left_junction.lock().unwrap().get_stream_definition();
                    let right_def = right_junction.lock().unwrap().get_stream_definition();
                    let left_len = left_def.abstract_definition.attribute_list.len();
                    let right_len = right_def.abstract_definition.attribute_list.len();

                    let left_meta = MetaStreamEvent::new_for_single_input(left_def);
                    let mut right_meta = MetaStreamEvent::new_for_single_input(right_def);
                    right_meta.apply_attribute_offset(left_len);

                    let left_meta_arc = Arc::new(left_meta);
                    let right_meta_arc = Arc::new(right_meta);

                    let mut stream_meta_map = HashMap::new();
                    stream_meta_map.insert(left_id.clone(), Arc::clone(&left_meta_arc));
                    stream_meta_map.insert(right_id.clone(), Arc::clone(&right_meta_arc));
                    // Add aliases to stream_meta_map so expressions like "a.symbol" resolve correctly
                    if let Some(ref alias) = left_alias {
                        stream_meta_map.insert(alias.clone(), Arc::clone(&left_meta_arc));
                    }
                    if let Some(ref alias) = right_alias {
                        stream_meta_map.insert(alias.clone(), Arc::clone(&right_meta_arc));
                    }
                    let table_meta_map = HashMap::new();

                    // Build stream_positions map with both stream IDs and aliases
                    let stream_positions = {
                        let mut m = HashMap::new();
                        m.insert(left_id.clone(), 0);
                        m.insert(right_id.clone(), 1);
                        // Add aliases to stream_positions for expression parsing
                        if let Some(ref alias) = left_alias {
                            m.insert(alias.clone(), 0);
                        }
                        if let Some(ref alias) = right_alias {
                            m.insert(alias.clone(), 1);
                        }
                        m
                    };

                    let cond_exec = if let Some(expr) = &join_stream.on_compare {
                        Some(
                            parse_expression(
                                expr,
                                &ExpressionParserContext {
                                    eventflux_app_context: Arc::clone(eventflux_app_context),
                                    eventflux_query_context: Arc::clone(&eventflux_query_context),
                                    stream_meta_map: stream_meta_map.clone(),
                                    table_meta_map: table_meta_map.clone(),
                                    window_meta_map: HashMap::new(),
                                    aggregation_meta_map: HashMap::new(),
                                    state_meta_map: HashMap::new(),
                                    stream_positions: stream_positions.clone(),
                                    default_source: left_id.clone(),
                                    query_name: &query_name,
                                    is_mutation_context: false,
                                },
                            )
                            .map_err(|e| e.to_string())?,
                        )
                    } else {
                        None
                    };

                    let join_proc = Arc::new(Mutex::new(JoinProcessor::new(
                        join_stream.join_type,
                        cond_exec,
                        left_len,
                        right_len,
                        Arc::clone(eventflux_app_context),
                        Arc::clone(&eventflux_query_context),
                    )));
                    let left_side =
                        JoinProcessor::create_side_processor(&join_proc, JoinSide::Left);
                    let right_side =
                        JoinProcessor::create_side_processor(&join_proc, JoinSide::Right);

                    left_junction.lock().unwrap().subscribe(left_side.clone());
                    right_junction.lock().unwrap().subscribe(right_side.clone());

                    link_processor(left_side.clone());

                    ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&eventflux_query_context),
                        stream_meta_map,
                        table_meta_map,
                        window_meta_map: HashMap::new(),
                        aggregation_meta_map: HashMap::new(),
                        state_meta_map: HashMap::new(),
                        stream_positions,
                        default_source: left_id.clone(),
                        query_name: &query_name,
                        is_mutation_context: false,
                    }
                }
            }
            ApiInputStream::State(state_stream) => {
                use crate::core::event::complex_event::ComplexEvent;
                use crate::core::query::input::stream::state::pattern_chain_builder::{
                    PatternChainBuilder as PCB, PatternStepConfig,
                };
                use crate::core::query::input::stream::state::stream_pre_state_processor::StateType;
                use crate::query_api::execution::query::input::state::logical_state_element::Type as ApiLogicalType;
                use crate::query_api::execution::query::input::state::state_element::StateElement;

                /// Adapter that wraps a PreStateProcessor to implement the Processor trait
                /// This allows PreStateProcessors to be subscribed to StreamJunctions
                #[derive(Debug)]
                struct PreStateProcessorAdapter {
                    pre_processor:
                        Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
                    app_context:
                        Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>,
                    query_context:
                        Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
                    next_processor: Option<Arc<Mutex<dyn Processor>>>,
                }

                impl PreStateProcessorAdapter {
                    fn new(
                        pre_processor: Arc<
                            Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>,
                        >,
                        app_context: Arc<
                            crate::core::config::eventflux_app_context::EventFluxAppContext,
                        >,
                        query_context: Arc<
                            crate::core::config::eventflux_query_context::EventFluxQueryContext,
                        >,
                    ) -> Self {
                        Self {
                            pre_processor,
                            app_context,
                            query_context,
                            next_processor: None,
                        }
                    }
                }

                impl Processor for PreStateProcessorAdapter {
                    fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
                        // Convert ComplexEvent to StreamEvent and process
                        if let Some(event) = chunk {
                            if let Some(stream_event) = event.as_any().downcast_ref::<crate::core::event::stream::stream_event::StreamEvent>() {
                                // Clone the stream event since we need owned data
                                let se_clone = stream_event.clone();

                                // Process through the PreStateProcessor
                                let mut pre = self.pre_processor.lock().unwrap();

                                // CRITICAL: Update state FIRST to move new events (from add_state) to pending
                                // This allows events forwarded from previous processors to be matched
                                pre.update_state();

                                // Now process the incoming event against pending states
                                let _result = pre.process_and_return(Some(Box::new(se_clone)));
                            }
                        }
                    }

                    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
                        self.next_processor.clone()
                    }

                    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
                        self.next_processor = next;
                    }

                    fn clone_processor(
                        &self,
                        _ctx: &Arc<
                            crate::core::config::eventflux_query_context::EventFluxQueryContext,
                        >,
                    ) -> Box<dyn Processor> {
                        Box::new(PreStateProcessorAdapter {
                            pre_processor: Arc::clone(&self.pre_processor),
                            app_context: Arc::clone(&self.app_context),
                            query_context: Arc::clone(&self.query_context),
                            next_processor: self.next_processor.clone(),
                        })
                    }

                    fn get_eventflux_app_context(
                        &self,
                    ) -> Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>
                    {
                        Arc::clone(&self.app_context)
                    }

                    fn get_eventflux_query_context(
                        &self,
                    ) -> Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>
                    {
                        Arc::clone(&self.query_context)
                    }

                    fn get_processing_mode(&self) -> crate::core::query::processor::ProcessingMode {
                        crate::core::query::processor::ProcessingMode::DEFAULT
                    }

                    fn is_stateful(&self) -> bool {
                        true
                    }
                }

                // TerminalPostStateProcessor is now defined at module level

                /// Adapter for N-element same-stream patterns
                ///
                /// For same-stream patterns (e.g., e1=Trades -> e2=Trades -> e3=Trades),
                /// events must be processed carefully to ensure each event matches at
                /// exactly one position in the pattern.
                ///
                /// The key insight is:
                /// 1. Call update_state() on ALL processors FIRST to move forwarded states
                ///    from new_list to pending_list
                /// 2. Then process the event - only processors with pending states will match
                /// 3. When a processor matches, it forwards state to the next processor's
                ///    new_list, which won't be available until the NEXT event's update_state()
                ///
                /// This ensures that each event can only match one position, because forwarded
                /// states don't become "pending" until after the current event is processed.
                #[derive(Debug)]
                struct NElementSameStreamAdapter {
                    pre_processors: Vec<
                        Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>,
                    >,
                    app_context:
                        Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>,
                    query_context:
                        Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
                }

                impl NElementSameStreamAdapter {
                    fn new(
                        pre_processors: Vec<
                            Arc<
                                Mutex<
                                    dyn crate::core::query::input::stream::state::PreStateProcessor,
                                >,
                            >,
                        >,
                        app_context: Arc<
                            crate::core::config::eventflux_app_context::EventFluxAppContext,
                        >,
                        query_context: Arc<
                            crate::core::config::eventflux_query_context::EventFluxQueryContext,
                        >,
                    ) -> Self {
                        Self {
                            pre_processors,
                            app_context,
                            query_context,
                        }
                    }
                }

                impl Processor for NElementSameStreamAdapter {
                    fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
                        // Step 1: Update state on ALL processors FIRST
                        // This moves any forwarded states from new_list to pending_list
                        // BEFORE we process the current event
                        for pre in &self.pre_processors {
                            pre.lock().unwrap().update_state();
                        }

                        // Step 2: Process the event through each processor
                        // Only processors with pending states will actually match
                        if let Some(event) = chunk {
                            if let Some(stream_event) = event.as_any().downcast_ref::<crate::core::event::stream::stream_event::StreamEvent>() {
                                for pre in &self.pre_processors {
                                    let se_clone = stream_event.clone();
                                    let mut pre_guard = pre.lock().unwrap();
                                    // Note: We don't call update_state() here - it was done above
                                    let _result = pre_guard.process_and_return(Some(Box::new(se_clone)));
                                }
                            }
                        }
                    }

                    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
                        None // Terminal adapter - output goes through PostStateProcessor chain
                    }

                    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {
                        // No-op - wiring is done through PostStateProcessor chain
                    }

                    fn clone_processor(
                        &self,
                        _ctx: &Arc<
                            crate::core::config::eventflux_query_context::EventFluxQueryContext,
                        >,
                    ) -> Box<dyn Processor> {
                        Box::new(NElementSameStreamAdapter {
                            pre_processors: self.pre_processors.clone(),
                            app_context: Arc::clone(&self.app_context),
                            query_context: Arc::clone(&self.query_context),
                        })
                    }

                    fn get_eventflux_app_context(
                        &self,
                    ) -> Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>
                    {
                        Arc::clone(&self.app_context)
                    }

                    fn get_eventflux_query_context(
                        &self,
                    ) -> Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>
                    {
                        Arc::clone(&self.query_context)
                    }

                    fn get_processing_mode(&self) -> crate::core::query::processor::ProcessingMode {
                        crate::core::query::processor::ProcessingMode::DEFAULT
                    }

                    fn is_stateful(&self) -> bool {
                        true
                    }
                }

                /// Holds information about a pattern element
                struct PatternElementInfo {
                    stream_id: String,
                    alias: Option<String>,
                    min_count: i32,
                    max_count: i32,
                }

                /// Extract pattern element info from a StateElement
                fn extract_element_info(se: &StateElement) -> Option<PatternElementInfo> {
                    match se {
                        StateElement::Stream(s) => {
                            let alias = s
                                .get_single_input_stream()
                                .get_stream_reference_id_str()
                                .map(|s| s.to_string());
                            Some(PatternElementInfo {
                                stream_id: s
                                    .get_single_input_stream()
                                    .get_stream_id_str()
                                    .to_string(),
                                alias,
                                min_count: 1,
                                max_count: 1,
                            })
                        }
                        StateElement::Every(ev) => extract_element_info(&ev.state_element),
                        StateElement::Count(c) => {
                            let alias = c
                                .stream_state_element
                                .get_single_input_stream()
                                .get_stream_reference_id_str()
                                .map(|s| s.to_string());
                            Some(PatternElementInfo {
                                stream_id: c
                                    .stream_state_element
                                    .get_single_input_stream()
                                    .get_stream_id_str()
                                    .to_string(),
                                alias,
                                min_count: c.min_count,
                                max_count: c.max_count,
                            })
                        }
                        _ => None,
                    }
                }

                /// Recursively extract all pattern elements from a nested Next structure
                /// For pattern A -> B -> C -> D (represented as Next(A, Next(B, Next(C, D))))
                /// Returns [A, B, C, D] in order
                fn extract_all_sequence_elements(
                    se: &StateElement,
                ) -> Option<Vec<PatternElementInfo>> {
                    match se {
                        StateElement::Stream(_) | StateElement::Count(_) => {
                            extract_element_info(se).map(|info| vec![info])
                        }
                        StateElement::Every(ev) => extract_all_sequence_elements(&ev.state_element),
                        StateElement::Next(next_elem) => {
                            let mut elements =
                                extract_all_sequence_elements(&next_elem.state_element)?;
                            let next_elements =
                                extract_all_sequence_elements(&next_elem.next_state_element)?;
                            elements.extend(next_elements);
                            Some(elements)
                        }
                        _ => None, // Logical handled separately
                    }
                }

                /// Represents a pattern type for unified handling
                enum PatternType {
                    /// Sequence pattern (A -> B, A -> B -> C, etc.)
                    Sequence(Vec<PatternElementInfo>),
                    /// Logical pattern (A AND B, A OR B)
                    Logical {
                        left: PatternElementInfo,
                        right: PatternElementInfo,
                        is_and: bool,
                    },
                }

                /// Parse the state element into a unified PatternType
                fn parse_pattern_type(se: &StateElement) -> Option<PatternType> {
                    match se {
                        StateElement::Next(_) => {
                            extract_all_sequence_elements(se).map(PatternType::Sequence)
                        }
                        StateElement::Stream(_) | StateElement::Count(_) => {
                            // Single element pattern (unusual but valid)
                            extract_element_info(se).map(|info| PatternType::Sequence(vec![info]))
                        }
                        StateElement::Every(ev) => parse_pattern_type(&ev.state_element),
                        StateElement::Logical(log) => {
                            let left = extract_element_info(&log.stream_state_element_1)?;
                            let right = extract_element_info(&log.stream_state_element_2)?;
                            let is_and = matches!(log.logical_type, ApiLogicalType::And);
                            Some(PatternType::Logical {
                                left,
                                right,
                                is_and,
                            })
                        }
                        _ => None,
                    }
                }

                // Parse the pattern type
                let pattern_type = parse_pattern_type(state_stream.state_element.as_ref())
                    .ok_or_else(|| {
                        format!("Query '{query_name}': Unsupported pattern structure")
                    })?;

                // Collect all elements for metadata building
                let all_elements: Vec<&PatternElementInfo> = match &pattern_type {
                    PatternType::Sequence(elements) => elements.iter().collect(),
                    PatternType::Logical { left, right, .. } => vec![left, right],
                };

                // Build metadata maps for all elements
                let mut stream_meta_map = HashMap::new();
                let mut stream_positions_map: HashMap<String, i32> = HashMap::new();
                let mut total_attr_count = 0;
                let mut offset = 0;

                for (idx, elem) in all_elements.iter().enumerate() {
                    let junction = stream_junction_map
                        .get(&elem.stream_id)
                        .ok_or_else(|| format!("Input stream '{}' not found", elem.stream_id))?
                        .clone();
                    let stream_def = junction.lock().unwrap().get_stream_definition();
                    let attr_len = stream_def.abstract_definition.attribute_list.len();

                    let mut meta = MetaStreamEvent::new_for_single_input(stream_def.clone());
                    if offset > 0 {
                        meta.apply_attribute_offset(offset);
                    }
                    let meta = Arc::new(meta);

                    // Register by stream name
                    stream_meta_map.insert(elem.stream_id.clone(), Arc::clone(&meta));
                    stream_positions_map.insert(elem.stream_id.clone(), idx as i32);

                    // Register by alias if present
                    if let Some(ref alias) = elem.alias {
                        stream_meta_map.insert(alias.clone(), Arc::clone(&meta));
                        stream_positions_map.insert(alias.clone(), idx as i32);
                    }

                    offset += attr_len;
                    total_attr_count += attr_len;
                }

                let table_meta_map: HashMap<String, Arc<MetaStreamEvent>> = HashMap::new();
                let default_source = all_elements[0].stream_id.clone();

                // Handle patterns based on type
                match &pattern_type {
                    PatternType::Sequence(elements) => {
                        // Sequence patterns use PatternChainBuilder
                        let state_type = match state_stream.state_type {
                            crate::query_api::execution::query::input::stream::state_input_stream::Type::Pattern => StateType::Pattern,
                            crate::query_api::execution::query::input::stream::state_input_stream::Type::Sequence => StateType::Sequence,
                        };

                        let mut builder = PCB::new(state_type);

                        for elem in elements.iter() {
                            builder.add_step(PatternStepConfig::new(
                                elem.alias.clone().unwrap_or_else(|| elem.stream_id.clone()),
                                elem.stream_id.clone(),
                                elem.min_count as usize,
                                elem.max_count as usize,
                            ));
                        }

                        // Set WITHIN if present
                        if let Some(within_time) = state_stream.within_time.as_ref().and_then(|c| match c.get_value() {
                            crate::query_api::expression::constant::ConstantValueWithFloat::Time(t) => Some(*t),
                            crate::query_api::expression::constant::ConstantValueWithFloat::Long(l) => Some(*l),
                            crate::query_api::expression::constant::ConstantValueWithFloat::Int(i) => Some(*i as i64),
                            _ => None,
                        }) {
                            builder.set_within(within_time);
                        }

                        // Set EVERY if the top-level StateElement is Every
                        if matches!(state_stream.state_element.as_ref(), StateElement::Every(_)) {
                            builder.set_every(true);
                        }

                        // Build the processor chain
                        let mut chain = builder
                            .build(
                                Arc::clone(eventflux_app_context),
                                Arc::clone(&eventflux_query_context),
                            )
                            .map_err(|e| {
                                format!("Query '{query_name}': Failed to build pattern chain: {e}")
                            })?;

                        chain.init();

                        // Collect stream definitions for cloner setup
                        let mut stream_defs = Vec::new();
                        for elem in elements.iter() {
                            let junction = stream_junction_map.get(&elem.stream_id).unwrap();
                            let stream_def = junction.lock().unwrap().get_stream_definition();
                            stream_defs.push(stream_def);
                        }
                        chain.setup_cloners(stream_defs);

                        // Create TerminalPostStateProcessor
                        let terminal = Arc::new(Mutex::new(TerminalPostStateProcessor::new(
                            elements.len() - 1,
                            total_attr_count,
                        )));

                        // Wire the last post processor to the terminal
                        if let Some(last_post) = chain.post_processors.last() {
                            last_post.lock().unwrap().set_next_processor(
                                terminal.clone() as Arc<Mutex<dyn crate::core::query::input::stream::state::PostStateProcessor>>
                            );
                        }

                        // Group elements by stream_id
                        let mut stream_to_processors: HashMap<String, Vec<Arc<Mutex<dyn crate::core::query::input::stream::state::PreStateProcessor>>>> = HashMap::new();
                        for (idx, elem) in elements.iter().enumerate() {
                            if idx < chain.pre_processors.len() {
                                let pre = chain.pre_processors[idx].clone();
                                stream_to_processors
                                    .entry(elem.stream_id.clone())
                                    .or_insert_with(Vec::new)
                                    .push(pre);
                            }
                        }

                        // Subscribe adapters to junctions
                        for (stream_id, processors) in stream_to_processors {
                            let junction = stream_junction_map.get(&stream_id).unwrap().clone();
                            if processors.len() == 1 {
                                let adapter = Arc::new(Mutex::new(PreStateProcessorAdapter::new(
                                    processors[0].clone(),
                                    Arc::clone(eventflux_app_context),
                                    Arc::clone(&eventflux_query_context),
                                )));
                                junction.lock().unwrap().subscribe(adapter);
                            } else {
                                let n_adapter =
                                    Arc::new(Mutex::new(NElementSameStreamAdapter::new(
                                        processors.clone(),
                                        Arc::clone(eventflux_app_context),
                                        Arc::clone(&eventflux_query_context),
                                    )));
                                junction.lock().unwrap().subscribe(n_adapter);
                            }
                        }

                        n_element_terminal = Some(terminal);
                    }
                    PatternType::Logical {
                        left,
                        right,
                        is_and,
                    } => {
                        // Mark this as a logical pattern - subscription is handled below
                        is_logical_pattern = true;

                        // Logical patterns use a simple shared-buffer processor
                        use crate::core::event::stream::stream_event::StreamEvent;
                        use crate::core::event::stream::stream_event_cloner::StreamEventCloner;
                        use crate::core::event::stream::stream_event_factory::StreamEventFactory;
                        use crate::core::event::value::AttributeValue;

                        // Get junctions
                        let first_junction = stream_junction_map
                            .get(&left.stream_id)
                            .ok_or_else(|| format!("Input stream '{}' not found", left.stream_id))?
                            .clone();
                        let second_junction = stream_junction_map
                            .get(&right.stream_id)
                            .ok_or_else(|| format!("Input stream '{}' not found", right.stream_id))?
                            .clone();

                        let first_def = first_junction.lock().unwrap().get_stream_definition();
                        let second_def = second_junction.lock().unwrap().get_stream_definition();
                        let first_len = first_def.abstract_definition.attribute_list.len();
                        let second_len = second_def.abstract_definition.attribute_list.len();

                        // Shared state for logical pattern
                        struct SharedLogicalState {
                            is_and: bool,
                            first_buffer: Vec<StreamEvent>,
                            second_buffer: Vec<StreamEvent>,
                            first_len: usize,
                            second_len: usize,
                            factory: StreamEventFactory,
                            next_processor: Option<Arc<Mutex<dyn Processor>>>,
                        }

                        impl SharedLogicalState {
                            fn try_produce(&mut self) {
                                if self.is_and {
                                    while !self.first_buffer.is_empty()
                                        && !self.second_buffer.is_empty()
                                    {
                                        let first = self.first_buffer.remove(0);
                                        let second = self.second_buffer.remove(0);
                                        self.forward_joined(Some(&first), Some(&second));
                                    }
                                } else {
                                    while !self.first_buffer.is_empty() {
                                        let first = self.first_buffer.remove(0);
                                        self.forward_joined(Some(&first), None);
                                    }
                                    while !self.second_buffer.is_empty() {
                                        let second = self.second_buffer.remove(0);
                                        self.forward_joined(None, Some(&second));
                                    }
                                }
                            }

                            fn forward_joined(
                                &self,
                                first: Option<&StreamEvent>,
                                second: Option<&StreamEvent>,
                            ) {
                                let mut event = self.factory.new_instance();
                                event.timestamp = second
                                    .map(|s| s.timestamp)
                                    .or_else(|| first.map(|f| f.timestamp))
                                    .unwrap_or(0);
                                for i in 0..self.first_len {
                                    let val = first
                                        .and_then(|f| f.before_window_data.get(i).cloned())
                                        .unwrap_or(AttributeValue::Null);
                                    event.before_window_data[i] = val;
                                }
                                for j in 0..self.second_len {
                                    let val = second
                                        .and_then(|s| s.before_window_data.get(j).cloned())
                                        .unwrap_or(AttributeValue::Null);
                                    event.before_window_data[self.first_len + j] = val;
                                }
                                if let Some(ref next) = self.next_processor {
                                    if let Ok(mut proc) = next.lock() {
                                        proc.process(Some(Box::new(event)));
                                    }
                                }
                            }
                        }

                        let shared_state = Arc::new(Mutex::new(SharedLogicalState {
                            is_and: *is_and,
                            first_buffer: Vec::new(),
                            second_buffer: Vec::new(),
                            first_len,
                            second_len,
                            factory: StreamEventFactory::new(first_len + second_len, 0, 0),
                            next_processor: None,
                        }));

                        // Side processors
                        struct LogicalSideProcessor {
                            shared: Arc<Mutex<SharedLogicalState>>,
                            is_first: bool,
                            #[allow(dead_code)]
                            cloner: Option<StreamEventCloner>,
                            app_ctx: Arc<
                                crate::core::config::eventflux_app_context::EventFluxAppContext,
                            >,
                            query_ctx: Arc<
                                crate::core::config::eventflux_query_context::EventFluxQueryContext,
                            >,
                        }

                        impl std::fmt::Debug for LogicalSideProcessor {
                            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                                f.debug_struct("LogicalSideProcessor")
                                    .field("is_first", &self.is_first)
                                    .finish()
                            }
                        }

                        impl Processor for LogicalSideProcessor {
                            fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
                                if let Some(ce) = chunk {
                                    if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                                        if let Ok(mut state) = self.shared.lock() {
                                            let cloned = se.clone();
                                            if self.is_first {
                                                state.first_buffer.push(cloned);
                                            } else {
                                                state.second_buffer.push(cloned);
                                            }
                                            state.try_produce();
                                        }
                                    }
                                }
                            }

                            fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
                                self.shared
                                    .lock()
                                    .ok()
                                    .and_then(|s| s.next_processor.clone())
                            }

                            fn set_next_processor(
                                &mut self,
                                next: Option<Arc<Mutex<dyn Processor>>>,
                            ) {
                                if let Ok(mut state) = self.shared.lock() {
                                    state.next_processor = next;
                                }
                            }

                            fn clone_processor(
                                &self,
                                _ctx: &Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
                            ) -> Box<dyn Processor> {
                                Box::new(LogicalSideProcessor {
                                    shared: Arc::clone(&self.shared),
                                    is_first: self.is_first,
                                    cloner: None,
                                    app_ctx: Arc::clone(&self.app_ctx),
                                    query_ctx: Arc::clone(&self.query_ctx),
                                })
                            }

                            fn get_eventflux_app_context(
                                &self,
                            ) -> Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>
                            {
                                Arc::clone(&self.app_ctx)
                            }

                            fn get_eventflux_query_context(
                                &self,
                            ) -> Arc<
                                crate::core::config::eventflux_query_context::EventFluxQueryContext,
                            > {
                                Arc::clone(&self.query_ctx)
                            }

                            fn get_processing_mode(
                                &self,
                            ) -> crate::core::query::processor::ProcessingMode
                            {
                                crate::core::query::processor::ProcessingMode::DEFAULT
                            }

                            fn is_stateful(&self) -> bool {
                                true
                            }
                        }

                        let first_side: Arc<Mutex<dyn Processor>> =
                            Arc::new(Mutex::new(LogicalSideProcessor {
                                shared: Arc::clone(&shared_state),
                                is_first: true,
                                cloner: None,
                                app_ctx: Arc::clone(eventflux_app_context),
                                query_ctx: Arc::clone(&eventflux_query_context),
                            }));

                        let second_side: Arc<Mutex<dyn Processor>> =
                            Arc::new(Mutex::new(LogicalSideProcessor {
                                shared: Arc::clone(&shared_state),
                                is_first: false,
                                cloner: None,
                                app_ctx: Arc::clone(eventflux_app_context),
                                query_ctx: Arc::clone(&eventflux_query_context),
                            }));

                        // Subscribe to junctions
                        if left.stream_id == right.stream_id {
                            // Same-stream logical pattern
                            use std::sync::atomic::{AtomicBool, Ordering};
                            struct SameStreamLogicalAdapter {
                                first: Arc<Mutex<dyn Processor>>,
                                second: Arc<Mutex<dyn Processor>>,
                                has_first: AtomicBool,
                            }
                            impl std::fmt::Debug for SameStreamLogicalAdapter {
                                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                                    f.debug_struct("SameStreamLogicalAdapter").finish()
                                }
                            }
                            impl Processor for SameStreamLogicalAdapter {
                                fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
                                    use crate::core::event::complex_event::clone_box_complex_event;
                                    if !self.has_first.swap(true, Ordering::SeqCst) {
                                        self.first.lock().unwrap().process(chunk);
                                    } else {
                                        let c1 = chunk
                                            .as_ref()
                                            .map(|e| clone_box_complex_event(e.as_ref()));
                                        self.second.lock().unwrap().process(chunk);
                                        self.first.lock().unwrap().process(c1);
                                    }
                                }
                                fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
                                    self.first.lock().unwrap().next_processor()
                                }
                                fn set_next_processor(
                                    &mut self,
                                    next: Option<Arc<Mutex<dyn Processor>>>,
                                ) {
                                    self.first.lock().unwrap().set_next_processor(next);
                                }
                                fn clone_processor(
                                    &self,
                                    _: &Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
                                ) -> Box<dyn Processor> {
                                    Box::new(SameStreamLogicalAdapter {
                                        first: Arc::clone(&self.first),
                                        second: Arc::clone(&self.second),
                                        has_first: AtomicBool::new(false),
                                    })
                                }
                                fn get_eventflux_app_context(
                                    &self,
                                ) -> Arc<
                                    crate::core::config::eventflux_app_context::EventFluxAppContext,
                                > {
                                    self.first.lock().unwrap().get_eventflux_app_context()
                                }
                                fn get_eventflux_query_context(&self) -> Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>{
                                    self.first.lock().unwrap().get_eventflux_query_context()
                                }
                                fn get_processing_mode(
                                    &self,
                                ) -> crate::core::query::processor::ProcessingMode
                                {
                                    crate::core::query::processor::ProcessingMode::DEFAULT
                                }
                                fn is_stateful(&self) -> bool {
                                    true
                                }
                            }
                            let adapter = Arc::new(Mutex::new(SameStreamLogicalAdapter {
                                first: first_side.clone(),
                                second: second_side.clone(),
                                has_first: AtomicBool::new(false),
                            }));
                            first_junction.lock().unwrap().subscribe(adapter);
                        } else {
                            first_junction.lock().unwrap().subscribe(first_side.clone());
                            second_junction
                                .lock()
                                .unwrap()
                                .subscribe(second_side.clone());
                        }

                        link_processor(first_side.clone());
                    }
                }

                // Return expr_parser_context
                ExpressionParserContext {
                    eventflux_app_context: Arc::clone(eventflux_app_context),
                    eventflux_query_context: Arc::clone(&eventflux_query_context),
                    stream_meta_map,
                    table_meta_map,
                    window_meta_map: HashMap::new(),
                    aggregation_meta_map: HashMap::new(),
                    state_meta_map: HashMap::new(),
                    stream_positions: stream_positions_map,
                    default_source,
                    query_name: &query_name,
                    is_mutation_context: false,
                }

                // All ApiInputStream variants are handled above
            }
        };

        // 5. Selector (Projections)
        let api_selector = &api_query.selector; // Selector is not Option in query_api::Query
        let mut oaps = Vec::new();
        let mut output_attributes_for_def = Vec::new();

        for (idx, api_out_attr) in api_selector.selection_list.iter().enumerate() {
            let expr_exec = parse_expression(&api_out_attr.expression, &expr_parser_context)
                .map_err(|e| e.to_string())?;
            // OutputAttributeProcessor needs the output position.
            let oap = OutputAttributeProcessor::new(expr_exec);

            let attr_name = api_out_attr.rename.clone().unwrap_or_else(|| {
                // Try to infer name from expression, or generate. Very simplified.
                // Java's OutputAttribute(Variable) uses variable.getAttributeName().
                // If expression is a Variable, use its name.
                if let ApiExpression::Variable(v) = &api_out_attr.expression {
                    v.attribute_name.clone()
                } else {
                    format!("_col_{idx}")
                }
            });
            output_attributes_for_def.push(ApiAttribute::new(attr_name, oap.get_return_type()));
            oaps.push(oap);
        }

        // Determine the ID for the stream produced by this SelectProcessor
        let _output_stream_id_from_query = api_query
            .output_stream
            .get_target_id() // output_stream is not Option
            .ok_or_else(|| {
                format!("Query '{query_name}' must have a target output stream for INSERT INTO")
            })?;

        let mut temp_def =
            ApiStreamDefinition::new(format!("_internal_{query_name}_select_output"));
        for attr in output_attributes_for_def {
            temp_def.abstract_definition.attribute_list.push(attr);
        }
        let select_output_stream_def = Arc::new(temp_def);

        let having_executor = if let Some(expr) = &api_selector.having_expression {
            Some(parse_expression(expr, &expr_parser_context).map_err(|e| e.to_string())?)
        } else {
            None
        };

        let mut group_execs = Vec::new();
        for var in &api_selector.group_by_list {
            let expr = ApiExpression::Variable(var.clone());
            group_execs
                .push(parse_expression(&expr, &expr_parser_context).map_err(|e| e.to_string())?);
        }
        let group_by_key_generator = if group_execs.is_empty() {
            None
        } else {
            Some(GroupByKeyGenerator::new(group_execs))
        };

        let mut order_execs = Vec::new();
        let mut order_flags = Vec::new();
        for ob in &api_selector.order_by_list {
            let expr = ApiExpression::Variable(ob.get_variable().clone());
            order_execs
                .push(parse_expression(&expr, &expr_parser_context).map_err(|e| e.to_string())?);
            order_flags.push(*ob.get_order() == crate::query_api::execution::query::selection::order_by_attribute::Order::Asc);
        }
        let order_by_comparator = if order_execs.is_empty() {
            None
        } else {
            Some(OrderByEventComparator::new(order_execs, order_flags))
        };

        let select_processor = Arc::new(Mutex::new(SelectProcessor::new(
            api_selector,
            true,
            true,
            Arc::clone(eventflux_app_context),
            Arc::clone(&eventflux_query_context),
            oaps,
            select_output_stream_def,
            having_executor,
            group_by_key_generator,
            order_by_comparator,
            None,
        )));
        link_processor(select_processor.clone());

        if let Some(rate) = api_query.get_output_rate() {
            if let crate::query_api::execution::query::output::ratelimit::OutputRateVariant::Events(ev, beh) = &rate.variant {
                let limiter = Arc::new(Mutex::new(OutputRateLimiter::new(
                    None,
                    Arc::clone(eventflux_app_context),
                    Arc::clone(&eventflux_query_context),
                    ev.event_count as usize,
                    *beh,
                )));
                link_processor(limiter);
            }
        }

        // 6. Output Processor (e.g., InsertIntoStreamProcessor)
        // This needs to match on api_query.output_stream.action
        match &api_query.output_stream.action {
            crate::query_api::execution::query::output::output_stream::OutputStreamAction::InsertInto(insert_action) => {
                // Priority order: TABLE → STREAM → AGGREGATION
                // This ensures INSERT INTO uses the correct processor based on target type
                if let Some(table) = eventflux_app_context.get_eventflux_context().get_table(&insert_action.target_id) {
                    let insert_processor = Arc::new(Mutex::new(
                        crate::core::query::output::InsertIntoTableProcessor::new(
                            table,
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&eventflux_query_context),
                        ),
                    ));
                    link_processor(insert_processor);
                } else if let Some(target_junction) = stream_junction_map.get(&insert_action.target_id) {
                    let insert_processor = Arc::new(Mutex::new(InsertIntoStreamProcessor::new(
                        target_junction.clone(),
                        Arc::clone(eventflux_app_context),
                        Arc::clone(&eventflux_query_context),
                    )));
                    link_processor(insert_processor);
                } else if let Some(agg) = aggregation_map.get(&insert_action.target_id) {
                    let insert_processor = Arc::new(Mutex::new(
                        crate::core::query::output::InsertIntoAggregationProcessor::new(
                            Arc::clone(agg),
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&eventflux_query_context),
                        ),
                    ));
                    link_processor(insert_processor);
                } else {
                    return Err(format!(
                        "Output target '{}' not found for query '{}'",
                        insert_action.target_id, query_name
                    ));
                }
            }
            crate::query_api::execution::query::output::output_stream::OutputStreamAction::Update(update_action) => {
                if let Some(table) = eventflux_app_context.get_eventflux_context().get_table(&update_action.target_id) {
                    // Get table definition to create table metadata for expression parsing
                    let table_def = table_def_map
                        .get(&update_action.target_id)
                        .ok_or_else(|| format!("Table definition '{}' not found", update_action.target_id))?
                        .clone();

                    // Create MetaStreamEvent for the table
                    let table_stream_def = Arc::new(
                        crate::query_api::definition::stream_definition::StreamDefinition {
                            abstract_definition: table_def.abstract_definition.clone(),
                            with_config: None,
                        },
                    );
                    let table_meta = MetaStreamEvent::new_for_single_input(table_stream_def);

                    // Create mutation context with both stream and table metadata
                    let mut mutation_table_meta_map = expr_parser_context.table_meta_map.clone();
                    let table_meta_arc = Arc::new(table_meta);
                    mutation_table_meta_map.insert(update_action.target_id.clone(), Arc::clone(&table_meta_arc));
                    let mut mutation_stream_positions = expr_parser_context.stream_positions.clone();
                    mutation_stream_positions.insert(update_action.target_id.clone(), 1);

                    // Register target alias if present (e.g., "s" in "UPDATE stockTable AS s")
                    if let Some(ref target_alias) = update_action.target_alias {
                        mutation_table_meta_map.insert(target_alias.clone(), Arc::clone(&table_meta_arc));
                        mutation_stream_positions.insert(target_alias.clone(), 1);
                    }

                    // Clone stream_meta_map for mutation context, replacing original name with alias if present
                    let mut mutation_stream_meta_map = HashMap::new();

                    // Register source stream - use alias if present, otherwise use original name
                    // When aliased, standard SQL requires using the alias, so we replace rather than add
                    if let Some((original_name, source_meta)) = expr_parser_context.stream_meta_map.iter().next() {
                        if let Some(ref source_alias) = update_action.source_alias {
                            // Use alias instead of original name to avoid duplicate entries
                            mutation_stream_meta_map.insert(source_alias.clone(), Arc::clone(source_meta));
                            mutation_stream_positions.insert(source_alias.clone(), 0);
                        } else {
                            // No alias - use original name
                            mutation_stream_meta_map.insert(original_name.clone(), Arc::clone(source_meta));
                        }
                    }

                    // For UPDATE mutations, default unqualified columns to the target table
                    // This matches standard SQL semantics where unqualified columns in UPDATE
                    // statements refer to the target table (e.g., SET price = newPrice means
                    // SET stockTable.price = updateStream.newPrice)
                    let mutation_ctx = ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&eventflux_query_context),
                        stream_meta_map: mutation_stream_meta_map,
                        table_meta_map: mutation_table_meta_map,
                        window_meta_map: expr_parser_context.window_meta_map.clone(),
                        aggregation_meta_map: expr_parser_context.aggregation_meta_map.clone(),
                        state_meta_map: expr_parser_context.state_meta_map.clone(),
                        stream_positions: mutation_stream_positions,
                        default_source: update_action.target_id.clone(),
                        query_name: &query_name,
                        is_mutation_context: true,
                    };

                    // Parse condition expression for stream-triggered update
                    let condition_executor = parse_expression(&update_action.on_update_expression, &mutation_ctx)
                        .map_err(|e| format!("Failed to parse UPDATE condition: {}", e))?;

                    // Parse SET clause value expressions and compute column indices
                    let (set_value_executors, set_column_indices): (Vec<Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>>, Vec<usize>) =
                        if let Some(ref update_set) = update_action.update_set_clause {
                            let mut executors = Vec::new();
                            let mut indices = Vec::new();
                            let table_attrs = table_def.abstract_definition.get_attribute_list();
                            for set_attr in &update_set.set_attributes {
                                let exec = parse_expression(&set_attr.value_to_set, &mutation_ctx)
                                    .map_err(|e| format!("Failed to parse SET value: {}", e))?;
                                executors.push(exec);

                                // Look up column index by name
                                let col_name = set_attr.table_column.get_attribute_name();
                                if let Some(col_idx) = table_attrs.iter().position(|a| a.get_name() == col_name) {
                                    indices.push(col_idx);
                                } else {
                                    return Err(format!(
                                        "SET column '{}' not found in table '{}'",
                                        col_name, update_action.target_id
                                    ));
                                }
                            }
                            (executors, indices)
                        } else {
                            (Vec::new(), Vec::new())
                        };

                    let update_processor = Arc::new(Mutex::new(
                        crate::core::query::output::UpdateTableProcessor::new_with_expression(
                            table,
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&eventflux_query_context),
                            condition_executor,
                            set_value_executors,
                            set_column_indices,
                        ),
                    ));
                    link_processor(update_processor);
                } else {
                    return Err(format!(
                        "Update target '{}' not found for query '{}'",
                        update_action.target_id, query_name
                    ));
                }
            }
            crate::query_api::execution::query::output::output_stream::OutputStreamAction::Delete(delete_action) => {
                if let Some(table) = eventflux_app_context.get_eventflux_context().get_table(&delete_action.target_id) {
                    // Get table definition to create table metadata for expression parsing
                    let table_def = table_def_map
                        .get(&delete_action.target_id)
                        .ok_or_else(|| format!("Table definition '{}' not found", delete_action.target_id))?
                        .clone();

                    // Create MetaStreamEvent for the table
                    let table_stream_def = Arc::new(
                        crate::query_api::definition::stream_definition::StreamDefinition {
                            abstract_definition: table_def.abstract_definition.clone(),
                            with_config: None,
                        },
                    );
                    let table_meta = MetaStreamEvent::new_for_single_input(table_stream_def);

                    // Create mutation context with both stream and table metadata
                    let mut mutation_table_meta_map = expr_parser_context.table_meta_map.clone();
                    let table_meta_arc = Arc::new(table_meta);
                    mutation_table_meta_map.insert(delete_action.target_id.clone(), Arc::clone(&table_meta_arc));
                    let mut mutation_stream_positions = expr_parser_context.stream_positions.clone();
                    mutation_stream_positions.insert(delete_action.target_id.clone(), 1);

                    // Register target alias if present (e.g., "s" in "DELETE FROM stockTable AS s")
                    if let Some(ref target_alias) = delete_action.target_alias {
                        mutation_table_meta_map.insert(target_alias.clone(), Arc::clone(&table_meta_arc));
                        mutation_stream_positions.insert(target_alias.clone(), 1);
                    }

                    // Clone stream_meta_map for mutation context, replacing original name with alias if present
                    let mut mutation_stream_meta_map = HashMap::new();

                    // Register source stream - use alias if present, otherwise use original name
                    // When aliased, standard SQL requires using the alias, so we replace rather than add
                    if let Some((original_name, source_meta)) = expr_parser_context.stream_meta_map.iter().next() {
                        if let Some(ref source_alias) = delete_action.source_alias {
                            // Use alias instead of original name to avoid duplicate entries
                            mutation_stream_meta_map.insert(source_alias.clone(), Arc::clone(source_meta));
                            mutation_stream_positions.insert(source_alias.clone(), 0);
                        } else {
                            // No alias - use original name
                            mutation_stream_meta_map.insert(original_name.clone(), Arc::clone(source_meta));
                        }
                    }

                    // For DELETE mutations, default unqualified columns to the target table
                    // This matches standard SQL semantics where unqualified columns in DELETE
                    // statements refer to the target table
                    let mutation_ctx = ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&eventflux_query_context),
                        stream_meta_map: mutation_stream_meta_map,
                        table_meta_map: mutation_table_meta_map,
                        window_meta_map: expr_parser_context.window_meta_map.clone(),
                        aggregation_meta_map: expr_parser_context.aggregation_meta_map.clone(),
                        state_meta_map: expr_parser_context.state_meta_map.clone(),
                        stream_positions: mutation_stream_positions,
                        default_source: delete_action.target_id.clone(),
                        query_name: &query_name,
                        is_mutation_context: true,
                    };

                    // Parse condition expression for stream-triggered delete
                    let condition_executor = parse_expression(&delete_action.on_delete_expression, &mutation_ctx)
                        .map_err(|e| format!("Failed to parse DELETE condition: {}", e))?;

                    let delete_processor = Arc::new(Mutex::new(
                        crate::core::query::output::DeleteTableProcessor::new_with_expression(
                            table,
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&eventflux_query_context),
                            condition_executor,
                        ),
                    ));
                    link_processor(delete_processor);
                } else {
                    return Err(format!(
                        "Delete target '{}' not found for query '{}'",
                        delete_action.target_id, query_name
                    ));
                }
            }
            crate::query_api::execution::query::output::output_stream::OutputStreamAction::UpdateOrInsert(upsert_action) => {
                if let Some(table) = eventflux_app_context.get_eventflux_context().get_table(&upsert_action.target_id) {
                    // Get table definition to create table metadata for expression parsing
                    let table_def = table_def_map
                        .get(&upsert_action.target_id)
                        .ok_or_else(|| format!("Table definition '{}' not found", upsert_action.target_id))?
                        .clone();

                    // Create MetaStreamEvent for the table
                    let table_stream_def = Arc::new(
                        crate::query_api::definition::stream_definition::StreamDefinition {
                            abstract_definition: table_def.abstract_definition.clone(),
                            with_config: None,
                        },
                    );
                    let table_meta = MetaStreamEvent::new_for_single_input(table_stream_def);

                    // Create mutation context with both stream and table metadata
                    let mut mutation_table_meta_map = expr_parser_context.table_meta_map.clone();
                    let table_meta_arc = Arc::new(table_meta);
                    mutation_table_meta_map.insert(upsert_action.target_id.clone(), Arc::clone(&table_meta_arc));
                    let mut mutation_stream_positions = expr_parser_context.stream_positions.clone();
                    mutation_stream_positions.insert(upsert_action.target_id.clone(), 1);

                    // Register target alias if present (future support for "UPSERT INTO table AS t")
                    if let Some(ref target_alias) = upsert_action.target_alias {
                        mutation_table_meta_map.insert(target_alias.clone(), Arc::clone(&table_meta_arc));
                        mutation_stream_positions.insert(target_alias.clone(), 1);
                    }

                    // Clone stream_meta_map for mutation context, replacing original name with alias if present
                    let mut mutation_stream_meta_map = HashMap::new();

                    // Register source stream - use alias if present, otherwise use original name
                    // When aliased, standard SQL requires using the alias, so we replace rather than add
                    if let Some((original_name, source_meta)) = expr_parser_context.stream_meta_map.iter().next() {
                        if let Some(ref source_alias) = upsert_action.source_alias {
                            // Use alias instead of original name to avoid duplicate entries
                            mutation_stream_meta_map.insert(source_alias.clone(), Arc::clone(source_meta));
                            mutation_stream_positions.insert(source_alias.clone(), 0);
                        } else {
                            // No alias - use original name
                            mutation_stream_meta_map.insert(original_name.clone(), Arc::clone(source_meta));
                        }
                    }

                    // For UPSERT mutations, default unqualified columns to the target table
                    // This matches standard SQL semantics where unqualified columns in UPSERT
                    // statements refer to the target table
                    let mutation_ctx = ExpressionParserContext {
                        eventflux_app_context: Arc::clone(eventflux_app_context),
                        eventflux_query_context: Arc::clone(&eventflux_query_context),
                        stream_meta_map: mutation_stream_meta_map,
                        table_meta_map: mutation_table_meta_map,
                        window_meta_map: expr_parser_context.window_meta_map.clone(),
                        aggregation_meta_map: expr_parser_context.aggregation_meta_map.clone(),
                        state_meta_map: expr_parser_context.state_meta_map.clone(),
                        stream_positions: mutation_stream_positions,
                        default_source: upsert_action.target_id.clone(),
                        query_name: &query_name,
                        is_mutation_context: true,
                    };

                    // Parse ON condition expression for upsert
                    let condition_executor = parse_expression(&upsert_action.on_update_expression, &mutation_ctx)
                        .map_err(|e| format!("Failed to parse UPSERT ON condition: {}", e))?;

                    // Parse SET clause value expressions and compute column indices (if partial update specified)
                    let (set_value_executors, set_column_indices): (Vec<Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>>, Vec<usize>) =
                        if let Some(ref update_set) = upsert_action.update_set_clause {
                            let mut executors = Vec::new();
                            let mut indices = Vec::new();
                            let table_attrs = table_def.abstract_definition.get_attribute_list();
                            for set_attr in &update_set.set_attributes {
                                let exec = parse_expression(&set_attr.value_to_set, &mutation_ctx)
                                    .map_err(|e| format!("Failed to parse SET value: {}", e))?;
                                executors.push(exec);

                                // Look up column index by name
                                let col_name = set_attr.table_column.get_attribute_name();
                                if let Some(col_idx) = table_attrs.iter().position(|a| a.get_name() == col_name) {
                                    indices.push(col_idx);
                                } else {
                                    return Err(format!(
                                        "SET column '{}' not found in table '{}'",
                                        col_name, upsert_action.target_id
                                    ));
                                }
                            }
                            (executors, indices)
                        } else {
                            (Vec::new(), Vec::new())
                        };

                    let upsert_processor = Arc::new(Mutex::new(
                        crate::core::query::output::UpsertTableProcessor::new(
                            table,
                            Arc::clone(eventflux_app_context),
                            Arc::clone(&eventflux_query_context),
                            condition_executor,
                            set_value_executors,
                            set_column_indices,
                        ),
                    ));
                    link_processor(upsert_processor);
                } else {
                    return Err(format!(
                        "Upsert target '{}' not found for query '{}'",
                        upsert_action.target_id, query_name
                    ));
                }
            }
            _ => return Err(format!("Query '{query_name}': Only INSERT INTO, UPDATE, DELETE, UPSERT outputs supported for now.")),
        }

        // For N-element patterns, connect the terminal's output to the processor chain head
        // This bridges the PostStateProcessor chain to the Processor chain
        if let Some(ref terminal) = n_element_terminal {
            if let Some(ref head) = processor_chain_head {
                terminal
                    .lock()
                    .unwrap()
                    .set_output_processor(Arc::clone(head));
            }
        }

        // 7. Create QueryRuntime
        let mut query_runtime = QueryRuntime::new_with_context(
            query_name.clone(),
            Arc::new(api_query.clone()),
            Arc::clone(&eventflux_query_context),
        );
        query_runtime.processor_chain_head = processor_chain_head;

        // 8. Register the entry processor with the input stream junction if applicable
        // NOTE: For N-element patterns, skip this - the terminal bridges to the processor chain
        // NOTE: For logical patterns, skip this - they subscribe their processors directly to junctions
        if n_element_terminal.is_none() && !is_logical_pattern {
            if let Some(head_proc_arc) = &query_runtime.processor_chain_head {
                if let Some(junction) = stream_junction_map.get(&expr_parser_context.default_source)
                {
                    junction
                        .lock()
                        .expect("Input junction Mutex poisoned")
                        .subscribe(Arc::clone(head_proc_arc));
                }
            }
        }

        Ok(query_runtime)
    }
}
