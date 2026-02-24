// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext,
    eventflux_query_context::EventFluxQueryContext,
    stream_config::{FlatConfig, PropertySource},
};
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::extension::{AttributeAggregatorFactory, WindowProcessorFactory};
use eventflux::core::query::processor::stream::window::WindowProcessor;
use eventflux::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::core::util::parser::{
    parse_expression, EventFluxAppParser, ExpressionParserContext, QueryParser,
};
use eventflux::query_api::definition::{
    attribute::Type as AttrType, StreamDefinition, TableDefinition,
};
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::execution::query::input::handler::WindowHandler;
use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::selection::Selector;
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::expression::Expression;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct DummyWindowProcessor {
    meta: CommonProcessorMeta,
}
impl Processor for DummyWindowProcessor {
    fn process(&self, c: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            next.lock().unwrap().process(c);
        }
    }
    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }
    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }
    fn clone_processor(&self, q: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(DummyWindowProcessor {
            meta: CommonProcessorMeta::new(
                Arc::clone(&self.meta.eventflux_app_context),
                Arc::clone(q),
            ),
        })
    }
    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::clone(&self.meta.eventflux_query_context)
    }
    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }
    fn is_stateful(&self) -> bool {
        true
    }
}
impl WindowProcessor for DummyWindowProcessor {}

#[derive(Debug, Clone)]
struct DummyWindowFactory;
impl WindowProcessorFactory for DummyWindowFactory {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create(
        &self,
        _h: &WindowHandler,
        app: Arc<EventFluxAppContext>,
        q: Arc<EventFluxQueryContext>,
        _parse_ctx: &eventflux::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(DummyWindowProcessor {
            meta: CommonProcessorMeta::new(app, q),
        })))
    }
    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self)
    }
}

#[derive(Debug, Clone)]
struct ConstAggFactory;
#[derive(Debug, Default)]
struct ConstAggExec;
impl AttributeAggregatorFactory for ConstAggFactory {
    fn name(&self) -> &'static str {
        "constAgg"
    }
    fn create(
        &self,
    ) -> Box<
        dyn eventflux::core::query::selector::attribute::aggregator::AttributeAggregatorExecutor,
    >{
        Box::new(ConstAggExec)
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
        Box::new(Self)
    }
}
use eventflux::core::executor::expression_executor::ExpressionExecutor;
use eventflux::core::query::selector::attribute::aggregator::AttributeAggregatorExecutor;
impl AttributeAggregatorExecutor for ConstAggExec {
    fn init(
        &mut self,
        _e: Vec<Box<dyn ExpressionExecutor>>,
        _m: ProcessingMode,
        _ex: bool,
        _ctx: &EventFluxQueryContext,
    ) -> Result<(), String> {
        Ok(())
    }
    fn process_add(&self, _d: Option<AttributeValue>) -> Option<AttributeValue> {
        Some(AttributeValue::Int(42))
    }
    fn process_remove(&self, _d: Option<AttributeValue>) -> Option<AttributeValue> {
        Some(AttributeValue::Int(42))
    }
    fn reset(&self) -> Option<AttributeValue> {
        Some(AttributeValue::Int(42))
    }
    fn clone_box(&self) -> Box<dyn AttributeAggregatorExecutor> {
        Box::new(ConstAggExec)
    }
}
impl ExpressionExecutor for ConstAggExec {
    fn execute(&self, _e: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        Some(AttributeValue::Int(42))
    }
    fn get_return_type(&self) -> AttrType {
        AttrType::INT
    }
    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ConstAggExec)
    }
    fn is_attribute_aggregator(&self) -> bool {
        true
    }
}

fn make_ctx_with_manager(
    manager: &EventFluxManager,
    name: &str,
) -> ExpressionParserContext<'static> {
    let app = Arc::new(EventFluxApp::new("app".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        manager.eventflux_context(),
        "app".to_string(),
        Arc::clone(&app),
        String::new(),
    ));
    let q_ctx = Arc::new(EventFluxQueryContext::new(
        Arc::clone(&app_ctx),
        name.to_string(),
        None,
    ));
    let stream_def =
        Arc::new(StreamDefinition::new("s".to_string()).attribute("v".to_string(), AttrType::INT));
    let meta =
        eventflux::core::event::stream::meta_stream_event::MetaStreamEvent::new_for_single_input(
            Arc::clone(&stream_def),
        );
    let mut smap = HashMap::new();
    smap.insert("s".to_string(), Arc::new(meta));
    ExpressionParserContext {
        eventflux_app_context: app_ctx,
        eventflux_query_context: q_ctx,
        stream_meta_map: smap,
        table_meta_map: HashMap::new(),
        window_meta_map: HashMap::new(),
        aggregation_meta_map: HashMap::new(),
        state_meta_map: HashMap::new(),
        stream_positions: {
            let mut m = HashMap::new();
            m.insert("s".to_string(), 0);
            m
        },
        default_source: "s".to_string(),
        query_name: Box::leak(name.to_string().into_boxed_str()),
        is_mutation_context: false,
    }
}

fn setup_query_env(
    manager: &EventFluxManager,
) -> (
    Arc<EventFluxAppContext>,
    HashMap<String, Arc<Mutex<StreamJunction>>>,
) {
    let ctx = manager.eventflux_context();
    let app = Arc::new(EventFluxApp::new("A".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        ctx,
        "A".to_string(),
        Arc::clone(&app),
        String::new(),
    ));

    let in_def = Arc::new(
        StreamDefinition::new("Input".to_string()).attribute("v".to_string(), AttrType::INT),
    );
    let out_def = Arc::new(
        StreamDefinition::new("Out".to_string()).attribute("v".to_string(), AttrType::INT),
    );

    let in_j = Arc::new(Mutex::new(
        StreamJunction::new(
            "Input".to_string(),
            Arc::clone(&in_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));
    let out_j = Arc::new(Mutex::new(
        StreamJunction::new(
            "Out".to_string(),
            Arc::clone(&out_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));

    let mut map = HashMap::new();
    map.insert("Input".to_string(), in_j);
    map.insert("Out".to_string(), out_j);
    (app_ctx, map)
}

#[test]
fn test_register_window_factory() {
    let manager = EventFluxManager::new();
    manager.add_window_factory("dummy".to_string(), Box::new(DummyWindowFactory));
    let handler = WindowHandler::new("dummy".to_string(), None, vec![]);
    let app = Arc::new(EventFluxApp::new("A".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        manager.eventflux_context(),
        "A".to_string(),
        Arc::clone(&app),
        String::new(),
    ));
    let q_ctx = Arc::new(EventFluxQueryContext::new(
        Arc::clone(&app_ctx),
        "q".to_string(),
        None,
    ));

    // Create minimal ExpressionParserContext for the test
    let parse_ctx =
        eventflux::core::util::parser::expression_parser::ExpressionParserContext {
            eventflux_app_context: Arc::clone(&app_ctx),
            eventflux_query_context: Arc::clone(&q_ctx),
            stream_meta_map: std::collections::HashMap::new(),
            table_meta_map: std::collections::HashMap::new(),
            window_meta_map: std::collections::HashMap::new(),
            aggregation_meta_map: std::collections::HashMap::new(),
            state_meta_map: std::collections::HashMap::new(),
            stream_positions: std::collections::HashMap::new(),
            default_source: String::new(),
            query_name: "test_query",
            is_mutation_context: false,
        };

    let res = eventflux::core::query::processor::stream::window::create_window_processor(
        &handler, app_ctx, q_ctx, &parse_ctx,
    );
    assert!(res.is_ok());
}

#[test]
fn test_register_attribute_aggregator_factory() {
    let manager = EventFluxManager::new();
    manager.add_attribute_aggregator_factory("constAgg".to_string(), Box::new(ConstAggFactory));
    let ctx = make_ctx_with_manager(&manager, "agg");
    let expr = Expression::function_no_ns("constAgg".to_string(), vec![]);
    let exec = parse_expression(&expr, &ctx).unwrap();
    assert_eq!(exec.execute(None), Some(AttributeValue::Int(42)));
}

#[test]
fn test_query_parser_uses_custom_window_factory() {
    let manager = EventFluxManager::new();
    manager.add_window_factory("dummyWin".to_string(), Box::new(DummyWindowFactory));

    let (app_ctx, junctions) = setup_query_env(&manager);

    let si = SingleInputStream::new_basic("Input".to_string(), false, false, None, Vec::new())
        .window(None, "dummyWin".to_string(), vec![]);
    let input = InputStream::Single(si);
    let selector = Selector::new();
    let insert_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);

    let runtime = QueryParser::parse_query(
        &query,
        &app_ctx,
        &junctions,
        &HashMap::new(),
        &HashMap::new(),
        None,
        0, // query_index
    )
    .unwrap();
    let head = runtime.processor_chain_head.expect("head");
    let dbg = format!("{:?}", head.lock().unwrap());
    assert!(dbg.contains("DummyWindowProcessor"));
}

// TODO: NOT PART OF M1 - Old EventFluxQL syntax for custom window
// This test uses "define stream" and old query syntax.
// Custom window functionality works (see other tests in this file using programmatic API),
// but SQL syntax for custom windows is not part of M1.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn app_runner_custom_window() {
    let manager = EventFluxManager::new();
    manager.add_window_factory("ptWin".to_string(), Box::new(DummyWindowFactory));
    let app = "\
        define stream In (v int);\n\
        define stream Out (v int);\n\
        from In#window:ptWin() select v insert into Out;\n";
    let runner = common::AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(1)]]);
}

#[test]
fn test_table_factory_invoked() {
    static CREATED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, Clone)]
    struct RecordingFactory;
    impl eventflux::core::extension::TableFactory for RecordingFactory {
        fn name(&self) -> &'static str {
            "rec"
        }
        fn create(
            &self,
            _n: String,
            _p: HashMap<String, String>,
            _c: Arc<eventflux::core::config::eventflux_context::EventFluxContext>,
        ) -> Result<Arc<dyn eventflux::core::table::Table>, String> {
            CREATED.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(eventflux::core::table::InMemoryTable::new()))
        }
        fn clone_box(&self) -> Box<dyn eventflux::core::extension::TableFactory> {
            Box::new(self.clone())
        }
    }

    let manager = EventFluxManager::new();
    manager.add_table_factory("rec".to_string(), Box::new(RecordingFactory));

    let ctx = manager.eventflux_context();
    let app = Arc::new(EventFluxApp::new("TApp".to_string()));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&ctx),
        "TApp".to_string(),
        Arc::clone(&app),
        String::new(),
    ));

    // Create WITH config: WITH ('extension' = 'rec')
    let mut with_config = FlatConfig::new();
    with_config.set("extension", "rec", PropertySource::SqlWith);

    let mut table_def =
        TableDefinition::id("T1".to_string()).attribute("v".to_string(), AttrType::INT);
    table_def.with_config = Some(with_config);

    let mut app_obj = EventFluxApp::new("TApp".to_string());
    app_obj
        .table_definition_map
        .insert("T1".to_string(), Arc::new(table_def));

    let _ = EventFluxAppParser::parse_eventflux_app_runtime_builder(
        &app_obj,
        Arc::clone(&app_ctx),
        None,
    )
    .unwrap();

    assert_eq!(CREATED.load(Ordering::SeqCst), 1);
    assert!(app_ctx.get_eventflux_context().get_table("T1").is_some());
}
