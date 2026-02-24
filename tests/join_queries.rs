// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_query_context::EventFluxQueryContext;
use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
};
use eventflux::core::event::complex_event::ComplexEvent;
use eventflux::core::event::event::Event;
use eventflux::core::event::state::{MetaStateEvent, StateEvent};
use eventflux::core::event::stream::meta_stream_event::MetaStreamEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::query::output::callback_processor::CallbackProcessor;
use eventflux::core::query::processor::stream::join::{
    JoinProcessor, JoinProcessorSide, JoinSide,
};
use eventflux::core::query::processor::{ProcessingMode, Processor};
use eventflux::core::stream::output::stream_callback::StreamCallback;
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::core::util::parser::QueryParser;
use eventflux::core::util::parser::{parse_expression, ExpressionParserContext};
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::execution::query::input::stream::{
    InputStream, JoinType, SingleInputStream,
};
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::expression::condition::compare::Operator as CompareOp;
use eventflux::query_api::expression::{variable::Variable, Expression};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn setup_context() -> (
    Arc<EventFluxAppContext>,
    HashMap<String, Arc<Mutex<StreamJunction>>>,
) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
        "TestApp".to_string(),
    ));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "TestApp".to_string(),
        Arc::clone(&app),
        String::new(),
    ));

    let left_def = Arc::new(
        StreamDefinition::new("LeftStream".to_string()).attribute("id".to_string(), AttrType::INT),
    );
    let right_def = Arc::new(
        StreamDefinition::new("RightStream".to_string()).attribute("id".to_string(), AttrType::INT),
    );
    let out_def = Arc::new(
        StreamDefinition::new("OutStream".to_string())
            .attribute("l".to_string(), AttrType::INT)
            .attribute("r".to_string(), AttrType::INT),
    );

    let left_junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "LeftStream".to_string(),
            Arc::clone(&left_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));
    let right_junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "RightStream".to_string(),
            Arc::clone(&right_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));
    let out_junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "OutStream".to_string(),
            Arc::clone(&out_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));

    let mut map = HashMap::new();
    map.insert("LeftStream".to_string(), left_junction);
    map.insert("RightStream".to_string(), right_junction);
    map.insert("OutStream".to_string(), out_junction);

    (app_ctx, map)
}

fn build_join_query(join_type: JoinType) -> Query {
    let left =
        SingleInputStream::new_basic("LeftStream".to_string(), false, false, None, Vec::new());
    let right =
        SingleInputStream::new_basic("RightStream".to_string(), false, false, None, Vec::new());
    let cond = Expression::compare(
        Expression::Variable(Variable::new("id".to_string()).of_stream("LeftStream".to_string())),
        CompareOp::Equal,
        Expression::Variable(Variable::new("id".to_string()).of_stream("RightStream".to_string())),
    );
    let input = InputStream::join_stream(left, join_type, right, Some(cond), None, None, None);
    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("l".to_string()),
            Expression::Variable(
                Variable::new("id".to_string()).of_stream("LeftStream".to_string()),
            ),
        ),
        OutputAttribute::new(
            Some("r".to_string()),
            Expression::Variable(
                Variable::new("id".to_string()).of_stream("RightStream".to_string()),
            ),
        ),
    ];
    let insert_action = InsertIntoStreamAction {
        target_id: "OutStream".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream)
}

#[test]
fn test_parse_inner_join() {
    let (app_ctx, junctions) = setup_context();
    let q = build_join_query(JoinType::InnerJoin);
    assert!(QueryParser::parse_query_test(
        &q,
        &app_ctx,
        &junctions,
        &HashMap::new(),
        &HashMap::new(),
        None
    )
    .is_ok());
}

#[test]
fn test_parse_left_outer_join() {
    let (app_ctx, junctions) = setup_context();
    let q = build_join_query(JoinType::LeftOuterJoin);
    assert!(QueryParser::parse_query_test(
        &q,
        &app_ctx,
        &junctions,
        &HashMap::new(),
        &HashMap::new(),
        None
    )
    .is_ok());
}

#[derive(Debug)]
struct CollectCallback {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
}

impl StreamCallback for CollectCallback {
    fn receive_events(&self, events: &[Event]) {
        let mut vec = self.events.lock().unwrap();
        for e in events {
            vec.push(e.data.clone());
        }
    }
}

fn collect_from_out_stream(
    app_ctx: &Arc<EventFluxAppContext>,
    junctions: &HashMap<String, Arc<Mutex<StreamJunction>>>,
) -> Arc<Mutex<Vec<Vec<AttributeValue>>>> {
    let out_junction = junctions.get("OutStream").unwrap().clone();
    let collected = Arc::new(Mutex::new(Vec::new()));
    let cb = CollectCallback {
        events: Arc::clone(&collected),
    };
    let cb_proc = Arc::new(Mutex::new(CallbackProcessor::new(
        Arc::new(Mutex::new(Box::new(cb) as Box<dyn StreamCallback>)),
        Arc::clone(app_ctx),
        Arc::new(
            eventflux::core::config::eventflux_query_context::EventFluxQueryContext::new(
                Arc::clone(app_ctx),
                "callback".to_string(),
                None,
            ),
        ),
    )));
    out_junction.lock().unwrap().subscribe(cb_proc);
    collected
}

#[test]
fn test_inner_join_runtime() {
    let (app_ctx, junctions) = setup_context();
    let q = build_join_query(JoinType::InnerJoin);
    assert!(QueryParser::parse_query_test(
        &q,
        &app_ctx,
        &junctions,
        &HashMap::new(),
        &HashMap::new(),
        None
    )
    .is_ok());
    let collected = collect_from_out_stream(&app_ctx, &junctions);

    {
        let left = junctions.get("LeftStream").unwrap();
        left.lock()
            .unwrap()
            .send_event(Event::new_with_data(0, vec![AttributeValue::Int(1)]));
    }
    {
        let right = junctions.get("RightStream").unwrap();
        right
            .lock()
            .unwrap()
            .send_event(Event::new_with_data(0, vec![AttributeValue::Int(1)]));
    }

    let out = collected.lock().unwrap().clone();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(1)]]
    );
}

#[test]
fn test_left_outer_join_runtime() {
    let (app_ctx, junctions) = setup_context();
    let q = build_join_query(JoinType::LeftOuterJoin);
    assert!(QueryParser::parse_query_test(
        &q,
        &app_ctx,
        &junctions,
        &HashMap::new(),
        &HashMap::new(),
        None
    )
    .is_ok());
    let collected = collect_from_out_stream(&app_ctx, &junctions);

    {
        let left = junctions.get("LeftStream").unwrap();
        left.lock()
            .unwrap()
            .send_event(Event::new_with_data(0, vec![AttributeValue::Int(2)]));
    }

    let out = collected.lock().unwrap().clone();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(2), AttributeValue::Null]]
    );
}

#[derive(Debug)]
struct CollectStateEvents {
    events: Arc<Mutex<Vec<(Option<i32>, Option<i32>)>>>,
}

impl Processor for CollectStateEvents {
    fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
        let mut cur = chunk;
        while let Some(mut ce) = cur {
            cur = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StateEvent>() {
                let l = se
                    .get_stream_event(0)
                    .and_then(|e| match e.before_window_data.get(0) {
                        Some(AttributeValue::Int(v)) => Some(*v),
                        _ => None,
                    });
                let r = se
                    .get_stream_event(1)
                    .and_then(|e| match e.before_window_data.get(0) {
                        Some(AttributeValue::Int(v)) => Some(*v),
                        _ => None,
                    });
                self.events.lock().unwrap().push((l, r));
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }
    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {}
    fn clone_processor(&self, _ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(CollectStateEvents {
            events: Arc::clone(&self.events),
        })
    }
    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::new(EventFluxAppContext::new(
            Arc::new(EventFluxContext::new()),
            "T".to_string(),
            Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                "T".to_string(),
            )),
            String::new(),
        ))
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::new(EventFluxQueryContext::new(
            Arc::new(EventFluxAppContext::new(
                Arc::new(EventFluxContext::new()),
                "T".to_string(),
                Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
                    "T".to_string(),
                )),
                String::new(),
            )),
            "q".to_string(),
            None,
        ))
    }
    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }
    fn is_stateful(&self) -> bool {
        false
    }
}

fn setup_state_join(
    join_type: JoinType,
) -> (
    Arc<Mutex<JoinProcessorSide>>,
    Arc<Mutex<JoinProcessorSide>>,
    Arc<Mutex<Vec<(Option<i32>, Option<i32>)>>>,
) {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let app = Arc::new(eventflux::query_api::eventflux_app::EventFluxApp::new(
        "App".to_string(),
    ));
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::clone(&eventflux_context),
        "App".to_string(),
        Arc::clone(&app),
        String::new(),
    ));
    let query_ctx = Arc::new(
        eventflux::core::config::eventflux_query_context::EventFluxQueryContext::new(
            Arc::clone(&app_ctx),
            "q".to_string(),
            None,
        ),
    );

    let left_def = Arc::new(
        StreamDefinition::new("Left".to_string()).attribute("id".to_string(), AttrType::INT),
    );
    let right_def = Arc::new(
        StreamDefinition::new("Right".to_string()).attribute("id".to_string(), AttrType::INT),
    );

    let left_meta = MetaStreamEvent::new_for_single_input(Arc::clone(&left_def));
    let mut right_meta = MetaStreamEvent::new_for_single_input(Arc::clone(&right_def));
    right_meta.apply_attribute_offset(left_def.abstract_definition.attribute_list.len());

    let mut mse = MetaStateEvent::new(2);
    mse.meta_stream_events[0] = Some(left_meta);
    mse.meta_stream_events[1] = Some(right_meta);

    let mut stream_meta = HashMap::new();
    stream_meta.insert(
        "Left".to_string(),
        Arc::new(mse.get_meta_stream_event(0).unwrap().clone()),
    );
    stream_meta.insert(
        "Right".to_string(),
        Arc::new(mse.get_meta_stream_event(1).unwrap().clone()),
    );

    let ctx = ExpressionParserContext {
        eventflux_app_context: Arc::clone(&app_ctx),
        eventflux_query_context: Arc::clone(&query_ctx),
        stream_meta_map: stream_meta,
        table_meta_map: HashMap::new(),
        window_meta_map: HashMap::new(),
        aggregation_meta_map: HashMap::new(),
        state_meta_map: HashMap::new(),
        stream_positions: {
            let mut m = HashMap::new();
            m.insert("Left".to_string(), 0);
            m.insert("Right".to_string(), 1);
            m
        },
        default_source: "Left".to_string(),
        query_name: "q",
        is_mutation_context: false,
    };

    let cond_exec = None;

    let join = Arc::new(Mutex::new(JoinProcessor::new(
        join_type,
        cond_exec,
        mse,
        Arc::clone(&app_ctx),
        Arc::clone(&query_ctx),
    )));
    let left = JoinProcessor::create_side_processor(&join, JoinSide::Left);
    let right = JoinProcessor::create_side_processor(&join, JoinSide::Right);

    let collected = Arc::new(Mutex::new(Vec::new()));
    let collector = Arc::new(Mutex::new(CollectStateEvents {
        events: Arc::clone(&collected),
    }));
    left.lock().unwrap().set_next_processor(Some(collector));

    (left, right, collected)
}

#[test]
fn test_state_join_inner() {
    let (left, right, out) = setup_state_join(JoinType::InnerJoin);
    let mut le = StreamEvent::new(0, 1, 0, 0);
    le.before_window_data[0] = AttributeValue::Int(1);
    left.lock().unwrap().process(Some(Box::new(le)));
    let mut re = StreamEvent::new(0, 1, 0, 0);
    re.before_window_data[0] = AttributeValue::Int(1);
    right.lock().unwrap().process(Some(Box::new(re)));
    let res = out.lock().unwrap().clone();
    assert_eq!(res, vec![(Some(1), Some(1))]);
}

#[test]
fn test_state_join_left_outer() {
    let (left, _right, out) = setup_state_join(JoinType::LeftOuterJoin);
    let mut le = StreamEvent::new(0, 1, 0, 0);
    le.before_window_data[0] = AttributeValue::Int(2);
    left.lock().unwrap().process(Some(Box::new(le)));
    let res = out.lock().unwrap().clone();
    assert_eq!(res, vec![(Some(2), None)]);
}
