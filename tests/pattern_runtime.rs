// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::event::Event as CoreEvent;
use eventflux::core::event::value::AttributeValue as CoreAttributeValue;
use eventflux::core::eventflux_app_runtime::EventFluxAppRuntime;
use eventflux::core::stream::output::stream_callback::StreamCallback;
use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp;
use eventflux::query_api::execution::query::input::state::{State, StateElement};
use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::execution::ExecutionElement;
use eventflux::query_api::expression::Expression;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct CollectCallback {
    events: Arc<Mutex<Vec<CoreEvent>>>,
}
impl CollectCallback {
    fn new(v: Arc<Mutex<Vec<CoreEvent>>>) -> Self {
        Self { events: v }
    }
}
impl StreamCallback for CollectCallback {
    fn receive_events(&self, events: &[CoreEvent]) {
        self.events.lock().unwrap().extend_from_slice(events);
    }
}

/// Test sequence pattern WITHOUT EVERY - matches only once
///
/// Pattern: A -> B (no EVERY)
/// Events: A(1), B(2), A(3), B(4)
/// Expected: 1 match (first complete sequence only)
///
/// Without EVERY, the pattern matches once and stops. The second A(3)->B(4)
/// sequence does not match because the pattern has already completed.
/// This is standard CEP behavior - use EVERY for continuous matching.
#[test]
fn test_sequence_runtime_processing() {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let mut app = ApiEventFluxApp::new("TestApp".to_string());

    let a_def =
        StreamDefinition::new("AStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def =
        StreamDefinition::new("BStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("OutStream".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("AStream".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("BStream".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("OutStream".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("AStream".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("BStream".to_string(), false, false, None, Vec::new());
    let sse1 = State::stream(a_si);
    let sse2 = State::stream(b_si);
    let next = State::next(StateElement::Stream(sse1), StateElement::Stream(sse2));
    let state_stream = StateInputStream::sequence_stream(next, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("AStream".to_string()),
            ),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("BStream".to_string()),
            ),
        ),
    ];

    let insert_action = InsertIntoStreamAction {
        target_id: "OutStream".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let app = Arc::new(app);
    let runtime =
        EventFluxAppRuntime::new(Arc::clone(&app), eventflux_context, None).expect("runtime");
    let collected = Arc::new(Mutex::new(Vec::new()));
    runtime
        .add_callback(
            "OutStream",
            Box::new(CollectCallback::new(Arc::clone(&collected))),
        )
        .unwrap();
    let _ = runtime.start();
    let a_handler = runtime.get_input_handler("AStream").unwrap();
    let b_handler = runtime.get_input_handler("BStream").unwrap();

    a_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(0, vec![CoreAttributeValue::Int(1)])
        .unwrap();
    b_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(1, vec![CoreAttributeValue::Int(2)])
        .unwrap();
    a_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(2, vec![CoreAttributeValue::Int(3)])
        .unwrap();
    b_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(3, vec![CoreAttributeValue::Int(4)])
        .unwrap();

    runtime.shutdown();

    let events = collected.lock().unwrap();
    // Without EVERY, pattern matches only once (first complete sequence)
    assert_eq!(events.len(), 1, "Without EVERY, pattern matches only once");
    assert_eq!(events[0].data[0], CoreAttributeValue::Int(1));
    assert_eq!(events[0].data[1], CoreAttributeValue::Int(2));
}

/// Test EVERY pattern - pattern restarts after each complete match
///
/// Pattern: EVERY(A -> B)
/// Events: A(1), B(2), A(3), B(4)
/// Expected: 2 matches (pattern restarts after each completion)
///
/// EVERY must wrap the entire pattern. After A(1)->B(2) completes,
/// the pattern restarts and matches A(3)->B(4).
#[test]
fn test_every_sequence() {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let mut app = ApiEventFluxApp::new("TestApp2".to_string());

    let a_def =
        StreamDefinition::new("AStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def =
        StreamDefinition::new("BStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("OutStream".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("AStream".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("BStream".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("OutStream".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("AStream".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("BStream".to_string(), false, false, None, Vec::new());
    let sse1 = State::stream(a_si);
    let sse2 = State::stream(b_si);
    // EVERY must wrap the ENTIRE pattern at top level
    let inner_pattern = State::next(StateElement::Stream(sse1), StateElement::Stream(sse2));
    let every_pattern = State::every(inner_pattern);
    // EVERY requires PATTERN mode (not SEQUENCE mode)
    let state_stream = StateInputStream::pattern_stream(every_pattern, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("AStream".to_string()),
            ),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("BStream".to_string()),
            ),
        ),
    ];

    let insert_action = InsertIntoStreamAction {
        target_id: "OutStream".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let app = Arc::new(app);
    let runtime =
        EventFluxAppRuntime::new(Arc::clone(&app), eventflux_context, None).expect("runtime");
    let collected = Arc::new(Mutex::new(Vec::new()));
    runtime
        .add_callback(
            "OutStream",
            Box::new(CollectCallback::new(Arc::clone(&collected))),
        )
        .unwrap();
    let _ = runtime.start();
    let a_handler = runtime.get_input_handler("AStream").unwrap();
    let b_handler = runtime.get_input_handler("BStream").unwrap();

    a_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(0, vec![CoreAttributeValue::Int(1)])
        .unwrap();
    b_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(1, vec![CoreAttributeValue::Int(2)])
        .unwrap();
    a_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(2, vec![CoreAttributeValue::Int(3)])
        .unwrap();
    b_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(3, vec![CoreAttributeValue::Int(4)])
        .unwrap();

    runtime.shutdown();

    let events = collected.lock().unwrap();
    assert_eq!(events.len(), 2, "EVERY pattern should produce 2 matches");
    assert_eq!(events[0].data[0], CoreAttributeValue::Int(1));
    assert_eq!(events[0].data[1], CoreAttributeValue::Int(2));
    assert_eq!(events[1].data[0], CoreAttributeValue::Int(3));
    assert_eq!(events[1].data[1], CoreAttributeValue::Int(4));
}

#[test]
fn test_logical_and_pattern() {
    let eventflux_context = Arc::new(EventFluxContext::new());
    let mut app = ApiEventFluxApp::new("TestApp3".to_string());

    let a_def =
        StreamDefinition::new("AStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let b_def =
        StreamDefinition::new("BStream".to_string()).attribute("val".to_string(), AttrType::INT);
    let out_def = StreamDefinition::new("OutStream".to_string())
        .attribute("aval".to_string(), AttrType::INT)
        .attribute("bval".to_string(), AttrType::INT);
    app.stream_definition_map
        .insert("AStream".to_string(), Arc::new(a_def));
    app.stream_definition_map
        .insert("BStream".to_string(), Arc::new(b_def));
    app.stream_definition_map
        .insert("OutStream".to_string(), Arc::new(out_def));

    let a_si = SingleInputStream::new_basic("AStream".to_string(), false, false, None, Vec::new());
    let b_si = SingleInputStream::new_basic("BStream".to_string(), false, false, None, Vec::new());
    let sse1 = State::stream(a_si);
    let sse2 = State::stream(b_si);
    let logical = State::logical_and(sse1, sse2);
    let state_stream = StateInputStream::pattern_stream(logical, None);
    let input = InputStream::State(Box::new(state_stream));

    let mut selector = Selector::new();
    selector.selection_list = vec![
        OutputAttribute::new(
            Some("aval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("AStream".to_string()),
            ),
        ),
        OutputAttribute::new(
            Some("bval".to_string()),
            Expression::Variable(
                eventflux::query_api::expression::variable::Variable::new("val".to_string())
                    .of_stream("BStream".to_string()),
            ),
        ),
    ];

    let insert_action = InsertIntoStreamAction {
        target_id: "OutStream".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(insert_action), None);
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream);
    app.execution_element_list
        .push(ExecutionElement::Query(query));

    let app = Arc::new(app);
    let runtime =
        EventFluxAppRuntime::new(Arc::clone(&app), eventflux_context, None).expect("runtime");
    let collected = Arc::new(Mutex::new(Vec::new()));
    runtime
        .add_callback(
            "OutStream",
            Box::new(CollectCallback::new(Arc::clone(&collected))),
        )
        .unwrap();
    let _ = runtime.start();
    let a_handler = runtime.get_input_handler("AStream").unwrap();
    let b_handler = runtime.get_input_handler("BStream").unwrap();

    a_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(0, vec![CoreAttributeValue::Int(1)])
        .unwrap();
    b_handler
        .lock()
        .unwrap()
        .send_event_with_timestamp(1, vec![CoreAttributeValue::Int(2)])
        .unwrap();

    runtime.shutdown();

    let events = collected.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data[0], CoreAttributeValue::Int(1));
    assert_eq!(events[0].data[1], CoreAttributeValue::Int(2));
}
