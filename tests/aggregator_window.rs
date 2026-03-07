// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
    eventflux_query_context::EventFluxQueryContext,
};
use eventflux::core::event::complex_event::{ComplexEvent, ComplexEventType};
use eventflux::core::event::stream::meta_stream_event::MetaStreamEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::util::parser::{parse_expression, ExpressionParserContext};
use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::{variable::Variable, Expression};
use std::collections::HashMap;
use std::sync::Arc;

fn make_ctx_with_attr(
    name: &str,
    attr_name: &str,
    attr_type: AttrType,
) -> ExpressionParserContext<'static> {
    let app_ctx = Arc::new(EventFluxAppContext::new(
        Arc::new(EventFluxContext::default()),
        "app".to_string(),
        Arc::new(EventFluxApp::new("app".to_string())),
        String::new(),
    ));
    let q_ctx = Arc::new(EventFluxQueryContext::new(
        Arc::clone(&app_ctx),
        name.to_string(),
        None,
    ));
    let stream_def = Arc::new(
        StreamDefinition::new("s".to_string()).attribute(attr_name.to_string(), attr_type),
    );
    let meta = MetaStreamEvent::new_for_single_input(Arc::clone(&stream_def));
    let mut stream_map = HashMap::new();
    stream_map.insert("s".to_string(), Arc::new(meta));
    let qn: &'static str = Box::leak(name.to_string().into_boxed_str());
    ExpressionParserContext {
        eventflux_app_context: Arc::clone(&app_ctx),
        eventflux_query_context: q_ctx,
        stream_meta_map: stream_map,
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
        query_name: qn,
        is_mutation_context: false,
    }
}

fn make_ctx(name: &str) -> ExpressionParserContext<'static> {
    make_ctx_with_attr(name, "price", AttrType::INT)
}

#[test]
fn test_sum_aggregator() {
    let ctx = make_ctx("sumq");
    let expr = Expression::function_no_ns(
        "sum".to_string(),
        vec![Expression::Variable(Variable::new("price".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();
    // build events
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Int(5);
    let mut e2 = StreamEvent::new(0, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Int(10);

    assert_eq!(exec.execute(Some(&e1)), Some(AttributeValue::Long(5)));
    assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Long(15)));
    let mut reset = StreamEvent::new(0, 0, 0, 0);
    reset.set_event_type(ComplexEventType::Reset);
    exec.execute(Some(&reset));
    let mut e3 = StreamEvent::new(0, 1, 0, 0);
    e3.before_window_data[0] = AttributeValue::Int(4);
    assert_eq!(exec.execute(Some(&e3)), Some(AttributeValue::Long(4)));
}

#[test]
fn test_window_variable_resolution() {
    let mut ctx = make_ctx("win_var");
    let win_def = Arc::new(
        StreamDefinition::new("Win".to_string()).attribute("v".to_string(), AttrType::INT),
    );
    let mut win_meta = MetaStreamEvent::new_for_single_input(Arc::clone(&win_def));
    win_meta.event_type =
        eventflux::core::event::stream::meta_stream_event::MetaStreamEventType::WINDOW;
    ctx.window_meta_map
        .insert("Win".to_string(), Arc::new(win_meta));

    let var = Variable::new("v".to_string()).of_stream("Win".to_string());
    let expr = Expression::Variable(var);
    let exec = parse_expression(&expr, &ctx).unwrap();
    assert_eq!(exec.get_return_type(), AttrType::INT);
}

#[test]
fn test_aggregation_variable_resolution() {
    let mut ctx = make_ctx("agg_var");
    let agg_def = Arc::new(
        StreamDefinition::new("Agg".to_string()).attribute("total".to_string(), AttrType::LONG),
    );
    let mut agg_meta = MetaStreamEvent::new_for_single_input(Arc::clone(&agg_def));
    agg_meta.event_type =
        eventflux::core::event::stream::meta_stream_event::MetaStreamEventType::AGGREGATE;
    ctx.aggregation_meta_map
        .insert("Agg".to_string(), Arc::new(agg_meta));

    let var = Variable::new("total".to_string()).of_stream("Agg".to_string());
    let expr = Expression::Variable(var);
    let exec = parse_expression(&expr, &ctx).unwrap();
    assert_eq!(exec.get_return_type(), AttrType::LONG);
}

fn make_bool_ctx(name: &str) -> ExpressionParserContext<'static> {
    make_ctx_with_attr(name, "flag", AttrType::BOOL)
}

#[test]
fn test_and_aggregator() {
    let ctx = make_bool_ctx("andq");
    let expr = Expression::function_no_ns(
        "and".to_string(),
        vec![Expression::Variable(Variable::new("flag".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();

    // Add true → true (all values are true)
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e1)), Some(AttributeValue::Bool(true)));

    // Add true → still true
    let mut e2 = StreamEvent::new(0, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Bool(true)));

    // Add false → false (not all values are true)
    let mut e3 = StreamEvent::new(0, 1, 0, 0);
    e3.before_window_data[0] = AttributeValue::Bool(false);
    assert_eq!(exec.execute(Some(&e3)), Some(AttributeValue::Bool(false)));

    // Reset → false (empty window)
    let mut reset = StreamEvent::new(0, 0, 0, 0);
    reset.set_event_type(ComplexEventType::Reset);
    assert_eq!(
        exec.execute(Some(&reset)),
        Some(AttributeValue::Bool(false))
    );

    // Add true after reset → true
    let mut e4 = StreamEvent::new(0, 1, 0, 0);
    e4.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e4)), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_or_aggregator() {
    let ctx = make_bool_ctx("orq");
    let expr = Expression::function_no_ns(
        "or".to_string(),
        vec![Expression::Variable(Variable::new("flag".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();

    // Add false → false
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Bool(false);
    assert_eq!(exec.execute(Some(&e1)), Some(AttributeValue::Bool(false)));

    // Add false → still false
    let mut e2 = StreamEvent::new(0, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Bool(false);
    assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Bool(false)));

    // Add true → true
    let mut e3 = StreamEvent::new(0, 1, 0, 0);
    e3.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e3)), Some(AttributeValue::Bool(true)));

    // Reset → false
    let mut reset = StreamEvent::new(0, 0, 0, 0);
    reset.set_event_type(ComplexEventType::Reset);
    assert_eq!(
        exec.execute(Some(&reset)),
        Some(AttributeValue::Bool(false))
    );
}

#[test]
fn test_and_aggregator_with_remove() {
    let ctx = make_bool_ctx("and_rem");
    let expr = Expression::function_no_ns(
        "and".to_string(),
        vec![Expression::Variable(Variable::new("flag".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();

    // Add true, true → true
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Bool(true);
    exec.execute(Some(&e1));

    let mut e2 = StreamEvent::new(0, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Bool(true)));

    // Add false → false
    let mut e3 = StreamEvent::new(0, 1, 0, 0);
    e3.before_window_data[0] = AttributeValue::Bool(false);
    assert_eq!(exec.execute(Some(&e3)), Some(AttributeValue::Bool(false)));

    // Remove the false → back to true
    let mut exp = StreamEvent::new(0, 1, 0, 0);
    exp.before_window_data[0] = AttributeValue::Bool(false);
    exp.set_event_type(ComplexEventType::Expired);
    assert_eq!(exec.execute(Some(&exp)), Some(AttributeValue::Bool(true)));
}

#[test]
fn test_or_aggregator_with_remove() {
    let ctx = make_bool_ctx("or_rem");
    let expr = Expression::function_no_ns(
        "or".to_string(),
        vec![Expression::Variable(Variable::new("flag".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();

    // Add false, true → true
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Bool(false);
    exec.execute(Some(&e1));

    let mut e2 = StreamEvent::new(0, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Bool(true);
    assert_eq!(exec.execute(Some(&e2)), Some(AttributeValue::Bool(true)));

    // Remove the true → back to false
    let mut exp = StreamEvent::new(0, 1, 0, 0);
    exp.before_window_data[0] = AttributeValue::Bool(true);
    exp.set_event_type(ComplexEventType::Expired);
    assert_eq!(exec.execute(Some(&exp)), Some(AttributeValue::Bool(false)));
}
