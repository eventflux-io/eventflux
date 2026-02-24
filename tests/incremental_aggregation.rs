// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use eventflux::core::aggregation::IncrementalExecutor;
use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
    eventflux_query_context::EventFluxQueryContext,
};
use eventflux::core::event::stream::meta_stream_event::MetaStreamEvent;
use eventflux::core::event::stream::stream_event::StreamEvent;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::table::InMemoryTable;
use eventflux::core::util::parser::{parse_expression, ExpressionParserContext};
use eventflux::query_api::aggregation::time_period::Duration as TimeDuration;
use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::expression::{variable::Variable, Expression};
use std::collections::HashMap;
use std::sync::Arc;

fn make_ctx(name: &str) -> ExpressionParserContext<'static> {
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
        StreamDefinition::new("InStream".to_string()).attribute("value".to_string(), AttrType::INT),
    );
    let meta = MetaStreamEvent::new_for_single_input(Arc::clone(&stream_def));
    let mut stream_map = HashMap::new();
    stream_map.insert("InStream".to_string(), Arc::new(meta));
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
            m.insert("InStream".to_string(), 0);
            m
        },
        default_source: "InStream".to_string(),
        query_name: qn,
        is_mutation_context: false,
    }
}

#[test]
fn test_incremental_executor_basic() {
    let ctx = make_ctx("inc");
    let expr = Expression::function_no_ns(
        "sum".to_string(),
        vec![Expression::Variable(Variable::new("value".to_string()))],
    );
    let exec = parse_expression(&expr, &ctx).unwrap();
    let table = Arc::new(InMemoryTable::new());
    let mut inc = IncrementalExecutor::new(
        TimeDuration::Seconds,
        vec![exec],
        Box::new(|_| "key".to_string()),
        Arc::clone(&table),
    );
    let mut e1 = StreamEvent::new(0, 1, 0, 0);
    e1.before_window_data[0] = AttributeValue::Int(1);
    inc.execute(&e1);
    let mut e2 = StreamEvent::new(1500, 1, 0, 0);
    e2.before_window_data[0] = AttributeValue::Int(2);
    inc.execute(&e2);
    // flush last bucket
    inc.execute(&StreamEvent::new(2000, 1, 0, 0));
    let rows = table.all_rows();
    assert_eq!(
        rows,
        vec![vec![AttributeValue::Long(1)], vec![AttributeValue::Long(2)]]
    );
}
