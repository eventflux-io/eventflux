// SPDX-License-Identifier: MIT OR Apache-2.0

use eventflux::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_context::EventFluxContext,
};
use eventflux::core::stream::stream_junction::StreamJunction;
use eventflux::core::util::parser::QueryParser;
use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::execution::query::input::state::{State, StateElement};
use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
use eventflux::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use eventflux::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::selection::{OutputAttribute, Selector};
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::expression::Expression;
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

    let a_def = Arc::new(
        StreamDefinition::new("AStream".to_string()).attribute("val".to_string(), AttrType::INT),
    );
    let b_def = Arc::new(
        StreamDefinition::new("BStream".to_string()).attribute("val".to_string(), AttrType::INT),
    );
    let out_def = Arc::new(
        StreamDefinition::new("OutStream".to_string())
            .attribute("aval".to_string(), AttrType::INT)
            .attribute("bval".to_string(), AttrType::INT),
    );

    let a_junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "AStream".to_string(),
            Arc::clone(&a_def),
            Arc::clone(&app_ctx),
            1024,
            false,
            None,
        )
        .unwrap(),
    ));
    let b_junction = Arc::new(Mutex::new(
        StreamJunction::new(
            "BStream".to_string(),
            Arc::clone(&b_def),
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
    map.insert("AStream".to_string(), a_junction);
    map.insert("BStream".to_string(), b_junction);
    map.insert("OutStream".to_string(), out_junction);

    (app_ctx, map)
}

fn build_sequence_query() -> Query {
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
    Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream)
}

#[test]
fn test_sequence_query_parse() {
    let (app_ctx, mut junctions) = setup_context();
    let q = build_sequence_query();
    let res = QueryParser::parse_query_test(
        &q,
        &app_ctx,
        &mut junctions,
        &HashMap::new(),
        &HashMap::new(),
        None,
    );
    if let Err(e) = &res {
        println!("parse err: {}", e);
    }
    assert!(res.is_ok());
}
