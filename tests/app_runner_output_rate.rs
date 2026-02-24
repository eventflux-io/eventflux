// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{InMemoryPersistenceStore, PersistenceStore};
use eventflux::query_api::definition::{attribute::Type as AttrType, StreamDefinition};
use eventflux::query_api::eventflux_app::EventFluxApp;
use eventflux::query_api::execution::query::input::InputStream;
use eventflux::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use eventflux::query_api::execution::query::output::ratelimit::{
    OutputRate, OutputRateBehavior,
};
use eventflux::query_api::execution::query::selection::Selector;
use eventflux::query_api::execution::query::Query;
use eventflux::query_api::execution::ExecutionElement;
use eventflux::query_api::expression::{constant::Constant, variable::Variable};
use std::sync::Arc;

fn make_app() -> EventFluxApp {
    let mut app = EventFluxApp::new("RateApp".to_string());
    app.add_stream_definition(
        StreamDefinition::new("In".to_string()).attribute("v".to_string(), AttrType::INT),
    );
    app.add_stream_definition(
        StreamDefinition::new("Out".to_string()).attribute("v".to_string(), AttrType::INT),
    );
    let input = InputStream::stream("In".to_string());
    let selector = Selector::new().select_variable(Variable::new("v".to_string()));
    let out_action = InsertIntoStreamAction {
        target_id: "Out".to_string(),
        is_inner_stream: false,
        is_fault_stream: false,
    };
    let out_stream = OutputStream::new(OutputStreamAction::InsertInto(out_action), None);
    let rate = OutputRate::per_events(Constant::int(2), OutputRateBehavior::All).unwrap();
    let query = Query::query()
        .from(input)
        .select(selector)
        .out_stream(out_stream)
        .output(rate);
    app.add_execution_element(ExecutionElement::Query(query));
    app
}

#[tokio::test]
async fn rate_limit_emits_in_batches() {
    let app = make_app();
    let runner = AppRunner::new_from_api(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 0);
    runner.send("In", vec![AttributeValue::Int(2)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2);
    runner.send("In", vec![AttributeValue::Int(3)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2);
    runner.send("In", vec![AttributeValue::Int(4)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 4);
    let out = runner.shutdown();
    assert_eq!(out.len(), 4);
}

#[tokio::test]
async fn rate_limit_persist_restore() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = make_app();
    let runner = AppRunner::new_from_api_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(2)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2);
    runner.restore_revision(&rev);
    runner.send("In", vec![AttributeValue::Int(3)]);
    runner.send("In", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 5);
}
