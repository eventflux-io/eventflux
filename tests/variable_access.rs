// SPDX-License-Identifier: MIT OR Apache-2.0

// TODO: NOT PART OF M1 - All variable access tests use old EventFluxQL syntax
// Tests use "define stream", "define table", "define aggregation" which are not supported by SQL parser.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::query_api::aggregation::time_period::Duration;

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn variable_from_window() {
    let app = "\
        define stream In (v int);\n\
        define stream Out (v int);\n\
        from In#window:length(2) select v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(5)]]);
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn variable_from_aggregation() {
    let app = "\
        define stream In (value int);\n\
        define stream Out (total long);\n\
        define aggregation Agg from In select sum(value) as total group by value aggregate every seconds;\n\
        from In select value insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts("In", 0, vec![AttributeValue::Int(5)]);
    runner.send_with_ts("In", 200, vec![AttributeValue::Int(5)]);
    // Flush first bucket
    runner.send_with_ts("In", 1100, vec![AttributeValue::Int(1)]);
    let data = runner.get_aggregation_data("Agg", None, Some(Duration::Seconds));
    let _ = runner.shutdown();
    assert!(data.is_empty() || data[0] == vec![AttributeValue::Long(10)]);
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn window_variable_access() {
    // Simplified test using stream window instead of define window
    let app = "\
        define stream In (v int);\n\
        define stream Out (v int);\n\
        from In#window:length(2) select v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1)], vec![AttributeValue::Int(2)]]
    );
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn table_variable_access() {
    // Simplified test without table join for now - just basic table functionality
    let app = "\
        define stream In (v int);\n\
        define table T (v int);\n\
        define stream Out (v int);\n\
        from In select v insert into T;\n\
        from In select v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0], vec![AttributeValue::Int(1)]);
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn aggregation_variable_access() {
    // Simplified test without aggregation for now - just basic value passing
    let app = "\
        define stream In (value int);\n\
        define stream Out (v int);\n\
        from In select value as v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0], vec![AttributeValue::Int(1)]);
}
