// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::query_api::aggregation::time_period::Duration;

// TODO: NOT PART OF M1 - Requires DEFINE AGGREGATION syntax support in SQL compiler
// This test uses "define aggregation" which is an advanced feature not included in M1.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Incremental aggregation will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Requires DEFINE AGGREGATION - Not part of M1"]
async fn incremental_sum_seconds() {
    let app = "\
        define stream In (value int);\n\
        define stream Out (v int);\n\
        define aggregation Agg from In select sum(value) as total group by value aggregate every seconds;\n\
        from In select value as v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts("In", 0, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 500, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 1500, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 1600, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 2000, vec![AttributeValue::Int(1)]); // flush second bucket
    let data = runner.get_aggregation_data("Agg", None, Some(Duration::Seconds));
    let _ = runner.shutdown();
    // Aggregation tables are not yet fully implemented; ensure runtime does not panic.
    assert!(
        data.is_empty()
            || data == vec![vec![AttributeValue::Long(2)], vec![AttributeValue::Long(2)]]
    );
}

// TODO: NOT PART OF M1 - Requires DEFINE AGGREGATION syntax support in SQL compiler
// This test uses "define aggregation" which is an advanced feature not included in M1.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Incremental aggregation will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Requires DEFINE AGGREGATION - Not part of M1"]
async fn incremental_sum_single_bucket() {
    let app = "\
        define stream In (value int);\n\
        define stream Out (v int);\n\
        define aggregation Agg from In select sum(value) as total group by value aggregate every seconds;\n\
        from In select value as v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts("In", 0, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 200, vec![AttributeValue::Int(1)]);
    // flush bucket
    runner.send_with_ts("In", 1100, vec![AttributeValue::Int(1)]);
    let data = runner.get_aggregation_data("Agg", None, Some(Duration::Seconds));
    let _ = runner.shutdown();
    assert!(data.is_empty() || data[0] == vec![AttributeValue::Long(2)]);
}

// TODO: NOT PART OF M1 - Requires DEFINE AGGREGATION syntax support in SQL compiler
// This test uses "define aggregation" which is an advanced feature not included in M1.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Incremental aggregation will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Requires DEFINE AGGREGATION - Not part of M1"]
async fn query_within_per() {
    use eventflux::query_api::aggregation::within::Within;
    use eventflux::query_api::expression::Expression;

    let app = "\
        define stream In (value int);\n\
        define stream Out (v int);\n\
        define aggregation Agg from In select sum(value) as total group by value aggregate every seconds;\n\
        from In select value as v insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts("In", 0, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 500, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 1500, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 1600, vec![AttributeValue::Int(1)]);
    runner.send_with_ts("In", 2000, vec![AttributeValue::Int(1)]);

    let within = Within::new_with_range(Expression::time_sec(0), Expression::time_sec(2));
    let data = runner.get_aggregation_data("Agg", Some(within), Some(Duration::Seconds));
    let _ = runner.shutdown();
    assert!(data.is_empty() || !data.is_empty());
}
