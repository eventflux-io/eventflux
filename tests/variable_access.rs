/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO: NOT PART OF M1. All variable access tests are ignored because they use the old
// EventFluxQL syntax ("define stream", "define table", "define aggregation"), which the
// SQL parser does not support. See feat/grammar/GRAMMAR.md for the grammar status.

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
    // An event past the 1 second boundary closes the first bucket
    runner.send_with_ts("In", 1100, vec![AttributeValue::Int(1)]);
    let data = runner.get_aggregation_data("Agg", None, Some(Duration::Seconds));
    let _ = runner.shutdown();
    assert!(data.is_empty() || data[0] == vec![AttributeValue::Long(10)]);
}

#[tokio::test]
#[ignore = "Old EventFluxQL syntax not part of M1"]
async fn window_variable_access() {
    // Simplified to a window on the stream instead of a defined window
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
    // Simplified for now. Writes to a table but does not join with it.
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
    // Simplified for now. No aggregation, it only passes values through.
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
