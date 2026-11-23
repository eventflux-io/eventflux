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

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

/// CAST turns a VARCHAR into a DOUBLE.
#[tokio::test]
async fn app_runner_cast_string_to_double() {
    let app = "\
        CREATE STREAM In (price VARCHAR);\n\
        CREATE STREAM Out (numeric_price DOUBLE);\n\
        INSERT INTO Out SELECT CAST(price AS DOUBLE) as numeric_price FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("123.45".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(123.45)]]);
}

/// CAST turns an INT into a VARCHAR.
#[tokio::test]
async fn app_runner_cast_int_to_string() {
    let app = "\
        CREATE STREAM In (count INT);\n\
        CREATE STREAM Out (count_str VARCHAR);\n\
        INSERT INTO Out SELECT CAST(count AS VARCHAR) as count_str FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(42)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::String("42".to_string())]]);
}

/// CAST inside AVG turns a VARCHAR price into a DOUBLE before averaging.
#[tokio::test]
async fn app_runner_cast_with_avg() {
    let app = "\
        CREATE STREAM In (price VARCHAR);\n\
        CREATE STREAM Out (avg_price DOUBLE);\n\
        INSERT INTO Out SELECT AVG(CAST(price AS DOUBLE)) as avg_price FROM In WINDOW('lengthBatch', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("10.0".to_string())]);
    runner.send("In", vec![AttributeValue::String("20.0".to_string())]);
    let out = runner.shutdown();
    // The batch of 2 events averages to 15.0. Only the last output is checked.
    assert!(!out.is_empty());
    let last_output = out.last().unwrap();
    assert_eq!(last_output, &vec![AttributeValue::Double(15.0)]);
}

// TODO: Ignored as "not part of M1". M1 Query 7 covers built in functions with ROUND
// only. LOG and UPPER are now registered as built in functions, but this query has no
// INSERT INTO Out and no Out stream, so it needs fixing before the ignore can go.
// See feat/grammar/GRAMMAR.md for the grammar status.
#[tokio::test]
#[ignore = "Requires LOG/UPPER functions - Not part of M1"]
async fn app_runner_builtin_functions() {
    let app = "\
        CREATE STREAM In (a DOUBLE);\n\
        SELECT LOG(a) as l, UPPER('abc') as u FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(0.0),
            AttributeValue::String("ABC".to_string())
        ]]
    );
}
