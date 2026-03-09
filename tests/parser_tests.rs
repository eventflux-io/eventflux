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

#[tokio::test]
async fn test_filter_projection() {
    // Test WHERE clause filtering with projection
    let app = "\
        CREATE STREAM InputStream (a INT);\n\
        CREATE STREAM OutStream (a INT);\n\
        INSERT INTO OutStream\n\
        SELECT a FROM InputStream WHERE a > 10;\n";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send("InputStream", vec![AttributeValue::Int(5)]);
    runner.send("InputStream", vec![AttributeValue::Int(20)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(20)]]);
}

#[tokio::test]
async fn test_length_window() {
    // Converted to SQL syntax - length window is M1 feature
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(3)],
        ]
    );
}

#[tokio::test]
async fn test_sum_aggregation() {
    // Converted to SQL syntax - sum aggregation is M1 feature
    let app = "\
        CREATE STREAM InStream (v INT);\n\
        CREATE STREAM OutStream (total BIGINT);\n\
        INSERT INTO OutStream\n\
        SELECT SUM(v) as total FROM InStream;\n";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send("InStream", vec![AttributeValue::Int(2)]);
    runner.send("InStream", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Long(2)], vec![AttributeValue::Long(5)]]
    );
}

#[tokio::test]
async fn test_join_query() {
    // Converted to SQL JOIN syntax - JOINs are M1 feature and now working
    let app = "\
        CREATE STREAM Left (a INT);\n\
        CREATE STREAM Right (a INT);\n\
        CREATE STREAM Out (a INT, b INT);\n\
        INSERT INTO Out\n\
        SELECT Left.a as a, Right.a as b FROM Left JOIN Right ON Left.a = Right.a;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("Left", vec![AttributeValue::Int(5)]);
    runner.send("Right", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(5), AttributeValue::Int(5)]]
    );
}

#[tokio::test]
#[ignore = "Namespaced functions (str:length) not yet supported - needs LENGTH() or similar"]
async fn test_builtin_function_in_query() {
    // TODO: Converted to SQL syntax, but str:length() function not in M1
    // Need to determine if we support LENGTH() or need to implement str namespace
    let app = "\
        CREATE STREAM In (v VARCHAR);\n\
        CREATE STREAM Out (len INT);\n\
        INSERT INTO Out\n\
        SELECT LENGTH(v) as len FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("abc".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(3)]]);
}
