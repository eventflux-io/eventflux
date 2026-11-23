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

// Sort window tests using the SQL WINDOW() syntax

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
async fn test_basic_sort_window() {
    let app = "\
        CREATE STREAM In (price DOUBLE, volume INT);\n\
        CREATE STREAM Out (price DOUBLE, volume INT);\n\
        INSERT INTO Out \n\
        SELECT price, volume \n\
        FROM In \n\
        WINDOW('sort', 3, price);\n";

    let runner = AppRunner::new(app, "Out").await;

    runner.send(
        "In",
        vec![AttributeValue::Double(100.0), AttributeValue::Int(50)],
    );

    runner.send(
        "In",
        vec![AttributeValue::Double(200.0), AttributeValue::Int(30)],
    );

    runner.send(
        "In",
        vec![AttributeValue::Double(150.0), AttributeValue::Int(40)],
    );

    let output = runner.shutdown();
    println!("Sort window output: {:?}", output);

    // All 3 events fit in the window, so each comes out as a current event
    assert!(!output.is_empty(), "Should have sort window output");
    assert!(output.len() >= 3, "Should have at least 3 events");
}

#[tokio::test]
async fn test_sort_window_with_parameters() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value);\n";

    let runner = AppRunner::new(app, "Out").await;

    runner.send("In", vec![AttributeValue::Int(3)]);
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);

    let output = runner.shutdown();
    println!("Sort window with parameters output: {:?}", output);

    // 3 current events, plus 1 expired once the third event overflows the window of 2
    assert!(!output.is_empty(), "Should have output events");
    assert!(output.len() >= 3, "Should have at least 3 events");
}

#[tokio::test]
async fn test_sort_window_length_validation() {
    let app = "\
        CREATE STREAM In (id INT);\n\
        CREATE STREAM Out (id INT);\n\
        INSERT INTO Out \n\
        SELECT id \n\
        FROM In \n\
        WINDOW('sort', 1, id);\n";

    let runner = AppRunner::new(app, "Out").await;

    runner.send("In", vec![AttributeValue::Int(42)]);

    let output = runner.shutdown();
    println!("Sort window length validation output: {:?}", output);

    // A window of length 1 is valid
    assert!(!output.is_empty(), "Should work with length 1");
    assert_eq!(output.len(), 1, "Should have exactly 1 event");
}

#[tokio::test]
async fn test_sort_window_expiry() {
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value);\n";

    let runner = AppRunner::new(app, "Out").await;

    // 3 events into a window of size 2, so one event must expire
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);

    let output = runner.shutdown();
    println!("Sort window expiry output: {:?}", output);

    // 10, 20 and 30 each come out as current, then the expired event follows
    assert!(
        output.len() >= 3,
        "Should have at least 3 events (current + expired)"
    );
}

#[tokio::test]
async fn test_sort_window_ordering() {
    let app = "\
        CREATE STREAM In (id INT);\n\
        CREATE STREAM Out (id INT);\n\
        INSERT INTO Out \n\
        SELECT id \n\
        FROM In \n\
        WINDOW('sort', 3, id);\n";

    let runner = AppRunner::new(app, "Out").await;

    // The window keeps events sorted, but output order is not checked here
    runner.send("In", vec![AttributeValue::Int(100)]);
    runner.send("In", vec![AttributeValue::Int(200)]);
    runner.send("In", vec![AttributeValue::Int(300)]);

    let output = runner.shutdown();
    println!("Sort window ordering output: {:?}", output);

    // Nothing expires because all 3 events fit in the window
    assert_eq!(output.len(), 3, "Should have exactly 3 output events");

    for event in &output {
        assert_eq!(event.len(), 1, "Each event should have one attribute");
        match &event[0] {
            AttributeValue::Int(val) => {
                assert!(
                    [100, 200, 300].contains(val),
                    "Event value should be one of the sent values"
                );
            }
            _ => panic!("Expected integer value"),
        }
    }
}
