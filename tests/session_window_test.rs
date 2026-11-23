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

// TODO: These session window tests use SQL syntax and are all ignored. The session
// window is implemented, but the `WINDOW session(...)` form used here still needs
// checking. See feat/grammar/GRAMMAR.md for the grammar status.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
#[ignore = "GROUP BY with session window syntax needs verification"]
async fn test_basic_session_window() {
    // TODO: Check the syntax for a session window with GROUP BY and an aggregation
    let app = "\
        CREATE STREAM In (user VARCHAR, value INT);\n\
        CREATE STREAM Out (user VARCHAR, total INT);\n\
        INSERT INTO Out\n\
        SELECT user, SUM(value) as total FROM In WINDOW session(5000, user) GROUP BY user;\n";

    let runner = AppRunner::new(app, "Out").await;

    // Two events for alice within the 5000 ms session gap
    runner.send(
        "In",
        vec![
            AttributeValue::String("alice".to_string()),
            AttributeValue::Int(10),
        ],
    );

    runner.send(
        "In",
        vec![
            AttributeValue::String("alice".to_string()),
            AttributeValue::Int(20),
        ],
    );

    // bob has a different session key, so he gets his own session
    runner.send(
        "In",
        vec![
            AttributeValue::String("bob".to_string()),
            AttributeValue::Int(15),
        ],
    );

    // Continue alice's session
    runner.send(
        "In",
        vec![
            AttributeValue::String("alice".to_string()),
            AttributeValue::Int(5),
        ],
    );

    let output = runner.shutdown();
    println!("Session window output: {:?}", output);

    // Session output depends on timing, so only check that something came out
    assert!(!output.is_empty(), "Should have session output");
}

#[tokio::test]
#[ignore = "Session window with aggregations needs syntax verification"]
async fn test_default_session_key() {
    // TODO: Check the syntax for a session window with aggregations
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (total INT, count BIGINT);\n\
        INSERT INTO Out\n\
        SELECT SUM(value) as total, COUNT() as count FROM In WINDOW session(3000);\n";

    let runner = AppRunner::new(app, "Out").await;

    // No session key is given, so all events share the default session
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);

    let output = runner.shutdown();
    println!("Default session key output: {:?}", output);

    // Only checks that output exists, not that it was aggregated into one session
    assert!(!output.is_empty(), "Should have session output");
}

#[tokio::test]
#[ignore = "Session window with GROUP BY needs syntax verification"]
async fn test_session_window_gap_validation() {
    // TODO: Check the syntax for a session window with GROUP BY
    let app = "\
        CREATE STREAM In (id VARCHAR, data INT);\n\
        CREATE STREAM Out (id VARCHAR, count BIGINT);\n\
        INSERT INTO Out\n\
        SELECT id, COUNT() as count FROM In WINDOW session(1000) GROUP BY id;\n";

    let runner = AppRunner::new(app, "Out").await;

    runner.send(
        "In",
        vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(1),
        ],
    );

    let output = runner.shutdown();
    println!("Session gap test output: {:?}", output);

    // A valid 1000 ms gap must be accepted. The test passes if it gets here without a panic.
}
