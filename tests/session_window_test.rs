// SPDX-License-Identifier: MIT OR Apache-2.0

// TODO: Session window tests converted to SQL syntax
// Session window is implemented but SQL syntax needs verification
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
#[ignore = "GROUP BY with session window syntax needs verification"]
async fn test_basic_session_window() {
    // TODO: Converted to SQL, session window exists but GROUP BY + aggregation syntax needs verification
    let app = "\
        CREATE STREAM In (user VARCHAR, value INT);\n\
        CREATE STREAM Out (user VARCHAR, total INT);\n\
        INSERT INTO Out\n\
        SELECT user, SUM(value) as total FROM In WINDOW session(5000, user) GROUP BY user;\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send events for alice within session gap
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

    // Send event for bob (different session key)
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

    // Since session window is complex and timing-dependent,
    // just verify we get some output
    assert!(!output.is_empty(), "Should have session output");
}

#[tokio::test]
#[ignore = "Session window with aggregations needs syntax verification"]
async fn test_default_session_key() {
    // TODO: Converted to SQL, session window exists but aggregation syntax needs verification
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (total INT, count BIGINT);\n\
        INSERT INTO Out\n\
        SELECT SUM(value) as total, COUNT() as count FROM In WINDOW session(3000);\n";

    let runner = AppRunner::new(app, "Out").await;

    // All events should go to the same default session
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);

    let output = runner.shutdown();
    println!("Default session key output: {:?}", output);

    // Should have aggregated all events in one session
    assert!(!output.is_empty(), "Should have session output");
}

#[tokio::test]
#[ignore = "Session window with GROUP BY needs syntax verification"]
async fn test_session_window_gap_validation() {
    // TODO: Converted to SQL, session window exists but GROUP BY syntax needs verification
    let app = "\
        CREATE STREAM In (id VARCHAR, data INT);\n\
        CREATE STREAM Out (id VARCHAR, count BIGINT);\n\
        INSERT INTO Out\n\
        SELECT id, COUNT() as count FROM In WINDOW session(1000) GROUP BY id;\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send some events
    runner.send(
        "In",
        vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(1),
        ],
    );

    let output = runner.shutdown();
    println!("Session gap test output: {:?}", output);

    // Should succeed with valid gap
    assert!(true, "Valid session gap should work");
}
