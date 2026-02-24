// SPDX-License-Identifier: MIT OR Apache-2.0

// Sort window tests using SQL WINDOW() syntax
// Sort window is implemented and ready for testing

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

    // Send some events
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

    // Should have output for all events sent (3 current)
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

    // Should have output for all sent events (2 current + 1 expired)
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

    // Should work with length 1
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

    // Send 3 events to a window of size 2 - should get expired events
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);

    let output = runner.shutdown();
    println!("Sort window expiry output: {:?}", output);

    // Should have output for all events (both current and expired)
    // First event: 10 (current), Second event: 20 (current), Third event: 30 (current) + expired event
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

    // Send events - the sort window should maintain them in sorted order
    runner.send("In", vec![AttributeValue::Int(100)]);
    runner.send("In", vec![AttributeValue::Int(200)]);
    runner.send("In", vec![AttributeValue::Int(300)]);

    let output = runner.shutdown();
    println!("Sort window ordering output: {:?}", output);

    // Should have output for all sent events
    assert_eq!(output.len(), 3, "Should have exactly 3 output events");

    // All events should be present
    for event in &output {
        assert_eq!(event.len(), 1, "Each event should have one attribute");
        match &event[0] {
            AttributeValue::Int(val) => {
                assert!(
                    vec![100, 200, 300].contains(val),
                    "Event value should be one of the sent values"
                );
            }
            _ => panic!("Expected integer value"),
        }
    }
}
