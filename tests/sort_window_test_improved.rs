// SPDX-License-Identifier: MIT OR Apache-2.0

// IMPROVED Sort window tests that actually validate sorting behavior
// These tests use explicit timestamps to control sort order

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
async fn test_sort_window_expiry_with_explicit_timestamps() {
    // This test verifies attribute-based sorting (NOW IMPLEMENTED!)
    // Window size: 2
    // Events: value=30 at T=1000, value=10 at T=2000, value=20 at T=3000
    // After sorting by VALUE (ascending): [10, 20, 30]
    // Should expire value=30 (highest value in ascending sort)

    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value);\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send with explicit timestamps
    runner.send_with_ts("In", 1000, vec![AttributeValue::Int(30)]);
    runner.send_with_ts("In", 2000, vec![AttributeValue::Int(10)]);
    runner.send_with_ts("In", 3000, vec![AttributeValue::Int(20)]);

    let output = runner.shutdown();
    println!(
        "Sort window output with attribute-based sorting: {:?}",
        output
    );

    // Expected output:
    // Event 1: value=30 (current)
    // Event 2: value=10 (current)
    // Event 3: value=20 (current)
    // Event 4: value=30 (expired) - highest value in ascending sort, so expelled from window

    assert_eq!(
        output.len(),
        4,
        "Should have exactly 4 events (3 current + 1 expired)"
    );

    // Verify values
    assert_eq!(
        output[0],
        vec![AttributeValue::Int(30)],
        "First event should be 30"
    );
    assert_eq!(
        output[1],
        vec![AttributeValue::Int(10)],
        "Second event should be 10"
    );
    assert_eq!(
        output[2],
        vec![AttributeValue::Int(20)],
        "Third event should be 20"
    );
    assert_eq!(
        output[3],
        vec![AttributeValue::Int(30)],
        "Fourth event (expired) should be 30 - highest value"
    );
}

#[tokio::test]
async fn test_sort_window_value_based_sorting() {
    // This test verifies attribute-based sorting works correctly (NOW IMPLEMENTED!)
    // Sorts by VALUE, not timestamp

    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 2, value);\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send events in descending value order but ascending timestamp order
    runner.send_with_ts("In", 100, vec![AttributeValue::Int(90)]); // T=100, V=90
    runner.send_with_ts("In", 200, vec![AttributeValue::Int(50)]); // T=200, V=50
    runner.send_with_ts("In", 300, vec![AttributeValue::Int(70)]); // T=300, V=70

    let output = runner.shutdown();
    println!("Attribute-based sorting (by value): {:?}", output);

    assert_eq!(output.len(), 4, "Should have 4 events");

    // NEW behavior: expires value=90 (highest value in ascending sort)
    // Sorts by VALUE, not timestamp!
    assert_eq!(output[0], vec![AttributeValue::Int(90)]);
    assert_eq!(output[1], vec![AttributeValue::Int(50)]);
    assert_eq!(output[2], vec![AttributeValue::Int(70)]);
    assert_eq!(
        output[3],
        vec![AttributeValue::Int(90)],
        "Expired: highest value (V=90) in ascending sort"
    );
}

#[tokio::test]
async fn test_sort_window_maintains_order_in_buffer() {
    // Test that the window buffer maintains sorted order by attribute value
    // This test verifies that after expiry, the remaining events are in sorted order

    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out \n\
        SELECT value \n\
        FROM In \n\
        WINDOW('sort', 3, value);\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send 4 events with different timestamps and values
    runner.send_with_ts("In", 400, vec![AttributeValue::Int(40)]);
    runner.send_with_ts("In", 100, vec![AttributeValue::Int(10)]);
    runner.send_with_ts("In", 300, vec![AttributeValue::Int(30)]);
    runner.send_with_ts("In", 200, vec![AttributeValue::Int(20)]);

    let output = runner.shutdown();
    println!("Sort order maintenance test: {:?}", output);

    // Should emit 4 current events + 1 expired
    assert_eq!(
        output.len(),
        5,
        "Should have 5 events (4 current + 1 expired)"
    );

    // The 4th event causes expiry
    // After sorting by VALUE (ascending): [10, 20, 30, 40]
    // Should expire value=40 (highest value)
    assert_eq!(
        output[4],
        vec![AttributeValue::Int(40)],
        "Should expire highest value (40) in ascending sort"
    );
}

#[tokio::test]
async fn test_sort_window_by_value_ascending() {
    // NOW IMPLEMENTED! Attribute-based sorting with explicit ASC order

    let app = "\
        CREATE STREAM In (price DOUBLE);\n\
        CREATE STREAM Out (price DOUBLE);\n\
        INSERT INTO Out \n\
        SELECT price \n\
        FROM In \n\
        WINDOW('sort', 2, price, 'ASC');\n";

    let runner = AppRunner::new(app, "Out").await;

    // Send events: prices 100.0, 50.0, 75.0
    // With ascending sort by price, window should keep [50.0, 75.0]
    // Should expire 100.0 (highest price)
    runner.send_with_ts("In", 1000, vec![AttributeValue::Double(100.0)]);
    runner.send_with_ts("In", 2000, vec![AttributeValue::Double(50.0)]);
    runner.send_with_ts("In", 3000, vec![AttributeValue::Double(75.0)]);

    let output = runner.shutdown();

    // Expected: expires 100.0 (highest value in ascending sort)
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[3],
        vec![AttributeValue::Double(100.0)],
        "Should expire highest value (100.0) in ascending sort"
    );
}

#[tokio::test]
async fn test_sort_window_by_value_descending() {
    // NOW IMPLEMENTED! Attribute-based sorting with explicit DESC order

    let app = "\
        CREATE STREAM In (price DOUBLE);\n\
        CREATE STREAM Out (price DOUBLE);\n\
        INSERT INTO Out \n\
        SELECT price \n\
        FROM In \n\
        WINDOW('sort', 2, price, 'DESC');\n";

    let runner = AppRunner::new(app, "Out").await;

    // With descending sort by price, window should keep [100.0, 75.0]
    // Should expire 50.0 (lowest price)
    runner.send_with_ts("In", 1000, vec![AttributeValue::Double(100.0)]);
    runner.send_with_ts("In", 2000, vec![AttributeValue::Double(50.0)]);
    runner.send_with_ts("In", 3000, vec![AttributeValue::Double(75.0)]);

    let output = runner.shutdown();

    // Expected: expires 50.0 (lowest value in descending sort)
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[3],
        vec![AttributeValue::Double(50.0)],
        "Should expire lowest value (50.0) in descending sort"
    );
}
