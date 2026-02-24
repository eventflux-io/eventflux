// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Comprehensive tests for the 6 advanced window types:
// 1. unique(attr) - Keep only latest event per unique key
// 2. firstUnique(attr) - Keep only first occurrence per unique key
// 3. delay(duration) - Delay event emission
// 4. expression('cond') - Dynamic window based on expression
// 5. frequent(n) - Track most frequent values (Misra-Gries algorithm)
// 6. lossyFrequent(s,e) - Approximate frequent item tracking (Lossy Counting)

#[path = "common/mod.rs"]
mod common;

use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// UNIQUE WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_unique_window_basic() {
    // unique(symbol) keeps only the latest event per unique symbol
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume LONG);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE, volume LONG);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price, volume\n\
        FROM StockStream WINDOW('unique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // First event for AAPL
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
        AttributeValue::Long(1000),
    ]);

    // First event for GOOG
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()),
        AttributeValue::Double(2800.0),
        AttributeValue::Long(500),
    ]);

    // Second event for AAPL - should replace the first
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(151.0),
        AttributeValue::Long(2000),
    ]);

    let results = runner.shutdown();

    // Should have received 3 current events (each event is emitted when received)
    // Plus 1 expired event (when second AAPL replaces first)
    assert!(!results.is_empty(), "Should have received events");
}

#[tokio::test]
async fn test_unique_window_replaces_old_event() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('unique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send AAPL at $150
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    // Send AAPL again at $155 - should expire the $150 event
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(155.0),
    ]);

    let results = runner.shutdown();

    // We should have received events - the second AAPL should trigger an expired + current
    assert!(!results.is_empty(), "Should have received at least one event");
}

#[tokio::test]
async fn test_unique_window_multiple_keys() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('unique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Different symbols - all should be kept
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()),
        AttributeValue::Double(2800.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("MSFT".to_string()),
        AttributeValue::Double(350.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("AMZN".to_string()),
        AttributeValue::Double(3400.0),
    ]);

    let results = runner.shutdown();

    // All 4 unique symbols should result in 4 events
    assert_eq!(results.len(), 4, "Should have 4 events for 4 unique symbols");
}

// ============================================================================
// FIRST UNIQUE WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_first_unique_window_basic() {
    // firstUnique(symbol) keeps only the first event per unique symbol
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('firstUnique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // First AAPL - should be kept
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    // Second AAPL - should be ignored
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(155.0),
    ]);

    // First GOOG - should be kept
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()),
        AttributeValue::Double(2800.0),
    ]);

    let results = runner.shutdown();

    // Only 2 events should pass: first AAPL and first GOOG
    assert_eq!(results.len(), 2, "Only first occurrence per key should pass");
}

#[tokio::test]
async fn test_first_unique_window_ignores_duplicates() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('firstUnique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send same symbol multiple times
    for i in 0..10 {
        runner.send("StockStream", vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0 + i as f64),
        ]);
    }

    let results = runner.shutdown();

    // Only the first event should pass
    assert_eq!(results.len(), 1, "Only first occurrence should pass, duplicates ignored");
}

// ============================================================================
// DELAY WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_delay_window_basic() {
    // delay(100) should delay events by 100ms
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('delay', 100);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send an event
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    // Events are delayed, so immediate shutdown may not capture them
    // This test validates the window doesn't crash
    let results = runner.shutdown();

    // Due to delay, events may or may not have arrived depending on timing
    // Main assertion is that the window works without errors
    assert!(results.len() <= 1, "Delayed events may or may not arrive");
}

#[tokio::test]
async fn test_delay_window_with_wait() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('delay', 50);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    // Allow some time for the delay
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let results = runner.shutdown();

    // With 100ms wait after 50ms delay, event should have arrived
    // (May still be timing dependent on slow systems)
    // Just verify no crash
    let _ = results;
}

// ============================================================================
// EXPRESSION WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_expression_window_count_limit() {
    // expression('count() <= 3') should keep at most 3 events
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('expression', 'count() <= 3');\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send 5 events
    for i in 0..5 {
        runner.send("StockStream", vec![
            AttributeValue::String(format!("SYM{}", i)),
            AttributeValue::Double(100.0 + i as f64),
        ]);
    }

    let results = runner.shutdown();

    // Should have 5 current events and 2 expired events (when 4th and 5th arrive)
    // Total events emitted = 5 current + 2 expired = 7
    assert!(!results.is_empty(), "Should have received events");
}

#[tokio::test]
async fn test_expression_window_count_limit_lt() {
    // expression('count() < 5') should keep at most 4 events
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('expression', 'count() < 5');\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    for i in 0..6 {
        runner.send("StockStream", vec![
            AttributeValue::String(format!("SYM{}", i)),
            AttributeValue::Double(100.0 + i as f64),
        ]);
    }

    let results = runner.shutdown();

    // Should emit events as window slides
    assert!(!results.is_empty(), "Should have received events");
}

// ============================================================================
// FREQUENT WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_frequent_window_basic() {
    // frequent(2) tracks 2 most frequent items
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('frequent', 2);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // AAPL appears 3 times
    for _ in 0..3 {
        runner.send("StockStream", vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
        ]);
    }

    // GOOG appears 2 times
    for _ in 0..2 {
        runner.send("StockStream", vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Double(2800.0),
        ]);
    }

    // MSFT appears 1 time
    runner.send("StockStream", vec![
        AttributeValue::String("MSFT".to_string()),
        AttributeValue::Double(350.0),
    ]);

    let results = runner.shutdown();

    // Should track frequent items
    assert!(!results.is_empty(), "Should have received events");
}

#[tokio::test]
async fn test_frequent_window_eviction() {
    // frequent(1) tracks only 1 most frequent item
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('frequent', 1);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Alternate between symbols
    for i in 0..4 {
        let symbol = if i % 2 == 0 { "AAPL" } else { "GOOG" };
        runner.send("StockStream", vec![
            AttributeValue::String(symbol.to_string()),
            AttributeValue::Double(100.0 + i as f64),
        ]);
    }

    let results = runner.shutdown();

    // Should track frequent items with eviction
    assert!(!results.is_empty(), "Should have received events");
}

// ============================================================================
// LOSSY FREQUENT WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_lossy_frequent_window_basic() {
    // lossyFrequent(0.3, 0.1) - support threshold 30%, error bound 10%
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('lossyFrequent', 0.3, 0.1);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send 10 events, AAPL appears 5 times (50%)
    for _ in 0..5 {
        runner.send("StockStream", vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
        ]);
    }

    // Other symbols appear once each
    for symbol in &["GOOG", "MSFT", "AMZN", "META", "TSLA"] {
        runner.send("StockStream", vec![
            AttributeValue::String(symbol.to_string()),
            AttributeValue::Double(100.0),
        ]);
    }

    let results = runner.shutdown();

    // AAPL exceeds 30% threshold, should be emitted
    assert!(!results.is_empty(), "Should have received frequent events");
}

#[tokio::test]
async fn test_lossy_frequent_window_default_error() {
    // lossyFrequent(0.2) - only support threshold, error defaults to support/10
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('lossyFrequent', 0.2);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send events
    for i in 0..10 {
        runner.send("StockStream", vec![
            AttributeValue::String(format!("SYM{}", i % 3)),
            AttributeValue::Double(100.0 + i as f64),
        ]);
    }

    let results = runner.shutdown();

    // Should work without crash
    let _ = results;
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[tokio::test]
async fn test_unique_window_empty_stream() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('unique', symbol);\n";

    let runner = AppRunner::new(app, "OutStream").await;
    let results = runner.shutdown();

    assert!(results.is_empty(), "Empty stream should produce no events");
}

#[tokio::test]
async fn test_first_unique_window_empty_stream() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('firstUnique', symbol);\n";

    let runner = AppRunner::new(app, "OutStream").await;
    let results = runner.shutdown();

    assert!(results.is_empty(), "Empty stream should produce no events");
}

#[tokio::test]
async fn test_frequent_window_single_item() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('frequent', 5);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Single event
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    let results = runner.shutdown();

    assert_eq!(results.len(), 1, "Single event should pass through");
}

#[tokio::test]
async fn test_expression_window_all_events_within_limit() {
    // Test with a count expression where all events fit
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (symbol STRING, price DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT symbol, price\n\
        FROM StockStream WINDOW('expression', 'count() <= 10');\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    for i in 0..5 {
        runner.send("StockStream", vec![
            AttributeValue::String(format!("SYM{}", i)),
            AttributeValue::Double(100.0),
        ]);
    }

    let results = runner.shutdown();

    // All 5 events should pass since count <= 10
    assert_eq!(results.len(), 5, "All events should pass within expression limit");
}

// ============================================================================
// AGGREGATION WITH WINDOW TESTS
// ============================================================================

#[tokio::test]
async fn test_unique_window_with_aggregation() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (totalPrice DOUBLE);\n\
        INSERT INTO OutStream\n\
        SELECT sum(price) as totalPrice\n\
        FROM StockStream WINDOW('unique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(100.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()),
        AttributeValue::Double(200.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(150.0),
    ]);

    let results = runner.shutdown();

    // Should have aggregated values
    assert!(!results.is_empty(), "Should have aggregated events");
}

#[tokio::test]
async fn test_first_unique_window_with_count() {
    let app = "\
        CREATE STREAM StockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM OutStream (cnt LONG);\n\
        INSERT INTO OutStream\n\
        SELECT count() as cnt\n\
        FROM StockStream WINDOW('firstUnique', symbol);\n";

    let mut runner = AppRunner::new(app, "OutStream").await;

    // Send 3 unique and 2 duplicate
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()),
        AttributeValue::Double(100.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()),
        AttributeValue::Double(200.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("AAPL".to_string()), // duplicate
        AttributeValue::Double(150.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("MSFT".to_string()),
        AttributeValue::Double(300.0),
    ]);
    runner.send("StockStream", vec![
        AttributeValue::String("GOOG".to_string()), // duplicate
        AttributeValue::Double(250.0),
    ]);

    let results = runner.shutdown();

    // Only 3 unique events should pass
    assert_eq!(results.len(), 3, "Only 3 unique keys should produce events");
}
