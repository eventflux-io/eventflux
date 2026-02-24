// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Comprehensive tests for OUTPUT RATE LIMITING functionality.
// Tests cover event-based rate limiting with ALL, FIRST, and LAST behaviors,
// including edge cases, state persistence, and negative test cases.

#[path = "common/mod.rs"]
mod common;

use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{InMemoryPersistenceStore, PersistenceStore};
use std::sync::Arc;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Extract integer value from output event
fn get_int(event: &[AttributeValue], index: usize) -> i32 {
    match &event[index] {
        AttributeValue::Int(v) => *v,
        _ => panic!("Expected Int at index {}", index),
    }
}

/// Extract float value from output event
fn get_float(event: &[AttributeValue], index: usize) -> f32 {
    match &event[index] {
        AttributeValue::Float(v) => *v,
        _ => panic!("Expected Float at index {}", index),
    }
}

/// Extract string value from output event
fn get_string(event: &[AttributeValue], index: usize) -> String {
    match &event[index] {
        AttributeValue::String(v) => v.clone(),
        _ => panic!("Expected String at index {}", index),
    }
}

// ============================================================================
// POSITIVE TESTS: EVENT-BASED ALL BEHAVIOR
// ============================================================================

/// Test: ALL behavior emits all accumulated events when batch size is reached
/// Verifies: Exact event count, correct values, proper batching
#[tokio::test]
async fn test_all_behavior_exact_batch_emission() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 3 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 3 events - should emit all 3
    runner.send("Input", vec![AttributeValue::Int(10)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 0, "No output before batch complete");

    runner.send("Input", vec![AttributeValue::Int(20)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 0, "No output before batch complete");

    runner.send("Input", vec![AttributeValue::Int(30)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 3, "All 3 events emitted on batch complete");

    // Verify the actual values emitted
    {
        let collected = runner.collected.lock().unwrap();
        assert_eq!(get_int(&collected[0], 0), 10, "First event value");
        assert_eq!(get_int(&collected[1], 0), 20, "Second event value");
        assert_eq!(get_int(&collected[2], 0), 30, "Third event value");
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 3, "Total events after shutdown");
}

/// Test: ALL behavior with multiple complete batches
/// Verifies: Counter resets correctly, multiple emissions work
#[tokio::test]
async fn test_all_behavior_multiple_batches() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 2 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // First batch
    runner.send("Input", vec![AttributeValue::Int(1)]);
    runner.send("Input", vec![AttributeValue::Int(2)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2, "First batch emitted");

    // Second batch
    runner.send("Input", vec![AttributeValue::Int(3)]);
    runner.send("Input", vec![AttributeValue::Int(4)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 4, "Second batch emitted");

    // Third batch
    runner.send("Input", vec![AttributeValue::Int(5)]);
    runner.send("Input", vec![AttributeValue::Int(6)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 6, "Third batch emitted");

    // Verify values from each batch
    let out = runner.shutdown();
    assert_eq!(out.len(), 6);
    assert_eq!(get_int(&out[0], 0), 1);
    assert_eq!(get_int(&out[1], 0), 2);
    assert_eq!(get_int(&out[2], 0), 3);
    assert_eq!(get_int(&out[3], 0), 4);
    assert_eq!(get_int(&out[4], 0), 5);
    assert_eq!(get_int(&out[5], 0), 6);
}

/// Test: ALL behavior preserves event order within batch
#[tokio::test]
async fn test_all_behavior_preserves_order() {
    let app = r#"
        CREATE STREAM Input (id INT, name STRING);
        CREATE STREAM Output (id INT, name STRING);
        INSERT INTO Output
        SELECT id, name FROM Input
        OUTPUT ALL EVERY 4 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    runner.send("Input", vec![AttributeValue::Int(1), AttributeValue::String("first".to_string())]);
    runner.send("Input", vec![AttributeValue::Int(2), AttributeValue::String("second".to_string())]);
    runner.send("Input", vec![AttributeValue::Int(3), AttributeValue::String("third".to_string())]);
    runner.send("Input", vec![AttributeValue::Int(4), AttributeValue::String("fourth".to_string())]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 4);

    // Verify order is preserved
    assert_eq!(get_int(&out[0], 0), 1);
    assert_eq!(get_string(&out[0], 1), "first");
    assert_eq!(get_int(&out[1], 0), 2);
    assert_eq!(get_string(&out[1], 1), "second");
    assert_eq!(get_int(&out[2], 0), 3);
    assert_eq!(get_string(&out[2], 1), "third");
    assert_eq!(get_int(&out[3], 0), 4);
    assert_eq!(get_string(&out[3], 1), "fourth");
}

// ============================================================================
// POSITIVE TESTS: EVENT-BASED FIRST BEHAVIOR
// ============================================================================

/// Test: FIRST behavior only emits first event of each batch
/// Verifies: Only first event is kept, others are discarded
#[tokio::test]
async fn test_first_behavior_keeps_only_first() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 3 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // First batch: send 10, 20, 30 - only 10 should be emitted
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]);
    runner.send("Input", vec![AttributeValue::Int(30)]);

    {
        let collected = runner.collected.lock().unwrap();
        assert_eq!(collected.len(), 1, "Only first event of batch emitted");
        assert_eq!(get_int(&collected[0], 0), 10, "First event value is 10");
    }

    // Second batch: send 40, 50, 60 - only 40 should be emitted
    runner.send("Input", vec![AttributeValue::Int(40)]);
    runner.send("Input", vec![AttributeValue::Int(50)]);
    runner.send("Input", vec![AttributeValue::Int(60)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 2, "Two first events from two batches");
    assert_eq!(get_int(&out[0], 0), 10, "First batch's first event");
    assert_eq!(get_int(&out[1], 0), 40, "Second batch's first event");
}

/// Test: FIRST behavior with multiple batches verifies correct first event selection
#[tokio::test]
async fn test_first_behavior_multiple_batches() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 2 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 6 events in 3 batches of 2
    for i in 1..=6 {
        runner.send("Input", vec![AttributeValue::Int(i * 10)]);
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 3, "3 first events from 3 batches");
    assert_eq!(get_int(&out[0], 0), 10, "First of batch 1 (events 1-2)");
    assert_eq!(get_int(&out[1], 0), 30, "First of batch 2 (events 3-4)");
    assert_eq!(get_int(&out[2], 0), 50, "First of batch 3 (events 5-6)");
}

/// Test: FIRST behavior discards subsequent events but counts them
#[tokio::test]
async fn test_first_behavior_discards_subsequent() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 5 events - only first should be kept
    runner.send("Input", vec![AttributeValue::Int(100)]);
    runner.send("Input", vec![AttributeValue::Int(200)]); // discarded
    runner.send("Input", vec![AttributeValue::Int(300)]); // discarded
    runner.send("Input", vec![AttributeValue::Int(400)]); // discarded
    runner.send("Input", vec![AttributeValue::Int(500)]); // discarded, triggers emit

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(get_int(&out[0], 0), 100, "Only first event value retained");
}

// ============================================================================
// POSITIVE TESTS: EVENT-BASED LAST BEHAVIOR
// ============================================================================

/// Test: LAST behavior only emits last event of each batch
/// Verifies: Buffer is replaced on each event, only last is kept
#[tokio::test]
async fn test_last_behavior_keeps_only_last() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 3 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // First batch: send 10, 20, 30 - only 30 should be emitted
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]);
    runner.send("Input", vec![AttributeValue::Int(30)]);

    {
        let collected = runner.collected.lock().unwrap();
        assert_eq!(collected.len(), 1, "Only last event of batch emitted");
        assert_eq!(get_int(&collected[0], 0), 30, "Last event value is 30");
    }

    // Second batch: send 40, 50, 60 - only 60 should be emitted
    runner.send("Input", vec![AttributeValue::Int(40)]);
    runner.send("Input", vec![AttributeValue::Int(50)]);
    runner.send("Input", vec![AttributeValue::Int(60)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 2, "Two last events from two batches");
    assert_eq!(get_int(&out[0], 0), 30, "First batch's last event");
    assert_eq!(get_int(&out[1], 0), 60, "Second batch's last event");
}

/// Test: LAST behavior with multiple batches
#[tokio::test]
async fn test_last_behavior_multiple_batches() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 2 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 6 events in 3 batches of 2
    for i in 1..=6 {
        runner.send("Input", vec![AttributeValue::Int(i * 10)]);
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 3, "3 last events from 3 batches");
    assert_eq!(get_int(&out[0], 0), 20, "Last of batch 1 (events 1-2)");
    assert_eq!(get_int(&out[1], 0), 40, "Last of batch 2 (events 3-4)");
    assert_eq!(get_int(&out[2], 0), 60, "Last of batch 3 (events 5-6)");
}

/// Test: LAST behavior replaces previous events
#[tokio::test]
async fn test_last_behavior_replaces_previous() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 5 events - each replaces the previous in buffer
    runner.send("Input", vec![AttributeValue::Int(100)]); // kept
    runner.send("Input", vec![AttributeValue::Int(200)]); // replaces 100
    runner.send("Input", vec![AttributeValue::Int(300)]); // replaces 200
    runner.send("Input", vec![AttributeValue::Int(400)]); // replaces 300
    runner.send("Input", vec![AttributeValue::Int(500)]); // replaces 400, triggers emit

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(get_int(&out[0], 0), 500, "Only last event value retained");
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Test: Partial batch is flushed on shutdown
/// Verifies: Drop handler emits remaining buffered events
#[tokio::test]
async fn test_partial_batch_flushed_on_shutdown() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send only 3 events (partial batch)
    runner.send("Input", vec![AttributeValue::Int(1)]);
    runner.send("Input", vec![AttributeValue::Int(2)]);
    runner.send("Input", vec![AttributeValue::Int(3)]);

    // No output yet (batch not complete)
    assert_eq!(runner.collected.lock().unwrap().len(), 0);

    // Shutdown should flush the partial batch
    let out = runner.shutdown();
    assert_eq!(out.len(), 3, "Partial batch flushed on shutdown");
    assert_eq!(get_int(&out[0], 0), 1);
    assert_eq!(get_int(&out[1], 0), 2);
    assert_eq!(get_int(&out[2], 0), 3);
}

/// Test: Partial batch with FIRST behavior on shutdown
#[tokio::test]
async fn test_partial_batch_first_on_shutdown() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 3 events (partial batch) - only first should be in buffer
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]);
    runner.send("Input", vec![AttributeValue::Int(30)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1, "Only first event flushed");
    assert_eq!(get_int(&out[0], 0), 10);
}

/// Test: Partial batch with LAST behavior on shutdown
#[tokio::test]
async fn test_partial_batch_last_on_shutdown() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 3 events (partial batch) - only last should be in buffer
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]);
    runner.send("Input", vec![AttributeValue::Int(30)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1, "Only last event flushed");
    assert_eq!(get_int(&out[0], 0), 30);
}

/// Test: Batch size of 1 emits every event immediately
#[tokio::test]
async fn test_batch_size_one() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 1 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Each event should emit immediately
    runner.send("Input", vec![AttributeValue::Int(1)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 1);

    runner.send("Input", vec![AttributeValue::Int(2)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2);

    runner.send("Input", vec![AttributeValue::Int(3)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 3);

    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
}

/// Test: Large batch size with few events
#[tokio::test]
async fn test_large_batch_size() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 100 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send only 5 events (far fewer than batch size)
    for i in 1..=5 {
        runner.send("Input", vec![AttributeValue::Int(i)]);
    }

    // No output during normal operation
    assert_eq!(runner.collected.lock().unwrap().len(), 0);

    // All flushed on shutdown
    let out = runner.shutdown();
    assert_eq!(out.len(), 5);
}

/// Test: Mixed complete and partial batches
#[tokio::test]
async fn test_mixed_complete_and_partial_batches() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 3 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Complete batch 1: 3 events
    runner.send("Input", vec![AttributeValue::Int(1)]);
    runner.send("Input", vec![AttributeValue::Int(2)]);
    runner.send("Input", vec![AttributeValue::Int(3)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 3);

    // Complete batch 2: 3 more events
    runner.send("Input", vec![AttributeValue::Int(4)]);
    runner.send("Input", vec![AttributeValue::Int(5)]);
    runner.send("Input", vec![AttributeValue::Int(6)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 6);

    // Partial batch: 2 events
    runner.send("Input", vec![AttributeValue::Int(7)]);
    runner.send("Input", vec![AttributeValue::Int(8)]);
    assert_eq!(runner.collected.lock().unwrap().len(), 6); // Still 6, partial not emitted

    let out = runner.shutdown();
    assert_eq!(out.len(), 8, "All events including partial batch");
}

/// Test: Single event with no shutdown (verifies no emission)
#[tokio::test]
async fn test_single_event_no_emission_until_threshold() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 10 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    runner.send("Input", vec![AttributeValue::Int(42)]);

    // Event is buffered but not emitted
    assert_eq!(runner.collected.lock().unwrap().len(), 0);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(get_int(&out[0], 0), 42);
}

/// Test: Rate limiting with window (common use case)
#[tokio::test]
async fn test_rate_limit_with_window() {
    let app = r#"
        CREATE STREAM Input (symbol STRING, price FLOAT);
        CREATE STREAM Output (symbol STRING, price FLOAT);
        INSERT INTO Output
        SELECT symbol, price
        FROM Input WINDOW('length', 10)
        OUTPUT ALL EVERY 2 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    runner.send("Input", vec![
        AttributeValue::String("IBM".to_string()),
        AttributeValue::Float(100.0),
    ]);
    assert_eq!(runner.collected.lock().unwrap().len(), 0);

    runner.send("Input", vec![
        AttributeValue::String("MSFT".to_string()),
        AttributeValue::Float(200.0),
    ]);
    assert_eq!(runner.collected.lock().unwrap().len(), 2);

    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(get_string(&out[0], 0), "IBM");
    assert_eq!(get_float(&out[0], 1), 100.0);
    assert_eq!(get_string(&out[1], 0), "MSFT");
    assert_eq!(get_float(&out[1], 1), 200.0);
}

/// Test: Rate limiting with aggregation
#[tokio::test]
async fn test_rate_limit_with_aggregation() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (total BIGINT);
        INSERT INTO Output
        SELECT sum(value) AS total
        FROM Input WINDOW('length', 10)
        OUTPUT ALL EVERY 3 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Events: 10, 20, 30 - sum should update with each
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]);
    runner.send("Input", vec![AttributeValue::Int(30)]);

    // After 3 events, we should have 3 aggregated outputs
    let out = runner.shutdown();
    assert_eq!(out.len(), 3, "3 aggregation outputs");
}

// ============================================================================
// STATE PERSISTENCE TESTS
// ============================================================================

/// Test: State persistence mid-batch
#[tokio::test]
async fn test_state_persistence_mid_batch() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 5 EVENTS;
    "#;

    let runner = AppRunner::new_with_store(app, "Output", Arc::clone(&store)).await;

    // Send 2 events (partial batch)
    runner.send("Input", vec![AttributeValue::Int(1)]);
    runner.send("Input", vec![AttributeValue::Int(2)]);

    // Persist state
    let rev = runner.persist();

    // Send 1 more event
    runner.send("Input", vec![AttributeValue::Int(3)]);

    // Restore to revision (should go back to 2 events in buffer)
    runner.restore_revision(&rev);

    // Send 3 more events to complete batch (2 restored + 3 new = 5)
    runner.send("Input", vec![AttributeValue::Int(100)]);
    runner.send("Input", vec![AttributeValue::Int(200)]);
    runner.send("Input", vec![AttributeValue::Int(300)]);

    let out = runner.shutdown();
    // Should have: [1, 2, 100, 200, 300] from the batch
    assert_eq!(out.len(), 5);
    assert_eq!(get_int(&out[0], 0), 1);
    assert_eq!(get_int(&out[1], 0), 2);
    assert_eq!(get_int(&out[2], 0), 100);
    assert_eq!(get_int(&out[3], 0), 200);
    assert_eq!(get_int(&out[4], 0), 300);
}

/// Test: State persistence with FIRST behavior
#[tokio::test]
async fn test_state_persistence_first_behavior() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 4 EVENTS;
    "#;

    let runner = AppRunner::new_with_store(app, "Output", Arc::clone(&store)).await;

    // Send 2 events
    runner.send("Input", vec![AttributeValue::Int(10)]); // first, kept
    runner.send("Input", vec![AttributeValue::Int(20)]); // discarded

    let rev = runner.persist();

    // Send 2 more to complete batch
    runner.send("Input", vec![AttributeValue::Int(30)]);
    runner.send("Input", vec![AttributeValue::Int(40)]);

    // Should have emitted first event (10)
    assert_eq!(runner.collected.lock().unwrap().len(), 1);

    // Restore - buffer should have [10], counter should be 2
    runner.restore_revision(&rev);

    // Send 2 more events to complete batch
    runner.send("Input", vec![AttributeValue::Int(50)]);
    runner.send("Input", vec![AttributeValue::Int(60)]);

    let out = runner.shutdown();
    // After restore + 2 new events, batch completes with first=10
    assert_eq!(out.len(), 2); // One from before restore, one after
}

/// Test: State persistence with LAST behavior
#[tokio::test]
async fn test_state_persistence_last_behavior() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 4 EVENTS;
    "#;

    let runner = AppRunner::new_with_store(app, "Output", Arc::clone(&store)).await;

    // Send 2 events
    runner.send("Input", vec![AttributeValue::Int(10)]);
    runner.send("Input", vec![AttributeValue::Int(20)]); // buffer now has [20]

    let rev = runner.persist();

    // Restore
    runner.restore_revision(&rev);

    // Send 2 more to complete batch (counter=2 + 2 new = 4)
    runner.send("Input", vec![AttributeValue::Int(30)]); // replaces 20
    runner.send("Input", vec![AttributeValue::Int(40)]); // replaces 30, triggers emit

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(get_int(&out[0], 0), 40, "Last event of restored batch");
}

// ============================================================================
// NEGATIVE TESTS: PARSER ERRORS
// ============================================================================

/// Test: Parser rejects SNAPSHOT with EVENTS unit
/// This test verifies the SQL parser correctly rejects invalid syntax
#[test]
fn test_parser_rejects_snapshot_with_events() {
    use eventflux::sql_compiler;

    // SNAPSHOT mode is only valid with time units, not EVENTS
    let sql = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT SNAPSHOT EVERY 10 EVENTS;
    "#;

    // The parser should reject SNAPSHOT with EVENTS unit
    let result = sql_compiler::parse(sql);
    assert!(result.is_err(), "Should reject SNAPSHOT with EVENTS unit");
}

/// Test: Converter rejects zero event count
#[test]
fn test_converter_rejects_zero_events() {
    use eventflux::sql_compiler;

    let sql = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 0 EVENTS;
    "#;

    let result = sql_compiler::parse(sql);
    assert!(result.is_err(), "Should reject zero event count");
}

/// Test: Converter rejects zero time duration
#[test]
fn test_converter_rejects_zero_time() {
    use eventflux::sql_compiler;

    let sql = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 0 SECONDS;
    "#;

    let result = sql_compiler::parse(sql);
    assert!(result.is_err(), "Should reject zero time duration");
}

/// Test: MILLISECONDS time unit is accepted
#[tokio::test]
async fn test_time_unit_milliseconds_accepted() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 100 MILLISECONDS;
    "#;

    // Should parse without error - we just need to create the runner
    let runner = AppRunner::new(app, "Output").await;
    runner.shutdown();
}

/// Test: SECONDS time unit is accepted
#[tokio::test]
async fn test_time_unit_seconds_accepted() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 1 SECONDS;
    "#;

    let runner = AppRunner::new(app, "Output").await;
    runner.shutdown();
}

/// Test: MINUTES time unit is accepted
#[tokio::test]
async fn test_time_unit_minutes_accepted() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 1 MINUTES;
    "#;

    let runner = AppRunner::new(app, "Output").await;
    runner.shutdown();
}

/// Test: HOURS time unit is accepted
#[tokio::test]
async fn test_time_unit_hours_accepted() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 1 HOURS;
    "#;

    let runner = AppRunner::new(app, "Output").await;
    runner.shutdown();
}

// ============================================================================
// STRESS TESTS
// ============================================================================

/// Test: Large number of events with rate limiting
#[tokio::test]
async fn test_large_event_volume() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT ALL EVERY 100 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 1000 events
    for i in 0..1000 {
        runner.send("Input", vec![AttributeValue::Int(i)]);
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 1000, "All events should be emitted in 10 batches");

    // Verify first and last values
    assert_eq!(get_int(&out[0], 0), 0);
    assert_eq!(get_int(&out[999], 0), 999);
}

/// Test: FIRST behavior with large volume
#[tokio::test]
async fn test_first_large_volume() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT FIRST EVERY 10 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 100 events (10 batches of 10)
    for i in 0..100 {
        runner.send("Input", vec![AttributeValue::Int(i)]);
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 10, "10 first events from 10 batches");

    // Verify each is first of its batch
    for (batch_idx, event) in out.iter().enumerate() {
        let expected = (batch_idx * 10) as i32;
        assert_eq!(get_int(event, 0), expected, "First of batch {}", batch_idx);
    }
}

/// Test: LAST behavior with large volume
#[tokio::test]
async fn test_last_large_volume() {
    let app = r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT LAST EVERY 10 EVENTS;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send 100 events (10 batches of 10)
    for i in 0..100 {
        runner.send("Input", vec![AttributeValue::Int(i)]);
    }

    let out = runner.shutdown();
    assert_eq!(out.len(), 10, "10 last events from 10 batches");

    // Verify each is last of its batch
    for (batch_idx, event) in out.iter().enumerate() {
        let expected = ((batch_idx + 1) * 10 - 1) as i32;
        assert_eq!(get_int(event, 0), expected, "Last of batch {}", batch_idx);
    }
}

// ============================================================================
// BEHAVIOR COMPARISON TESTS
// ============================================================================

/// Test: Compare ALL vs FIRST vs LAST with same input
#[tokio::test]
async fn test_behavior_comparison() {
    let base_app = |behavior: &str| format!(r#"
        CREATE STREAM Input (value INT);
        CREATE STREAM Output (value INT);
        INSERT INTO Output
        SELECT value FROM Input
        OUTPUT {} EVERY 3 EVENTS;
    "#, behavior);

    // Test ALL
    let runner_all = AppRunner::new(&base_app("ALL"), "Output").await;
    for i in 1..=6 {
        runner_all.send("Input", vec![AttributeValue::Int(i)]);
    }
    let out_all = runner_all.shutdown();
    assert_eq!(out_all.len(), 6, "ALL: all events");

    // Test FIRST
    let runner_first = AppRunner::new(&base_app("FIRST"), "Output").await;
    for i in 1..=6 {
        runner_first.send("Input", vec![AttributeValue::Int(i)]);
    }
    let out_first = runner_first.shutdown();
    assert_eq!(out_first.len(), 2, "FIRST: 2 events (first of each batch)");
    assert_eq!(get_int(&out_first[0], 0), 1);
    assert_eq!(get_int(&out_first[1], 0), 4);

    // Test LAST
    let runner_last = AppRunner::new(&base_app("LAST"), "Output").await;
    for i in 1..=6 {
        runner_last.send("Input", vec![AttributeValue::Int(i)]);
    }
    let out_last = runner_last.shutdown();
    assert_eq!(out_last.len(), 2, "LAST: 2 events (last of each batch)");
    assert_eq!(get_int(&out_last[0], 0), 3);
    assert_eq!(get_int(&out_last[1], 0), 6);
}
