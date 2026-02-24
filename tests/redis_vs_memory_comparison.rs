// SPDX-License-Identifier: MIT OR Apache-2.0

// tests/redis_vs_memory_comparison.rs

//! Direct comparison test between InMemory and Redis persistence stores
//! to isolate where the problem lies.
//!
//! All tests updated to use M1 SQL syntax (CREATE STREAM, INSERT INTO, WINDOW)

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::distributed::RedisConfig;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{
    InMemoryPersistenceStore, PersistenceStore, RedisPersistenceStore,
};
use std::sync::Arc;

#[tokio::test]
async fn test_memory_store_simple_works() {
    // Test persist and restore within the same runtime (M1 pattern)
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('length', 2);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(3)]);
    // Restore to previous state (before event 3)
    runner.restore_revision(&rev);
    runner.send("In", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();

    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}

#[tokio::test]
async fn test_memory_store_with_count() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Build window state: send 3 events to fill the window
    runner.send("In", vec![AttributeValue::Int(1)]); // Window: [1] COUNT=1
    runner.send("In", vec![AttributeValue::Int(2)]); // Window: [1,2] COUNT=2
    runner.send("In", vec![AttributeValue::Int(3)]); // Window: [1,2,3] COUNT=3

    let rev = runner.persist();

    // Send event after checkpoint - window slides to [2,3,4] COUNT=3
    runner.send("In", vec![AttributeValue::Int(4)]);

    // Restore from checkpoint - back to window state [1,2,3], count=3
    runner.restore_revision(&rev);

    // Send new event - window becomes [2,3,5], COUNT=3
    runner.send("In", vec![AttributeValue::Int(5)]);

    let out = runner.shutdown();

    // Verify output structure:
    // Events 1,2,3 each generate 1 output (count 1,2,3)
    // Event 4 generates 2 outputs (expired event 1 with count 2, current event 4 with count 3)
    // After restore: Event 5 generates 2 outputs (expired event 1 with count 2, current event 5 with count 3)
    assert_eq!(out.len(), 7, "Should have 7 output events");

    // Verify the final count is 3
    let last_event = out.last().expect("Expected at least one output event");
    assert_eq!(
        last_event.get(1),
        Some(&AttributeValue::Long(3)),
        "Count should be 3 for length(3) window"
    );

    // Verify restoration worked: event 5 should cause event 1 to expire again
    let event_5_idx = out
        .iter()
        .position(|e| e.get(0) == Some(&AttributeValue::Int(5)))
        .expect("Event 5 should be in output");

    // The event before event 5 should be event 1 expiring with count 2
    if event_5_idx > 0 {
        let prev_event = &out[event_5_idx - 1];
        assert_eq!(
            prev_event.get(0),
            Some(&AttributeValue::Int(1)),
            "Event 1 should expire before event 5 enters"
        );
        assert_eq!(
            prev_event.get(1),
            Some(&AttributeValue::Long(2)),
            "Count should be 2 when event 1 expires"
        );
    }
}

#[tokio::test]
async fn test_count_aggregator_multiple_checkpoints() {
    // This test verifies COUNT aggregator works correctly with multiple checkpoint/restore cycles
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Phase 1: Fill window [10,20,30]
    runner.send("In", vec![AttributeValue::Int(10)]);
    runner.send("In", vec![AttributeValue::Int(20)]);
    runner.send("In", vec![AttributeValue::Int(30)]);
    let checkpoint1 = runner.persist();

    // Phase 2: Slide window to [30,40,50]
    runner.send("In", vec![AttributeValue::Int(40)]);
    runner.send("In", vec![AttributeValue::Int(50)]);
    let _checkpoint2 = runner.persist();

    // Phase 3: Continue sliding
    runner.send("In", vec![AttributeValue::Int(60)]);

    // Restore to checkpoint1 - window resets to [10,20,30]
    runner.restore_revision(&checkpoint1);

    // Send new events - behaves as if checkpoint2 and phase3 never happened
    runner.send("In", vec![AttributeValue::Int(70)]); // Window: [20,30,70]
    runner.send("In", vec![AttributeValue::Int(80)]); // Window: [30,70,80]

    let out = runner.shutdown();

    // Verify: event 80 should have count=3 (window is [30,70,80])
    let event_80_idx = out
        .iter()
        .rposition(|e| e.get(0) == Some(&AttributeValue::Int(80)))
        .expect("Event 80 should be in output");

    assert_eq!(
        out[event_80_idx].get(1),
        Some(&AttributeValue::Long(3)),
        "Event 80 should have count=3"
    );
}

#[tokio::test]
async fn test_redis_store_with_count() {
    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 1000,
        key_prefix: "test:debug:".to_string(),
        ttl_seconds: Some(300),
    };

    let store = match RedisPersistenceStore::new_with_config(config) {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // `new_with_config` is intentionally lazy in async contexts; validate connectivity
    // up-front so this test cleanly skips when Redis isn't reachable.
    if store.test_connection().is_err() {
        println!("Redis not reachable, skipping test");
        return;
    }

    let store: Arc<dyn PersistenceStore> = Arc::new(store);

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT, count BIGINT);\n\
        INSERT INTO Out SELECT v, COUNT() as count FROM In WINDOW('length', 3);\n";

    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;

    // Build window state: send 3 events to fill the window
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);

    let rev = runner.persist();

    // Send event after checkpoint - window slides to [2,3,4]
    runner.send("In", vec![AttributeValue::Int(4)]);

    // Restore from checkpoint - back to window state [1,2,3]
    runner.restore_revision(&rev);

    // Send new event - window becomes [2,3,5], COUNT=3
    runner.send("In", vec![AttributeValue::Int(5)]);

    let out = runner.shutdown();

    // Verify the final count is 3 for length(3) window
    let last_event = out.last().expect("Expected at least one output event");
    assert_eq!(
        last_event.get(1),
        Some(&AttributeValue::Long(3)),
        "Count should be 3 for length(3) window"
    );
}
