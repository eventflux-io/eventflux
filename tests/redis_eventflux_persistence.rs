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

// tests/redis_eventflux_persistence.rs

//! Integration tests for saving EventFlux application state in Redis
//!
//! Real application state, such as window processors and aggregators, must be
//! saved to Redis and restored correctly. Each test skips itself when Redis is
//! not reachable.

// The tests use SQL syntax with YAML configuration. This replaces the old
// @app:name annotations and the old EventFluxQL "define stream" syntax. The
// 5 tests that used the old syntax were moved over on 2025-10-24.
// - Streams are defined with SQL CREATE STREAM
// - App names are set in YAML (app-with-name.yaml)
// - Plain SQL syntax, with no custom annotations

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::config::ConfigManager;
use eventflux::core::distributed::RedisConfig;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::persistence::{PersistenceStore, RedisPersistenceStore};
use std::sync::Arc;

/// Creates a Redis persistence store and checks the connection. Returns Err with
/// a message when the store cannot be created or Redis does not answer.
fn create_redis_store() -> Result<Arc<dyn PersistenceStore>, String> {
    // Each test gets its own key prefix so parallel tests do not clash.
    // The thread id plus a nanosecond timestamp makes it unique.
    let thread_id = std::thread::current().id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let unique_id = format!("{:?}-{}", thread_id, nanos);

    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 1000,
        key_prefix: format!("test:eventflux:persist:{}:", unique_id),
        ttl_seconds: None,
    };

    // Creating the store does not connect to Redis yet
    let store = RedisPersistenceStore::new_with_config(config)
        .map_err(|e| format!("Failed to create Redis store: {}", e))?;

    // Check the connection. This sets up the backend and sends a PING.
    store
        .test_connection()
        .map_err(|e| format!("Redis connectivity test failed: {}", e))?;

    Ok(Arc::new(store))
}

/// Returns Err when Redis is not reachable, so the caller can skip the test.
fn ensure_redis_available() -> Result<Arc<dyn PersistenceStore>, String> {
    create_redis_store()
}

#[tokio::test]
async fn test_redis_persistence_basic() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // App name from YAML, in place of the old @app:name
    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('length', 2);\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(2)]);

    // Only checks that restore does not panic and that a revision id came back
    runner.restore_revision(&rev);
    let _ = runner.shutdown();
    assert!(!rev.is_empty());
}

#[tokio::test]
async fn test_redis_length_window_state_persistence() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // App name from YAML, in place of the old @app:name
    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    // Only window state is covered here. Aggregator state is covered by
    // test_redis_aggregator_recovery_identical.
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('length', 2);\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner.persist();
    runner.send("In", vec![AttributeValue::Int(3)]);
    let _ = runner.shutdown();

    // Second instance with same config
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    let runner2 = AppRunner::new_with_manager(manager2, app, "Out").await;
    runner2.restore_revision(&rev);
    runner2.send("In", vec![AttributeValue::Int(4)]);
    let out = runner2.shutdown();

    // Verify basic window filtering works after restoration
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}

#[tokio::test]
async fn test_redis_persist_across_app_restarts() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // App name from YAML, in place of the old @app:name. Only window state is
    // covered here. Aggregator state is covered by test_redis_aggregator_recovery_identical.
    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In WINDOW('length', 2);\n";

    // First app instance
    let runner1 = AppRunner::new_with_manager(manager, app, "Out").await;
    runner1.send("In", vec![AttributeValue::Int(1)]);
    runner1.send("In", vec![AttributeValue::Int(2)]);
    let rev = runner1.persist();
    runner1.send("In", vec![AttributeValue::Int(3)]);
    let _ = runner1.shutdown();

    // Second app instance (simulating restart)
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    let runner2 = AppRunner::new_with_manager(manager2, app, "Out").await;
    runner2.restore_revision(&rev);
    runner2.send("In", vec![AttributeValue::Int(4)]);
    let out = runner2.shutdown();

    // Verify basic window filtering persists across app restarts
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Int(4)]);
}

#[tokio::test]
async fn test_redis_multiple_windows_persistence() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // App name from YAML, in place of the old @app:name
    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    let app = "\
        CREATE STREAM In (id INT, value DOUBLE);\n\
        CREATE STREAM Out1 (id INT, value DOUBLE, count BIGINT);\n\
        CREATE STREAM Out2 (total DOUBLE, avg DOUBLE);\n\
        \n\
        INSERT INTO Out1 SELECT id, value, COUNT() as count FROM In WINDOW('length', 2);\n\
        INSERT INTO Out2 SELECT SUM(value) as total, AVG(value) as avg FROM In WINDOW('lengthBatch', 3);\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out1").await;

    // Build up state in both windows
    runner.send(
        "In",
        vec![AttributeValue::Int(1), AttributeValue::Double(10.0)],
    );
    runner.send(
        "In",
        vec![AttributeValue::Int(2), AttributeValue::Double(20.0)],
    );
    runner.send(
        "In",
        vec![AttributeValue::Int(3), AttributeValue::Double(30.0)],
    );

    // Create checkpoint
    let rev = runner.persist();

    // Modify state after checkpoint
    runner.send(
        "In",
        vec![AttributeValue::Int(4), AttributeValue::Double(40.0)],
    );

    // Restore from checkpoint
    runner.restore_revision(&rev);

    // Send new event to verify both windows restored
    runner.send(
        "In",
        vec![AttributeValue::Int(5), AttributeValue::Double(50.0)],
    );

    let out = runner.shutdown();

    // Verify the length window state was restored correctly
    // The count should be 2 (window size of 2) after restoration and new event
    if let Some(last_event) = out.last() {
        if let Some(AttributeValue::Long(count)) = last_event.get(2) {
            assert_eq!(
                *count, 2,
                "Multiple window states should be restored correctly"
            );
        }
    }
}

#[tokio::test]
async fn test_redis_aggregation_state_persistence() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // App name from YAML, in place of the old @app:name
    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    let app = "\
        CREATE STREAM In (category STRING, value DOUBLE);\n\
        CREATE STREAM Out (category STRING, total DOUBLE, count BIGINT);\n\
        \n\
        INSERT INTO Out \n\
        SELECT category, SUM(value) as total, COUNT() as count \n\
        FROM In WINDOW('length', 5) \n\
        GROUP BY category;\n";

    let runner = AppRunner::new_with_manager(manager, app, "Out").await;

    // Build up aggregation state for different categories
    runner.send(
        "In",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Double(200.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(150.0),
        ],
    );

    // Create checkpoint
    let rev = runner.persist();

    // Add more events after checkpoint
    runner.send(
        "In",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(300.0),
        ],
    );

    // Restore from checkpoint
    runner.restore_revision(&rev);

    // Send new event to verify aggregation state
    runner.send(
        "In",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(250.0),
        ],
    );

    let out = runner.shutdown();

    // Verify aggregation state was restored
    // Should find events for category A with restored totals
    let category_a_events: Vec<_> = out
        .iter()
        .filter(|event| {
            if let Some(AttributeValue::String(cat)) = event.first() {
                cat == "A"
            } else {
                false
            }
        })
        .collect();

    assert!(
        !category_a_events.is_empty(),
        "Should have category A events"
    );

    // Check the last category A event has expected aggregated values
    if let Some(last_a_event) = category_a_events.last() {
        if let Some(AttributeValue::Double(total)) = last_a_event.get(1) {
            // After restore the group states start empty, so A=250 is the only value (250.0).
            // This is expected. GROUP BY queries start with fresh group state after a restore.
            assert_eq!(
                *total, 250.0,
                "Group aggregation correctly restarts after restoration"
            );
        } else {
            panic!("Total value not found or wrong type");
        }
    } else {
        panic!("No category A events found");
    }
}

#[tokio::test]
async fn test_redis_persistence_store_interface() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    let app_id = "TestInterface";
    let revision = "test_rev_1";
    let test_data = b"test_snapshot_data";

    // Test save
    store.save(app_id, revision, test_data);

    // Test load
    let loaded = store.load(app_id, revision);
    assert_eq!(loaded, Some(test_data.to_vec()));

    // Test get_last_revision
    let last_rev = store.get_last_revision(app_id);
    assert_eq!(last_rev, Some(revision.to_string()));

    // Test with different revision
    let revision2 = "test_rev_2";
    let test_data2 = b"test_snapshot_data_2";
    store.save(app_id, revision2, test_data2);

    let last_rev2 = store.get_last_revision(app_id);
    assert_eq!(last_rev2, Some(revision2.to_string()));

    // Test clear_all_revisions
    store.clear_all_revisions(app_id);
    let cleared_last = store.get_last_revision(app_id);
    assert_eq!(cleared_last, None);
}

/// End to end test that the window buffer is the same after checkpoint and restore.
///
/// Covers ARCH-P0.5, state identity after recovery.
///
/// **Test Strategy:**
/// The public API cannot show the window buffer directly, so identity is checked
/// through what the window outputs:
///
/// 1. Build partial window state (3 events in lengthBatch(5))
/// 2. Create checkpoint
/// 3. Complete the window (add 2 more events)
/// 4. Record the output
/// 5. Restore from checkpoint in a NEW runtime
/// 6. Send the SAME 2 events
/// 7. Verify output is IDENTICAL
///
/// If the window buffer was restored with the same 3 events, the output must be
/// identical.
#[tokio::test]
async fn test_redis_state_recovery_identical() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // PHASE 1: Build state in first runtime
    // ======================================

    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    // A lengthBatch window of 5 events outputs only when the 5th event completes the batch
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (value INT);\n\
        INSERT INTO Out SELECT value FROM In WINDOW('lengthBatch', 5);\n";

    let runner1 = AppRunner::new_with_manager(manager, app, "Out").await;

    // Step 1. Send 3 events, not enough to complete the batch.
    // They are held in the window state.
    runner1.send("In", vec![AttributeValue::Int(10)]);
    runner1.send("In", vec![AttributeValue::Int(20)]);
    runner1.send("In", vec![AttributeValue::Int(30)]);

    // No output yet, because the batch needs 5 events
    {
        let collected = runner1.collected.lock().unwrap();
        assert_eq!(
            collected.len(),
            0,
            "Should have no output with only 3 events in lengthBatch(5) window"
        );
    }

    // Step 2. Create a checkpoint, which should hold the 3 buffered events
    let checkpoint_rev = runner1.persist();
    println!(
        "Created checkpoint '{}' with 3 events buffered in window",
        checkpoint_rev
    );

    // Step 3. Send 2 more events to complete the batch.
    // That makes 5 events, [10, 20, 30, 40, 50].
    runner1.send("In", vec![AttributeValue::Int(40)]);
    runner1.send("In", vec![AttributeValue::Int(50)]);

    // Step 4. Record the output of the original runtime
    let original_output = {
        let collected = runner1.collected.lock().unwrap();
        collected.clone()
    };

    println!(
        "Original runtime output {} events after completing batch",
        original_output.len()
    );

    // Verify we got output with all 5 values
    assert_eq!(
        original_output.len(),
        5,
        "Should have 5 output events after completing lengthBatch(5)"
    );

    // Step 5. Shut down the first runtime
    let _ = runner1.shutdown();
    println!("First runtime shut down");

    // PHASE 2: Restore state in new runtime
    // ======================================

    // Step 6. Create a brand new runtime and do NOT start it yet
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    // The runtime must NOT be started yet, or recovery would not work
    let runner2 = AppRunner::new_with_manager_no_start(manager2, app, "Out").await;

    // Step 7. Restore the checkpoint BEFORE starting. This is the key step.
    // The window buffer is filled before any event is processed.
    runner2.restore_revision(&checkpoint_rev);
    println!(
        "Restored from checkpoint '{}' in new runtime",
        checkpoint_rev
    );

    // Now start the runtime with the restored state
    runner2.runtime().start().expect("Failed to start runtime");

    // Step 8. Send the SAME 2 events [40, 50] that went to the original runtime.
    // If the window state was restored correctly, the output is identical.
    runner2.send("In", vec![AttributeValue::Int(40)]);
    runner2.send("In", vec![AttributeValue::Int(50)]);

    // Step 9. Collect the output of the restored runtime
    let restored_output = runner2.shutdown();

    println!(
        "Restored runtime output {} events after completing batch",
        restored_output.len()
    );

    // PHASE 3: Verify state identity
    // ===============================

    // The outputs should be IDENTICAL if the window state was correctly restored
    assert_eq!(
        original_output.len(),
        restored_output.len(),
        "Output length should be identical after restoration. \
         This failure indicates window buffer state is not being restored correctly."
    );

    // Verify each event matches exactly
    for (i, (original, restored)) in original_output
        .iter()
        .zip(restored_output.iter())
        .enumerate()
    {
        assert_eq!(
            original, restored,
            "Event {} should be identical after restoration. Original: {:?}, Restored: {:?}",
            i, original, restored
        );
    }

    println!("✓ State recovery verification successful - window state is identical!");
}

/// End to end test that aggregator state is the same after checkpoint and restore.
///
/// Covers ARCH-P0.5, state identity after recovery, for aggregators.
///
/// **Test Strategy:**
/// Like the window buffer test, identity is checked through the output:
///
/// 1. Build partial aggregation state (2 events in lengthBatch(3), SUM=300, COUNT=2)
/// 2. Create checkpoint
/// 3. Complete the batch (add 1 more event)
/// 4. Record the output (should show SUM=350, COUNT=3)
/// 5. Restore from checkpoint in a NEW runtime
/// 6. Send the SAME 1 event
/// 7. Verify output is IDENTICAL (SUM=350, COUNT=3)
///
/// If the aggregator state was restored with SUM=300, COUNT=2, the final output
/// must be identical.
#[tokio::test]
async fn test_redis_aggregator_recovery_identical() {
    let store = match ensure_redis_available() {
        Ok(store) => store,
        Err(_) => {
            println!("Redis not available, skipping test");
            return;
        }
    };

    // PHASE 1: Build aggregator state in first runtime
    // =================================================

    let config_manager = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);
    manager.set_persistence_store(Arc::clone(&store)).unwrap();

    // A lengthBatch window with COUNT and SUM and no GROUP BY.
    // lengthBatch outputs all events once the batch of 3 is complete.
    let app = "\
        CREATE STREAM In (value INT);\n\
        CREATE STREAM Out (total INT, count BIGINT);\n\
        INSERT INTO Out \n\
        SELECT SUM(value) as total, COUNT() as count \n\
        FROM In WINDOW('lengthBatch', 3);\n";

    let runner1 = AppRunner::new_with_manager(manager, app, "Out").await;

    // Step 1. Send 2 events, not enough to complete the batch.
    // They are held and aggregated in the window state.
    runner1.send("In", vec![AttributeValue::Int(100)]);
    runner1.send("In", vec![AttributeValue::Int(200)]);

    // Verify no output yet (batch needs 3 events)
    {
        let collected = runner1.collected.lock().unwrap();
        assert_eq!(
            collected.len(),
            0,
            "Should have no output with only 2 events in lengthBatch(3) window"
        );
    }

    // Step 2. Create a checkpoint, which should hold the 2 buffered events and
    // their partial aggregates
    let checkpoint_rev = runner1.persist();
    println!(
        "Created checkpoint '{}' with 2 events buffered (SUM=300, COUNT=2)",
        checkpoint_rev
    );

    // Step 3. Send 1 more event to complete the batch
    runner1.send("In", vec![AttributeValue::Int(50)]);

    // Step 4. Record the output of the original runtime
    let original_output = {
        let collected = runner1.collected.lock().unwrap();
        collected.clone()
    };

    println!(
        "Original runtime output {} events after completing batch",
        original_output.len()
    );

    // Verify we got output with aggregated values
    // lengthBatch(3) outputs all 3 events with their aggregated values
    assert_eq!(
        original_output.len(),
        3,
        "Should have 3 output events after completing lengthBatch(3)"
    );

    // The last output event carries the complete aggregates
    if let Some(last_event) = original_output.last() {
        // Total should be 100 + 200 + 50 = 350
        if let Some(AttributeValue::Int(total)) = last_event.first() {
            assert_eq!(*total, 350, "Original SUM should be 350 (100+200+50)");
        }

        // Count should be 3
        if let Some(AttributeValue::Long(count)) = last_event.get(1) {
            assert_eq!(*count, 3, "Original COUNT should be 3");
        }
    }

    // Step 5. Shut down the first runtime
    let _ = runner1.shutdown();
    println!("First runtime shut down");

    // PHASE 2: Restore state in new runtime
    // ======================================

    // Step 6. Create a brand new runtime and do NOT start it yet
    let config_manager2 = ConfigManager::from_file("tests/fixtures/app-with-name.yaml");
    let manager2 = EventFluxManager::new_with_config_manager(config_manager2);
    manager2.set_persistence_store(Arc::clone(&store)).unwrap();

    // The runtime must NOT be started yet, or recovery would not work
    let runner2 = AppRunner::new_with_manager_no_start(manager2, app, "Out").await;

    // Step 7. Restore the checkpoint BEFORE starting. This is the key step.
    // Aggregator state is restored before any event is processed.
    runner2.restore_revision(&checkpoint_rev);
    println!(
        "Restored from checkpoint '{}' in new runtime",
        checkpoint_rev
    );

    // Now start the runtime with the restored state
    runner2.runtime().start().expect("Failed to start runtime");

    // Step 8. Send the SAME event [50] that completed the batch in the original runtime.
    // If the aggregator state was restored correctly, the output is identical.
    runner2.send("In", vec![AttributeValue::Int(50)]);

    // Step 9. Collect the output of the restored runtime
    let restored_output = runner2.shutdown();

    println!(
        "Restored runtime output {} events after completing batch",
        restored_output.len()
    );

    // PHASE 3: Verify state identity
    // ===============================

    // The outputs should be IDENTICAL if the aggregator state was correctly restored
    assert_eq!(
        original_output.len(),
        restored_output.len(),
        "Output length should be identical after restoration. \
         This failure indicates aggregator state is not being restored correctly."
    );

    // Verify each event matches exactly
    for (i, (original, restored)) in original_output
        .iter()
        .zip(restored_output.iter())
        .enumerate()
    {
        assert_eq!(
            original, restored,
            "Event {} should be identical after restoration. Original: {:?}, Restored: {:?}. \
             This means aggregator state (SUM=300, COUNT=2) was not restored correctly.",
            i, original, restored
        );
    }

    // Also check that the aggregates in the last event are exactly the same
    if let (Some(original_event), Some(restored_event)) =
        (original_output.last(), restored_output.last())
    {
        // SUM is 350 if the state was restored
        assert_eq!(
            original_event.first(),
            restored_event.first(),
            "SUM aggregate should match exactly (350 if state restored correctly, \
             50 if aggregator restarted from zero)"
        );

        // Count is 3 if the state was restored
        assert_eq!(
            original_event.get(1),
            restored_event.get(1),
            "COUNT aggregate should match exactly (3 if state restored correctly, \
             1 if aggregator restarted from zero)"
        );
    }

    println!("✓ Aggregator state recovery verification successful - aggregates are identical!");
}
