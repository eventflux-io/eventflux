// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::{InMemoryPersistenceStore, PersistenceStore};
use std::sync::Arc;

/// Basic start → shutdown → start → send → shutdown cycle.
#[tokio::test]
async fn test_start_stop_restart() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    let rt = runner.runtime();

    // Send an event before first shutdown
    runner.send("In", vec![AttributeValue::Int(1)]);

    // Shutdown and restart
    rt.shutdown();
    rt.start().expect("restart should succeed");

    // Send after restart
    runner.send("In", vec![AttributeValue::Int(2)]);

    rt.shutdown();
    let out = runner.collected.lock().unwrap().clone();
    // Both events should have been received
    assert!(out.contains(&vec![AttributeValue::Int(1)]));
    assert!(out.contains(&vec![AttributeValue::Int(2)]));
}

/// Callbacks registered before first start still fire after restart.
#[tokio::test]
async fn test_restart_callback_preserved() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    let rt = runner.runtime();

    rt.shutdown();
    rt.start().expect("restart");

    runner.send("In", vec![AttributeValue::Int(42)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    assert!(out.contains(&vec![AttributeValue::Int(42)]));
}

/// Multiple streams and queries work correctly after restart.
#[tokio::test]
async fn test_restart_with_multiple_streams() {
    let app = "\
        CREATE STREAM A (x INT);\n\
        CREATE STREAM B (y INT);\n\
        CREATE STREAM Out (z INT);\n\
        INSERT INTO Out SELECT x as z FROM A;\n\
        INSERT INTO Out SELECT y as z FROM B;\n";
    let runner = AppRunner::new(app, "Out").await;
    let rt = runner.runtime();

    runner.send("A", vec![AttributeValue::Int(1)]);
    rt.shutdown();
    rt.start().expect("restart");

    runner.send("A", vec![AttributeValue::Int(2)]);
    runner.send("B", vec![AttributeValue::Int(3)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    assert!(out.contains(&vec![AttributeValue::Int(1)]));
    assert!(out.contains(&vec![AttributeValue::Int(2)]));
    assert!(out.contains(&vec![AttributeValue::Int(3)]));
}

/// Calling shutdown() twice is a safe no-op, and the runtime remains restartable.
#[tokio::test]
async fn test_double_shutdown_is_safe() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    let rt = runner.runtime();

    rt.shutdown();
    rt.shutdown(); // second call should be a no-op

    // Runtime should still be usable after double shutdown
    rt.start().expect("restart after double shutdown");
    runner.send("In", vec![AttributeValue::Int(99)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    assert!(out.contains(&vec![AttributeValue::Int(99)]));
}

/// Calling start() on a running runtime is a no-op.
#[tokio::test]
async fn test_start_idempotent() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    let rt = runner.runtime();

    // Already running — second start should be fine
    rt.start().expect("idempotent start");

    runner.send("In", vec![AttributeValue::Int(7)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    assert!(out.contains(&vec![AttributeValue::Int(7)]));
}

/// With a persistence store: shutdown auto-persists, start auto-restores.
#[tokio::test]
async fn test_restart_with_auto_persist_restore() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (total LONG);\n\
        INSERT INTO Out\n\
        SELECT sum(v) as total FROM In WINDOW('length', 3);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    let rt = runner.runtime();

    // Build up state: sum=10, window=[10]
    runner.send("In", vec![AttributeValue::Int(10)]);
    // sum=30, window=[10,20]
    runner.send("In", vec![AttributeValue::Int(20)]);

    // Shutdown triggers auto-persist
    rt.shutdown();

    // Verify a revision was saved
    let rev = store.get_last_revision(&rt.name);
    assert!(rev.is_some(), "auto-persist should have saved a revision");

    // Restart triggers auto-restore
    rt.start().expect("restart with auto-restore");

    // After restore: state should be sum=30, window=[10,20]
    // Sending 5 → sum=35, window=[10,20,5]
    runner.send("In", vec![AttributeValue::Int(5)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    let last = out.last().expect("should have output");
    assert_eq!(last, &vec![AttributeValue::Long(35)]);
}

/// clear_state() removes persisted state so restart starts fresh.
#[tokio::test]
async fn test_clear_state_then_start() {
    let store: Arc<dyn PersistenceStore> = Arc::new(InMemoryPersistenceStore::new());
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (total LONG);\n\
        INSERT INTO Out\n\
        SELECT sum(v) as total FROM In WINDOW('length', 3);\n";
    let runner = AppRunner::new_with_store(app, "Out", Arc::clone(&store)).await;
    let rt = runner.runtime();

    // Build up state: sum=10
    runner.send("In", vec![AttributeValue::Int(10)]);

    // Shutdown (auto-persist)
    rt.shutdown();
    assert!(store.get_last_revision(&rt.name).is_some());

    // Clear all state, then restart
    rt.clear_state();
    assert!(
        store.get_last_revision(&rt.name).is_none(),
        "revisions should be cleared"
    );

    rt.start().expect("fresh restart");

    // After clear + restart: no old aggregation state
    // sum should start from 0: sending 5 → sum=5
    runner.send("In", vec![AttributeValue::Int(5)]);
    rt.shutdown();

    let out = runner.collected.lock().unwrap().clone();
    let last = out.last().expect("should have output");
    assert_eq!(last, &vec![AttributeValue::Long(5)]);
}
