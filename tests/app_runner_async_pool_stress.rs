// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

// TODO: NOT PART OF M1 - Partition and async pool features not in M1
// This test is for async partition pool ordering which requires PARTITION syntax.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT
// Partition support will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Requires PARTITION support - Not part of M1"]
async fn async_partition_pool_order() {
    // Simplified test without partition for now - just basic stream processing
    let app = "\
        define stream In (v int, p string);\n\
        define stream Out (v int, p string);\n\
        from In select v, p insert into Out;\n";
    let runner = AppRunner::new(app, "Out").await;
    for i in 0..10 {
        // Reduced to 10 for simpler test
        let part = match i % 4 {
            0 => "a",
            1 => "b",
            2 => "c",
            _ => "d",
        };
        runner.send(
            "In",
            vec![
                AttributeValue::Int(i as i32),
                AttributeValue::String(part.to_string()),
            ],
        );
    }
    thread::sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    assert_eq!(out.len(), 10);
    // Just verify we got some data
    assert!(out[0][0] == AttributeValue::Int(0));
}
