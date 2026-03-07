// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::thread::sleep;
use std::time::Duration;

#[tokio::test]
async fn filter_projection_simple() {
    let app = "\
        CREATE STREAM In (a INT);\n\
        CREATE STREAM Out (a INT);\n\
        INSERT INTO Out\n\
        SELECT a FROM In WHERE a > 10;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(5)]);
    runner.send("In", vec![AttributeValue::Int(15)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(15)]]);
}

#[tokio::test]
async fn length_window_basic() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(3)],
        ]
    );
}

#[tokio::test]
async fn length_window_batch() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('length', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "In",
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(3)],
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(3)],
        ]
    );
}

#[tokio::test]
async fn time_window_expiry() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('time', 100 MILLISECONDS);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(5)]);
    sleep(Duration::from_millis(150));
    let out = runner.shutdown();
    assert!(out.len() >= 2);
    assert_eq!(out[0], vec![AttributeValue::Int(5)]);
}

#[tokio::test]
async fn length_batch_window() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('lengthBatch', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    runner.send("In", vec![AttributeValue::Int(2)]);
    runner.send("In", vec![AttributeValue::Int(3)]);
    runner.send("In", vec![AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(3)],
            vec![AttributeValue::Int(4)],
        ]
    );
}

#[tokio::test]
async fn time_batch_window() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('timeBatch', 100 MILLISECONDS);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    sleep(Duration::from_millis(120));
    runner.send("In", vec![AttributeValue::Int(2)]);
    sleep(Duration::from_millis(120));
    let out = runner.shutdown();
    assert!(out.len() >= 3);
    assert_eq!(out[0], vec![AttributeValue::Int(1)]);
}

#[tokio::test]
async fn external_time_window_basic() {
    let app = "\
        CREATE STREAM In (ts BIGINT, v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('externalTime', ts, 100 MILLISECONDS);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts(
        "In",
        0,
        vec![AttributeValue::Long(0), AttributeValue::Int(1)],
    );
    runner.send_with_ts(
        "In",
        150,
        vec![AttributeValue::Long(150), AttributeValue::Int(2)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
        ]
    );
}

#[tokio::test]
async fn external_time_batch_window() {
    let app = "\
        CREATE STREAM In (ts BIGINT, v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW('externalTimeBatch', ts, 100 MILLISECONDS);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_with_ts(
        "In",
        0,
        vec![AttributeValue::Long(0), AttributeValue::Int(1)],
    );
    runner.send_with_ts(
        "In",
        60,
        vec![AttributeValue::Long(60), AttributeValue::Int(2)],
    );
    runner.send_with_ts(
        "In",
        120,
        vec![AttributeValue::Long(120), AttributeValue::Int(3)],
    );
    runner.send_with_ts(
        "In",
        240,
        vec![AttributeValue::Long(240), AttributeValue::Int(4)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(3)],
        ]
    );
}

// TODO: NOT PART OF M1 - lossyCounting window SQL syntax not yet supported
// See comment above for details.
#[tokio::test]
#[ignore = "lossyCounting window SQL syntax not supported in M1"]
async fn lossy_counting_window() {
    let app = "\
        CREATE STREAM In (v TEXT);\n\
        CREATE STREAM Out (v TEXT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW lossyCounting(1,1);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("A".to_string())]);
    runner.send("In", vec![AttributeValue::String("B".to_string())]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::String("A".to_string())],
            vec![AttributeValue::String("B".to_string())],
        ]
    );
}

#[tokio::test]
async fn test_and_aggregator_length_batch() {
    let app = "\
        CREATE STREAM In (isFraud BOOL);\n\
        CREATE STREAM Out (allFraud BOOL);\n\
        INSERT INTO Out\n\
        SELECT and(isFraud) as allFraud FROM In WINDOW('lengthBatch', 3);\n";
    let runner = AppRunner::new(app, "Out").await;
    // Batch 1: true, true, true → and = true
    runner.send("In", vec![AttributeValue::Bool(true)]);
    runner.send("In", vec![AttributeValue::Bool(true)]);
    runner.send("In", vec![AttributeValue::Bool(true)]);
    // Batch 2: true, false, true → and = false
    runner.send("In", vec![AttributeValue::Bool(true)]);
    runner.send("In", vec![AttributeValue::Bool(false)]);
    runner.send("In", vec![AttributeValue::Bool(true)]);
    let out = runner.shutdown();
    // lengthBatch emits one output per event in the batch, all with the same aggregated value
    assert!(out.contains(&vec![AttributeValue::Bool(true)]));
    assert!(out.contains(&vec![AttributeValue::Bool(false)]));
}

#[tokio::test]
async fn test_or_aggregator_length_batch() {
    let app = "\
        CREATE STREAM In (isFraud BOOL);\n\
        CREATE STREAM Out (anyFraud BOOL);\n\
        INSERT INTO Out\n\
        SELECT or(isFraud) as anyFraud FROM In WINDOW('lengthBatch', 3);\n";
    let runner = AppRunner::new(app, "Out").await;
    // Batch 1: false, false, false → or = false
    runner.send("In", vec![AttributeValue::Bool(false)]);
    runner.send("In", vec![AttributeValue::Bool(false)]);
    runner.send("In", vec![AttributeValue::Bool(false)]);
    // Batch 2: false, true, false → or = true
    runner.send("In", vec![AttributeValue::Bool(false)]);
    runner.send("In", vec![AttributeValue::Bool(true)]);
    runner.send("In", vec![AttributeValue::Bool(false)]);
    let out = runner.shutdown();
    assert!(out.contains(&vec![AttributeValue::Bool(false)]));
    assert!(out.contains(&vec![AttributeValue::Bool(true)]));
}

#[tokio::test]
async fn test_and_or_with_group_by() {
    let app = "\
        CREATE STREAM In (category INT, flag BOOL);\n\
        CREATE STREAM Out (category INT, allTrue BOOL, anyTrue BOOL);\n\
        INSERT INTO Out\n\
        SELECT category, and(flag) as allTrue, or(flag) as anyTrue FROM In GROUP BY category;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "In",
        vec![
            vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
            vec![AttributeValue::Int(1), AttributeValue::Bool(false)],
            vec![AttributeValue::Int(2), AttributeValue::Bool(true)],
            vec![AttributeValue::Int(2), AttributeValue::Bool(true)],
        ],
    );
    let out = runner.shutdown();
    // Group 1: and(true, false) = false, or(true, false) = true
    assert!(out.contains(&vec![
        AttributeValue::Int(1),
        AttributeValue::Bool(false),
        AttributeValue::Bool(true),
    ]));
    // Group 2: and(true, true) = true, or(true, true) = true
    assert!(out.contains(&vec![
        AttributeValue::Int(2),
        AttributeValue::Bool(true),
        AttributeValue::Bool(true),
    ]));
}

// TODO: NOT PART OF M1 - cron window SQL syntax not yet supported
// See comment above for details.
#[tokio::test]
#[ignore = "cron window SQL syntax not supported in M1"]
async fn cron_window_basic() {
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out\n\
        SELECT v FROM In WINDOW cron('*/1 * * * * *');\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(1)]);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0], vec![AttributeValue::Int(1)]);
}
