// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
async fn partition_forward() {
    let app = "\
        CREATE STREAM InStream (symbol VARCHAR, volume INT);
        CREATE STREAM OutStream (vol INT);
        PARTITION WITH (symbol OF InStream)
        BEGIN
            INSERT INTO OutStream SELECT volume AS vol FROM InStream;
        END;";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send(
        "InStream",
        vec![AttributeValue::String("a".into()), AttributeValue::Int(1)],
    );
    runner.send(
        "InStream",
        vec![AttributeValue::String("b".into()), AttributeValue::Int(2)],
    );
    runner.send(
        "InStream",
        vec![AttributeValue::String("a".into()), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(3)],
        ]
    );
}

#[tokio::test]
async fn partition_sum_by_symbol() {
    let app = "\
        CREATE STREAM InStream (symbol VARCHAR, volume INT);
        CREATE STREAM OutStream (sumvol BIGINT);
        PARTITION WITH (symbol OF InStream)
        BEGIN
            INSERT INTO OutStream SELECT SUM(volume) AS sumvol FROM InStream;
        END;";
    let runner = AppRunner::new(app, "OutStream").await;
    runner.send(
        "InStream",
        vec![AttributeValue::String("x".into()), AttributeValue::Int(1)],
    );
    runner.send(
        "InStream",
        vec![AttributeValue::String("x".into()), AttributeValue::Int(2)],
    );
    runner.send(
        "InStream",
        vec![AttributeValue::String("y".into()), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Long(1)],
            vec![AttributeValue::Long(3)],
            vec![AttributeValue::Long(6)],
        ]
    );
}

#[tokio::test]
async fn partition_join_streams() {
    let app = "\
        CREATE STREAM A (symbol VARCHAR, v INT);
        CREATE STREAM B (symbol VARCHAR, v INT);
        CREATE STREAM Out (a INT, b INT);
        PARTITION WITH (symbol OF A, symbol OF B)
        BEGIN
            INSERT INTO Out SELECT A.v AS a, B.v AS b FROM A JOIN B ON A.symbol = B.symbol;
        END;";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "A",
        vec![AttributeValue::String("s".into()), AttributeValue::Int(1)],
    );
    runner.send(
        "B",
        vec![AttributeValue::String("s".into()), AttributeValue::Int(2)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(2)]]
    );
}

#[tokio::test]
async fn partition_with_window() {
    let app = "\
        CREATE STREAM In (symbol VARCHAR, v INT);
        CREATE STREAM Out (v INT);
        PARTITION WITH (symbol OF In)
        BEGIN
            INSERT INTO Out SELECT v FROM In WINDOW('length', 1);
        END;";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "In",
        vec![AttributeValue::String("p".into()), AttributeValue::Int(1)],
    );
    runner.send(
        "In",
        vec![AttributeValue::String("p".into()), AttributeValue::Int(2)],
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
