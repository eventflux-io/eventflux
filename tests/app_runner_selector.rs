// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
async fn group_by_having_order_limit_offset() {
    let app = "\
        CREATE STREAM In (a INT, b INT);\n\
        CREATE STREAM Out (b INT, s BIGINT);\n\
        INSERT INTO Out\n\
        SELECT b, SUM(a) as s FROM In GROUP BY b HAVING SUM(a) > 5 ORDER BY b DESC LIMIT 2 OFFSET 1;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "In",
        vec![
            vec![AttributeValue::Int(3), AttributeValue::Int(1)],
            vec![AttributeValue::Int(4), AttributeValue::Int(1)],
            vec![AttributeValue::Int(10), AttributeValue::Int(2)],
            vec![AttributeValue::Int(1), AttributeValue::Int(3)],
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Long(7)]]
    );
}

#[tokio::test]
async fn group_by_having_order_asc() {
    let app = "\
        CREATE STREAM In (a INT, b INT);\n\
        CREATE STREAM Out (b INT, s BIGINT);\n\
        INSERT INTO Out\n\
        SELECT b, SUM(a) as s FROM In GROUP BY b HAVING SUM(a) >= 3 ORDER BY b ASC;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "In",
        vec![
            vec![AttributeValue::Int(1), AttributeValue::Int(2)],
            vec![AttributeValue::Int(2), AttributeValue::Int(1)],
            vec![AttributeValue::Int(3), AttributeValue::Int(1)],
            vec![AttributeValue::Int(2), AttributeValue::Int(2)],
        ],
    );
    let out = runner.shutdown();
    let expected = vec![
        vec![AttributeValue::Int(1), AttributeValue::Long(5)],
        vec![AttributeValue::Int(2), AttributeValue::Long(3)],
    ];
    assert_eq!(out, expected);
}

#[tokio::test]
async fn order_by_desc_limit_offset() {
    let app = "\
        CREATE STREAM In (a INT);\n\
        CREATE STREAM Out (a INT);\n\
        INSERT INTO Out\n\
        SELECT a FROM In ORDER BY a DESC LIMIT 2 OFFSET 1;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "In",
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(5)],
            vec![AttributeValue::Int(3)],
            vec![AttributeValue::Int(2)],
        ],
    );
    let out = runner.shutdown();
    let expected = vec![vec![AttributeValue::Int(3)], vec![AttributeValue::Int(2)]];
    assert_eq!(out, expected);
}
