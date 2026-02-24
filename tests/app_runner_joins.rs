// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// All JOIN tests converted to SQL syntax - JOINs are part of M1

#[tokio::test]
async fn inner_join_simple() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L JOIN R ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    runner.send("R", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(1), AttributeValue::Int(1)]]
    );
}

#[tokio::test]
async fn left_outer_join_no_match() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L LEFT OUTER JOIN R ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(2), AttributeValue::Null]]
    );
}

#[tokio::test]
async fn join_with_condition_gt() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L JOIN R ON L.id > R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    runner.send("L", vec![AttributeValue::Int(3)]);
    runner.send("R", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Int(3), AttributeValue::Int(1)]]
    );
}

// TODO: NOT PART OF M1 - Complex nested expressions in JOIN ON clause
// M1 Query 5 tests basic JOIN with simple equality condition (ON stream1.col = stream2.col).
// Complex nested conditions like (expr1 AND expr2) OR expr3 are not part of M1.
// M1 covers: Basic JOIN with simple conditions, comparison operators (=, >, <, etc.)
// Complex nested JOIN conditions will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Complex nested JOIN conditions - Not part of M1"]
async fn join_complex_condition() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L JOIN R ON (L.id > R.id AND R.id > 0) OR L.id = 10;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    runner.send("R", vec![AttributeValue::Int(1)]);
    runner.send("L", vec![AttributeValue::Int(10)]);
    runner.send("R", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![
            vec![AttributeValue::Int(10), AttributeValue::Int(1)],
            vec![AttributeValue::Int(10), AttributeValue::Int(2)],
        ]
    );
}

#[tokio::test]
async fn right_outer_join_no_match() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L RIGHT OUTER JOIN R ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("R", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![AttributeValue::Null, AttributeValue::Int(5)]]
    );
}

#[tokio::test]
async fn full_outer_join_basic() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id as l, R.id as r\n\
        FROM L FULL OUTER JOIN R ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    runner.send("R", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert!(out.contains(&vec![AttributeValue::Int(1), AttributeValue::Null]));
    assert!(out.contains(&vec![AttributeValue::Null, AttributeValue::Int(2)]));
}

#[tokio::test]
async fn join_with_group_by() {
    let app = "\
        CREATE STREAM L (id INT, cat INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.cat as cat, COUNT(*) as c\n\
        FROM L JOIN R ON L.id = R.id\n\
        GROUP BY cat\n\
        ORDER BY cat ASC;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send_batch(
        "L",
        vec![
            vec![AttributeValue::Int(1), AttributeValue::Int(10)],
            vec![AttributeValue::Int(1), AttributeValue::Int(10)],
            vec![AttributeValue::Int(2), AttributeValue::Int(20)],
        ],
    );
    runner.send_batch(
        "R",
        vec![
            vec![AttributeValue::Int(1)],
            vec![AttributeValue::Int(2)],
            vec![AttributeValue::Int(1)],
        ],
    );
    let out = runner.shutdown();
    let expected = vec![
        vec![AttributeValue::Int(10), AttributeValue::Long(1)],
        vec![AttributeValue::Int(10), AttributeValue::Long(2)],
        vec![AttributeValue::Int(20), AttributeValue::Long(1)],
        vec![AttributeValue::Int(10), AttributeValue::Long(3)],
        vec![AttributeValue::Int(10), AttributeValue::Long(4)],
    ];
    assert_eq!(out, expected);
}
