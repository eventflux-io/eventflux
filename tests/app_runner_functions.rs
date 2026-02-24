// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

/// Test CAST expression: convert string to double
#[tokio::test]
async fn app_runner_cast_string_to_double() {
    let app = "\
        CREATE STREAM In (price VARCHAR);\n\
        CREATE STREAM Out (numeric_price DOUBLE);\n\
        INSERT INTO Out SELECT CAST(price AS DOUBLE) as numeric_price FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("123.45".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(123.45)]]);
}

/// Test CAST expression: convert int to string
#[tokio::test]
async fn app_runner_cast_int_to_string() {
    let app = "\
        CREATE STREAM In (count INT);\n\
        CREATE STREAM Out (count_str VARCHAR);\n\
        INSERT INTO Out SELECT CAST(count AS VARCHAR) as count_str FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(42)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::String("42".to_string())]]);
}

/// Test CAST with AVG aggregation: convert string price to double for averaging
#[tokio::test]
async fn app_runner_cast_with_avg() {
    let app = "\
        CREATE STREAM In (price VARCHAR);\n\
        CREATE STREAM Out (avg_price DOUBLE);\n\
        INSERT INTO Out SELECT AVG(CAST(price AS DOUBLE)) as avg_price FROM In WINDOW('lengthBatch', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::String("10.0".to_string())]);
    runner.send("In", vec![AttributeValue::String("20.0".to_string())]);
    let out = runner.shutdown();
    // Window outputs after each batch of 2 events: avg of 10.0 and 20.0 = 15.0
    // Check that the final output contains the correct average
    assert!(!out.is_empty());
    let last_output = out.last().unwrap();
    assert_eq!(last_output, &vec![AttributeValue::Double(15.0)]);
}

// TODO: NOT PART OF M1 - Only ROUND function is in M1, not LOG/UPPER
// M1 Query 7 tests built-in functions but specifically uses ROUND function only.
// LOG and UPPER functions are not part of M1 implementation.
// M1 covers: Basic queries, Windows, Joins, GROUP BY, HAVING, ORDER BY, LIMIT, ROUND function
// Additional built-in functions (LOG, UPPER, etc.) will be implemented in Phase 2.
// See feat/grammar/GRAMMAR_STATUS.md for M1 feature list.
#[tokio::test]
#[ignore = "Requires LOG/UPPER functions - Not part of M1"]
async fn app_runner_builtin_functions() {
    // Converted to SQL syntax - built-in functions are part of M1
    let app = "\
        CREATE STREAM In (a DOUBLE);\n\
        SELECT LOG(a) as l, UPPER('abc') as u FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(0.0),
            AttributeValue::String("ABC".to_string())
        ]]
    );
}
