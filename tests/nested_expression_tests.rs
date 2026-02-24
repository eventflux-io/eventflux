// SPDX-License-Identifier: MIT OR Apache-2.0

//! Comprehensive tests for nested (parenthesized) expressions in SQL.
//!
//! Tests cover various scenarios where parentheses are used in SQL expressions:
//! - Arithmetic grouping for operator precedence
//! - Nested expressions in SELECT items
//! - Nested expressions in WHERE clauses
//! - Nested expressions in function arguments
//! - Deeply nested expressions
//! - Nested expressions with pattern/sequence aliases

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// =============================================================================
// Basic Nested Arithmetic Tests
// =============================================================================

/// Test simple nested arithmetic: (a + b) * c
/// Without parentheses: a + b * c = a + (b * c) due to precedence
/// With parentheses: (a + b) * c = (a + b) * c
#[tokio::test]
async fn nested_simple_addition_then_multiply() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (a + b) * c AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (2.0 + 3.0) * 4.0 = 5.0 * 4.0 = 20.0
    runner.send(
        "In",
        vec![
            AttributeValue::Double(2.0),
            AttributeValue::Double(3.0),
            AttributeValue::Double(4.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(20.0)]]);
}

/// Test nested subtraction and division: (a - b) / c
#[tokio::test]
async fn nested_subtraction_then_divide() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (a - b) / c AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (10.0 - 4.0) / 2.0 = 6.0 / 2.0 = 3.0
    runner.send(
        "In",
        vec![
            AttributeValue::Double(10.0),
            AttributeValue::Double(4.0),
            AttributeValue::Double(2.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(3.0)]]);
}

/// Test percentage calculation: ((new - old) / old) * 100
/// This is the exact pattern used in the crypto demo
#[tokio::test]
async fn nested_percentage_calculation() {
    let app = "\
        CREATE STREAM In (old_price DOUBLE, new_price DOUBLE);\n\
        CREATE STREAM Out (pct_change DOUBLE);\n\
        INSERT INTO Out SELECT ((new_price - old_price) / old_price) * 100.0 AS pct_change FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // ((110.0 - 100.0) / 100.0) * 100.0 = (10.0 / 100.0) * 100.0 = 0.1 * 100.0 = 10.0
    runner.send(
        "In",
        vec![AttributeValue::Double(100.0), AttributeValue::Double(110.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(10.0)]]);
}

// =============================================================================
// Deeply Nested Expression Tests
// =============================================================================

/// Test deeply nested expression: ((a + b) * c) / d
#[tokio::test]
async fn nested_deeply_three_levels() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT ((a + b) * c) / d AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // ((2.0 + 3.0) * 4.0) / 2.0 = (5.0 * 4.0) / 2.0 = 20.0 / 2.0 = 10.0
    runner.send(
        "In",
        vec![
            AttributeValue::Double(2.0),
            AttributeValue::Double(3.0),
            AttributeValue::Double(4.0),
            AttributeValue::Double(2.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(10.0)]]);
}

/// Test redundant parentheses: (((a)))
#[tokio::test]
async fn nested_redundant_parentheses() {
    let app = "\
        CREATE STREAM In (a DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (((a))) AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Double(42.0)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(42.0)]]);
}

/// Test mixed nesting: ((a + b) * (c - d))
#[tokio::test]
async fn nested_mixed_operations() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (a + b) * (c - d) AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (2.0 + 3.0) * (10.0 - 4.0) = 5.0 * 6.0 = 30.0
    runner.send(
        "In",
        vec![
            AttributeValue::Double(2.0),
            AttributeValue::Double(3.0),
            AttributeValue::Double(10.0),
            AttributeValue::Double(4.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(30.0)]]);
}

// =============================================================================
// Nested Expressions in WHERE Clause
// =============================================================================

/// Test nested expression in WHERE clause comparison
#[tokio::test]
async fn nested_in_where_clause() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, threshold DOUBLE);\n\
        CREATE STREAM Out (a DOUBLE, b DOUBLE);\n\
        INSERT INTO Out SELECT a, b FROM In WHERE (a + b) > threshold;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (3.0 + 4.0) = 7.0 > 5.0, should pass
    runner.send(
        "In",
        vec![
            AttributeValue::Double(3.0),
            AttributeValue::Double(4.0),
            AttributeValue::Double(5.0),
        ],
    );
    // (1.0 + 2.0) = 3.0 > 5.0 is false, should be filtered
    runner.send(
        "In",
        vec![
            AttributeValue::Double(1.0),
            AttributeValue::Double(2.0),
            AttributeValue::Double(5.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(3.0),
            AttributeValue::Double(4.0)
        ]]
    );
}

/// Test nested arithmetic comparison in WHERE: (a - b) > 0
#[tokio::test]
async fn nested_difference_in_where() {
    let app = "\
        CREATE STREAM In (current DOUBLE, previous DOUBLE);\n\
        CREATE STREAM Out (current DOUBLE, previous DOUBLE);\n\
        INSERT INTO Out SELECT current, previous FROM In WHERE (current - previous) > 0;\n";
    let runner = AppRunner::new(app, "Out").await;
    // Increasing: 110 - 100 = 10 > 0, pass
    runner.send(
        "In",
        vec![AttributeValue::Double(110.0), AttributeValue::Double(100.0)],
    );
    // Decreasing: 90 - 100 = -10 > 0 is false, filter
    runner.send(
        "In",
        vec![AttributeValue::Double(90.0), AttributeValue::Double(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(110.0),
            AttributeValue::Double(100.0)
        ]]
    );
}

// =============================================================================
// Nested Expressions in Function Arguments
// =============================================================================

/// Test nested expression inside ROUND function
#[tokio::test]
async fn nested_in_round_function() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT ROUND((a + b) / 3.0) AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // ROUND((10.0 + 5.0) / 3.0) = ROUND(15.0 / 3.0) = ROUND(5.0) = 5.0
    runner.send(
        "In",
        vec![AttributeValue::Double(10.0), AttributeValue::Double(5.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(5.0)]]);
}

/// Test nested expression inside CAST
#[tokio::test]
async fn nested_in_cast_function() {
    let app = "\
        CREATE STREAM In (a INT, b INT);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT CAST((a + b) AS DOUBLE) AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("In", vec![AttributeValue::Int(3), AttributeValue::Int(4)]);
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(7.0)]]);
}

// =============================================================================
// Nested Expressions with Aggregations
// =============================================================================

/// Test nested expression in aggregation: SUM((a + b))
#[tokio::test]
async fn nested_in_sum_aggregation() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM Out (total DOUBLE);\n\
        INSERT INTO Out SELECT SUM((a + b)) AS total FROM In WINDOW('lengthBatch', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    // First event: (1.0 + 2.0) = 3.0
    runner.send(
        "In",
        vec![AttributeValue::Double(1.0), AttributeValue::Double(2.0)],
    );
    // Second event: (3.0 + 4.0) = 7.0
    // Total: 3.0 + 7.0 = 10.0
    runner.send(
        "In",
        vec![AttributeValue::Double(3.0), AttributeValue::Double(4.0)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Double(10.0)]);
}

/// Test nested expression in AVG with percentage formula
#[tokio::test]
async fn nested_avg_percentage() {
    let app = "\
        CREATE STREAM In (old_val DOUBLE, new_val DOUBLE);\n\
        CREATE STREAM Out (avg_pct DOUBLE);\n\
        INSERT INTO Out SELECT AVG(((new_val - old_val) / old_val) * 100.0) AS avg_pct FROM In WINDOW('lengthBatch', 2);\n";
    let runner = AppRunner::new(app, "Out").await;
    // First: ((110 - 100) / 100) * 100 = 10%
    runner.send(
        "In",
        vec![AttributeValue::Double(100.0), AttributeValue::Double(110.0)],
    );
    // Second: ((120 - 100) / 100) * 100 = 20%
    // Avg: (10 + 20) / 2 = 15%
    runner.send(
        "In",
        vec![AttributeValue::Double(100.0), AttributeValue::Double(120.0)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out.last().unwrap(), &vec![AttributeValue::Double(15.0)]);
}

// =============================================================================
// Nested Expressions in Sequence Queries (Pattern Matching)
// =============================================================================

/// Test nested expression with sequence aliases: (e2.price - e1.price)
#[tokio::test]
async fn nested_in_sequence_with_aliases() {
    let app = "\
        CREATE STREAM Prices (symbol VARCHAR, price DOUBLE);\n\
        CREATE STREAM Out (symbol VARCHAR, price_diff DOUBLE);\n\
        INSERT INTO Out SELECT e2.symbol AS symbol, (e2.price - e1.price) AS price_diff FROM SEQUENCE (e1=Prices -> e2=Prices);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Prices",
        vec![
            AttributeValue::String("BTC".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "Prices",
        vec![
            AttributeValue::String("BTC".to_string()),
            AttributeValue::Double(110.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::String("BTC".to_string()),
            AttributeValue::Double(10.0)
        ]]
    );
}

/// Test complex nested expression with sequence aliases for percentage
#[tokio::test]
async fn nested_percentage_in_sequence() {
    let app = "\
        CREATE STREAM Stats (count BIGINT, avg_price DOUBLE);\n\
        CREATE STREAM Out (count_diff BIGINT, pct_change DOUBLE);\n\
        INSERT INTO Out SELECT \
            e2.count - e1.count AS count_diff, \
            ((e2.avg_price - e1.avg_price) / e1.avg_price) * 100.0 AS pct_change \
        FROM SEQUENCE (e1=Stats -> e2=Stats);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Stats",
        vec![AttributeValue::Long(100), AttributeValue::Double(50.0)],
    );
    runner.send(
        "Stats",
        vec![AttributeValue::Long(120), AttributeValue::Double(55.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // count_diff = 120 - 100 = 20
    // pct_change = ((55.0 - 50.0) / 50.0) * 100.0 = (5.0 / 50.0) * 100.0 = 10.0
    assert_eq!(out[0][0], AttributeValue::Long(20));
    if let AttributeValue::Double(pct) = out[0][1] {
        assert!((pct - 10.0).abs() < 0.001, "Expected 10.0, got {}", pct);
    } else {
        panic!("Expected Double for pct_change");
    }
}

// =============================================================================
// Multiple Nested Expressions in Single Query
// =============================================================================

/// Test multiple nested expressions in same SELECT
#[tokio::test]
async fn nested_multiple_in_select() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM Out (sum_ab DOUBLE, diff_bc DOUBLE, product DOUBLE);\n\
        INSERT INTO Out SELECT (a + b) AS sum_ab, (b - c) AS diff_bc, (a + b) * (b - c) AS product FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // a=2, b=5, c=3
    // sum_ab = (2 + 5) = 7
    // diff_bc = (5 - 3) = 2
    // product = 7 * 2 = 14
    runner.send(
        "In",
        vec![
            AttributeValue::Double(2.0),
            AttributeValue::Double(5.0),
            AttributeValue::Double(3.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(
        out,
        vec![vec![
            AttributeValue::Double(7.0),
            AttributeValue::Double(2.0),
            AttributeValue::Double(14.0)
        ]]
    );
}

// =============================================================================
// Edge Cases
// =============================================================================

/// Test nested expression with negative result
#[tokio::test]
async fn nested_negative_result() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (a - b) AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (5.0 - 10.0) = -5.0
    runner.send(
        "In",
        vec![AttributeValue::Double(5.0), AttributeValue::Double(10.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(-5.0)]]);
}

/// Test nested expression with zero
#[tokio::test]
async fn nested_with_zero() {
    let app = "\
        CREATE STREAM In (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM Out (result DOUBLE);\n\
        INSERT INTO Out SELECT (a + b) * 0.0 AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "In",
        vec![AttributeValue::Double(100.0), AttributeValue::Double(200.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Double(0.0)]]);
}

/// Test nested expression preserving integer types
#[tokio::test]
async fn nested_integer_arithmetic() {
    let app = "\
        CREATE STREAM In (a INT, b INT, c INT);\n\
        CREATE STREAM Out (result INT);\n\
        INSERT INTO Out SELECT (a + b) * c AS result FROM In;\n";
    let runner = AppRunner::new(app, "Out").await;
    // (2 + 3) * 4 = 20
    runner.send(
        "In",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(3),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out, vec![vec![AttributeValue::Int(20)]]);
}
