// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Arithmetic Operations Compatibility Tests
// Reference: query/function/FunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC ARITHMETIC OPERATIONS
// ============================================================================

/// Arithmetic - addition
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn arithmetic_test_addition() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT a + b AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(15));
}

/// Arithmetic - subtraction
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn arithmetic_test_subtraction() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT a - b AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(7));
}

/// Arithmetic - multiplication
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn arithmetic_test_multiplication() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT a * b AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(50));
}

/// Arithmetic - division (returns DOUBLE for integer division)
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn arithmetic_test_division() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT a / b AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(2)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(5.0));
}

/// Arithmetic - modulo (returns DOUBLE in EventFlux)
/// Reference: FunctionTestCase.java
/// Note: Modulo operator not yet supported in SQL converter
#[tokio::test]
#[ignore = "Modulo operator not yet supported in SQL converter"]
async fn arithmetic_test_modulo() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT a % b AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(1.0));
}

// ============================================================================
// COMPLEX ARITHMETIC EXPRESSIONS
// ============================================================================

/// Complex arithmetic expression
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn arithmetic_test_complex_expression() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT (a + b) * c AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(3),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // (2 + 3) * 4 = 20
    assert_eq!(out[0][0], AttributeValue::Int(20));
}

/// Arithmetic with round
#[tokio::test]
async fn function_test_arithmetic_with_round() {
    let app = "\
        CREATE STREAM inputStream (price DOUBLE, quantity INT);\n\
        CREATE STREAM outputStream (total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT round(price * quantity) AS total FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(9.99), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // 9.99 * 3 = 29.97, rounded = 30.0
    assert_eq!(out[0][0], AttributeValue::Double(30.0));
}

/// Complex arithmetic expression
#[tokio::test]
async fn function_test_complex_arithmetic() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT (a + b) / c AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Double(10.0),
            AttributeValue::Double(20.0),
            AttributeValue::Double(6.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // (10 + 20) / 6 = 5.0
    assert_eq!(out[0][0], AttributeValue::Double(5.0));
}

/// Nested arithmetic operations
#[tokio::test]
async fn function_test_nested_arithmetic() {
    let app = "\
        CREATE STREAM inputStream (x INT, y INT, z INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT x * (y + z) - (x - y) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Int(5),
            AttributeValue::Int(3),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // 5 * (3 + 2) - (5 - 3) = 5 * 5 - 2 = 25 - 2 = 23
    assert_eq!(out[0][0], AttributeValue::Int(23));
}
