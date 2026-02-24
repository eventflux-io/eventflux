// SPDX-License-Identifier: MIT OR Apache-2.0
//
// CASE Expression Compatibility Tests
// Reference: query/function/FunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// CASE EXPRESSION TESTS
// ============================================================================

/// CASE WHEN expression - simple case
/// Reference: Function tests
#[tokio::test]
async fn function_test_case_when_simple() {
    let app = "\
        CREATE STREAM inputStream (status INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN status = 1 THEN 'Active' WHEN status = 0 THEN 'Inactive' ELSE 'Unknown' END AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    runner.send("inputStream", vec![AttributeValue::Int(0)]);
    runner.send("inputStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::String("Active".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("Inactive".to_string()));
    assert_eq!(out[2][0], AttributeValue::String("Unknown".to_string()));
}

/// CASE WHEN expression - with comparison
/// Reference: Function tests
#[tokio::test]
async fn function_test_case_when_comparison() {
    let app = "\
        CREATE STREAM inputStream (price FLOAT);\n\
        CREATE STREAM outputStream (category STRING);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN price < 50.0 THEN 'Cheap' WHEN price < 100.0 THEN 'Medium' ELSE 'Expensive' END AS category FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Float(30.0)]);
    runner.send("inputStream", vec![AttributeValue::Float(75.0)]);
    runner.send("inputStream", vec![AttributeValue::Float(150.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::String("Cheap".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("Medium".to_string()));
    assert_eq!(out[2][0], AttributeValue::String("Expensive".to_string()));
}

/// CASE WHEN with numeric return
/// Note: CASE WHEN returns BIGINT for integer constants
#[tokio::test]
async fn function_test_case_when_numeric() {
    let app = "\
        CREATE STREAM inputStream (grade STRING);\n\
        CREATE STREAM outputStream (points BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN grade = 'A' THEN 4 WHEN grade = 'B' THEN 3 WHEN grade = 'C' THEN 2 ELSE 0 END AS points FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("A".to_string())]);
    runner.send("inputStream", vec![AttributeValue::String("C".to_string())]);
    runner.send("inputStream", vec![AttributeValue::String("F".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::Long(4));
    assert_eq!(out[1][0], AttributeValue::Long(2));
    assert_eq!(out[2][0], AttributeValue::Long(0));
}

/// CASE WHEN with boolean result
#[tokio::test]
async fn function_test_case_when_boolean() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (isPositive BOOLEAN);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN value > 0 THEN true ELSE false END AS isPositive FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(5)]);
    runner.send("inputStream", vec![AttributeValue::Int(-3)]);
    runner.send("inputStream", vec![AttributeValue::Int(0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::Bool(true));
    assert_eq!(out[1][0], AttributeValue::Bool(false));
    assert_eq!(out[2][0], AttributeValue::Bool(false));
}

/// Conditional expression using CASE WHEN
#[tokio::test]
async fn function_test_case_conditional() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN value > 10 THEN 'HIGH' ELSE 'LOW' END AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(15)]);
    runner.send("inputStream", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("HIGH".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("LOW".to_string()));
}
