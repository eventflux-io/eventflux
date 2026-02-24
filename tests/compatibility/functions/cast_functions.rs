// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Cast Function Compatibility Tests
// Reference: query/function/ConversionFunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC CAST OPERATIONS
// ============================================================================

/// Test cast function
/// Reference: ConversionFunctionTestCase.java:testConversionQuery1
#[tokio::test]
async fn function_test_cast() {
    let app = "\
        CREATE STREAM inputStream (strVal STRING, intVal INT);\n\
        CREATE STREAM outputStream (intResult INT, strResult STRING);\n\
        INSERT INTO outputStream\n\
        SELECT CAST(strVal AS INT) AS intResult, CAST(intVal AS STRING) AS strResult\n\
        FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("123".to_string()),
            AttributeValue::Int(456),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(123));
    assert_eq!(out[0][1], AttributeValue::String("456".to_string()));
}

// ============================================================================
// CAST FUNCTION VARIATIONS
// ============================================================================

/// Cast INT to BIGINT
#[tokio::test]
async fn function_test_cast_int_to_long() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT CAST(value AS BIGINT) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(12345)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(12345));
}

/// Cast FLOAT to DOUBLE
#[tokio::test]
async fn function_test_cast_float_to_double() {
    let app = "\
        CREATE STREAM inputStream (value FLOAT);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT CAST(value AS DOUBLE) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Float(3.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(d) = out[0][0] {
        assert!((d - 3.5).abs() < 0.001);
    } else {
        panic!("Expected Double");
    }
}

/// Cast DOUBLE to INT (truncation)
#[tokio::test]
async fn function_test_cast_double_to_int() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT CAST(value AS INT) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(9.99)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Truncates to 9
    assert_eq!(out[0][0], AttributeValue::Int(9));
}

/// Cast STRING to FLOAT
#[tokio::test]
async fn function_test_cast_string_to_float() {
    let app = "\
        CREATE STREAM inputStream (value STRING);\n\
        CREATE STREAM outputStream (result FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT CAST(value AS FLOAT) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("123.45".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Float(f) = out[0][0] {
        assert!((f - 123.45).abs() < 0.01);
    } else {
        panic!("Expected Float");
    }
}

/// concat with numbers converted to string
#[tokio::test]
async fn function_test_concat_with_cast() {
    let app = "\
        CREATE STREAM inputStream (prefix STRING, num INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(prefix, '-', cast(num AS STRING)) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("ID".to_string()),
            AttributeValue::Int(123),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ID-123".to_string()));
}
