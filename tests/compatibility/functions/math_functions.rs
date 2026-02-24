// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Math Function Compatibility Tests
// Reference: query/function/FunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC MATH FUNCTIONS
// ============================================================================

/// Math function - abs
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_abs() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (absValue INT);\n\
        INSERT INTO outputStream\n\
        SELECT abs(value) AS absValue FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(-10)]);
    runner.send("inputStream", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(10));
    assert_eq!(out[1][0], AttributeValue::Int(5));
}

/// Math function - round
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_round() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (roundedValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT round(value) AS roundedValue FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.7)]);
    runner.send("inputStream", vec![AttributeValue::Double(3.2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
    assert_eq!(out[1][0], AttributeValue::Double(3.0));
}

/// Math function: sqrt
/// Reference: Math function tests
#[tokio::test]
async fn function_test_sqrt() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sqrt(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(16.0)]);
    runner.send("inputStream", vec![AttributeValue::Double(25.0)]);
    let out = runner.shutdown();
    assert!(out.len() >= 2);
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
    assert_eq!(out[1][0], AttributeValue::Double(5.0));
}

// ============================================================================
// TRIGONOMETRIC FUNCTIONS
// ============================================================================

/// Math function - log (natural logarithm)
/// Reference: Math function tests
#[tokio::test]
async fn function_test_log() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT log(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // log(e) = 1, log(e^2) = 2
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(std::f64::consts::E)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(val) = out[0][0] {
        assert!((val - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected Double");
    }
}

/// Math function - sin
/// Reference: Math function tests
#[tokio::test]
async fn function_test_sin() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // sin(0) = 0, sin(PI/2) = 1
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(std::f64::consts::FRAC_PI_2)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    if let AttributeValue::Double(val) = out[0][0] {
        assert!(val.abs() < 0.0001);
    }
    if let AttributeValue::Double(val) = out[1][0] {
        assert!((val - 1.0).abs() < 0.0001);
    }
}

/// Math function - cos
/// Reference: Math function tests
#[tokio::test]
async fn function_test_cos() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT cos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // cos(0) = 1
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(val) = out[0][0] {
        assert!((val - 1.0).abs() < 0.0001);
    }
}

/// Math function - tan
/// Reference: Math function tests
#[tokio::test]
async fn function_test_tan() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT tan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // tan(0) = 0, tan(PI/4) = 1
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(std::f64::consts::FRAC_PI_4)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    if let AttributeValue::Double(val) = out[0][0] {
        assert!(val.abs() < 0.0001);
    }
    if let AttributeValue::Double(val) = out[1][0] {
        assert!((val - 1.0).abs() < 0.0001);
    }
}

/// Math function - power
/// Reference: Math function tests
#[tokio::test]
async fn function_test_power() {
    let app = "\
        CREATE STREAM inputStream (base DOUBLE, exp DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT power(base, exp) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // 2^3 = 8
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(2.0), AttributeValue::Double(3.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(8.0));
}

/// Math function - floor
/// Reference: Math function tests
#[tokio::test]
async fn function_test_floor() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT floor(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.7)]);
    runner.send("inputStream", vec![AttributeValue::Double(-2.3)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Double(3.0));
    assert_eq!(out[1][0], AttributeValue::Double(-3.0));
}

/// Math function - ceil
/// Reference: Math function tests
#[tokio::test]
async fn function_test_ceil() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT ceil(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.2)]);
    runner.send("inputStream", vec![AttributeValue::Double(-2.7)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
    assert_eq!(out[1][0], AttributeValue::Double(-2.0));
}

// ============================================================================
// SQRT EDGE CASES
// ============================================================================

/// Sqrt of zero
#[tokio::test]
async fn function_test_sqrt_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sqrt(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// Sqrt of one
#[tokio::test]
async fn function_test_sqrt_one() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sqrt(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(1.0));
}

// ============================================================================
// ABS EDGE CASES
// ============================================================================

/// abs function with positive number
#[tokio::test]
async fn function_test_abs_positive() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT abs(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(42.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(42.5));
}

/// abs function with negative number
#[tokio::test]
async fn function_test_abs_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT abs(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-42.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(42.5));
}

/// abs function with zero
#[tokio::test]
async fn function_test_abs_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT abs(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

// ============================================================================
// FLOOR/CEIL EDGE CASES
// ============================================================================

/// floor function edge case - positive
#[tokio::test]
async fn function_test_floor_positive() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT floor(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.7)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(3.0));
}

/// floor function with negative number
#[tokio::test]
async fn function_test_floor_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT floor(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-3.2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-4.0));
}

/// ceil function edge case - positive
#[tokio::test]
async fn function_test_ceil_positive() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT ceil(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
}

/// ceil function with negative number
#[tokio::test]
async fn function_test_ceil_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT ceil(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-3.7)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-3.0));
}

/// round function edge case - half up
#[tokio::test]
async fn function_test_round_half_up() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT round(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(4.0));
}

// ============================================================================
// LOGARITHMIC AND EXPONENTIAL FUNCTIONS
// ============================================================================

/// power function edge case - two to three
#[tokio::test]
async fn function_test_power_two_three() {
    let app = "\
        CREATE STREAM inputStream (base DOUBLE, exp DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT power(base, exp) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(2.0), AttributeValue::Double(3.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(8.0));
}

/// log function edge case - natural log of e
#[tokio::test]
async fn function_test_ln_of_e() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT ln(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(std::f64::consts::E)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Allow small floating point error
    if let AttributeValue::Double(val) = out[0][0] {
        assert!((val - 1.0).abs() < 0.0001);
    }
}

/// log10 function
#[tokio::test]
async fn function_test_log10() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT log10(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(100.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(2.0));
}

/// exp function
#[tokio::test]
async fn function_test_exp() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT exp(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(1.0));
}

// ============================================================================
// TRIGONOMETRIC EDGE CASES
// ============================================================================

/// sin function edge case - sin of zero
#[tokio::test]
async fn function_test_sin_of_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// cos function edge case - cos of zero
#[tokio::test]
async fn function_test_cos_of_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT cos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(1.0));
}

/// tan function edge case - tan of zero
#[tokio::test]
async fn function_test_tan_of_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT tan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// Multiple functions in SELECT
#[tokio::test]
async fn function_test_multiple_functions() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (floored DOUBLE, ceiled DOUBLE, rounded DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT floor(value) AS floored, ceil(value) AS ceiled, round(value) AS rounded FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(3.0));
    assert_eq!(out[0][1], AttributeValue::Double(4.0));
    assert_eq!(out[0][2], AttributeValue::Double(4.0));
}

// ============================================================================
// NEW MATH FUNCTIONS - maximum, minimum, mod, sign, trunc
// ============================================================================

/// Test maximum function with two arguments
#[tokio::test]
async fn function_test_maximum_two_args() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT maximum(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(10.5), AttributeValue::Double(20.3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(20.3));
}

/// Test maximum function with three arguments
#[tokio::test]
async fn function_test_maximum_three_args() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT maximum(a, b, c) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Double(5.0),
            AttributeValue::Double(15.0),
            AttributeValue::Double(10.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(15.0));
}

/// Test maximum with negative numbers
#[tokio::test]
async fn function_test_maximum_negative() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT maximum(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(-10.0), AttributeValue::Double(-5.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-5.0)); // -5 > -10
}

/// Test minimum function with two arguments
#[tokio::test]
async fn function_test_minimum_two_args() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT minimum(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(10.5), AttributeValue::Double(20.3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(10.5));
}

/// Test minimum function with three arguments
#[tokio::test]
async fn function_test_minimum_three_args() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE, c DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT minimum(a, b, c) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Double(5.0),
            AttributeValue::Double(15.0),
            AttributeValue::Double(10.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(5.0));
}

/// Test minimum with negative numbers
#[tokio::test]
async fn function_test_minimum_negative() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT minimum(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(-10.0), AttributeValue::Double(-5.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-10.0)); // -10 < -5
}

/// Test mod function (modulo operation)
#[tokio::test]
async fn function_test_mod_basic() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT mod(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(17.0), AttributeValue::Double(5.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(2.0)); // 17 % 5 = 2
}

/// Test mod function with decimal numbers
#[tokio::test]
async fn function_test_mod_decimal() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT mod(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(10.5), AttributeValue::Double(3.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(1.5)); // 10.5 % 3.0 = 1.5
}

/// Test mod with negative dividend
#[tokio::test]
async fn function_test_mod_negative() {
    let app = "\
        CREATE STREAM inputStream (a DOUBLE, b DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT mod(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Double(-17.0), AttributeValue::Double(5.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-2.0)); // -17 % 5 = -2
}

/// Test sign function with positive number
#[tokio::test]
async fn function_test_sign_positive() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT sign(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(42.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Test sign function with negative number
#[tokio::test]
async fn function_test_sign_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT sign(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-42.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(-1));
}

/// Test sign function with zero
#[tokio::test]
async fn function_test_sign_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT sign(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(0));
}

/// Test trunc function - truncate to integer (no precision)
#[tokio::test]
async fn function_test_trunc_basic() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT trunc(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.789)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(3.0)); // Truncate towards zero
}

/// Test trunc function with negative number
#[tokio::test]
async fn function_test_trunc_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT trunc(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-3.789)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(-3.0)); // Truncate towards zero
}

/// Test trunc function with precision
#[tokio::test]
async fn function_test_trunc_with_precision() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT trunc(value, 2) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(3.14159)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(3.14)); // Truncate to 2 decimal places
}

/// Test truncate alias
#[tokio::test]
async fn function_test_truncate_alias() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT truncate(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(9.99)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(9.0));
}

// ============================================================================
// INVERSE TRIGONOMETRIC FUNCTIONS - asin, acos, atan
// ============================================================================

/// Test asin function - basic
#[tokio::test]
async fn function_test_asin_basic() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT asin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // asin(0.5) ≈ 0.5235987755982989 (π/6)
        assert!((result - 0.5235987755982989).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test asin(0) = 0
#[tokio::test]
async fn function_test_asin_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT asin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// Test asin(1) = π/2
#[tokio::test]
async fn function_test_asin_one() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT asin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // asin(1) = π/2 ≈ 1.5707963267948966
        assert!((result - std::f64::consts::FRAC_PI_2).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test asin out of bounds returns NaN
#[tokio::test]
async fn function_test_asin_out_of_bounds() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT asin(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(2.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        assert!(result.is_nan(), "asin(2.0) should be NaN");
    } else {
        panic!("Expected Double value");
    }
}

/// Test acos function - basic
#[tokio::test]
async fn function_test_acos_basic() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT acos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // acos(0.5) ≈ 1.0471975511965979 (π/3)
        assert!((result - 1.0471975511965979).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test acos(1) = 0
#[tokio::test]
async fn function_test_acos_one() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT acos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        assert!(result.abs() < 0.0001, "acos(1) should be 0");
    } else {
        panic!("Expected Double value");
    }
}

/// Test acos(0) = π/2
#[tokio::test]
async fn function_test_acos_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT acos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // acos(0) = π/2 ≈ 1.5707963267948966
        assert!((result - std::f64::consts::FRAC_PI_2).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test acos out of bounds returns NaN
#[tokio::test]
async fn function_test_acos_out_of_bounds() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT acos(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(2.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        assert!(result.is_nan(), "acos(2.0) should be NaN");
    } else {
        panic!("Expected Double value");
    }
}

/// Test atan function - basic
#[tokio::test]
async fn function_test_atan_basic() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT atan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(1.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // atan(1) = π/4 ≈ 0.7853981633974483
        assert!((result - std::f64::consts::FRAC_PI_4).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test atan(0) = 0
#[tokio::test]
async fn function_test_atan_zero() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT atan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(0.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// Test atan with large positive value approaches π/2
#[tokio::test]
async fn function_test_atan_large_positive() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT atan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(1000.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // atan(large) approaches π/2
        assert!((result - std::f64::consts::FRAC_PI_2).abs() < 0.01);
    } else {
        panic!("Expected Double value");
    }
}

/// Test atan with negative value
#[tokio::test]
async fn function_test_atan_negative() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT atan(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Double(-1.0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][0] {
        // atan(-1) = -π/4 ≈ -0.7853981633974483
        assert!((result + std::f64::consts::FRAC_PI_4).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}
