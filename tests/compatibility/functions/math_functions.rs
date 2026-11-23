/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
// Math function compatibility tests
// Reference: query/function/FunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC MATH FUNCTIONS
// ============================================================================

/// abs
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

/// round
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

/// sqrt
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
// LOG, TRIGONOMETRIC, POWER, FLOOR AND CEIL FUNCTIONS
// ============================================================================

/// log, the natural logarithm
/// Reference: Math function tests
#[tokio::test]
async fn function_test_log() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT log(value) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // log(e) = 1
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

/// sin
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

/// cos
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

/// tan
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

/// power
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

/// floor rounds down, so -2.3 becomes -3.0
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

/// ceil rounds up, so -2.7 becomes -2.0
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

/// sqrt of zero
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

/// sqrt of one
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

/// abs of a positive number
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

/// abs of a negative number
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

/// abs of zero
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
// FLOOR, CEIL AND ROUND EDGE CASES
// ============================================================================

/// floor of a positive number
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

/// floor of a negative number
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

/// ceil of a positive number
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

/// ceil of a negative number
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

/// round takes 3.5 up to 4.0
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
// POWER, LOGARITHM AND EXPONENT FUNCTIONS
// ============================================================================

/// power(2, 3) = 8
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

/// ln(e) = 1
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
    // Allow a small floating point error
    if let AttributeValue::Double(val) = out[0][0] {
        assert!((val - 1.0).abs() < 0.0001);
    }
}

/// log10(100) = 2
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

/// exp(0) = 1
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

/// sin(0) = 0
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

/// cos(0) = 1
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

/// tan(0) = 0
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

/// floor, ceil and round of the same value in one SELECT
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
// MAXIMUM, MINIMUM, MOD, SIGN AND TRUNC
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

/// Test mod function, the remainder after division
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

/// Test mod with a negative dividend. The result takes the sign of the dividend.
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

/// Test trunc with no precision, which cuts the value to a whole number
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
    assert_eq!(out[0][0], AttributeValue::Double(3.0)); // cut toward zero
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
    assert_eq!(out[0][0], AttributeValue::Double(-3.0)); // cut toward zero, not down to -4
}

/// Test trunc function with precision
#[tokio::test]
#[allow(clippy::approx_constant)]
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
    #[allow(clippy::approx_constant)]
    let expected = AttributeValue::Double(3.14); // cut to 2 decimal places
    assert_eq!(out[0][0], expected);
}

/// Test truncate, an alias for trunc
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
// INVERSE TRIGONOMETRIC FUNCTIONS: asin, acos, atan
// ============================================================================

/// Test asin(0.5)
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
        // asin(0.5) is about π/6
        assert!((result - std::f64::consts::FRAC_PI_6).abs() < 0.0001);
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
        // asin(1) = π/2, about 1.5707963267948966
        assert!((result - std::f64::consts::FRAC_PI_2).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test asin returns NaN for input outside [-1, 1]
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

/// Test acos(0.5)
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
        // acos(0.5) is about π/3
        assert!((result - std::f64::consts::FRAC_PI_3).abs() < 0.0001);
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
        // acos(0) = π/2, about 1.5707963267948966
        assert!((result - std::f64::consts::FRAC_PI_2).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test acos returns NaN for input outside [-1, 1]
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

/// Test atan(1)
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
        // atan(1) = π/4, about 0.7853981633974483
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

/// Test atan of a large positive value comes close to π/2
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
        // atan(1000) is within 0.01 of π/2
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
        // atan(-1) = -π/4, about -0.7853981633974483
        assert!((result + std::f64::consts::FRAC_PI_4).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}
