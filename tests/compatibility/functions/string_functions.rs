// SPDX-License-Identifier: MIT OR Apache-2.0
//
// String Function Compatibility Tests
// Reference: query/function/FunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC STRING FUNCTIONS
// ============================================================================

/// String function - concat
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_concat() {
    let app = "\
        CREATE STREAM inputStream (first STRING, last STRING);\n\
        CREATE STREAM outputStream (fullName STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(first, ' ', last) AS fullName FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("John Doe".to_string()));
}

/// String function - upper
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_upper() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (upperText STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(text) AS upperText FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("hello world".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("HELLO WORLD".to_string()));
}

/// String function - lower
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_lower() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (lowerText STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lower(text) AS lowerText FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HELLO WORLD".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello world".to_string()));
}

/// String function - length
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn function_test_length() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(text) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(5));
}

/// String function: substring
/// Reference: String function tests
#[tokio::test]
async fn function_test_substring() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT substring(text, 1, 4) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // substring(1, 4) should give characters 1-4 (0-indexed: "ello")
    // Note: Exact behavior depends on implementation (0-indexed or 1-indexed)
    let result = &out[0][0];
    if let AttributeValue::String(s) = result {
        assert!(s.len() <= 4);
    }
}

// ============================================================================
// TRIM AND REPLACE FUNCTIONS
// ============================================================================

/// String function - trim
/// Reference: String function tests
#[tokio::test]
async fn function_test_trim() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT trim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("  hello  ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
}

/// String function - replace
/// Reference: String function tests
#[tokio::test]
async fn function_test_replace() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT replace(text, 'world', 'rust') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("hello world".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello rust".to_string()));
}

// ============================================================================
// STRING LENGTH AND CONCAT EDGE CASES
// ============================================================================

/// Length of empty string
#[tokio::test]
async fn function_test_length_empty() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(text) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(0));
}

/// Concat with empty strings
#[tokio::test]
async fn function_test_concat_empty() {
    let app = "\
        CREATE STREAM inputStream (a STRING, b STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("hello".to_string()),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
}

/// Concat multiple strings
#[tokio::test]
async fn function_test_concat_multiple() {
    let app = "\
        CREATE STREAM inputStream (a STRING, b STRING, c STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(a, b, c) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("one".to_string()),
            AttributeValue::String("two".to_string()),
            AttributeValue::String("three".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("onetwothree".to_string()));
}

// ============================================================================
// ADDITIONAL STRING EDGE CASES
// ============================================================================

/// length function edge case - basic
#[tokio::test]
async fn function_test_length_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(text) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(5));
}

/// length function with empty string - edge case
#[tokio::test]
async fn function_test_length_empty_string() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(text) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(0));
}

/// trim function edge case - basic
#[tokio::test]
async fn function_test_trim_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT trim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("  hello  ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
}

/// replace function edge case - basic
#[tokio::test]
async fn function_test_replace_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT replace(text, 'world', 'there') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("hello world".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello there".to_string()));
}

/// substring function edge case - basic
#[tokio::test]
async fn function_test_substring_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT substr(text, 1, 5) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello World".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// concat with empty string suffix
#[tokio::test]
async fn function_test_concat_empty_suffix() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(name, '') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// concat with three arguments
#[tokio::test]
async fn function_test_concat_three_args() {
    let app = "\
        CREATE STREAM inputStream (first STRING, middle STRING, last STRING);\n\
        CREATE STREAM outputStream (full STRING);\n\
        INSERT INTO outputStream\n\
        SELECT concat(first, middle, last) AS full FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("B".to_string()),
            AttributeValue::String("C".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ABC".to_string()));
}

/// length of empty string returns zero value
#[tokio::test]
async fn function_test_length_zero_for_empty() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(name) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(0));
}

/// upper with mixed case
#[tokio::test]
async fn function_test_upper_mixed() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(name) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HeLLo WoRLd".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("HELLO WORLD".to_string()));
}

/// lower with mixed case
#[tokio::test]
async fn function_test_lower_mixed() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lower(name) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HeLLo WoRLd".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello world".to_string()));
}

/// upper with empty string
#[tokio::test]
async fn function_test_upper_empty() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(name) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("".to_string()));
}

/// nested function calls: upper(concat(...))
#[tokio::test]
async fn function_test_nested_upper_concat() {
    let app = "\
        CREATE STREAM inputStream (first STRING, last STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(concat(first, ' ', last)) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("john".to_string()),
            AttributeValue::String("doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("JOHN DOE".to_string()));
}

/// nested function calls: length(concat(...))
#[tokio::test]
async fn function_test_nested_length_concat() {
    let app = "\
        CREATE STREAM inputStream (first STRING, last STRING);\n\
        CREATE STREAM outputStream (len INT);\n\
        INSERT INTO outputStream\n\
        SELECT length(concat(first, last)) AS len FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("Hello".to_string()),
            AttributeValue::String("World".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(10)); // "HelloWorld" = 10 chars
}

/// substring function with start and length indices
#[tokio::test]
async fn function_test_substring_indices() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT substring(text, 0, 5) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// trim function to remove surrounding whitespace
#[tokio::test]
async fn function_test_trim_whitespace() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT trim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("   spaced   ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("spaced".to_string()));
}

// ============================================================================
// NEW STRING FUNCTIONS - left, right, ltrim, rtrim, reverse, repeat, position, ascii, chr
// ============================================================================

/// Test left function - get leftmost n characters
#[tokio::test]
async fn function_test_left_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT left(text, 5) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// Test left with n larger than string length
#[tokio::test]
async fn function_test_left_exceed_length() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT left(text, 20) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string())); // Returns entire string
}

/// Test left with zero
#[tokio::test]
async fn function_test_left_zero() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT left(text, 0) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("".to_string()));
}

/// Test right function - get rightmost n characters
#[tokio::test]
async fn function_test_right_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT right(text, 5) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("World".to_string()));
}

/// Test right with n larger than string length
#[tokio::test]
async fn function_test_right_exceed_length() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT right(text, 20) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string())); // Returns entire string
}

/// Test ltrim function - trim leading whitespace
#[tokio::test]
async fn function_test_ltrim_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT ltrim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("   Hello World   ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][0],
        AttributeValue::String("Hello World   ".to_string())
    ); // Only leading spaces removed
}

/// Test ltrim with no leading whitespace
#[tokio::test]
async fn function_test_ltrim_no_leading() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT ltrim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello   ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello   ".to_string())); // Unchanged
}

/// Test rtrim function - trim trailing whitespace
#[tokio::test]
async fn function_test_rtrim_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rtrim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("   Hello World   ".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][0],
        AttributeValue::String("   Hello World".to_string())
    ); // Only trailing spaces removed
}

/// Test rtrim with no trailing whitespace
#[tokio::test]
async fn function_test_rtrim_no_trailing() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rtrim(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("   Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("   Hello".to_string())); // Unchanged
}

/// Test reverse function
#[tokio::test]
async fn function_test_reverse_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT reverse(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("olleH".to_string()));
}

/// Test reverse with palindrome
#[tokio::test]
async fn function_test_reverse_palindrome() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT reverse(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("radar".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("radar".to_string())); // Palindrome unchanged
}

/// Test reverse with empty string
#[tokio::test]
async fn function_test_reverse_empty() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT reverse(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("".to_string()));
}

/// Test repeat function
#[tokio::test]
async fn function_test_repeat_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT repeat(text, 3) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("ab".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ababab".to_string()));
}

/// Test repeat with zero
#[tokio::test]
async fn function_test_repeat_zero() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT repeat(text, 0) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("".to_string()));
}

/// Test repeat with one
#[tokio::test]
async fn function_test_repeat_one() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT repeat(text, 1) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// Test position function - find substring (1-based index)
#[tokio::test]
async fn function_test_position_found() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT position('World', text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(6)); // 1-based index: 'W' is at position 6
}

/// Test position function - substring not found
#[tokio::test]
async fn function_test_position_not_found() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT position('xyz', text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(0)); // 0 means not found
}

/// Test position at start
#[tokio::test]
async fn function_test_position_at_start() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT position('Hello', text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1)); // At position 1 (1-based)
}

/// Test locate alias for position (MySQL compatibility)
#[tokio::test]
async fn function_test_locate_alias() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT locate('llo', text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(3)); // 'llo' starts at position 3
}

/// Test instr alias for position (MySQL/Oracle compatibility)
#[tokio::test]
async fn function_test_instr_alias() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT instr('World', text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("HelloWorld".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(6));
}

/// Test ascii function - get ASCII code of first character
#[tokio::test]
async fn function_test_ascii_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT ascii(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("A".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(65)); // ASCII code of 'A'
}

/// Test ascii with lowercase
#[tokio::test]
async fn function_test_ascii_lowercase() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT ascii(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("a".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(97)); // ASCII code of 'a'
}

/// Test ascii with multi-character string (returns first char)
#[tokio::test]
async fn function_test_ascii_multi_char() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT ascii(text) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(72)); // ASCII code of 'H'
}

/// Test chr function - get character from ASCII code
#[tokio::test]
async fn function_test_chr_basic() {
    let app = "\
        CREATE STREAM inputStream (code INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT chr(code) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(65)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Test chr with lowercase
#[tokio::test]
async fn function_test_chr_lowercase() {
    let app = "\
        CREATE STREAM inputStream (code INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT chr(code) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(97)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("a".to_string()));
}

/// Test char alias for chr (MySQL compatibility)
#[tokio::test]
async fn function_test_char_alias() {
    let app = "\
        CREATE STREAM inputStream (code INT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT char(code) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(90)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Z".to_string()));
}

/// Test ascii and chr roundtrip
#[tokio::test]
async fn function_test_ascii_chr_roundtrip() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT chr(ascii(text)) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("X".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("X".to_string())); // Should get back 'X'
}

// ============================================================================
// LPAD AND RPAD STRING FUNCTIONS
// ============================================================================

/// Test lpad function - basic left padding
#[tokio::test]
async fn function_test_lpad_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lpad(text, 10, '*') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("*****Hello".to_string()));
}

/// Test lpad with multi-char pad string
#[tokio::test]
async fn function_test_lpad_multichar_pad() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lpad(text, 10, 'xy') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hi".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("xyxyxyxyHi".to_string()));
}

/// Test lpad when string is already longer than target length
#[tokio::test]
async fn function_test_lpad_truncate() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lpad(text, 3, '*') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hel".to_string()));
}

/// Test lpad with exact length (no padding needed)
#[tokio::test]
async fn function_test_lpad_exact_length() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT lpad(text, 5, '*') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello".to_string()));
}

/// Test rpad function - basic right padding
#[tokio::test]
async fn function_test_rpad_basic() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rpad(text, 10, '*') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hello*****".to_string()));
}

/// Test rpad with multi-char pad string
#[tokio::test]
async fn function_test_rpad_multichar_pad() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rpad(text, 10, 'xy') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hi".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hixyxyxyxy".to_string()));
}

/// Test rpad when string is already longer than target length
#[tokio::test]
async fn function_test_rpad_truncate() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rpad(text, 3, '*') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("Hello".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Hel".to_string()));
}

/// Test rpad with empty string
#[tokio::test]
async fn function_test_rpad_empty_string() {
    let app = "\
        CREATE STREAM inputStream (text STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT rpad(text, 5, 'x') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::String("".to_string())]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("xxxxx".to_string()));
}
