// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Filter Compatibility Tests
// Reference: query/FilterTestCase1.java, query/FilterTestCase2.java

use super::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC COMPARISON OPERATORS
// ============================================================================

/// Filter test1 - basic greater than comparison
/// Reference: FilterTestCase1.java:filterTest1
#[tokio::test]
async fn filter_test1_basic_greater_than() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE 70.0 > price;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(700.0),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.5),
            AttributeValue::Long(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

/// Filter test2 - volume filter with long type
/// Reference: FilterTestCase1.java:filterTest2
#[tokio::test]
async fn filter_test2_volume_filter() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE 150 > volume;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(700.0),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.5),
            AttributeValue::Long(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

/// Filter test3 - filter with multiple matching events
/// Reference: FilterTestCase1.java:testFilterQuery3
#[tokio::test]
async fn filter_test3_multiple_matches() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE 70.0 > price;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test4 - attribute greater than literal
/// Reference: FilterTestCase1.java:testFilterQuery4
#[tokio::test]
async fn filter_test4_attribute_gt_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Long(60),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Long(40),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(44.0),
            AttributeValue::Long(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test - less than operator
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test5_less_than() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price < 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(700.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.5),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

/// Filter test - greater than or equal
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test6_greater_than_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price >= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(99.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(101.0),
            AttributeValue::Int(300),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test - less than or equal
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test7_less_than_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price <= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(99.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(101.0),
            AttributeValue::Int(300),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

// ============================================================================
// STRING COMPARISONS
// ============================================================================

/// Filter test - equality with string
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test8_string_equality() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE symbol = 'IBM';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(700.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.5),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.0),
            AttributeValue::Int(300),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test - inequality with string
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test9_string_inequality() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE symbol != 'IBM';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(700.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.5),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

// ============================================================================
// LOGICAL OPERATORS
// ============================================================================

/// Filter test - AND operator
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test10_and_operator() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price > 50.0 AND volume > 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(40.0),
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Filter test - OR operator
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test11_or_operator() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price > 500.0 OR volume > 150;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(600.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(40.0),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test - NOT operator
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test12_not_operator() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE NOT price > 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Filter test - complex condition with parentheses
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test13_complex_condition() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE (price > 50.0 AND volume > 100) OR symbol = 'SPECIAL';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("SPECIAL".to_string()),
            AttributeValue::Float(10.0),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(40.0),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

// ============================================================================
// TYPE COERCION TESTS
// ============================================================================

/// Filter test14 - double type comparison
/// Reference: FilterTestCase1.java:testFilterQuery7
#[tokio::test]
async fn filter_test14_double_comparison() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Double(60.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(40.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(44.0),
            AttributeValue::Double(200.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter test15: int compared to double literal
/// Tests: quantity > 4d (int field vs double literal)
/// Reference: FilterTestCase1.java testFilterQuery15
#[tokio::test]
async fn filter_test15_int_gt_double_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume FLOAT, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, quantity FROM cseEventStream WHERE quantity > 4.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Float(60.0),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Float(60.0),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Float(200.0),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Only quantity=5 passes (> 4.0)
    assert!(!out.is_empty());
}

/// Filter test16: long compared to double literal
/// Tests: volume > 50d (long field vs double literal)
/// Reference: FilterTestCase1.java testFilterQuery16
#[tokio::test]
async fn filter_test16_long_gt_double_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Long(60),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Long(40),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(44.0),
            AttributeValue::Long(200),
        ],
    );
    let out = runner.shutdown();
    // volume=60 and volume=200 pass
    assert!(out.len() >= 2);
}

/// Filter test - int type comparison
/// Reference: FilterTestCase1.java
#[tokio::test]
async fn filter_test17_int_comparison() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, quantity FROM cseEventStream WHERE quantity > 5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Filter test20: less than with long
/// Tests: volume < 100 (long field)
/// Reference: FilterTestCase1.java testFilterQuery20
#[tokio::test]
async fn filter_test20_less_than() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume < 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Long(103),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Long(10),
        ],
    );
    let out = runner.shutdown();
    // Only volume=10 passes (< 100)
    assert!(!out.is_empty());
    assert_eq!(out[0][2], AttributeValue::Long(10));
}

/// Filter test21: not equal with long
/// Tests: volume != 100
/// Reference: FilterTestCase1.java testFilterQuery21
#[tokio::test]
async fn filter_test21_not_equal_long() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume != 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Long(10),
        ],
    );
    let out = runner.shutdown();
    // Only volume=10 passes (!= 100)
    assert!(!out.is_empty());
    assert_eq!(out[0][2], AttributeValue::Long(10));
}

/// Filter test22: complex AND with mixed types
/// Tests: volume > 12L and price < 56
/// Reference: FilterTestCase1.java testFilterQuery22
#[tokio::test]
async fn filter_test22_complex_and_mixed_types() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume > 12 AND price < 56;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Double(10.0),
        ],
    );
    let out = runner.shutdown();
    // Only first event passes (volume=100 > 12 AND price=55.6 < 56)
    assert!(!out.is_empty());
    assert_eq!(out[0][2], AttributeValue::Double(100.0));
}

/// Filter test23: complex AND with string and numeric inequalities
/// Tests: symbol != 'MSFT' and volume != 55L and price != 45f
/// Reference: FilterTestCase1.java testFilterQuery23
#[tokio::test]
async fn filter_test23_complex_and_multiple_not_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream \n\
        WHERE symbol != 'MSFT' AND volume != 55 AND price != 45.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(45.0),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(35.0),
            AttributeValue::Long(50),
        ],
    );
    let out = runner.shutdown();
    // Second event passes (symbol=IBM != MSFT, volume=50 != 55, price=35 != 45)
    assert!(!out.is_empty());
}

/// Filter test24: not equal with float literal comparison to long
/// Tests: volume != 50f
/// Reference: FilterTestCase1.java testFilterQuery24
#[tokio::test]
async fn filter_test24_not_equal_float_to_long() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE volume != 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(45.0),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(35.0),
            AttributeValue::Long(50),
        ],
    );
    let out = runner.shutdown();
    // First event passes (volume=100 != 50)
    assert!(!out.is_empty());
}

/// Filter test26: double not equal conditions
/// Tests: volume != 100 and volume != 70d
/// Reference: FilterTestCase1.java testFilterQuery26
#[tokio::test]
async fn filter_test26_double_not_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE volume != 100 AND volume != 70;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Long(10),
        ],
    );
    let out = runner.shutdown();
    // Second event passes (volume=10 != 100 and != 70)
    assert!(!out.is_empty());
}

/// Filter test27: OR with not equal
/// Tests: price != 53.6d or price != 87
/// Reference: FilterTestCase1.java testFilterQuery27
#[tokio::test]
async fn filter_test27_or_not_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, volume FROM cseEventStream WHERE price != 53.6 OR price != 87.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Long(100),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Long(10),
        ],
    );
    let out = runner.shutdown();
    // Both events pass due to OR condition
    assert!(out.len() >= 2);
}

// ============================================================================
// BOOLEAN TESTS
// ============================================================================

/// Filter test30: boolean not equal
/// Tests: available != true
/// Reference: FilterTestCase1.java testFilterQuery30
#[tokio::test]
async fn filter_test30_boolean_not_equal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, available BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, available BOOLEAN);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, available FROM cseEventStream WHERE available != true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    // Only second event passes (available=false != true)
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

/// NOT operator test
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_not_boolean() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, available BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE NOT available;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(57.6),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    // NOT available: available=false -> true (include)
    assert!(!out.is_empty());
}

// ============================================================================
// FILTERCASE2 TESTS (Type Literal Comparisons)
// ============================================================================

/// Filter test83: double less than long literal
/// Tests: volume < 60L (double field vs long literal)
/// Reference: FilterTestCase2.java testFilterQuery83
#[tokio::test]
async fn filter_test83_double_lt_long_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, quantity FROM cseEventStream WHERE volume < 60;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(500.0),
            AttributeValue::Double(50.0),
            AttributeValue::Int(6),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(60.0),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Double(300.0),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Only first event passes (volume=50 < 60)
    assert!(!out.is_empty());
}

/// Filter test84: float less than long literal
/// Tests: price < 60L (float field vs long literal)
/// Reference: FilterTestCase2.java testFilterQuery84
#[tokio::test]
async fn filter_test84_float_lt_long_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, quantity FROM cseEventStream WHERE price < 60;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(500.0),
            AttributeValue::Double(50.0),
            AttributeValue::Int(6),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(60.0),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Double(300.0),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Only third event passes (price=50 < 60)
    assert!(!out.is_empty());
}

/// Filter test85: int less than long literal
/// Tests: quantity < 4L (int field vs long literal)
/// Reference: FilterTestCase2.java testFilterQuery85
#[tokio::test]
async fn filter_test85_int_lt_long_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price, quantity FROM cseEventStream WHERE quantity < 4;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(500.0),
            AttributeValue::Double(50.0),
            AttributeValue::Int(6),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(60.0),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Double(300.0),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Only second event passes (quantity=2 < 4)
    assert!(!out.is_empty());
}

/// Filter test87: float less than double literal
/// Tests: price < 50d (float field vs double literal)
/// Reference: FilterTestCase2.java testFilterQuery87
#[tokio::test]
async fn filter_test87_float_lt_double_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price < 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Double(60.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(40.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(44.0),
            AttributeValue::Double(200.0),
        ],
    );
    let out = runner.shutdown();
    // Only third event passes (price=44 < 50)
    assert!(!out.is_empty());
}

/// Filter test88: float less than float literal
/// Tests: price < 55f
/// Reference: FilterTestCase2.java testFilterQuery88
#[tokio::test]
async fn filter_test88_float_lt_float_literal() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume DOUBLE);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM cseEventStream WHERE price < 55.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Double(60.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Double(40.0),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(44.0),
            AttributeValue::Double(200.0),
        ],
    );
    let out = runner.shutdown();
    // First and third events pass (price=50 < 55, price=44 < 55)
    assert!(out.len() >= 2);
}

// ============================================================================
// IS NULL / IS NOT NULL TESTS
// Reference: FilterTestCase2.java
// ============================================================================

/// Filter test - IS NULL operator
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_is_null() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM inputStream WHERE price IS NULL;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Null,
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // Only MSFT has null price
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

/// Filter test - IS NOT NULL operator
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_is_not_null() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM inputStream WHERE price IS NOT NULL;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Null,
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // IBM and GOOG have non-null price
    assert_eq!(out.len(), 2);
}

/// Filter test - Combined IS NULL with AND
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_is_null_combined() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT, active BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM inputStream WHERE price IS NULL AND active = true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Null,
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Null,
            AttributeValue::Bool(false),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Bool(true),
        ],
    );
    let out = runner.shutdown();
    // Only IBM has null price AND active=true
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

// ============================================================================
// IN OPERATOR TESTS
// Reference: FilterTestCase2.java
// ============================================================================

/// Filter test - IN operator with string list
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_in_string_list() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM inputStream WHERE symbol IN ('IBM', 'GOOG', 'MSFT');\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // All three (IBM, MSFT, GOOG) are in the list
    assert_eq!(out.len(), 3);
}

/// Filter test - NOT IN operator
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_not_in() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM inputStream WHERE symbol NOT IN ('IBM', 'GOOG');\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // MSFT and MSFT are not in the list
    assert_eq!(out.len(), 2);
}

/// Filter test - IN operator with integer list
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_in_int_list() {
    let app = "\
        CREATE STREAM inputStream (id INT, value FLOAT);\n\
        CREATE STREAM outputStream (id INT, value FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM inputStream WHERE id IN (1, 3, 5, 7);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(200.0)],
    );
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(300.0)],
    );
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(4), AttributeValue::Float(400.0)],
    );
    let out = runner.shutdown();
    // ids 1 and 3 are in the list
    assert_eq!(out.len(), 2);
}

// ============================================================================
// BETWEEN OPERATOR TESTS
// Reference: FilterTestCase2.java
// ============================================================================

/// Filter test - BETWEEN operator
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_between() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM inputStream WHERE price BETWEEN 50.0 AND 150.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(40.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(160.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("D".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // B (100) and D (50) are in range [50, 150]
    assert_eq!(out.len(), 2);
}

/// Filter test - NOT BETWEEN operator
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_not_between() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price FROM inputStream WHERE price NOT BETWEEN 50.0 AND 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(40.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(75.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // A (40) and C (150) are outside range [50, 100]
    assert_eq!(out.len(), 2);
}

// ============================================================================
// LIKE OPERATOR TESTS
// Reference: FilterTestCase2.java
// ============================================================================

/// Filter test - LIKE operator for string patterns
/// Reference: FilterTestCase2.java
#[tokio::test]
async fn filter_test_like_prefix() {
    let app = "\
        CREATE STREAM inputStream (name STRING, value INT);\n\
        CREATE STREAM outputStream (name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT name FROM inputStream WHERE name LIKE 'test%';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("test_one".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("other".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("testing".to_string()),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    // Only names starting with 'test' should match
    assert_eq!(out.len(), 2);
}

/// Filter test - LIKE operator with suffix pattern
#[tokio::test]
async fn filter_test_like_suffix() {
    let app = "\
        CREATE STREAM inputStream (name STRING, value INT);\n\
        CREATE STREAM outputStream (name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT name FROM inputStream WHERE name LIKE '%_end';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("start_end".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("middle".to_string()),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

// ============================================================================
// ARITHMETIC IN WHERE CLAUSE TESTS
// ============================================================================

/// Filter with arithmetic expression in WHERE clause
#[tokio::test]
async fn filter_test_arithmetic_in_where() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, discount FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE price - discount > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Float(20.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Float(20.0),
        ],
    );
    let out = runner.shutdown();
    // IBM: 100 - 20 = 80 > 50 (passes)
    // MSFT: 60 - 20 = 40 > 50 (fails)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

/// Filter with multiplication in WHERE clause
#[tokio::test]
async fn filter_test_multiply_in_where() {
    let app = "\
        CREATE STREAM orderStream (item STRING, quantity INT, unitPrice FLOAT);\n\
        CREATE STREAM outputStream (item STRING);\n\
        INSERT INTO outputStream\n\
        SELECT item FROM orderStream WHERE quantity * unitPrice > 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::Int(2),
            AttributeValue::Float(500.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("Mouse".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Float(10.0),
        ],
    );
    let out = runner.shutdown();
    // Laptop: 2 * 500 = 1000 > 100 (passes)
    // Mouse: 5 * 10 = 50 > 100 (fails)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Laptop".to_string()));
}

/// Filter with division in WHERE clause
#[tokio::test]
async fn filter_test_division_in_where() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE price / quantity < 10.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    // A: 100 / 20 = 5 < 10 (passes)
    // B: 100 / 5 = 20 < 10 (fails)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

// ============================================================================
// NESTED LOGICAL EXPRESSION TESTS
// ============================================================================

/// Filter with deeply nested AND/OR expressions
#[tokio::test]
async fn filter_test_deeply_nested_logic() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream \n\
        WHERE (price > 50.0 AND volume > 100) OR (price < 30.0 AND volume > 500);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(25.0),
            AttributeValue::Int(600),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(40.0),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // A: 60 > 50 AND 150 > 100 (passes first condition)
    // B: 25 < 30 AND 600 > 500 (passes second condition)
    // C: neither condition passes
    assert_eq!(out.len(), 2);
}

/// Filter with triple AND condition
#[tokio::test]
async fn filter_test_triple_and() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT, active BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream \n\
        WHERE price > 50.0 AND volume > 100 AND active = true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(150),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(60.0),
            AttributeValue::Int(150),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    // Only A passes all three conditions
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Filter with triple OR condition
#[tokio::test]
async fn filter_test_triple_or() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream \n\
        WHERE symbol = 'IBM' OR symbol = 'MSFT' OR symbol = 'GOOG';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // IBM and GOOG match, AAPL doesn't
    assert_eq!(out.len(), 2);
}

// ============================================================================
// FILTER WITH FUNCTION CALLS
// ============================================================================

/// Filter with function call in WHERE clause
#[tokio::test]
async fn filter_test_with_function_call() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE length(symbol) > 3;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MICROSOFT".to_string()),
            AttributeValue::Float(300.0),
        ],
    );
    let out = runner.shutdown();
    // IBM: length 3, not > 3 (fails)
    // GOOG: length 4 > 3 (passes)
    // MICROSOFT: length 9 > 3 (passes)
    assert_eq!(out.len(), 2);
}

/// Filter with coalesce function
#[tokio::test]
async fn filter_test_with_coalesce() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(symbol, 'UNKNOWN') AS symbol FROM stockStream WHERE price > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(30.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

// ============================================================================
// FILTER WITH CASE WHEN IN SELECT
// ============================================================================

/// Filter combined with CASE WHEN in SELECT
#[tokio::test]
async fn filter_test_with_case_when() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, category STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, \n\
               CASE WHEN price > 100.0 THEN 'expensive' ELSE 'cheap' END AS category\n\
        FROM stockStream WHERE price > 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("expensive".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("cheap".to_string()));
}

// ============================================================================
// NULL COMPARISON EDGE CASES
// ============================================================================

/// Filter comparing with NULL using coalesce workaround
/// Note: coalesce requires same types, use DOUBLE for consistency
#[tokio::test]
async fn filter_test_null_coalesce_workaround() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE coalesce(price, 0.0) > 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(0.0),
        ],
    );
    let out = runner.shutdown();
    // Only IBM has price > 0
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

// ============================================================================
// COMPARISON OPERATOR EDGE CASES
// ============================================================================

/// Greater than or equal (>=) operator
#[tokio::test]
async fn filter_test_gte_operator() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE price >= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(99.99),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(100.01),
        ],
    );
    let out = runner.shutdown();
    // B and C pass (>= 100)
    assert_eq!(out.len(), 2);
}

/// Less than or equal (<=) operator
#[tokio::test]
async fn filter_test_lte_operator() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE price <= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(99.99),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(100.01),
        ],
    );
    let out = runner.shutdown();
    // A and B pass (<= 100)
    assert_eq!(out.len(), 2);
}

/// Not equal (!=) operator
#[tokio::test]
async fn filter_test_neq_operator() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE symbol != 'IBM';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // GOOG and MSFT pass
    assert_eq!(out.len(), 2);
}

/// String equality comparison
#[tokio::test]
async fn filter_test_string_equality() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, status STRING);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE status = 'active';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::String("inactive".to_string()),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("active".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Integer comparison
#[tokio::test]
async fn filter_test_int_comparison() {
    let app = "\
        CREATE STREAM orderStream (orderId INT, quantity INT);\n\
        CREATE STREAM outputStream (orderId INT);\n\
        INSERT INTO outputStream\n\
        SELECT orderId FROM orderStream WHERE quantity > 10;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(15)],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(10)],
    );
    let out = runner.shutdown();
    // Only order 2 has quantity > 10
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Long comparison
#[tokio::test]
async fn filter_test_long_comparison() {
    let app = "\
        CREATE STREAM eventStream (id BIGINT, timestamp BIGINT);\n\
        CREATE STREAM outputStream (id BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE timestamp > 1000000000;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(1), AttributeValue::Long(999999999)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(2), AttributeValue::Long(1000000001)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(2));
}

/// Double comparison with precision
#[tokio::test]
async fn filter_test_double_precision() {
    let app = "\
        CREATE STREAM sensorStream (id INT, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM sensorStream WHERE value > 0.001;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.0001)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(0.01)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(3), AttributeValue::Double(0.001)],
    );
    let out = runner.shutdown();
    // Only sensor 2 has value > 0.001
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

// ============================================================================
// BOOLEAN EXPRESSIONS
// ============================================================================

/// NOT operator
#[tokio::test]
async fn filter_test_not_operator() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, isActive BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE NOT isActive;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    // MSFT has isActive = false, so NOT isActive = true
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
}

/// Boolean field in WHERE
#[tokio::test]
async fn filter_test_boolean_field() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, isActive BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE isActive;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

/// Boolean with comparison
#[tokio::test]
async fn filter_test_boolean_equals() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, isActive BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE isActive = true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

// ============================================================================
// COMPLEX CONDITION COMBINATIONS
// ============================================================================

/// Multiple conditions on same field
#[tokio::test]
async fn filter_test_range_condition() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream WHERE price > 50.0 AND price < 150.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(30.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // Only B is in range (50, 150)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("B".to_string()));
}

/// Multiple fields in condition
#[tokio::test]
async fn filter_test_multi_field_condition() {
    let app = "\
        CREATE STREAM orderStream (product STRING, quantity INT, price FLOAT);\n\
        CREATE STREAM outputStream (product STRING);\n\
        INSERT INTO outputStream\n\
        SELECT product FROM orderStream WHERE quantity > 5 AND price < 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(3),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // Only A passes both conditions
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Four conditions with AND
#[tokio::test]
async fn filter_test_four_and_conditions() {
    let app = "\
        CREATE STREAM eventStream (a INT, b INT, c INT, d INT);\n\
        CREATE STREAM outputStream (a INT);\n\
        INSERT INTO outputStream\n\
        SELECT a FROM eventStream WHERE a > 0 AND b > 0 AND c > 0 AND d > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(2),
            AttributeValue::Int(0),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    // Only first event passes all four conditions
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Four conditions with OR
#[tokio::test]
async fn filter_test_four_or_conditions() {
    let app = "\
        CREATE STREAM eventStream (a INT, b INT, c INT, d INT);\n\
        CREATE STREAM outputStream (a INT);\n\
        INSERT INTO outputStream\n\
        SELECT a FROM eventStream WHERE a > 10 OR b > 10 OR c > 10 OR d > 10;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(2),
            AttributeValue::Int(15),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    // Only second event has a field > 10
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

// ============================================================================
// NEGATIVE NUMBER COMPARISONS
// ============================================================================

/// Negative number comparison
#[tokio::test]
async fn filter_test_negative_numbers() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temperature FLOAT);\n\
        CREATE STREAM outputStream (location STRING);\n\
        INSERT INTO outputStream\n\
        SELECT location FROM tempStream WHERE temperature < 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Alaska".to_string()),
            AttributeValue::Float(-20.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Florida".to_string()),
            AttributeValue::Float(25.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Minnesota".to_string()),
            AttributeValue::Float(-5.0),
        ],
    );
    let out = runner.shutdown();
    // Alaska and Minnesota have negative temps
    assert_eq!(out.len(), 2);
}

/// Negative to negative comparison
/// Note: Unary minus in WHERE clause not yet supported
#[tokio::test]
async fn filter_test_negative_to_negative() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temperature FLOAT);\n\
        CREATE STREAM outputStream (location STRING);\n\
        INSERT INTO outputStream\n\
        SELECT location FROM tempStream WHERE temperature > -10.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Alaska".to_string()),
            AttributeValue::Float(-20.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Minnesota".to_string()),
            AttributeValue::Float(-5.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Florida".to_string()),
            AttributeValue::Float(25.0),
        ],
    );
    let out = runner.shutdown();
    // Minnesota (-5 > -10) and Florida (25 > -10)
    assert_eq!(out.len(), 2);
}

// ============================================================================
// ADDITIONAL FILTER EDGE CASE TESTS
// ============================================================================

/// Filter with NOT operator on status boolean
#[tokio::test]
async fn filter_test_not_status() {
    let app = "\
        CREATE STREAM statusStream (id INT, active BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM statusStream WHERE NOT active;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(3), AttributeValue::Bool(false)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with multiple OR conditions
#[tokio::test]
async fn filter_test_multiple_or_conditions() {
    let app = "\
        CREATE STREAM productStream (id INT, category STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM productStream WHERE category = 'A' OR category = 'B' OR category = 'C';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("D".to_string()),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("C".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with IN expression
#[tokio::test]
async fn filter_test_in_expression() {
    let app = "\
        CREATE STREAM orderStream (orderId INT, status STRING);\n\
        CREATE STREAM outputStream (orderId INT);\n\
        INSERT INTO outputStream\n\
        SELECT orderId FROM orderStream WHERE status IN ('pending', 'processing', 'shipped');\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("pending".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("completed".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("shipped".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with NOT IN expression
#[tokio::test]
async fn filter_test_not_in_expression() {
    let app = "\
        CREATE STREAM orderStream (orderId INT, status STRING);\n\
        CREATE STREAM outputStream (orderId INT);\n\
        INSERT INTO outputStream\n\
        SELECT orderId FROM orderStream WHERE status NOT IN ('cancelled', 'refunded');\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("pending".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("cancelled".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("completed".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with BETWEEN expression
#[tokio::test]
async fn filter_test_between_expression() {
    let app = "\
        CREATE STREAM scoreStream (studentId INT, score INT);\n\
        CREATE STREAM outputStream (studentId INT);\n\
        INSERT INTO outputStream\n\
        SELECT studentId FROM scoreStream WHERE score BETWEEN 60 AND 80;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(55)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(70)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(85)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(4), AttributeValue::Int(60)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(4));
}

/// Filter with modulo operator
#[tokio::test]
async fn filter_test_modulo_operator() {
    let app = "\
        CREATE STREAM numberStream (value INT);\n\
        CREATE STREAM outputStream (value INT);\n\
        INSERT INTO outputStream\n\
        SELECT value FROM numberStream WHERE value % 2 = 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("numberStream", vec![AttributeValue::Int(1)]);
    runner.send("numberStream", vec![AttributeValue::Int(2)]);
    runner.send("numberStream", vec![AttributeValue::Int(3)]);
    runner.send("numberStream", vec![AttributeValue::Int(4)]);
    runner.send("numberStream", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(4));
}

/// Filter with empty string comparison
#[tokio::test]
async fn filter_test_empty_string() {
    let app = "\
        CREATE STREAM userStream (userId INT, name STRING);\n\
        CREATE STREAM outputStream (userId INT);\n\
        INSERT INTO outputStream\n\
        SELECT userId FROM userStream WHERE name != '';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("Bob".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with zero comparison
#[tokio::test]
async fn filter_test_zero_comparison() {
    let app = "\
        CREATE STREAM balanceStream (accountId INT, balance FLOAT);\n\
        CREATE STREAM outputStream (accountId INT);\n\
        INSERT INTO outputStream\n\
        SELECT accountId FROM balanceStream WHERE balance != 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(0.0)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(-50.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with AND and OR combined (precedence test)
#[tokio::test]
async fn filter_test_and_or_precedence() {
    let app = "\
        CREATE STREAM productStream (id INT, price FLOAT, inStock BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM productStream WHERE price < 100.0 AND inStock = true OR price > 500.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // price < 100 AND inStock => id=1 matches
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(50.0),
            AttributeValue::Bool(true),
        ],
    );
    // price < 100 AND NOT inStock => no match
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Float(80.0),
            AttributeValue::Bool(false),
        ],
    );
    // price > 500 => id=3 matches
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Float(600.0),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with parentheses to override precedence
#[tokio::test]
async fn filter_test_parentheses_precedence() {
    let app = "\
        CREATE STREAM productStream (id INT, price FLOAT, inStock BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM productStream WHERE (price < 100.0 OR price > 500.0) AND inStock = true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // price < 100 AND inStock => id=1 matches
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(50.0),
            AttributeValue::Bool(true),
        ],
    );
    // price > 500 BUT NOT inStock => no match
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Float(600.0),
            AttributeValue::Bool(false),
        ],
    );
    // price > 500 AND inStock => id=3 matches
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Float(700.0),
            AttributeValue::Bool(true),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with LIKE pattern - starts with
#[tokio::test]
async fn filter_test_like_starts_with() {
    let app = "\
        CREATE STREAM userStream (userId INT, email STRING);\n\
        CREATE STREAM outputStream (userId INT);\n\
        INSERT INTO outputStream\n\
        SELECT userId FROM userStream WHERE email LIKE 'admin%';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("admin@example.com".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("user@example.com".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("administrator@test.com".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with LIKE pattern - ends with
#[tokio::test]
async fn filter_test_like_ends_with() {
    let app = "\
        CREATE STREAM userStream (userId INT, email STRING);\n\
        CREATE STREAM outputStream (userId INT);\n\
        INSERT INTO outputStream\n\
        SELECT userId FROM userStream WHERE email LIKE '%@example.com';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("admin@example.com".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("user@other.com".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Filter with IS NULL for multiple matches
#[tokio::test]
async fn filter_test_null_multiple() {
    let app = "\
        CREATE STREAM dataStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM dataStream WHERE value IS NULL;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Null],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(3), AttributeValue::Null],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with IS NOT NULL for multiple matches
#[tokio::test]
async fn filter_test_not_null_multiple() {
    let app = "\
        CREATE STREAM dataStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM dataStream WHERE value IS NOT NULL;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Null],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with chained comparisons (compound range check)
#[tokio::test]
async fn filter_test_chained_range() {
    let app = "\
        CREATE STREAM sensorStream (id INT, temp FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM sensorStream WHERE temp >= 20.0 AND temp <= 30.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(15.0)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(25.0)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(35.0)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(4), AttributeValue::Float(20.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(4));
}

/// Filter with type coercion (INT vs FLOAT comparison)
#[tokio::test]
async fn filter_test_type_coercion() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM priceStream WHERE price > 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(99.99)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.01)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Filter with case-sensitive string comparison
#[tokio::test]
async fn filter_test_case_sensitive() {
    let app = "\
        CREATE STREAM userStream (id INT, name STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM userStream WHERE name = 'Alice';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("ALICE".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with Boolean field directly in WHERE
#[tokio::test]
async fn filter_test_boolean_direct() {
    let app = "\
        CREATE STREAM flagStream (id INT, isActive BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM flagStream WHERE isActive;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "flagStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "flagStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    runner.send(
        "flagStream",
        vec![AttributeValue::Int(3), AttributeValue::Bool(true)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

// ============================================================================
// ADDITIONAL EDGE CASE TESTS
// ============================================================================

/// Filter with string length comparison in WHERE
#[tokio::test]
async fn filter_test_string_length_where() {
    let app = "\
        CREATE STREAM productStream (id INT, name STRING);\n\
        CREATE STREAM outputStream (id INT, name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, name FROM productStream WHERE length(name) >= 5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Apple".to_string()),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Pear".to_string()),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("Banana".to_string()),
        ],
    );
    let out = runner.shutdown();
    // Apple (5) and Banana (6) have length >= 5
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Filter with case-insensitive comparison using upper()
#[tokio::test]
async fn filter_test_case_insensitive_upper() {
    let app = "\
        CREATE STREAM userStream (id INT, status STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM userStream WHERE upper(status) = 'ACTIVE';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("inactive".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(4),
            AttributeValue::String("Active".to_string()),
        ],
    );
    let out = runner.shutdown();
    // All "active" variations (1, 2, 4) should match
    assert_eq!(out.len(), 3);
}

/// Filter with complex arithmetic expression
#[tokio::test]
async fn filter_test_complex_arithmetic_where() {
    let app = "\
        CREATE STREAM orderStream (id INT, price FLOAT, quantity INT, discount FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM orderStream WHERE (price * quantity) - discount > 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(50.0),
            AttributeValue::Int(3),
            AttributeValue::Float(20.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Float(20.0),
            AttributeValue::Int(4),
            AttributeValue::Float(10.0),
        ],
    );
    let out = runner.shutdown();
    // Order 1: (50*3) - 20 = 130 > 100 (passes)
    // Order 2: (20*4) - 10 = 70 > 100 (fails)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with five AND conditions
#[tokio::test]
async fn filter_test_five_and_conditions() {
    let app = "\
        CREATE STREAM eventStream (a INT, b INT, c INT, d INT, e INT);\n\
        CREATE STREAM outputStream (a INT);\n\
        INSERT INTO outputStream\n\
        SELECT a FROM eventStream WHERE a > 0 AND b > 0 AND c > 0 AND d > 0 AND e > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(2),
            AttributeValue::Int(2),
            AttributeValue::Int(0),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    // Only first event passes all five conditions
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with five OR conditions
#[tokio::test]
async fn filter_test_five_or_conditions() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol FROM stockStream \n\
        WHERE symbol = 'IBM' OR symbol = 'MSFT' OR symbol = 'GOOG' OR symbol = 'MSFT' OR symbol = 'AAPL';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TSLA".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // IBM and AAPL match, TSLA doesn't
    assert_eq!(out.len(), 2);
}

/// Filter with double NOT operator
#[tokio::test]
async fn filter_test_double_not() {
    let app = "\
        CREATE STREAM statusStream (id INT, active BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM statusStream WHERE NOT (NOT active);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    let out = runner.shutdown();
    // NOT (NOT true) = true, NOT (NOT false) = false
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with float precision boundary
#[tokio::test]
async fn filter_test_float_precision_boundary() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM priceStream WHERE price > 99.999;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(99.998)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(99.9999)],
    );
    let out = runner.shutdown();
    // id 2 and 3 are > 99.999
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Filter with large long values
#[tokio::test]
async fn filter_test_large_long_values() {
    let app = "\
        CREATE STREAM eventStream (id BIGINT, timestamp BIGINT);\n\
        CREATE STREAM outputStream (id BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE timestamp > 1609459200000;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(1), AttributeValue::Long(1609459200001)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(2), AttributeValue::Long(1609459199999)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(1));
}

/// Filter with double precision comparison
#[tokio::test]
async fn filter_test_double_precision_comparison() {
    let app = "\
        CREATE STREAM sensorStream (id INT, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM sensorStream WHERE value > 0.00001;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.000001)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(0.0001)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(3), AttributeValue::Double(0.00001)],
    );
    let out = runner.shutdown();
    // Only id 2 has value > 0.00001
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Filter combining arithmetic and logical operators
#[tokio::test]
async fn filter_test_arithmetic_and_logical() {
    let app = "\
        CREATE STREAM orderStream (id INT, price FLOAT, quantity INT, premium BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM orderStream WHERE price * quantity > 50.0 AND premium = true;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(20.0),
            AttributeValue::Int(5),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Float(20.0),
            AttributeValue::Int(5),
            AttributeValue::Bool(false),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Float(5.0),
            AttributeValue::Int(5),
            AttributeValue::Bool(true),
        ],
    );
    let out = runner.shutdown();
    // Order 1: 20*5=100 > 50 AND premium=true (passes)
    // Order 2: 20*5=100 > 50 AND premium=false (fails)
    // Order 3: 5*5=25 > 50 AND premium=true (fails)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with concat function in WHERE clause
#[tokio::test]
async fn filter_test_concat_in_where() {
    let app = "\
        CREATE STREAM userStream (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM userStream WHERE concat(firstName, ' ', lastName) = 'John Doe';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Jane".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

// ============================================================================
// MORE OPERATOR EDGE CASE TESTS
// ============================================================================

/// Modulo operator
#[tokio::test]
async fn filter_test_modulo() {
    let app = "\
        CREATE STREAM numberStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM numberStream WHERE value % 2 = 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "numberStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(4)],
    );
    runner.send(
        "numberStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(5)],
    );
    runner.send(
        "numberStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(6)],
    );
    let out = runner.shutdown();
    // id 1 (value=4) and id 3 (value=6) are even
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Less than or equal comparison
#[tokio::test]
async fn filter_test_less_than_equal() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM priceStream WHERE price <= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(99.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(101.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(2));
}

/// Greater than or equal comparison
#[tokio::test]
async fn filter_test_greater_than_equal() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM priceStream WHERE price >= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(99.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(101.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Not equal comparison for integers
#[tokio::test]
async fn filter_test_not_equal_int() {
    let app = "\
        CREATE STREAM eventStream (id INT, status INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE status != 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(1)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(-1)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[1][0], AttributeValue::Int(3));
}

/// Negative number less than zero
#[tokio::test]
async fn filter_test_negative_less_than_zero() {
    let app = "\
        CREATE STREAM tempStream (id INT, temp INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM tempStream WHERE temp < 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(-5)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Parentheses grouping in WHERE
#[tokio::test]
async fn filter_test_parentheses_grouping() {
    let app = "\
        CREATE STREAM eventStream (a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (a INT);\n\
        INSERT INTO outputStream\n\
        SELECT a FROM eventStream WHERE (a > 0 OR b > 0) AND c > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(0),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(0),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    // First two pass: (1>0 OR 0>0) AND 1>0, (0>0 OR 1>0) AND 1>0
    // Third fails: (1>0 OR 1>0) AND 0>0
    assert_eq!(out.len(), 2);
}

/// String equality with special characters
#[tokio::test]
async fn filter_test_string_special_chars() {
    let app = "\
        CREATE STREAM eventStream (id INT, data STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE data = 'hello-world';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello-world".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("hello_world".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Float vs Int comparison (type coercion)
#[tokio::test]
async fn filter_test_float_int_comparison() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM priceStream WHERE price = 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Zero comparison
#[tokio::test]
async fn filter_test_zero_exact() {
    let app = "\
        CREATE STREAM numberStream (id INT, value FLOAT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM numberStream WHERE value = 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "numberStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(0.0)],
    );
    runner.send(
        "numberStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(0.001)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Boolean false comparison
#[tokio::test]
async fn filter_test_boolean_false() {
    let app = "\
        CREATE STREAM statusStream (id INT, active BOOLEAN);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM statusStream WHERE active = false;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "statusStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Filter with subtraction in WHERE
#[tokio::test]
async fn filter_test_subtraction_where() {
    let app = "\
        CREATE STREAM orderStream (id INT, total INT, discount INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM orderStream WHERE total - discount > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(60),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    // Order 1: 100-30=70 > 50 (pass), Order 2: 60-20=40 (fail)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with multiplication in WHERE
#[tokio::test]
async fn filter_test_multiplication_where() {
    let app = "\
        CREATE STREAM productStream (id INT, quantity INT, price INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM productStream WHERE quantity * price > 500;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(10),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(5),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // Product 1: 10*100=1000 > 500 (pass), Product 2: 5*50=250 (fail)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with division in WHERE
#[tokio::test]
async fn filter_test_division_where() {
    let app = "\
        CREATE STREAM dataStream (id INT, total INT, count INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM dataStream WHERE total / count > 10;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(30),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    // Data 1: 100/5=20 > 10 (pass), Data 2: 30/5=6 (fail)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with string not equal
#[tokio::test]
async fn filter_test_string_not_equal() {
    let app = "\
        CREATE STREAM eventStream (id INT, category STRING);\n\
        CREATE STREAM outputStream (id INT, category STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, category FROM eventStream WHERE category != 'internal';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("external".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("internal".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with bigint type comparison (large numbers)
#[tokio::test]
async fn filter_test_bigint_comparison() {
    let app = "\
        CREATE STREAM eventStream (id INT, timestamp BIGINT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE timestamp > 1000000000000;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(1), AttributeValue::Long(1234567890123)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(2), AttributeValue::Long(999999999999)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with double type comparison
#[tokio::test]
async fn filter_test_double_comparison() {
    let app = "\
        CREATE STREAM sensorStream (id INT, reading DOUBLE);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM sensorStream WHERE reading > 0.5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.75)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(0.25)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with IN operator (multiple values)
#[tokio::test]
async fn filter_test_in_operator() {
    let app = "\
        CREATE STREAM logStream (id INT, level STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM logStream WHERE level IN ('ERROR', 'WARN');\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ERROR".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("INFO".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with BETWEEN operator (range check)
#[tokio::test]
async fn filter_test_between_range() {
    let app = "\
        CREATE STREAM dataStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM dataStream WHERE value BETWEEN 10 AND 20;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(15)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(25)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with multiple columns same comparison
#[tokio::test]
async fn filter_test_multi_column_compare() {
    let app = "\
        CREATE STREAM orderStream (id INT, quantity INT, minRequired INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM orderStream WHERE quantity >= minRequired;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(30),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // Order 1: 100 >= 50 (pass), Order 2: 30 >= 50 (fail)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with negative value check
#[tokio::test]
async fn filter_test_negative_value() {
    let app = "\
        CREATE STREAM balanceStream (id INT, balance INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, balance FROM balanceStream WHERE balance < 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(-50)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
    assert_eq!(out[0][1], AttributeValue::Int(-50));
}

/// Filter with modulo divisibility check
#[tokio::test]
async fn filter_test_modulo_divisibility() {
    let app = "\
        CREATE STREAM numStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, remainder INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, value % 3 AS remainder FROM numStream WHERE value % 3 = 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "numStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(9)],
    );
    runner.send(
        "numStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(10)],
    );
    runner.send(
        "numStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(12)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with subtraction for discount
#[tokio::test]
async fn filter_test_subtraction_discount() {
    let app = "\
        CREATE STREAM orderStream (id INT, price INT, discount INT);\n\
        CREATE STREAM outputStream (id INT, final_price INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, price - discount AS final_price FROM orderStream WHERE price - discount > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(60),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Int(80));
}

/// Filter with triple AND for product availability
#[tokio::test]
async fn filter_test_triple_and_product() {
    let app = "\
        CREATE STREAM productStream (id INT, price INT, stock INT, active INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM productStream WHERE price > 10 AND stock > 0 AND active = 1;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(50),
            AttributeValue::Int(100),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(50),
            AttributeValue::Int(0),
            AttributeValue::Int(1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with triple OR for category matching
#[tokio::test]
async fn filter_test_triple_or_category() {
    let app = "\
        CREATE STREAM eventStream (id INT, category STRING);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM eventStream WHERE category = 'A' OR category = 'B' OR category = 'C';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("D".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("C".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with nested parentheses
#[tokio::test]
async fn filter_test_nested_parentheses() {
    let app = "\
        CREATE STREAM dataStream (id INT, a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        INSERT INTO outputStream\n\
        SELECT id FROM dataStream WHERE (a > 10 AND b > 20) OR (c > 30);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(15),
            AttributeValue::Int(25),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(5),
            AttributeValue::Int(5),
            AttributeValue::Int(35),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(5),
            AttributeValue::Int(5),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with float greater than or equal
#[tokio::test]
async fn filter_test_float_gte() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE STREAM outputStream (id INT, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT id, price FROM priceStream WHERE price >= 100.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(50.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with double precision pi comparison
#[tokio::test]
async fn filter_test_double_pi() {
    let app = "\
        CREATE STREAM measureStream (id INT, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT, value DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM measureStream WHERE value > 1.23456;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(1.23457)],
    );
    runner.send(
        "measureStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(1.23455)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with string not equal to DEBUG
#[tokio::test]
async fn filter_test_string_ne_level() {
    let app = "\
        CREATE STREAM logStream (id INT, level STRING);\n\
        CREATE STREAM outputStream (id INT, level STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, level FROM logStream WHERE level != 'DEBUG';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ERROR".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("DEBUG".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("INFO".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with arithmetic in both SELECT and WHERE
#[tokio::test]
async fn filter_test_arithmetic_select_where() {
    let app = "\
        CREATE STREAM salesStream (id INT, units INT, unit_price INT);\n\
        CREATE STREAM outputStream (id INT, revenue INT, profit INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, units * unit_price AS revenue, units * unit_price - 100 AS profit\n\
        FROM salesStream WHERE units * unit_price > 500;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(10),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    // Arithmetic operations may return Long
    let revenue = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    let profit = match &out[0][2] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(revenue, 1000);
    assert_eq!(profit, 900);
}

/// Filter with uuid function in select
#[tokio::test]
async fn filter_test_uuid_select() {
    let app = "\
        CREATE STREAM eventStream (name STRING, priority INT);\n\
        CREATE STREAM outputStream (eventId STRING, name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT uuid() AS eventId, name FROM eventStream WHERE priority > 5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("HighPriority".to_string()),
            AttributeValue::Int(8),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("LowPriority".to_string()),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][0] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
    assert_eq!(
        out[0][1],
        AttributeValue::String("HighPriority".to_string())
    );
}

/// Filter with division total/count in WHERE
#[tokio::test]
async fn filter_test_division_rate() {
    let app = "\
        CREATE STREAM rateStream (id INT, total INT, count INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, total FROM rateStream WHERE total / count > 10;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "rateStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(5),
        ],
    ); // 100/5 = 20 > 10
    runner.send(
        "rateStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(50),
            AttributeValue::Int(10),
        ],
    ); // 50/10 = 5 < 10
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with subtraction income-expense in WHERE
#[tokio::test]
async fn filter_test_subtraction_income_expense() {
    let app = "\
        CREATE STREAM accountStream (id INT, income INT, expense INT);\n\
        CREATE STREAM outputStream (id INT, income INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, income FROM accountStream WHERE income - expense > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "accountStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1000),
            AttributeValue::Int(500),
        ],
    ); // 1000-500 = 500 > 0
    runner.send(
        "accountStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(500),
            AttributeValue::Int(800),
        ],
    ); // 500-800 = -300 < 0
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with case when in WHERE clause result
#[tokio::test]
async fn filter_test_case_when_where() {
    let app = "\
        CREATE STREAM scoreStream (id INT, score INT);\n\
        CREATE STREAM outputStream (id INT, grade STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade\n\
        FROM scoreStream WHERE score >= 70;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(95)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(75)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with DOUBLE temperature comparison
#[tokio::test]
async fn filter_test_double_temperature() {
    let app = "\
        CREATE STREAM sensorStream (id INT, temperature DOUBLE);\n\
        CREATE STREAM outputStream (id INT, temperature DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT id, temperature FROM sensorStream WHERE temperature > 25.5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(30.2)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(20.1)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with DOUBLE arithmetic price+tax
#[tokio::test]
async fn filter_test_double_price_tax() {
    let app = "\
        CREATE STREAM priceStream (id INT, price DOUBLE, tax DOUBLE);\n\
        CREATE STREAM outputStream (id INT, totalPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT id, price + tax AS totalPrice FROM priceStream WHERE price > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Double(100.0),
            AttributeValue::Double(10.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((total - 110.0).abs() < 0.001);
}

/// Filter with empty string content
#[tokio::test]
async fn filter_test_empty_string_content() {
    let app = "\
        CREATE STREAM messageStream (id INT, content STRING);\n\
        CREATE STREAM outputStream (id INT, content STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, content FROM messageStream WHERE content != '';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "messageStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello".to_string()),
        ],
    );
    runner.send(
        "messageStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with negative balance comparison
#[tokio::test]
async fn filter_test_negative_balance() {
    let app = "\
        CREATE STREAM balanceStream (id INT, balance INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, balance FROM balanceStream WHERE balance < 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(-50)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Int(-50));
}

/// Filter with zero count comparison
#[tokio::test]
async fn filter_test_zero_count() {
    let app = "\
        CREATE STREAM counterStream (id INT, count INT);\n\
        CREATE STREAM outputStream (id INT, count INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, count FROM counterStream WHERE count > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "counterStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "counterStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    runner.send(
        "counterStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(-1)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with coalesce primary select
#[tokio::test]
async fn filter_test_coalesce_primary() {
    let app = "\
        CREATE STREAM dataStream (id INT, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, coalesce(primary_val, backup_val) AS result FROM dataStream WHERE id > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("primary".to_string()),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("primary".to_string()));
}

/// Filter with DOUBLE comparison (price filter)
#[tokio::test]
async fn filter_test_double_price_filter() {
    let app = "\
        CREATE STREAM priceStream (item STRING, price DOUBLE);\n\
        CREATE STREAM outputStream (item STRING, price DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT item, price FROM priceStream WHERE price > 9.99;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(15.50),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Gadget".to_string()),
            AttributeValue::Double(5.00),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Widget".to_string()));
}

/// Filter with length function in WHERE
#[tokio::test]
async fn filter_test_length_where() {
    let app = "\
        CREATE STREAM textStream (id INT, content STRING);\n\
        CREATE STREAM outputStream (id INT, content STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, content FROM textStream WHERE length(content) > 5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hi".to_string()),
        ],
    );
    runner.send(
        "textStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("hello world".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Filter with concat in select
#[tokio::test]
async fn filter_test_concat_in_select() {
    let app = "\
        CREATE STREAM personStream (firstName STRING, lastName STRING, age INT);\n\
        CREATE STREAM outputStream (fullName STRING, age INT);\n\
        INSERT INTO outputStream\n\
        SELECT concat(firstName, ' ', lastName) AS fullName, age FROM personStream WHERE age >= 18;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
            AttributeValue::Int(25),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("John Doe".to_string()));
}

/// Filter with upper in select
#[tokio::test]
async fn filter_test_upper_in_select() {
    let app = "\
        CREATE STREAM msgStream (id INT, message STRING);\n\
        CREATE STREAM outputStream (id INT, upper_msg STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(message) AS upper_msg FROM msgStream WHERE id > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("HELLO".to_string()));
}

/// Filter with lower in select
#[tokio::test]
async fn filter_test_lower_in_select() {
    let app = "\
        CREATE STREAM msgStream (id INT, message STRING);\n\
        CREATE STREAM outputStream (id INT, lower_msg STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, lower(message) AS lower_msg FROM msgStream WHERE id > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("WORLD".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("world".to_string()));
}

/// Filter with multiplication in select
#[tokio::test]
async fn filter_test_multiplication_in_select() {
    let app = "\
        CREATE STREAM orderStream (id INT, quantity INT, price INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, quantity * price AS total FROM orderStream WHERE quantity > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(5),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100)); // 5 * 20 = 100
}

/// Filter with division in select
#[tokio::test]
async fn filter_test_division_in_select() {
    let app = "\
        CREATE STREAM salesStream (id INT, revenue INT, units INT);\n\
        CREATE STREAM outputStream (id INT, avg_price DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT id, revenue / units AS avg_price FROM salesStream WHERE units > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Long(v) => *v as f64,
        _ => panic!("Expected numeric"),
    };
    assert!((avg - 25.0).abs() < 0.001); // 100 / 4 = 25
}

/// Filter with NOT condition
#[tokio::test]
async fn filter_test_not_condition() {
    let app = "\
        CREATE STREAM flagStream (id INT, active BOOL);\n\
        CREATE STREAM outputStream (id INT, active BOOL);\n\
        INSERT INTO outputStream\n\
        SELECT id, active FROM flagStream WHERE NOT active;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "flagStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(false)],
    );
    runner.send(
        "flagStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(true)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with uuid in select
#[tokio::test]
async fn filter_test_uuid_in_select() {
    let app = "\
        CREATE STREAM triggerStream (id INT);\n\
        CREATE STREAM outputStream (id INT, uuid_val STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, uuid() AS uuid_val FROM triggerStream WHERE id > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("triggerStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Filter with LTE condition
#[tokio::test]
async fn filter_test_lte_condition() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, score INT);\n\
        INSERT INTO outputStream\n\
        SELECT player, score FROM scoreStream WHERE score <= 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Bob".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Alice".to_string()));
}

/// Filter with GTE condition
#[tokio::test]
async fn filter_test_gte_condition() {
    let app = "\
        CREATE STREAM valueStream (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM valueStream WHERE value >= 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(100)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(150)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Filter with temperature range condition
#[tokio::test]
async fn filter_test_temp_range_condition() {
    let app = "\
        CREATE STREAM tempStream (sensor STRING, temp INT);\n\
        CREATE STREAM outputStream (sensor STRING, temp INT);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, temp FROM tempStream WHERE temp >= 20 AND temp <= 30;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(15),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S2".to_string()),
            AttributeValue::Int(25),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S3".to_string()),
            AttributeValue::Int(35),
        ],
    );
    let out = runner.shutdown();
    // Only S2 (25) in range
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("S2".to_string()));
}

/// Filter with OR condition
#[tokio::test]
async fn filter_test_or_condition() {
    let app = "\
        CREATE STREAM alertStream (type STRING, level INT);\n\
        CREATE STREAM outputStream (type STRING, level INT);\n\
        INSERT INTO outputStream\n\
        SELECT type, level FROM alertStream WHERE type = 'critical' OR level > 90;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("critical".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("warning".to_string()),
            AttributeValue::Int(95),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("info".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    // critical and warning(level>90)
    assert_eq!(out.len(), 2);
}

/// Filter with AND condition
#[tokio::test]
async fn filter_test_and_condition() {
    let app = "\
        CREATE STREAM productStream (category STRING, price INT, stock INT);\n\
        CREATE STREAM outputStream (category STRING, price INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, price FROM productStream WHERE price < 50 AND stock > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(30),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(30),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Int(60),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    // Only A passes (price<50 AND stock>0)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
}

/// Filter with negative temperature value
#[tokio::test]
async fn filter_test_negative_temp() {
    let app = "\
        CREATE STREAM tempStream (sensor STRING, temp INT);\n\
        CREATE STREAM outputStream (sensor STRING, temp INT);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, temp FROM tempStream WHERE temp < 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(-10),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S2".to_string()),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-10));
}

/// Filter with zero check
#[tokio::test]
async fn filter_test_zero_check() {
    let app = "\
        CREATE STREAM countStream (id INT, count_val INT);\n\
        CREATE STREAM outputStream (id INT, count_val INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, count_val FROM countStream WHERE count_val = 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "countStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(0)],
    );
    runner.send(
        "countStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with status string not equal
#[tokio::test]
async fn filter_test_status_not_equal() {
    let app = "\
        CREATE STREAM statusStream (id INT, status STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, status FROM statusStream WHERE status != 'deleted';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "statusStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("deleted".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with arithmetic in where
#[tokio::test]
async fn filter_test_arithmetic_where() {
    let app = "\
        CREATE STREAM orderStream (id INT, price INT, qty INT, threshold INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, price * qty AS total FROM orderStream WHERE price * qty > threshold;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
            AttributeValue::Int(40),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(10),
            AttributeValue::Int(3),
            AttributeValue::Int(40),
        ],
    );
    let out = runner.shutdown();
    // id=1: 10*5=50 > 40, id=2: 10*3=30 < 40
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Filter with upper function in select
#[tokio::test]
async fn filter_test_upper_function() {
    let app = "\
        CREATE STREAM nameStream (id INT, name STRING);\n\
        CREATE STREAM outputStream (id INT, upper_name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(name) AS upper_name FROM nameStream WHERE id > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("HELLO".to_string()));
}
