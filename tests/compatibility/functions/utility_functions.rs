// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Utility Function Compatibility Tests
// Reference: query/function/FunctionTestCase.java, CoalesceFunctionTestCase.java,
//            UUIDFunctionTestCase.java

use crate::compatibility::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// COALESCE FUNCTIONS
// ============================================================================

/// Test coalesce function
/// Reference: CoalesceFunctionTestCase.java:testCoalesceQuery1
#[tokio::test]
async fn function_test_coalesce() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(symbol, 'DEFAULT') AS result\n\
        FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Null, AttributeValue::Float(55.6)],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("DEFAULT".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("IBM".to_string()));
}

/// Coalesce with filter condition
/// Reference: FunctionTestCase.java testFunctionQuery3
#[tokio::test]
async fn function_test_coalesce_in_filter() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price1 FLOAT, price2 FLOAT, volume BIGINT, quantity INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, quantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, coalesce(price1, price2) AS price, quantity\n\
        FROM cseEventStream\n\
        WHERE coalesce(price1, price2) > 0.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Float(60.0),
            AttributeValue::Long(60),
            AttributeValue::Int(6),
        ],
    );
    runner.send(
        "cseEventStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(70.0),
            AttributeValue::Null,
            AttributeValue::Long(40),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    // Both events pass (coalesce returns non-null positive values)
    assert!(out.len() >= 2);
    assert_eq!(out[0][1], AttributeValue::Float(50.0));
    assert_eq!(out[1][1], AttributeValue::Float(70.0));
}

/// Coalesce with multiple arguments
#[tokio::test]
async fn function_test_coalesce_multiple() {
    let app = "\
        CREATE STREAM inputStream (a STRING, b STRING, c STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(a, b, c) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Null,
            AttributeValue::Null,
            AttributeValue::String("third".to_string()),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Null,
            AttributeValue::String("second".to_string()),
            AttributeValue::String("third".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("third".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("second".to_string()));
}

/// Coalesce with all non-null (returns first)
#[tokio::test]
async fn function_test_coalesce_all_non_null() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(20)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(10));
}

/// coalesce with NULL first
#[tokio::test]
async fn function_test_coalesce_null_first() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Null, AttributeValue::Int(10)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(10));
}

/// coalesce with non-NULL first
#[tokio::test]
async fn function_test_coalesce_non_null_first() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(5), AttributeValue::Int(10)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(5));
}

/// coalesce with three arguments
#[tokio::test]
async fn function_test_coalesce_three_args() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(a, b, c) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::Null,
            AttributeValue::Null,
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(30));
}

// ============================================================================
// UUID FUNCTIONS
// ============================================================================

/// Test UUID function
/// Reference: UUIDFunctionTestCase.java:testUUIDQuery1
#[tokio::test]
async fn function_test_uuid() {
    let app = "\
        CREATE STREAM inputStream (val INT);\n\
        CREATE STREAM outputStream (id STRING);\n\
        INSERT INTO outputStream\n\
        SELECT UUID() AS id\n\
        FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    runner.send("inputStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    // Each UUID should be different
    assert_ne!(out[0][0], out[1][0]);
}

/// Test UUID generates unique values across many events
#[tokio::test]
async fn function_test_uuid_uniqueness() {
    let app = "\
        CREATE STREAM inputStream (val INT);\n\
        CREATE STREAM outputStream (id STRING);\n\
        INSERT INTO outputStream\n\
        SELECT UUID() AS id\n\
        FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=10 {
        runner.send("inputStream", vec![AttributeValue::Int(i)]);
    }
    let out = runner.shutdown();
    assert_eq!(out.len(), 10);
    // Check all UUIDs are unique
    let mut seen = std::collections::HashSet::new();
    for row in &out {
        if let AttributeValue::String(uuid) = &row[0] {
            assert!(seen.insert(uuid.clone()), "Duplicate UUID found");
        }
    }
}

/// uuid uniqueness across multiple events
#[tokio::test]
async fn function_test_uuid_multi_event_uniqueness() {
    let app = "\
        CREATE STREAM inputStream (id INT);\n\
        CREATE STREAM outputStream (eventId STRING);\n\
        INSERT INTO outputStream\n\
        SELECT uuid() AS eventId FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    runner.send("inputStream", vec![AttributeValue::Int(2)]);
    runner.send("inputStream", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    // All UUIDs should be unique
    assert_ne!(out[0][0], out[1][0]);
    assert_ne!(out[1][0], out[2][0]);
    assert_ne!(out[0][0], out[2][0]);
}

// ============================================================================
// EVENT TIMESTAMP FUNCTIONS
// ============================================================================

/// Test current timestamp function
/// Reference: FunctionTestCase.java:testFunctionQuery7_1
#[tokio::test]
async fn function_test_current_timestamp() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, ts BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, eventTimestamp() AS ts\n\
        FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send_with_ts(
        "inputStream",
        1234567890,
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Long(1234567890));
}

// ============================================================================
// IFNULL / NULLIF FUNCTIONS
// ============================================================================

/// ifnull function - returns first non-null value
/// Reference: Function tests
#[tokio::test]
async fn function_test_ifnull() {
    let app = "\
        CREATE STREAM inputStream (value FLOAT, fallback FLOAT);\n\
        CREATE STREAM outputStream (result FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT ifnull(value, fallback) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Null, AttributeValue::Float(100.0)],
    );
    runner.send(
        "inputStream",
        vec![AttributeValue::Float(50.0), AttributeValue::Float(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Float(100.0));
    assert_eq!(out[1][0], AttributeValue::Float(50.0));
}

/// nullif function - returns NULL if values are equal
/// Reference: Function tests
#[tokio::test]
async fn function_test_nullif() {
    let app = "\
        CREATE STREAM inputStream (a INT, b INT);\n\
        CREATE STREAM outputStream (result INT);\n\
        INSERT INTO outputStream\n\
        SELECT nullif(a, b) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(5), AttributeValue::Int(5)],
    );
    runner.send(
        "inputStream",
        vec![AttributeValue::Int(10), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Null);
    assert_eq!(out[1][0], AttributeValue::Int(10));
}

// ============================================================================
// NOW() FUNCTION - Current timestamp in milliseconds
// ============================================================================

/// now() function (replaces currentTimeMillis)
#[tokio::test]
async fn function_test_current_time_millis() {
    let app = "\
        CREATE STREAM inputStream (id INT);\n\
        CREATE STREAM outputStream (ts LONG);\n\
        INSERT INTO outputStream\n\
        SELECT now() AS ts FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Should be a reasonable timestamp
    if let AttributeValue::Long(ts) = out[0][0] {
        assert!(ts > 1600000000000); // After 2020
    }
}

/// now() increases over time
#[tokio::test]
async fn function_test_current_time_increases() {
    let app = "\
        CREATE STREAM inputStream (id INT);\n\
        CREATE STREAM outputStream (ts BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT now() AS ts FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    std::thread::sleep(std::time::Duration::from_millis(10));
    runner.send("inputStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    if let (AttributeValue::Long(ts1), AttributeValue::Long(ts2)) = (&out[0][0], &out[1][0]) {
        assert!(ts2 >= ts1);
    }
}

/// Test now() function returns current timestamp
#[tokio::test]
async fn function_test_now_basic() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (ts BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT now() AS ts FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    let after_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert_eq!(out.len(), 1);
    if let AttributeValue::Long(ts) = out[0][0] {
        assert!(
            ts >= before_ms && ts <= after_ms,
            "Timestamp {} should be between {} and {}",
            ts,
            before_ms,
            after_ms
        );
    } else {
        panic!("Expected Long value for now()");
    }
}

/// Test now() returns different values for different events
#[tokio::test]
async fn function_test_now_multiple_events() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (ts BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT now() AS ts FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(1)]);
    // Small delay to ensure different timestamps
    std::thread::sleep(std::time::Duration::from_millis(2));
    runner.send("inputStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    // Both should be Long values (timestamps increase or stay same)
    if let (AttributeValue::Long(ts1), AttributeValue::Long(ts2)) = (&out[0][0], &out[1][0]) {
        assert!(*ts2 >= *ts1, "Second timestamp should be >= first");
    }
}

// ============================================================================
// DEFAULT FUNCTION
// ============================================================================

/// Test default function - returns value when not null
#[tokio::test]
async fn function_test_default_non_null() {
    let app = "\
        CREATE STREAM inputStream (value BIGINT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 0) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Long(42)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(42)); // Returns actual value
}

/// Test default function - returns default when null
#[tokio::test]
async fn function_test_default_null_value() {
    let app = "\
        CREATE STREAM inputStream (value BIGINT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 99) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(99)); // Returns default
}

/// Test default function with strings
#[tokio::test]
async fn function_test_default_string() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT default(name, 'Unknown') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Unknown".to_string()));
}

/// Test default with non-null string
#[tokio::test]
async fn function_test_default_string_non_null() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT default(name, 'Unknown') AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![AttributeValue::String("John".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("John".to_string()));
}

/// Test ifnull alias (same as default)
#[tokio::test]
async fn function_test_ifnull_alias() {
    let app = "\
        CREATE STREAM inputStream (value DOUBLE);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT ifnull(value, 0.0) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Double(0.0));
}

/// Test default in combination with other functions
#[tokio::test]
async fn function_test_default_with_arithmetic() {
    let app = "\
        CREATE STREAM inputStream (value BIGINT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 0) + 10 AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(10)); // default(null, 0) + 10 = 10
}

// ============================================================================
// DEFAULT FUNCTION - TYPE WIDENING TESTS
// ============================================================================

/// Test default with INT value and LONG default (type widening)
#[tokio::test]
async fn function_test_default_int_to_long() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 999999999999) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(999999999999));
}

/// Test default with INT value returns INT when not null (widened context)
#[tokio::test]
async fn function_test_default_int_value_in_long_context() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (result BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 0) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Int(42)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Should return the value (42), possibly widened to LONG context
    match &out[0][0] {
        AttributeValue::Int(v) => assert_eq!(*v, 42),
        AttributeValue::Long(v) => assert_eq!(*v, 42),
        _ => panic!("Expected Int or Long value"),
    }
}

/// Test default with FLOAT and DOUBLE widening
#[tokio::test]
async fn function_test_default_float_to_double() {
    let app = "\
        CREATE STREAM inputStream (value FLOAT);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 3.14159265358979) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(v) = out[0][0] {
        assert!((v - 3.14159265358979).abs() < 0.0001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test default with INT and DOUBLE widening
#[tokio::test]
async fn function_test_default_int_to_double() {
    let app = "\
        CREATE STREAM inputStream (value INT);\n\
        CREATE STREAM outputStream (result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT default(value, 3.14) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(v) = out[0][0] {
        assert!((v - 3.14).abs() < 0.001);
    } else {
        panic!("Expected Double value");
    }
}

/// Coalesce with function
#[tokio::test]
async fn function_test_coalesce_with_function() {
    let app = "\
        CREATE STREAM inputStream (name STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT upper(coalesce(name, 'unknown')) AS result FROM inputStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("inputStream", vec![AttributeValue::Null]);
    runner.send(
        "inputStream",
        vec![AttributeValue::String("alice".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("UNKNOWN".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("ALICE".to_string()));
}
