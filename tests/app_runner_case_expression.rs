// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration tests for CASE expression support (SQL syntax)
//!
//! Tests both Searched CASE and Simple CASE expressions with various scenarios
//! including NULL handling, type validation, and SQL standard compliance.

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

/// Test Searched CASE with boolean conditions
#[tokio::test]
async fn test_searched_case_basic() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE, volume BIGINT);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN price > 100 THEN 'HIGH'\n\
                   WHEN price > 50 THEN 'MEDIUM'\n\
                   ELSE 'LOW'\n\
               END as category\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
            AttributeValue::Long(1000),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(75.0),
            AttributeValue::Long(500),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(25.0),
            AttributeValue::Long(2000),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("HIGH".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("MEDIUM".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("LOW".to_string())
        ]
    );
}

/// Test Simple CASE with value matching
#[tokio::test]
async fn test_simple_case_basic() {
    let app = "\
        CREATE STREAM In (symbol STRING, status STRING);\n\
        SELECT symbol,\n\
               CASE status\n\
                   WHEN 'ACTIVE' THEN 1\n\
                   WHEN 'PENDING' THEN 2\n\
                   WHEN 'INACTIVE' THEN 3\n\
                   ELSE 0\n\
               END as status_code\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("PENDING".to_string()),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("INACTIVE".to_string()),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("TSLA".to_string()),
            AttributeValue::String("UNKNOWN".to_string()),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Long(1) // SQL integer literals are BIGINT/LONG by default
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Long(2)
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Long(3)
        ]
    );
    assert_eq!(
        results[3],
        vec![
            AttributeValue::String("TSLA".to_string()),
            AttributeValue::Long(0)
        ]
    );
}

/// Test CASE with NULL handling (NULL != NULL in SQL standard)
/// NOTE: Disabled because SQL parser doesn't support NULL literal in WHEN clause yet
#[tokio::test]
#[ignore = "SQL parser doesn't support NULL in WHEN clause - feature pending"]
async fn test_case_null_semantics() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE);\n\
        SELECT symbol,\n\
               CASE price\n\
                   WHEN NULL THEN 'NULL_MATCH'\n\
                   WHEN 100.0 THEN 'HUNDRED'\n\
                   ELSE 'OTHER'\n\
               END as result\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Null, // NULL operand
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(50.0),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 3);
    // NULL != NULL in SQL, so should return ELSE
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("OTHER".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("HUNDRED".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("OTHER".to_string())
        ]
    );
}

/// Test CASE with nested expressions
#[tokio::test]
async fn test_case_nested_expressions() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE, volume BIGINT);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN price * volume > 10000 THEN 'HIGH_VALUE'\n\
                   WHEN price + volume > 1000 THEN 'MEDIUM_VALUE'\n\
                   ELSE 'LOW_VALUE'\n\
               END as value_category\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(100.0),
            AttributeValue::Long(200),
        ],
    ); // 100 * 200 = 20000 > 10000
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(50.0),
            AttributeValue::Long(1000),
        ],
    ); // 50 * 1000 = 50000 > 10000
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(10.0),
            AttributeValue::Long(100),
        ],
    ); // 10 * 100 = 1000, 10 + 100 = 110 < 1000

    let results = runner.shutdown();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("HIGH_VALUE".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("HIGH_VALUE".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("LOW_VALUE".to_string())
        ]
    );
}

/// Test CASE in WHERE clause
#[tokio::test]
async fn test_case_in_where_clause() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE);\n\
        SELECT *\n\
        FROM In\n\
        WHERE CASE\n\
                  WHEN price > 100 THEN true\n\
                  ELSE false\n\
              END;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(75.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(200.0),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0)
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(200.0)
        ]
    );
}

/// Test CASE with string results
#[tokio::test]
async fn test_case_string_results() {
    let app = "\
        CREATE STREAM In (symbol STRING, category BIGINT);\n\
        SELECT symbol,\n\
               CASE category\n\
                   WHEN 1 THEN 'Technology'\n\
                   WHEN 2 THEN 'Finance'\n\
                   WHEN 3 THEN 'Healthcare'\n\
                   ELSE 'Unknown'\n\
               END as sector\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Long(1), // Match stream definition BIGINT
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("JPM".to_string()),
            AttributeValue::Long(2),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("JNJ".to_string()),
            AttributeValue::Long(3),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("XYZ".to_string()),
            AttributeValue::Long(99),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("Technology".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("JPM".to_string()),
            AttributeValue::String("Finance".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("JNJ".to_string()),
            AttributeValue::String("Healthcare".to_string())
        ]
    );
    assert_eq!(
        results[3],
        vec![
            AttributeValue::String("XYZ".to_string()),
            AttributeValue::String("Unknown".to_string())
        ]
    );
}

/// Test short-circuit evaluation (first matching WHEN wins)
#[tokio::test]
async fn test_case_short_circuit() {
    let app = "\
        CREATE STREAM In (symbol STRING, value INT);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN value > 50 THEN 'FIRST'\n\
                   WHEN value > 25 THEN 'SECOND'\n\
                   WHEN value > 50 THEN 'THIRD'\n\
                   ELSE 'NONE'\n\
               END as result\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("TEST".to_string()),
            AttributeValue::Int(75),
        ],
    ); // Matches first condition

    let results = runner.shutdown();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("TEST".to_string()),
            AttributeValue::String("FIRST".to_string()) // Not "THIRD"
        ]
    );
}

/// Test CASE with boolean result type
#[tokio::test]
async fn test_case_boolean_result() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN price > 100 THEN true\n\
                   ELSE false\n\
               END as is_expensive\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(50.0),
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Bool(true)
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Bool(false)
        ]
    );
}

/// Test multiple CASE expressions in same SELECT
#[tokio::test]
async fn test_multiple_case_expressions() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE, volume BIGINT);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN price > 100 THEN 'EXPENSIVE'\n\
                   WHEN price > 50 THEN 'MODERATE'\n\
                   ELSE 'CHEAP'\n\
               END as price_category,\n\
               CASE\n\
                   WHEN volume > 1000 THEN 'HIGH_VOLUME'\n\
                   WHEN volume > 500 THEN 'MEDIUM_VOLUME'\n\
                   ELSE 'LOW_VOLUME'\n\
               END as volume_category\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0), // EXPENSIVE
            AttributeValue::Long(2000),    // HIGH_VOLUME
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(75.0), // MODERATE
            AttributeValue::Long(800),    // MEDIUM_VOLUME
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(25.0), // CHEAP
            AttributeValue::Long(300),    // LOW_VOLUME
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("EXPENSIVE".to_string()),
            AttributeValue::String("HIGH_VOLUME".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("MODERATE".to_string()),
            AttributeValue::String("MEDIUM_VOLUME".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("CHEAP".to_string()),
            AttributeValue::String("LOW_VOLUME".to_string())
        ]
    );
}

/// Test nested CASE expression (CASE inside CASE)
#[tokio::test]
async fn test_nested_case_expression() {
    let app = "\
        CREATE STREAM In (symbol STRING, price DOUBLE, volume BIGINT);\n\
        SELECT symbol,\n\
               CASE\n\
                   WHEN price > 100 THEN\n\
                       CASE\n\
                           WHEN volume > 1000 THEN 'PREMIUM_HIGH_VOL'\n\
                           ELSE 'PREMIUM_LOW_VOL'\n\
                       END\n\
                   ELSE\n\
                       CASE\n\
                           WHEN volume > 1000 THEN 'BUDGET_HIGH_VOL'\n\
                           ELSE 'BUDGET_LOW_VOL'\n\
                       END\n\
               END as classification\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(150.0), // price > 100
            AttributeValue::Long(2000),    // volume > 1000
        ],
    ); // Expected: PREMIUM_HIGH_VOL
    runner.send(
        "In",
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::Double(150.0), // price > 100
            AttributeValue::Long(500),     // volume <= 1000
        ],
    ); // Expected: PREMIUM_LOW_VOL
    runner.send(
        "In",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Double(50.0), // price <= 100
            AttributeValue::Long(2000),   // volume > 1000
        ],
    ); // Expected: BUDGET_HIGH_VOL
    runner.send(
        "In",
        vec![
            AttributeValue::String("TSLA".to_string()),
            AttributeValue::Double(50.0), // price <= 100
            AttributeValue::Long(500),    // volume <= 1000
        ],
    ); // Expected: BUDGET_LOW_VOL

    let results = runner.shutdown();
    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::String("PREMIUM_HIGH_VOL".to_string())
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("GOOGL".to_string()),
            AttributeValue::String("PREMIUM_LOW_VOL".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::String("BUDGET_HIGH_VOL".to_string())
        ]
    );
    assert_eq!(
        results[3],
        vec![
            AttributeValue::String("TSLA".to_string()),
            AttributeValue::String("BUDGET_LOW_VOL".to_string())
        ]
    );
}

/// Test Simple CASE with INT column and numeric WHEN literals (cross-type Int<->Long comparison)
/// SQL parser emits integer literals as Long, but INT columns produce Int values.
/// This test verifies that Int(1) matches Long(1) in Simple CASE expressions.
#[tokio::test]
async fn test_simple_case_int_column_cross_type() {
    let app = "\
        CREATE STREAM In (symbol STRING, code INT);\n\
        SELECT symbol,\n\
               CASE code\n\
                   WHEN 1 THEN 'ONE'\n\
                   WHEN 2 THEN 'TWO'\n\
                   WHEN 3 THEN 'THREE'\n\
                   ELSE 'OTHER'\n\
               END as label\n\
        FROM In;\n";

    let runner = AppRunner::new(app, "OutputStream").await;
    runner.send(
        "In",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(1), // INT column produces Int, WHEN literal is Long
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "In",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Int(99), // No match, should fall to ELSE
        ],
    );

    let results = runner.shutdown();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("ONE".to_string()) // Int(1) must match Long(1)
        ]
    );
    assert_eq!(
        results[1],
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::String("TWO".to_string())
        ]
    );
    assert_eq!(
        results[2],
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::String("OTHER".to_string())
        ]
    );
}
