// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Comprehensive Alias Tests
// Tests for stream/table aliases in JOIN, SELECT, WHERE, and ON clauses

mod common;

use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// STREAM JOIN ALIAS TESTS
// ============================================================================

/// Test: Basic stream join with aliases (happy path)
#[tokio::test]
async fn alias_basic_stream_join() {
    let app = "\
        CREATE STREAM Orders (orderId INT, symbol STRING, quantity INT);\n\
        CREATE STREAM Prices (symbol STRING, price FLOAT);\n\
        CREATE STREAM Output (orderId INT, symbol STRING, total FLOAT);\n\
        INSERT INTO Output\n\
        SELECT o.orderId AS orderId, o.symbol AS symbol, o.quantity * p.price AS total\n\
        FROM Orders AS o WINDOW('length', 10)\n\
        JOIN Prices AS p WINDOW('length', 10)\n\
        ON o.symbol = p.symbol;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Prices",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "Orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(10),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::String("AAPL".to_string()));
    assert_eq!(out[0][2], AttributeValue::Float(1500.0));
}

/// Test: Mixed aliases - only left stream has alias
#[tokio::test]
async fn alias_mixed_left_only() {
    let app = "\
        CREATE STREAM StreamA (id INT, value STRING);\n\
        CREATE STREAM StreamB (id INT, data STRING);\n\
        CREATE STREAM Output (aValue STRING, bData STRING);\n\
        INSERT INTO Output\n\
        SELECT a.value AS aValue, StreamB.data AS bData\n\
        FROM StreamA AS a WINDOW('length', 10)\n\
        JOIN StreamB WINDOW('length', 10)\n\
        ON a.id = StreamB.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "StreamA",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    runner.send(
        "StreamB",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("world".to_string()),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("world".to_string()));
}

/// Test: Mixed aliases - only right stream has alias
#[tokio::test]
async fn alias_mixed_right_only() {
    let app = "\
        CREATE STREAM StreamA (id INT, value STRING);\n\
        CREATE STREAM StreamB (id INT, data STRING);\n\
        CREATE STREAM Output (aValue STRING, bData STRING);\n\
        INSERT INTO Output\n\
        SELECT StreamA.value AS aValue, b.data AS bData\n\
        FROM StreamA WINDOW('length', 10)\n\
        JOIN StreamB AS b WINDOW('length', 10)\n\
        ON StreamA.id = b.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "StreamA",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    runner.send(
        "StreamB",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("world".to_string()),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("world".to_string()));
}

/// Test: Alias with underscore and numbers
#[tokio::test]
async fn alias_with_underscore_and_numbers() {
    let app = "\
        CREATE STREAM Stream1 (id INT, val INT);\n\
        CREATE STREAM Stream2 (id INT, val INT);\n\
        CREATE STREAM Output (sum INT);\n\
        INSERT INTO Output\n\
        SELECT s_1.val + s_2.val AS sum\n\
        FROM Stream1 AS s_1 WINDOW('length', 10)\n\
        JOIN Stream2 AS s_2 WINDOW('length', 10)\n\
        ON s_1.id = s_2.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "Stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(150));
}

/// Test: Self-join with correct value attribution
/// Ensures left and right sides are properly distinguished
#[tokio::test]
async fn alias_self_join_value_attribution() {
    let app = "\
        CREATE STREAM Events (id INT, value INT);\n\
        CREATE STREAM Output (leftId INT, leftVal INT, rightId INT, rightVal INT);\n\
        INSERT INTO Output\n\
        SELECT l.id AS leftId, l.value AS leftVal, r.id AS rightId, r.value AS rightVal\n\
        FROM Events AS l WINDOW('length', 10)\n\
        JOIN Events AS r WINDOW('length', 10)\n\
        ON l.id = r.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    // Send first event
    runner.send(
        "Events",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    // Send second event with same id but different value
    runner.send(
        "Events",
        vec![AttributeValue::Int(1), AttributeValue::Int(200)],
    );

    let out = runner.shutdown();
    // Should have matches: (100,100), (100,200), (200,100), (200,200)
    assert!(!out.is_empty());
    // Verify structure - all outputs should have matching IDs
    for row in &out {
        assert_eq!(row[0], row[2]); // leftId == rightId
    }
}

/// Test: Self-join with inequality condition
#[tokio::test]
async fn alias_self_join_inequality() {
    let app = "\
        CREATE STREAM Numbers (id INT, value INT);\n\
        CREATE STREAM Output (smaller INT, larger INT);\n\
        INSERT INTO Output\n\
        SELECT a.value AS smaller, b.value AS larger\n\
        FROM Numbers AS a WINDOW('length', 10)\n\
        JOIN Numbers AS b WINDOW('length', 10)\n\
        ON a.id = b.id AND a.value < b.value;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Numbers",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    runner.send(
        "Numbers",
        vec![AttributeValue::Int(1), AttributeValue::Int(20)],
    );
    runner.send(
        "Numbers",
        vec![AttributeValue::Int(1), AttributeValue::Int(30)],
    );

    let out = runner.shutdown();
    // Should have pairs: (10,20), (10,30), (20,30)
    assert!(!out.is_empty());
    for row in &out {
        let smaller = if let AttributeValue::Int(v) = row[0] {
            v
        } else {
            panic!()
        };
        let larger = if let AttributeValue::Int(v) = row[1] {
            v
        } else {
            panic!()
        };
        assert!(smaller < larger);
    }
}

// ============================================================================
// LEFT/RIGHT/FULL OUTER JOIN WITH ALIASES
// ============================================================================

/// Test: LEFT OUTER JOIN with aliases
#[tokio::test]
async fn alias_left_outer_join() {
    let app = "\
        CREATE STREAM Left (id INT, name STRING);\n\
        CREATE STREAM Right (id INT, value INT);\n\
        CREATE STREAM Output (name STRING, value INT);\n\
        INSERT INTO Output\n\
        SELECT l.name AS name, r.value AS value\n\
        FROM Left AS l WINDOW('length', 10)\n\
        LEFT OUTER JOIN Right AS r WINDOW('length', 10)\n\
        ON l.id = r.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Left",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    // No matching right event - should still produce output with NULL

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Alice".to_string()));
}

/// Test: RIGHT OUTER JOIN with aliases
#[tokio::test]
async fn alias_right_outer_join() {
    let app = "\
        CREATE STREAM Left (id INT, name STRING);\n\
        CREATE STREAM Right (id INT, value INT);\n\
        CREATE STREAM Output (name STRING, value INT);\n\
        INSERT INTO Output\n\
        SELECT l.name AS name, r.value AS value\n\
        FROM Left AS l WINDOW('length', 10)\n\
        RIGHT OUTER JOIN Right AS r WINDOW('length', 10)\n\
        ON l.id = r.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Right",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    // No matching left event - should still produce output with NULL

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Test: FULL OUTER JOIN with aliases
#[tokio::test]
async fn alias_full_outer_join() {
    let app = "\
        CREATE STREAM Left (id INT, name STRING);\n\
        CREATE STREAM Right (id INT, value INT);\n\
        CREATE STREAM Output (id INT, name STRING, value INT);\n\
        INSERT INTO Output\n\
        SELECT coalesce(l.id, r.id) AS id, l.name AS name, r.value AS value\n\
        FROM Left AS l WINDOW('length', 10)\n\
        FULL OUTER JOIN Right AS r WINDOW('length', 10)\n\
        ON l.id = r.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Left",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send(
        "Right",
        vec![AttributeValue::Int(2), AttributeValue::Int(200)],
    );

    let out = runner.shutdown();
    // Should have at least 2 rows - one for each unmatched side
    assert!(out.len() >= 2);
}

// ============================================================================
// TABLE JOIN WITH ALIASES
// ============================================================================

/// Test: Stream-Table join with both aliases
#[tokio::test]
async fn alias_stream_table_join() {
    let app = "\
        CREATE STREAM Orders (orderId INT, productId INT);\n\
        CREATE TABLE Products (productId INT PRIMARY KEY, name STRING, price FLOAT);\n\
        CREATE STREAM InsertProducts (productId INT, name STRING, price FLOAT);\n\
        CREATE STREAM Output (orderId INT, productName STRING, price FLOAT);\n\
        INSERT INTO Products\n\
        SELECT productId, name, price FROM InsertProducts;\n\
        INSERT INTO Output\n\
        SELECT o.orderId AS orderId, p.name AS productName, p.price AS price\n\
        FROM Orders AS o JOIN Products AS p\n\
        ON o.productId = p.productId;\n";

    let runner = AppRunner::new(app, "Output").await;
    // Insert product first
    runner.send(
        "InsertProducts",
        vec![
            AttributeValue::Int(100),
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Float(9.99),
        ],
    );
    // Then send order
    runner.send(
        "Orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::String("Widget".to_string()));
    assert_eq!(out[0][2], AttributeValue::Float(9.99));
}

/// Test: Stream-Table join with only stream alias
#[tokio::test]
async fn alias_stream_table_stream_alias_only() {
    let app = "\
        CREATE STREAM Events (keyId INT, data STRING);\n\
        CREATE TABLE LookupTable (keyId INT, label STRING);\n\
        CREATE STREAM InsertLookup (keyId INT, label STRING);\n\
        CREATE STREAM Output (data STRING, label STRING);\n\
        INSERT INTO LookupTable\n\
        SELECT * FROM InsertLookup;\n\
        INSERT INTO Output\n\
        SELECT e.data AS data, LookupTable.label AS label\n\
        FROM Events AS e JOIN LookupTable\n\
        ON e.keyId = LookupTable.keyId;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "InsertLookup",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Label1".to_string()),
        ],
    );
    runner.send(
        "Events",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Data1".to_string()),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Data1".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("Label1".to_string()));
}

/// Test: Stream-Table join with only table alias
#[tokio::test]
async fn alias_stream_table_table_alias_only() {
    let app = "\
        CREATE STREAM Events (keyId INT, data STRING);\n\
        CREATE TABLE LookupTable (keyId INT, label STRING);\n\
        CREATE STREAM InsertLookup (keyId INT, label STRING);\n\
        CREATE STREAM Output (data STRING, label STRING);\n\
        INSERT INTO LookupTable\n\
        SELECT * FROM InsertLookup;\n\
        INSERT INTO Output\n\
        SELECT Events.data AS data, t.label AS label\n\
        FROM Events JOIN LookupTable AS t\n\
        ON Events.keyId = t.keyId;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "InsertLookup",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Label1".to_string()),
        ],
    );
    runner.send(
        "Events",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Data1".to_string()),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Data1".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("Label1".to_string()));
}

// ============================================================================
// SELECT AS (ATTRIBUTE RENAMING) TESTS
// ============================================================================

/// Test: Multiple AS renames in SELECT
#[tokio::test]
async fn alias_select_multiple_renames() {
    let app = "\
        CREATE STREAM Input (a INT, b INT, c INT);\n\
        CREATE STREAM Output (x INT, y INT, z INT);\n\
        INSERT INTO Output\n\
        SELECT a AS x, b AS y, c AS z\n\
        FROM Input;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Input",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(3),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Int(2));
    assert_eq!(out[0][2], AttributeValue::Int(3));
}

/// Test: Expression with AS rename
#[tokio::test]
async fn alias_select_expression_rename() {
    let app = "\
        CREATE STREAM Input (price FLOAT, quantity INT);\n\
        CREATE STREAM Output (total FLOAT, doubled LONG);\n\
        INSERT INTO Output\n\
        SELECT price * quantity AS total, quantity * 2 AS doubled\n\
        FROM Input;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Input",
        vec![AttributeValue::Float(10.5), AttributeValue::Int(4)],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Float(42.0));
    assert_eq!(out[0][1], AttributeValue::Long(8)); // INT * INT = LONG
}

/// Test: Function call with AS rename
#[tokio::test]
async fn alias_select_function_rename() {
    let app = "\
        CREATE STREAM Input (name STRING);\n\
        CREATE STREAM Output (upperName STRING, nameLen INT);\n\
        INSERT INTO Output\n\
        SELECT upper(name) AS upperName, length(name) AS nameLen\n\
        FROM Input;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send("Input", vec![AttributeValue::String("hello".to_string())]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("HELLO".to_string()));
    assert_eq!(out[0][1], AttributeValue::Int(5));
}

// ============================================================================
// COMPLEX EXPRESSION WITH ALIASES
// ============================================================================

/// Test: Nested arithmetic with aliases
#[tokio::test]
async fn alias_complex_arithmetic() {
    let app = "\
        CREATE STREAM A (x INT, y INT);\n\
        CREATE STREAM B (x INT, z INT);\n\
        CREATE STREAM Output (result INT);\n\
        INSERT INTO Output\n\
        SELECT (a.x + a.y) * (b.x - b.z) AS result\n\
        FROM A AS a WINDOW('length', 10)\n\
        JOIN B AS b WINDOW('length', 10)\n\
        ON a.x = b.x;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send("A", vec![AttributeValue::Int(5), AttributeValue::Int(3)]);
    runner.send("B", vec![AttributeValue::Int(5), AttributeValue::Int(2)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // (5 + 3) * (5 - 2) = 8 * 3 = 24
    assert_eq!(out[0][0], AttributeValue::Int(24));
}

/// Test: CASE WHEN with aliases
#[tokio::test]
async fn alias_case_when_expression() {
    let app = "\
        CREATE STREAM Left (id INT, category STRING);\n\
        CREATE STREAM Right (id INT, value INT);\n\
        CREATE STREAM Output (id INT, label STRING);\n\
        INSERT INTO Output\n\
        SELECT l.id AS id,\n\
               CASE WHEN r.value > 100 THEN 'high'\n\
                    WHEN r.value > 50 THEN 'medium'\n\
                    ELSE 'low' END AS label\n\
        FROM Left AS l WINDOW('length', 10)\n\
        JOIN Right AS r WINDOW('length', 10)\n\
        ON l.id = r.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Left",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "Right",
        vec![AttributeValue::Int(1), AttributeValue::Int(150)],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::String("high".to_string()));
}

/// Test: Coalesce with aliases
#[tokio::test]
async fn alias_coalesce_expression() {
    let app = "\
        CREATE STREAM A (id INT, value INT);\n\
        CREATE STREAM B (id INT, value INT);\n\
        CREATE STREAM Output (id INT, combined INT);\n\
        INSERT INTO Output\n\
        SELECT a.id AS id, coalesce(a.value, b.value) AS combined\n\
        FROM A AS a WINDOW('length', 10)\n\
        LEFT OUTER JOIN B AS b WINDOW('length', 10)\n\
        ON a.id = b.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

// ============================================================================
// AGGREGATE WITH ALIASES
// ============================================================================

/// Test: Aggregate function with aliased stream in windowed join
/// Note: GROUP BY with qualified column names (alias.column) not yet supported
#[tokio::test]
#[ignore = "GROUP BY with qualified column names not yet supported"]
async fn alias_aggregate_in_join() {
    let app = "\
        CREATE STREAM Sales (productId INT, amount FLOAT);\n\
        CREATE STREAM Products (productId INT, name STRING);\n\
        CREATE STREAM Output (productId INT, name STRING, totalAmount FLOAT);\n\
        INSERT INTO Output\n\
        SELECT s.productId AS productId, p.name AS name, sum(s.amount) AS totalAmount\n\
        FROM Sales AS s WINDOW('length', 100)\n\
        JOIN Products AS p WINDOW('length', 100)\n\
        ON s.productId = p.productId\n\
        GROUP BY s.productId, p.name;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Widget".to_string()),
        ],
    );
    runner.send(
        "Sales",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    runner.send(
        "Sales",
        vec![AttributeValue::Int(1), AttributeValue::Float(20.0)],
    );

    let out = runner.shutdown();
    assert!(!out.is_empty());
}

// ============================================================================
// EDGE CASES
// ============================================================================

/// Test: Single character aliases
#[tokio::test]
async fn alias_single_char() {
    let app = "\
        CREATE STREAM X (id INT, val INT);\n\
        CREATE STREAM Y (id INT, val INT);\n\
        CREATE STREAM Output (sum INT);\n\
        INSERT INTO Output\n\
        SELECT a.val + b.val AS sum\n\
        FROM X AS a WINDOW('length', 10)\n\
        JOIN Y AS b WINDOW('length', 10)\n\
        ON a.id = b.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send("X", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    runner.send("Y", vec![AttributeValue::Int(1), AttributeValue::Int(50)]);

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(150));
}

/// Test: Multiple columns from same aliased stream
#[tokio::test]
async fn alias_multiple_columns_same_stream() {
    let app = "\
        CREATE STREAM Data (id INT, col1 STRING, col2 STRING, col3 INT);\n\
        CREATE STREAM Lookup (id INT, label STRING);\n\
        CREATE STREAM Output (c1 STRING, c2 STRING, c3 INT, lbl STRING);\n\
        INSERT INTO Output\n\
        SELECT d.col1 AS c1, d.col2 AS c2, d.col3 AS c3, l.label AS lbl\n\
        FROM Data AS d WINDOW('length', 10)\n\
        JOIN Lookup AS l WINDOW('length', 10)\n\
        ON d.id = l.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "Lookup",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Label1".to_string()),
        ],
    );
    runner.send(
        "Data",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(42),
        ],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("B".to_string()));
    assert_eq!(out[0][2], AttributeValue::Int(42));
    assert_eq!(out[0][3], AttributeValue::String("Label1".to_string()));
}

/// Test: Long alias names
#[tokio::test]
async fn alias_long_names() {
    let app = "\
        CREATE STREAM FirstStream (id INT, value INT);\n\
        CREATE STREAM SecondStream (id INT, value INT);\n\
        CREATE STREAM Output (total INT);\n\
        INSERT INTO Output\n\
        SELECT first_stream_alias.value + second_stream_alias.value AS total\n\
        FROM FirstStream AS first_stream_alias WINDOW('length', 10)\n\
        JOIN SecondStream AS second_stream_alias WINDOW('length', 10)\n\
        ON first_stream_alias.id = second_stream_alias.id;\n";

    let runner = AppRunner::new(app, "Output").await;
    runner.send(
        "FirstStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "SecondStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(200)],
    );

    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(300));
}
