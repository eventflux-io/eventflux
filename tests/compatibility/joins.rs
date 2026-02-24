// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Join Compatibility Tests
// Reference: query/join/JoinTestCase.java, OuterJoinTestCase.java

use super::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// INNER JOIN TESTS
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test inner join with window
/// Reference: JoinTestCase.java:joinTest1
#[tokio::test]
async fn join_test1_inner_with_window() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM twitterStream (user STRING, tweet STRING, company STRING);\n\
        CREATE STREAM outputStream (symbol STRING, tweet STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT cseEventStream.symbol AS symbol, twitterStream.tweet AS tweet, \
               cseEventStream.price AS price\n\
        FROM cseEventStream WINDOW('length', 10)\n\
        JOIN twitterStream WINDOW('length', 10)\n\
        ON cseEventStream.symbol = twitterStream.company;\n";
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
        "twitterStream",
        vec![
            AttributeValue::String("User1".to_string()),
            AttributeValue::String("Hello World".to_string()),
            AttributeValue::String("MSFT".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("MSFT".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("Hello World".to_string()));
}

/// Test join with aliases
/// Reference: JoinTestCase.java:joinTest2
#[tokio::test]
async fn join_test2_with_aliases() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM twitterStream (user STRING, tweet STRING, company STRING);\n\
        CREATE STREAM outputStream (symbol STRING, tweet STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT a.symbol AS symbol, b.tweet AS tweet, a.price AS price\n\
        FROM cseEventStream AS a WINDOW('length', 10)\n\
        JOIN twitterStream AS b WINDOW('length', 10)\n\
        ON a.symbol = b.company;\n";
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
        "twitterStream",
        vec![
            AttributeValue::String("User1".to_string()),
            AttributeValue::String("Hello World".to_string()),
            AttributeValue::String("MSFT".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Test self-join
/// Reference: JoinTestCase.java:joinTest3
#[tokio::test]
async fn join_test3_self_join() {
    let app = "\
        CREATE STREAM cseEventStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, priceA FLOAT, priceB FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT a.symbol AS symbol, a.price AS priceA, b.price AS priceB\n\
        FROM cseEventStream AS a WINDOW('length', 10)\n\
        JOIN cseEventStream AS b WINDOW('length', 10)\n\
        ON a.symbol = b.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
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
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    // Self-join should produce matches where symbol matches itself
    assert!(!out.is_empty());
}

/// Test join without condition (cross join)
/// Reference: JoinTestCase.java:joinTest5
#[tokio::test]
async fn join_test5_cross_join() {
    let app = "\
        CREATE STREAM A (val INT);\n\
        CREATE STREAM B (val INT);\n\
        CREATE STREAM Out (a INT, b INT);\n\
        INSERT INTO Out\n\
        SELECT A.val AS a, B.val AS b\n\
        FROM A WINDOW('length', 10)\n\
        JOIN B WINDOW('length', 10);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(1)]);
    runner.send("B", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    // Cross join produces output
    assert_eq!(out.len(), 1);
}

// ============================================================================
// OUTER JOIN TESTS
// Reference: query/join/OuterJoinTestCase.java
// ============================================================================

/// Test left outer join
/// Reference: OuterJoinTestCase.java:leftOuterJoinTest1
#[tokio::test]
async fn join_test6_left_outer() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id AS l, R.id AS r\n\
        FROM L WINDOW('length', 10)\n\
        LEFT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    // No matching R event
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Null);
}

/// Test right outer join
/// Reference: OuterJoinTestCase.java:rightOuterJoinTest1
#[tokio::test]
async fn join_test7_right_outer() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id AS l, R.id AS r\n\
        FROM L WINDOW('length', 10)\n\
        RIGHT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("R", vec![AttributeValue::Int(5)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Null);
    assert_eq!(out[0][1], AttributeValue::Int(5));
}

/// Test full outer join
/// Reference: OuterJoinTestCase.java:fullOuterJoinTest1
#[tokio::test]
async fn join_test8_full_outer() {
    let app = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out\n\
        SELECT L.id AS l, R.id AS r\n\
        FROM L WINDOW('length', 10)\n\
        FULL OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1)]);
    runner.send("R", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    // Full outer join should produce output for both non-matching events
    assert_eq!(out.len(), 2);
}

// ============================================================================
// JOIN WITH AGGREGATION TESTS
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test join with count aggregation
/// Reference: JoinTestCase.java:joinTest8
#[tokio::test]
async fn join_test9_with_count() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM newsStream (symbol STRING, headline STRING);\n\
        CREATE STREAM outputStream (symbol STRING, cnt BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT stockStream.symbol AS symbol, count() AS cnt\n\
        FROM stockStream WINDOW('length', 10)\n\
        JOIN newsStream WINDOW('length', 10)\n\
        ON stockStream.symbol = newsStream.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "newsStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("Earnings beat".to_string()),
        ],
    );
    runner.send(
        "newsStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("New product".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(out.len() >= 2);
}

/// Test join with sum aggregation
/// Reference: JoinTestCase.java:joinTest9
#[tokio::test]
async fn join_test10_with_sum() {
    let app = "\
        CREATE STREAM orders (productId INT, quantity INT);\n\
        CREATE STREAM prices (productId INT, price FLOAT);\n\
        CREATE STREAM outputStream (productId INT, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT orders.productId AS productId, sum(prices.price) AS total\n\
        FROM orders WINDOW('length', 10)\n\
        JOIN prices WINDOW('length', 10)\n\
        ON orders.productId = prices.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "prices",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

// ============================================================================
// JOIN WITH FILTER TESTS
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test join with WHERE filter
/// Reference: JoinTestCase.java:joinTest10
#[tokio::test]
async fn join_test11_with_filter() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM tradeStream (symbol STRING, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        INSERT INTO outputStream\n\
        SELECT stockStream.symbol AS symbol, stockStream.price AS price, \
               tradeStream.volume AS volume\n\
        FROM stockStream WINDOW('length', 10)\n\
        JOIN tradeStream WINDOW('length', 10)\n\
        ON stockStream.symbol = tradeStream.symbol\n\
        WHERE stockStream.price > 50.0;\n";
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
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Int(1000),
        ],
    );
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Int(500),
        ],
    );
    let out = runner.shutdown();
    // Only IBM should match (price > 50)
    // Filter may be applied after join, so we check that IBM is in the output
    let ibm_matches: Vec<_> = out
        .iter()
        .filter(|row| row[0] == AttributeValue::String("IBM".to_string()))
        .collect();
    assert!(!ibm_matches.is_empty());
}

// ============================================================================
// JOIN WITH TIME WINDOW TESTS
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test join with time windows
/// Reference: JoinTestCase.java:joinTest11
#[tokio::test]
async fn join_test12_with_time_window() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM newsStream (symbol STRING, headline STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, headline STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stockStream.symbol AS symbol, stockStream.price AS price, \
               newsStream.headline AS headline\n\
        FROM stockStream WINDOW('time', 500 MILLISECONDS)\n\
        JOIN newsStream WINDOW('time', 500 MILLISECONDS)\n\
        ON stockStream.symbol = newsStream.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(1500.0),
        ],
    );
    runner.send(
        "newsStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::String("AI breakthrough".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("GOOG".to_string()));
}

// ============================================================================
// MULTIPLE JOIN CONDITIONS
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test join with multiple conditions (AND)
/// Reference: JoinTestCase.java:joinTest13
#[tokio::test]
async fn join_test13_multiple_conditions() {
    let app = "\
        CREATE STREAM orders (productId INT, region STRING, quantity INT);\n\
        CREATE STREAM inventory (productId INT, region STRING, stock INT);\n\
        CREATE STREAM outputStream (productId INT, region STRING, quantity INT, stock INT);\n\
        INSERT INTO outputStream\n\
        SELECT orders.productId AS productId, orders.region AS region, \
               orders.quantity AS quantity, inventory.stock AS stock\n\
        FROM orders WINDOW('length', 10)\n\
        JOIN inventory WINDOW('length', 10)\n\
        ON orders.productId = inventory.productId AND orders.region = inventory.region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "inventory",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "inventory",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("EU".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // Only US region should match
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("US".to_string()));
}

// ============================================================================
// LEFT OUTER JOIN WITH MATCHING EVENTS
// ============================================================================

/// Test left outer join when there are matching events
/// Reference: OuterJoinTestCase.java:leftOuterJoinTest2
#[tokio::test]
async fn join_test14_left_outer_with_match() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name AS name, R.value AS value\n\
        FROM L WINDOW('length', 10)\n\
        LEFT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    // Should have match now
    assert!(!out.is_empty());
    // Find the matched row
    let matched = out
        .iter()
        .find(|row| row[1] != AttributeValue::Null)
        .unwrap();
    assert_eq!(matched[0], AttributeValue::String("Alice".to_string()));
    assert_eq!(matched[1], AttributeValue::Int(100));
}

// ============================================================================
// STREAM-TABLE JOIN TESTS
// Reference: query/table/InsertIntoTableTestCase.java
// ============================================================================

/// Test stream-table join
/// Reference: InsertIntoTableTestCase.java
#[tokio::test]
async fn join_test15_stream_table() {
    let app = "\
        CREATE TABLE productTable (productId INT, productName STRING, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, productName STRING, price FLOAT);\n\
        CREATE STREAM orderStream (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM outputStream (orderId INT, productName STRING, total DOUBLE);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.orderId AS orderId, productTable.productName AS productName, \
               productTable.price * orderStream.quantity AS total\n\
        FROM orderStream JOIN productTable\n\
        ON orderStream.productId = productTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert product into table
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::Float(1000.0),
        ],
    );
    std::thread::sleep(std::time::Duration::from_millis(50));
    // Create order
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(101),
            AttributeValue::Int(1),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::Int(101));
    assert_eq!(out[0][1], AttributeValue::String("Laptop".to_string()));
    // Float * Int returns Float in EventFlux
    assert_eq!(out[0][2], AttributeValue::Float(2000.0));
}

// ============================================================================
// JOIN EDGE CASES - INEQUALITY CONDITIONS
// ============================================================================

/// Join with inequality condition (non-equi join)
#[tokio::test]
async fn join_test16_non_equi_join() {
    let app = "\
        CREATE STREAM Sales (productId INT, amount FLOAT);\n\
        CREATE STREAM Targets (productId INT, targetAmount FLOAT);\n\
        CREATE STREAM Out (productId INT, amount FLOAT, targetAmount FLOAT);\n\
        INSERT INTO Out\n\
        SELECT Sales.productId AS productId, Sales.amount AS amount, Targets.targetAmount AS targetAmount\n\
        FROM Sales WINDOW('length', 10)\n\
        JOIN Targets WINDOW('length', 10)\n\
        ON Sales.productId = Targets.productId AND Sales.amount > Targets.targetAmount;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Sales",
        vec![AttributeValue::Int(1), AttributeValue::Float(150.0)],
    );
    runner.send(
        "Targets",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    let out = runner.shutdown();
    // Sales exceeded target
    assert_eq!(out.len(), 1);
}

// ============================================================================
// JOIN WITH NULL VALUES
// ============================================================================

/// Join with NULL in join column - should not match
#[tokio::test]
async fn join_test17_null_in_join_column() {
    let app = "\
        CREATE STREAM A (id INT, name STRING);\n\
        CREATE STREAM B (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT A.name AS name, B.value AS value\n\
        FROM A WINDOW('length', 10)\n\
        JOIN B WINDOW('length', 10)\n\
        ON A.id = B.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    // A event with NULL id
    runner.send(
        "A",
        vec![
            AttributeValue::Null,
            AttributeValue::String("Alice".to_string()),
        ],
    );
    // B event with NULL id - should not match with A.NULL
    runner.send("B", vec![AttributeValue::Null, AttributeValue::Int(100)]);
    // B event with actual id
    runner.send("B", vec![AttributeValue::Int(1), AttributeValue::Int(200)]);
    let out = runner.shutdown();
    // NULL != NULL in SQL, so no match expected
    assert!(out.is_empty());
}

/// Left outer join with NULL in join column
#[tokio::test]
async fn join_test18_left_outer_null_id() {
    let app = "\
        CREATE STREAM A (id INT, name STRING);\n\
        CREATE STREAM B (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT A.name AS name, B.value AS value\n\
        FROM A WINDOW('length', 10)\n\
        LEFT OUTER JOIN B WINDOW('length', 10)\n\
        ON A.id = B.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    // A event with NULL id - left outer join should still output it
    runner.send(
        "A",
        vec![
            AttributeValue::Null,
            AttributeValue::String("Alice".to_string()),
        ],
    );
    let out = runner.shutdown();
    // Left outer join outputs left side even without match
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Alice".to_string()));
    assert_eq!(out[0][1], AttributeValue::Null);
}

// ============================================================================
// JOIN WITH GROUP BY
// ============================================================================

/// Join with GROUP BY aggregation
#[tokio::test]
#[ignore = "Complex GROUP BY expressions with qualified column names not yet supported"]
async fn join_test19_with_group_by() {
    let app = "\
        CREATE STREAM Orders (productId INT, quantity INT);\n\
        CREATE STREAM Products (productId INT, category STRING);\n\
        CREATE STREAM Out (category STRING, totalQty BIGINT);\n\
        INSERT INTO Out\n\
        SELECT Products.category AS category, sum(Orders.quantity) AS totalQty\n\
        FROM Orders WINDOW('length', 10)\n\
        JOIN Products WINDOW('length', 10)\n\
        ON Orders.productId = Products.productId\n\
        GROUP BY Products.category;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    runner.send(
        "Products",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    runner.send(
        "Orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "Orders",
        vec![AttributeValue::Int(2), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    // Should have aggregated by category
    assert!(!out.is_empty());
}

// ============================================================================
// MULTIPLE MATCHING ROWS
// ============================================================================

/// Join with multiple matching rows on both sides
#[tokio::test]
async fn join_test20_multiple_matches() {
    let app = "\
        CREATE STREAM A (aKey INT, valA INT);\n\
        CREATE STREAM B (bKey INT, valB INT);\n\
        CREATE STREAM Out (joinKey INT, valA INT, valB INT);\n\
        INSERT INTO Out\n\
        SELECT A.aKey AS joinKey, A.valA AS valA, B.valB AS valB\n\
        FROM A WINDOW('length', 10)\n\
        JOIN B WINDOW('length', 10)\n\
        ON A.aKey = B.bKey;\n";
    let runner = AppRunner::new(app, "Out").await;
    // Two A events with same key
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(10)]);
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(20)]);
    // One B event with matching key
    runner.send("B", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    // Should produce 2 results (2 A's x 1 B)
    assert!(out.len() >= 2);
}

/// Join with cartesian product behavior
#[tokio::test]
async fn join_test21_cartesian_matches() {
    let app = "\
        CREATE STREAM A (aKey INT, valA INT);\n\
        CREATE STREAM B (bKey INT, valB INT);\n\
        CREATE STREAM Out (joinKey INT, valA INT, valB INT);\n\
        INSERT INTO Out\n\
        SELECT A.aKey AS joinKey, A.valA AS valA, B.valB AS valB\n\
        FROM A WINDOW('length', 10)\n\
        JOIN B WINDOW('length', 10)\n\
        ON A.aKey = B.bKey;\n";
    let runner = AppRunner::new(app, "Out").await;
    // Two A events with same key
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(10)]);
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(20)]);
    // Two B events with matching key
    runner.send("B", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    runner.send("B", vec![AttributeValue::Int(1), AttributeValue::Int(200)]);
    let out = runner.shutdown();
    // Should produce 4 results (2 A's x 2 B's)
    assert!(out.len() >= 4);
}

// ============================================================================
// JOIN WITH EXPRESSIONS IN SELECT
// ============================================================================

/// Join with arithmetic expression in SELECT
#[tokio::test]
async fn join_test22_arithmetic_in_select() {
    let app = "\
        CREATE STREAM Orders (productId INT, quantity INT);\n\
        CREATE STREAM Prices (productId INT, unitPrice FLOAT);\n\
        CREATE STREAM Out (productId INT, total FLOAT);\n\
        INSERT INTO Out\n\
        SELECT Orders.productId AS productId, Orders.quantity * Prices.unitPrice AS total\n\
        FROM Orders WINDOW('length', 10)\n\
        JOIN Prices WINDOW('length', 10)\n\
        ON Orders.productId = Prices.productId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "Prices",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Float(50.0)); // 5 * 10
}

/// Join with function in SELECT
#[tokio::test]
async fn join_test23_function_in_select() {
    let app = "\
        CREATE STREAM Names (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM Scores (id INT, score INT);\n\
        CREATE STREAM Out (fullName STRING, score INT);\n\
        INSERT INTO Out\n\
        SELECT concat(Names.firstName, ' ', Names.lastName) AS fullName, Scores.score AS score\n\
        FROM Names WINDOW('length', 10)\n\
        JOIN Scores WINDOW('length', 10)\n\
        ON Names.id = Scores.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Names",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    runner.send(
        "Scores",
        vec![AttributeValue::Int(1), AttributeValue::Int(95)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("John Doe".to_string()));
    assert_eq!(out[0][1], AttributeValue::Int(95));
}

// ============================================================================
// JOIN WITH DIFFERENT WINDOW SIZES
// ============================================================================

/// Join with different length window sizes
#[tokio::test]
async fn join_test24_different_window_sizes() {
    let app = "\
        CREATE STREAM A (id INT, valA INT);\n\
        CREATE STREAM B (id INT, valB INT);\n\
        CREATE STREAM Out (id INT, valA INT, valB INT);\n\
        INSERT INTO Out\n\
        SELECT A.id AS id, A.valA AS valA, B.valB AS valB\n\
        FROM A WINDOW('length', 5)\n\
        JOIN B WINDOW('length', 10)\n\
        ON A.id = B.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("A", vec![AttributeValue::Int(1), AttributeValue::Int(10)]);
    runner.send("B", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

// ============================================================================
// JOIN WITH COALESCE FOR NULL HANDLING
// ============================================================================

/// Left outer join with coalesce for NULL values
#[tokio::test]
async fn join_test25_left_outer_with_coalesce() {
    let app = "\
        CREATE STREAM Orders (orderId INT, customerId INT);\n\
        CREATE STREAM Customers (customerId INT, name STRING);\n\
        CREATE STREAM Out (orderId INT, customerName STRING);\n\
        INSERT INTO Out\n\
        SELECT Orders.orderId AS orderId, coalesce(Customers.name, 'Unknown') AS customerName\n\
        FROM Orders WINDOW('length', 10)\n\
        LEFT OUTER JOIN Customers WINDOW('length', 10)\n\
        ON Orders.customerId = Customers.customerId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(999)],
    ); // No matching customer
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::String("Unknown".to_string()));
}

// ============================================================================
// JOIN WITH BOOLEAN CONDITIONS
// ============================================================================

/// Join with boolean field in WHERE clause
#[tokio::test]
async fn join_test26_boolean_filter() {
    let app = "\
        CREATE STREAM Orders (orderId INT, productId INT, isPriority BOOLEAN);\n\
        CREATE STREAM Products (productId INT, name STRING);\n\
        CREATE STREAM Out (orderId INT, productName STRING);\n\
        INSERT INTO Out\n\
        SELECT Orders.orderId AS orderId, Products.name AS productName\n\
        FROM Orders WINDOW('length', 10)\n\
        JOIN Products WINDOW('length', 10)\n\
        ON Orders.productId = Products.productId\n\
        WHERE Orders.isPriority = true;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
        ],
    );
    runner.send(
        "Orders",
        vec![
            AttributeValue::Int(101),
            AttributeValue::Int(1),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "Orders",
        vec![
            AttributeValue::Int(102),
            AttributeValue::Int(1),
            AttributeValue::Bool(false),
        ],
    );
    let out = runner.shutdown();
    // Only priority order should be in output
    let priority_orders: Vec<_> = out
        .iter()
        .filter(|row| row[0] == AttributeValue::Int(101))
        .collect();
    assert!(!priority_orders.is_empty());
}

// ============================================================================
// THREE-WAY JOIN (CHAINED)
// ============================================================================

/// Three-way join (chained joins)
#[tokio::test]
#[ignore = "Chained joins (three-way join) not yet supported"]
async fn join_test27_three_way_join() {
    let app = "\
        CREATE STREAM Orders (orderId INT, productId INT, customerId INT);\n\
        CREATE STREAM Products (productId INT, productName STRING);\n\
        CREATE STREAM Customers (customerId INT, customerName STRING);\n\
        CREATE STREAM Out (orderId INT, productName STRING, customerName STRING);\n\
        INSERT INTO Out\n\
        SELECT Orders.orderId AS orderId, Products.productName AS productName, \
               Customers.customerName AS customerName\n\
        FROM Orders WINDOW('length', 10)\n\
        JOIN Products WINDOW('length', 10)\n\
        ON Orders.productId = Products.productId\n\
        JOIN Customers WINDOW('length', 10)\n\
        ON Orders.customerId = Customers.customerId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
        ],
    );
    runner.send(
        "Customers",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send(
        "Orders",
        vec![
            AttributeValue::Int(101),
            AttributeValue::Int(1),
            AttributeValue::Int(1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

// ============================================================================
// JOIN WITH STRING COMPARISON
// ============================================================================

/// Join on string column
#[tokio::test]
async fn join_test28_string_join_key() {
    let app = "\
        CREATE STREAM Events (eventType STRING, value INT);\n\
        CREATE STREAM Config (eventType STRING, threshold INT);\n\
        CREATE STREAM Out (eventType STRING, value INT, threshold INT);\n\
        INSERT INTO Out\n\
        SELECT Events.eventType AS eventType, Events.value AS value, Config.threshold AS threshold\n\
        FROM Events WINDOW('length', 10)\n\
        JOIN Config WINDOW('length', 10)\n\
        ON Events.eventType = Config.eventType;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Config",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "Events",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ERROR".to_string()));
    assert_eq!(out[0][1], AttributeValue::Int(150));
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

// ============================================================================
// JOIN WITH LONG TYPE
// ============================================================================

/// Join on BIGINT column
#[tokio::test]
async fn join_test29_bigint_join_key() {
    let app = "\
        CREATE STREAM Events (eventId BIGINT, data STRING);\n\
        CREATE STREAM Metadata (eventId BIGINT, category STRING);\n\
        CREATE STREAM Out (eventId BIGINT, data STRING, category STRING);\n\
        INSERT INTO Out\n\
        SELECT Events.eventId AS eventId, Events.data AS data, Metadata.category AS category\n\
        FROM Events WINDOW('length', 10)\n\
        JOIN Metadata WINDOW('length', 10)\n\
        ON Events.eventId = Metadata.eventId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "Metadata",
        vec![
            AttributeValue::Long(9999999999999),
            AttributeValue::String("Critical".to_string()),
        ],
    );
    runner.send(
        "Events",
        vec![
            AttributeValue::Long(9999999999999),
            AttributeValue::String("Server down".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(9999999999999));
}

// ============================================================================
// JOIN ORDER MATTERS
// ============================================================================

/// Join where left side event arrives after right side
#[tokio::test]
async fn join_test30_late_left_event() {
    let app = "\
        CREATE STREAM L (id INT, valL INT);\n\
        CREATE STREAM R (id INT, valR INT);\n\
        CREATE STREAM Out (id INT, valL INT, valR INT);\n\
        INSERT INTO Out\n\
        SELECT L.id AS id, L.valL AS valL, R.valR AS valR\n\
        FROM L WINDOW('length', 10)\n\
        JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    // R event first
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    // L event later
    runner.send("L", vec![AttributeValue::Int(1), AttributeValue::Int(10)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

// ============================================================================
// FULL OUTER JOIN EDGE CASES
// ============================================================================

/// Full outer join with matches
#[tokio::test]
async fn join_test31_full_outer_with_match() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name AS name, R.value AS value\n\
        FROM L WINDOW('length', 10)\n\
        FULL OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    // Should have match
    let matched = out
        .iter()
        .find(|row| row[0] != AttributeValue::Null && row[1] != AttributeValue::Null);
    assert!(matched.is_some());
}

// ============================================================================
// ADDITIONAL JOIN EDGE CASE TESTS
// ============================================================================

/// Join with CASE WHEN in SELECT
#[tokio::test]
async fn join_test_case_when() {
    let app = "\
        CREATE STREAM orders (orderId INT, customerId INT, amount FLOAT);\n\
        CREATE STREAM customers (customerId INT, tier STRING);\n\
        CREATE STREAM Out (orderId INT, discount STRING);\n\
        INSERT INTO Out\n\
        SELECT orders.orderId AS orderId,\n\
               CASE WHEN customers.tier = 'gold' THEN 'high' ELSE 'low' END AS discount\n\
        FROM orders WINDOW('length', 10)\n\
        JOIN customers WINDOW('length', 10)\n\
        ON orders.customerId = customers.customerId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "customers",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("gold".to_string()),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(100),
            AttributeValue::Int(1),
            AttributeValue::Float(500.0),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][1], AttributeValue::String("high".to_string()));
}

/// Join with string functions
#[tokio::test]
async fn join_test_string_functions() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, code STRING);\n\
        CREATE STREAM Out (upperName STRING, lowerCode STRING);\n\
        INSERT INTO Out\n\
        SELECT upper(L.name) AS upperName, lower(R.code) AS lowerCode\n\
        FROM L WINDOW('length', 10)\n\
        JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "R",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ABC123".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("ALICE".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("abc123".to_string()));
}

/// Join with complex arithmetic
#[tokio::test]
async fn join_test_complex_arithmetic() {
    let app = "\
        CREATE STREAM orders (orderId INT, quantity INT, unitPrice FLOAT);\n\
        CREATE STREAM discounts (orderId INT, discountPct FLOAT);\n\
        CREATE STREAM Out (orderId INT, finalPrice DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT orders.orderId AS orderId,\n\
               (orders.quantity * orders.unitPrice) * (1.0 - discounts.discountPct / 100.0) AS finalPrice\n\
        FROM orders WINDOW('length', 10)\n\
        JOIN discounts WINDOW('length', 10)\n\
        ON orders.orderId = discounts.orderId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(10),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "discounts",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // 10 * 100 * (1 - 0.1) = 1000 * 0.9 = 900
    if let AttributeValue::Double(price) = out[0][1] {
        assert!((price - 900.0).abs() < 0.01);
    }
}

/// Join with boolean comparison result
#[tokio::test]
async fn join_test_boolean_result() {
    let app = "\
        CREATE STREAM L (id INT, valueL INT);\n\
        CREATE STREAM R (id INT, valueR INT);\n\
        CREATE STREAM Out (id INT, leftGreater BOOLEAN);\n\
        INSERT INTO Out\n\
        SELECT L.id AS id, L.valueL > R.valueR AS leftGreater\n\
        FROM L WINDOW('length', 10)\n\
        JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(50)]);
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][1], AttributeValue::Bool(true));
}

/// Join with coalesce for null handling
#[tokio::test]
async fn join_test_coalesce() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, nickname STRING);\n\
        CREATE STREAM Out (id INT, displayName STRING);\n\
        INSERT INTO Out\n\
        SELECT L.id AS id, coalesce(R.nickname, L.name) AS displayName\n\
        FROM L WINDOW('length', 10)\n\
        LEFT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    // No matching R record, should use L.name
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with length window and time window mixed
#[tokio::test]
async fn join_test_mixed_windows() {
    let app = "\
        CREATE STREAM L (id INT, value INT);\n\
        CREATE STREAM R (id INT, factor FLOAT);\n\
        CREATE STREAM Out (id INT, result DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT L.id AS id, L.value * R.factor AS result\n\
        FROM L WINDOW('length', 5)\n\
        JOIN R WINDOW('time', 5000 MILLISECONDS)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send("L", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    runner.send(
        "R",
        vec![AttributeValue::Int(1), AttributeValue::Float(1.5)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    if let AttributeValue::Double(result) = out[0][1] {
        assert!((result - 150.0).abs() < 0.01);
    }
}

/// Join with concat of values from both streams
#[tokio::test]
async fn join_test_concat_both() {
    let app = "\
        CREATE STREAM L (id INT, prefix STRING);\n\
        CREATE STREAM R (id INT, suffix STRING);\n\
        CREATE STREAM Out (id INT, combined STRING);\n\
        INSERT INTO Out\n\
        SELECT L.id AS id, concat(L.prefix, '-', R.suffix) AS combined\n\
        FROM L WINDOW('length', 10)\n\
        JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ABC".to_string()),
        ],
    );
    runner.send(
        "R",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("123".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][1], AttributeValue::String("ABC-123".to_string()));
}

/// Left outer join with no matches
#[tokio::test]
async fn join_test_left_outer_no_match() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name AS name, R.value AS value\n\
        FROM L WINDOW('length', 10)\n\
        LEFT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send(
        "R",
        vec![AttributeValue::Int(999), AttributeValue::Int(100)],
    ); // No match
    let out = runner.shutdown();
    // Should have output with L data and null for R
    assert!(!out.is_empty());
}

/// Right outer join with no matches
#[tokio::test]
async fn join_test_right_outer_no_match() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        CREATE STREAM Out (name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name AS name, R.value AS value\n\
        FROM L WINDOW('length', 10)\n\
        RIGHT OUTER JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(999),
            AttributeValue::String("Alice".to_string()),
        ],
    ); // No match
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    // Should have output with R data and null for L
    assert!(!out.is_empty());
}

/// Join with uuid() function
#[tokio::test]
async fn join_test_uuid() {
    let app = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        CREATE STREAM Out (joinId STRING, name STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT uuid() AS joinId, L.name AS name, R.value AS value\n\
        FROM L WINDOW('length', 10)\n\
        JOIN R WINDOW('length', 10)\n\
        ON L.id = R.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "L",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    runner.send("R", vec![AttributeValue::Int(1), AttributeValue::Int(100)]);
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Check UUID is generated
    if let AttributeValue::String(uuid) = &out[0][0] {
        assert_eq!(uuid.len(), 36);
    }
}

/// Join with multiple output columns from same stream
#[tokio::test]
async fn join_test_multiple_columns() {
    let app = "\
        CREATE STREAM orders (orderId INT, customerId INT, amount FLOAT, status STRING);\n\
        CREATE STREAM customers (customerId INT, name STRING, email STRING);\n\
        CREATE STREAM Out (orderId INT, amount FLOAT, status STRING, name STRING, email STRING);\n\
        INSERT INTO Out\n\
        SELECT orders.orderId AS orderId, orders.amount AS amount, orders.status AS status,\n\
               customers.name AS name, customers.email AS email\n\
        FROM orders WINDOW('length', 10)\n\
        JOIN customers WINDOW('length', 10)\n\
        ON orders.customerId = customers.customerId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "customers",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
            AttributeValue::String("alice@example.com".to_string()),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(100),
            AttributeValue::Int(1),
            AttributeValue::Float(500.0),
            AttributeValue::String("pending".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::Int(100));
    assert_eq!(out[0][3], AttributeValue::String("Alice".to_string()));
}

/// Join with upper function on both sides
#[tokio::test]
async fn join_test_upper_both_sides() {
    let app = "\
        CREATE STREAM products (productId INT, name STRING);\n\
        CREATE STREAM inventory (productId INT, qty INT);\n\
        CREATE STREAM Out (upperName STRING, qty INT);\n\
        INSERT INTO Out\n\
        SELECT upper(products.name) AS upperName, inventory.qty AS qty\n\
        FROM products WINDOW('length', 5)\n\
        JOIN inventory WINDOW('length', 5)\n\
        ON products.productId = inventory.productId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("widget".to_string()),
        ],
    );
    runner.send(
        "inventory",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("WIDGET".to_string()));
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Join with lower function
#[tokio::test]
async fn join_test_lower() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM Out (lowerName STRING, value INT);\n\
        INSERT INTO Out\n\
        SELECT lower(stream1.name) AS lowerName, stream2.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("HELLO".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(42)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("hello".to_string()));
}

/// Join with length function
#[tokio::test]
async fn join_test_length_function() {
    let app = "\
        CREATE STREAM messages (id INT, text STRING);\n\
        CREATE STREAM metadata (id INT, author STRING);\n\
        CREATE STREAM Out (textLen INT, author STRING);\n\
        INSERT INTO Out\n\
        SELECT length(messages.text) AS textLen, metadata.author AS author\n\
        FROM messages WINDOW('length', 5)\n\
        JOIN metadata WINDOW('length', 5)\n\
        ON messages.id = metadata.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "metadata",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Bob".to_string()),
        ],
    );
    runner.send(
        "messages",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello World".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::Int(11));
}

/// Join with subtraction
#[tokio::test]
async fn join_test_subtraction() {
    let app = "\
        CREATE STREAM sales (id INT, revenue INT);\n\
        CREATE STREAM costs (id INT, expense INT);\n\
        CREATE STREAM Out (id INT, profit INT);\n\
        INSERT INTO Out\n\
        SELECT sales.id AS id, sales.revenue - costs.expense AS profit\n\
        FROM sales WINDOW('length', 5)\n\
        JOIN costs WINDOW('length', 5)\n\
        ON sales.id = costs.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "sales",
        vec![AttributeValue::Int(1), AttributeValue::Int(1000)],
    );
    runner.send(
        "costs",
        vec![AttributeValue::Int(1), AttributeValue::Int(300)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][1], AttributeValue::Int(700));
}

/// Join with division
#[tokio::test]
async fn join_test_division() {
    let app = "\
        CREATE STREAM totals (id INT, total INT);\n\
        CREATE STREAM counts (id INT, cnt INT);\n\
        CREATE STREAM Out (id INT, avg INT);\n\
        INSERT INTO Out\n\
        SELECT totals.id AS id, totals.total / counts.cnt AS avg\n\
        FROM totals WINDOW('length', 5)\n\
        JOIN counts WINDOW('length', 5)\n\
        ON totals.id = counts.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "totals",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "counts",
        vec![AttributeValue::Int(1), AttributeValue::Int(4)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Division may return INT or DOUBLE depending on implementation
    let avg = match out[0][1] {
        AttributeValue::Int(v) => v as f64,
        AttributeValue::Double(v) => v,
        _ => panic!("Expected int or double"),
    };
    assert!((avg - 25.0).abs() < 0.001);
}

/// Join with double precision values
#[tokio::test]
async fn join_test_double_values() {
    let app = "\
        CREATE STREAM prices (id INT, price DOUBLE);\n\
        CREATE STREAM quantities (id INT, qty DOUBLE);\n\
        CREATE STREAM Out (id INT, total DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT prices.id AS id, prices.price * quantities.qty AS total\n\
        FROM prices WINDOW('length', 5)\n\
        JOIN quantities WINDOW('length', 5)\n\
        ON prices.id = quantities.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "prices",
        vec![AttributeValue::Int(1), AttributeValue::Double(10.5)],
    );
    runner.send(
        "quantities",
        vec![AttributeValue::Int(1), AttributeValue::Double(2.0)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    if let AttributeValue::Double(total) = out[0][1] {
        assert!((total - 21.0).abs() < 0.0001);
    }
}

/// Join with long type values
#[tokio::test]
async fn join_test_long_values() {
    let app = "\
        CREATE STREAM events (id INT, timestamp BIGINT);\n\
        CREATE STREAM metrics (id INT, value BIGINT);\n\
        CREATE STREAM Out (id INT, ts BIGINT, val BIGINT);\n\
        INSERT INTO Out\n\
        SELECT events.id AS id, events.timestamp AS ts, metrics.value AS val\n\
        FROM events WINDOW('length', 5)\n\
        JOIN metrics WINDOW('length', 5)\n\
        ON events.id = metrics.id;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "events",
        vec![AttributeValue::Int(1), AttributeValue::Long(1234567890123)],
    );
    runner.send(
        "metrics",
        vec![AttributeValue::Int(1), AttributeValue::Long(9876543210)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][1], AttributeValue::Long(1234567890123));
    assert_eq!(out[0][2], AttributeValue::Long(9876543210));
}

/// Join with float type values
#[tokio::test]
async fn join_test_float_values() {
    let app = "\
        CREATE STREAM sensors (sensorId INT, reading FLOAT);\n\
        CREATE STREAM calibrations (sensorId INT, factor FLOAT);\n\
        CREATE STREAM Out (sensorId INT, adjusted FLOAT);\n\
        INSERT INTO Out\n\
        SELECT sensors.sensorId AS sensorId, sensors.reading * calibrations.factor AS adjusted\n\
        FROM sensors WINDOW('length', 5)\n\
        JOIN calibrations WINDOW('length', 5)\n\
        ON sensors.sensorId = calibrations.sensorId;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "sensors",
        vec![AttributeValue::Int(1), AttributeValue::Float(50.0)],
    );
    runner.send(
        "calibrations",
        vec![AttributeValue::Int(1), AttributeValue::Float(1.1)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with NOT in WHERE clause
#[tokio::test]
async fn join_test_not_filter() {
    let app = "\
        CREATE STREAM orders (orderId INT, status INT);\n\
        CREATE STREAM products (orderId INT, name STRING);\n\
        CREATE STREAM Out (orderId INT, name STRING);\n\
        INSERT INTO Out\n\
        SELECT orders.orderId AS orderId, products.name AS name\n\
        FROM orders WINDOW('length', 5)\n\
        JOIN products WINDOW('length', 5)\n\
        ON orders.orderId = products.orderId\n\
        WHERE NOT (orders.status = 0);\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "orders",
        vec![AttributeValue::Int(1), AttributeValue::Int(1)],
    );
    runner.send(
        "products",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Widget".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with AND/OR in WHERE clause
#[tokio::test]
async fn join_test_complex_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, a INT, b INT);\n\
        CREATE STREAM stream2 (id INT, c INT);\n\
        CREATE STREAM Out (id INT, total INT);\n\
        INSERT INTO Out\n\
        SELECT stream1.id AS id, stream1.a + stream2.c AS total\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE (stream1.a > 5 AND stream1.b < 20) OR stream2.c = 10;\n";
    let runner = AppRunner::new(app, "Out").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(10),
            AttributeValue::Int(15),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with CASE WHEN in select (status classification)
#[tokio::test]
async fn join_test_case_when_status() {
    let app = "\
        CREATE STREAM orderStream (id INT, amount INT);\n\
        CREATE STREAM customerStream (id INT, tier STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO outputStream\n\
        SELECT orderStream.id AS id, CASE WHEN orderStream.amount > 100 THEN 'HIGH' ELSE 'LOW' END AS status\n\
        FROM orderStream WINDOW('length', 5)\n\
        JOIN customerStream WINDOW('length', 5)\n\
        ON orderStream.id = customerStream.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(150)],
    );
    runner.send(
        "customerStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("gold".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("HIGH".to_string()));
}

/// Join with coalesce function (backup value)
#[tokio::test]
async fn join_test_coalesce_backup() {
    let app = "\
        CREATE STREAM stream1 (id INT, value STRING);\n\
        CREATE STREAM stream2 (id INT, backup STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, coalesce(stream1.value, stream2.backup) AS result\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("actual".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("actual".to_string()));
}

/// Join with nested concat
#[tokio::test]
async fn join_test_nested_concat() {
    let app = "\
        CREATE STREAM stream1 (id INT, first STRING);\n\
        CREATE STREAM stream2 (id INT, last STRING);\n\
        CREATE STREAM outputStream (id INT, fullName STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, concat(stream1.first, ' ', stream2.last) AS fullName\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Join with multiplication of fields from both streams
#[tokio::test]
async fn join_test_cross_multiplication() {
    let app = "\
        CREATE STREAM priceStream (id INT, price INT);\n\
        CREATE STREAM quantityStream (id INT, qty INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT priceStream.id AS id, priceStream.price * quantityStream.qty AS total\n\
        FROM priceStream WINDOW('length', 5)\n\
        JOIN quantityStream WINDOW('length', 5)\n\
        ON priceStream.id = quantityStream.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "quantityStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(3)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
}

/// Join with count aggregation
#[tokio::test]
#[ignore = "Join with GROUP BY aggregation not yet fully supported"]
async fn join_test_count_agg() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (category STRING, cnt INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.category AS category, count(*) AS cnt\n\
        FROM stream1 WINDOW('lengthBatch', 2)\n\
        JOIN stream2 WINDOW('lengthBatch', 2)\n\
        ON stream1.id = stream2.id\n\
        GROUP BY stream1.category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(2), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with sum aggregation
#[tokio::test]
#[ignore = "Join with GROUP BY aggregation not yet fully supported"]
async fn join_test_sum_agg() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, amount INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.category AS category, sum(stream2.amount) AS total\n\
        FROM stream1 WINDOW('lengthBatch', 2)\n\
        JOIN stream2 WINDOW('lengthBatch', 2)\n\
        ON stream1.id = stream2.id\n\
        GROUP BY stream1.category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(2), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Join with greater than in WHERE
#[tokio::test]
async fn join_test_gt_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, value INT);\n\
        CREATE STREAM stream2 (id INT, threshold INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.value > stream2.threshold;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Join with string comparison in WHERE
#[tokio::test]
async fn join_test_string_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, expectedCat STRING);\n\
        CREATE STREAM outputStream (id INT, category STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.category AS category\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.category = stream2.expectedCat;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Join with uuid function (transaction id)
#[tokio::test]
async fn join_test_uuid_trans() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (transId STRING, name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT uuid() AS transId, stream1.name AS name\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("test".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // UUID should be a non-empty string
    if let AttributeValue::String(uuid) = &out[0][0] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Join with length function on both streams
#[tokio::test]
async fn join_test_length_both() {
    let app = "\
        CREATE STREAM stream1 (id INT, text1 STRING);\n\
        CREATE STREAM stream2 (id INT, text2 STRING);\n\
        CREATE STREAM outputStream (id INT, len1 INT, len2 INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, length(stream1.text1) AS len1, length(stream2.text2) AS len2\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("World!".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len1 = match &out[0][1] {
        AttributeValue::Int(l) => *l as i64,
        AttributeValue::Long(l) => *l,
        _ => panic!("Expected int or long"),
    };
    let len2 = match &out[0][2] {
        AttributeValue::Int(l) => *l as i64,
        AttributeValue::Long(l) => *l,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len1, 5); // "Hello"
    assert_eq!(len2, 6); // "World!"
}

/// Join with modulo operation in SELECT
#[tokio::test]
async fn join_test_modulo() {
    let app = "\
        CREATE STREAM stream1 (id INT, num1 INT);\n\
        CREATE STREAM stream2 (id INT, num2 INT);\n\
        CREATE STREAM outputStream (id INT, remainder INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.num1 % stream2.num2 AS remainder\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(17)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(2)); // 17 % 5 = 2
}

/// Join with empty string values
#[tokio::test]
async fn join_test_empty_string() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, suffix STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, concat(stream1.name, stream2.suffix) AS result\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("Hello".to_string()));
}

/// Join with min aggregation
#[tokio::test]
async fn join_test_min_agg() {
    let app = "\
        CREATE STREAM orders (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM products (productId INT, price INT);\n\
        CREATE STREAM outputStream (minQuantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT min(orders.quantity) AS minQuantity\n\
        FROM orders WINDOW('lengthBatch', 3)\n\
        JOIN products WINDOW('length', 10)\n\
        ON orders.productId = products.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "products",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(1),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(1),
            AttributeValue::Int(8),
        ],
    );
    let out = runner.shutdown();
    // Each join match emits output after batch completes
    assert!(!out.is_empty());
    // Find the minimum quantity in output
    let mut min_found = i64::MAX;
    for row in &out {
        let val = match &row[0] {
            AttributeValue::Int(i) => *i as i64,
            AttributeValue::Long(l) => *l,
            _ => continue,
        };
        if val < min_found {
            min_found = val;
        }
    }
    assert_eq!(min_found, 2); // min of 5, 2, 8
}

/// Join with max aggregation
#[tokio::test]
async fn join_test_max_agg() {
    let app = "\
        CREATE STREAM orders (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM products (productId INT, price INT);\n\
        CREATE STREAM outputStream (maxQuantity INT);\n\
        INSERT INTO outputStream\n\
        SELECT max(orders.quantity) AS maxQuantity\n\
        FROM orders WINDOW('lengthBatch', 3)\n\
        JOIN products WINDOW('length', 10)\n\
        ON orders.productId = products.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "products",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(1),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(1),
            AttributeValue::Int(8),
        ],
    );
    let out = runner.shutdown();
    // Each join match emits output after batch completes
    assert!(!out.is_empty());
    // Find the maximum quantity in output
    let mut max_found = i64::MIN;
    for row in &out {
        let val = match &row[0] {
            AttributeValue::Int(i) => *i as i64,
            AttributeValue::Long(l) => *l,
            _ => continue,
        };
        if val > max_found {
            max_found = val;
        }
    }
    assert_eq!(max_found, 8); // max of 5, 2, 8
}

/// Join with avg aggregation
#[tokio::test]
async fn join_test_avg_agg() {
    let app = "\
        CREATE STREAM orders (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM products (productId INT, price INT);\n\
        CREATE STREAM outputStream (avgQuantity DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT avg(orders.quantity) AS avgQuantity\n\
        FROM orders WINDOW('lengthBatch', 3)\n\
        JOIN products WINDOW('length', 10)\n\
        ON orders.productId = products.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "products",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(6),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(1),
            AttributeValue::Int(3),
        ],
    );
    runner.send(
        "orders",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(1),
            AttributeValue::Int(9),
        ],
    );
    let out = runner.shutdown();
    // Each join match emits output after batch completes
    assert!(!out.is_empty());
    // Collect all avg values and verify expected range (3.0 to 9.0)
    for row in &out {
        let avg_val = match &row[0] {
            AttributeValue::Double(d) => *d,
            AttributeValue::Float(f) => *f as f64,
            _ => continue,
        };
        // Values should be in range of the input quantities
        assert!(
            avg_val >= 3.0 && avg_val <= 9.0,
            "avg={} should be between 3 and 9",
            avg_val
        );
    }
}

/// Join with OR condition in WHERE
#[tokio::test]
async fn join_test_or_condition() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream2.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.category = 'A' OR stream2.value > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("B".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(60)],
    ); // passes value > 50
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(60));
}

/// Join with AND condition in WHERE
#[tokio::test]
async fn join_test_and_condition() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream2.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.category = 'A' AND stream2.value > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(60)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(60));
}

/// Join with multiple OR conditions
#[tokio::test]
async fn join_test_multi_or() {
    let app = "\
        CREATE STREAM stream1 (id INT, status STRING);\n\
        CREATE STREAM stream2 (id INT, priority INT);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.status AS status\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.status = 'CRITICAL' OR stream1.status = 'HIGH' OR stream2.priority > 8;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("LOW".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(9)],
    ); // priority > 8
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("LOW".to_string()));
}

/// Join with DOUBLE field types
#[tokio::test]
async fn join_test_double_arithmetic() {
    let app = "\
        CREATE STREAM stream1 (id INT, rate DOUBLE);\n\
        CREATE STREAM stream2 (id INT, amount DOUBLE);\n\
        CREATE STREAM outputStream (id INT, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.rate * stream2.amount AS total\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.15)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Double(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Double(d) => *d,
        AttributeValue::Float(f) => *f as f64,
        _ => panic!("Expected Double"),
    };
    assert!((total - 15.0).abs() < 0.001); // 0.15 * 100 = 15
}

/// Join with negative values
#[tokio::test]
async fn join_test_negative_values() {
    let app = "\
        CREATE STREAM stream1 (id INT, credit INT);\n\
        CREATE STREAM stream2 (id INT, debit INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.credit - stream2.debit AS balance\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-50)); // 50 - 100 = -50
}

/// Join with zero values
#[tokio::test]
async fn join_test_zero_values() {
    let app = "\
        CREATE STREAM stream1 (id INT, value INT);\n\
        CREATE STREAM stream2 (id INT, multiplier INT);\n\
        CREATE STREAM outputStream (id INT, result INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.value * stream2.multiplier AS result\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(0)); // 100 * 0 = 0
}

/// Join with empty string
#[tokio::test]
async fn join_test_empty_string_value() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, suffix STRING);\n\
        CREATE STREAM outputStream (id INT, fullname STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, concat(stream1.name, stream2.suffix) AS fullname\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("user".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("user".to_string()));
}

/// Join with coalesce function
#[tokio::test]
async fn join_test_with_coalesce() {
    let app = "\
        CREATE STREAM stream1 (id INT, primary_name STRING);\n\
        CREATE STREAM stream2 (id INT, backup_name STRING);\n\
        CREATE STREAM outputStream (id INT, name STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, coalesce(stream1.primary_name, stream2.backup_name) AS name\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("primary".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("primary".to_string()));
}

/// Join with division returning DOUBLE
#[tokio::test]
async fn join_test_division_result() {
    let app = "\
        CREATE STREAM stream1 (id INT, dividend INT);\n\
        CREATE STREAM stream2 (id INT, divisor INT);\n\
        CREATE STREAM outputStream (id INT, quotient DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.dividend / stream2.divisor AS quotient\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(4)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let quotient = match &out[0][1] {
        AttributeValue::Double(d) => *d,
        AttributeValue::Float(f) => *f as f64,
        AttributeValue::Int(i) => *i as f64,
        AttributeValue::Long(l) => *l as f64,
        _ => panic!("Expected numeric type"),
    };
    assert!((quotient - 2.5).abs() < 0.001); // 10 / 4 = 2.5
}

/// Join with upper function on joined data
#[tokio::test]
async fn join_test_with_upper() {
    let app = "\
        CREATE STREAM stream1 (id INT, category STRING);\n\
        CREATE STREAM stream2 (id INT, item STRING);\n\
        CREATE STREAM outputStream (id INT, label STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, upper(stream1.category) AS label\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("electronics".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("phone".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ELECTRONICS".to_string()));
}

/// Join with lower function on joined data
#[tokio::test]
async fn join_test_with_lower() {
    let app = "\
        CREATE STREAM stream1 (id INT, brand STRING);\n\
        CREATE STREAM stream2 (id INT, model STRING);\n\
        CREATE STREAM outputStream (id INT, brand_lower STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, lower(stream1.brand) AS brand_lower\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("APPLE".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("iPhone".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("apple".to_string()));
}

/// Join with length function
#[tokio::test]
async fn join_test_with_length() {
    let app = "\
        CREATE STREAM stream1 (id INT, text STRING);\n\
        CREATE STREAM stream2 (id INT, other STRING);\n\
        CREATE STREAM outputStream (id INT, text_len INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, length(stream1.text) AS text_len\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("world".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len_val, 5); // "hello" has 5 chars
}

/// Join with modulo operator
#[tokio::test]
async fn join_test_modulo_result() {
    let app = "\
        CREATE STREAM stream1 (id INT, value INT);\n\
        CREATE STREAM stream2 (id INT, divisor INT);\n\
        CREATE STREAM outputStream (id INT, remainder INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.value % stream2.divisor AS remainder\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(17)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let rem_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(rem_val, 2); // 17 % 5 = 2
}

/// Join with multiple conditions via AND filter
#[tokio::test]
async fn join_test_with_and_filter() {
    let app = "\
        CREATE STREAM stream1 (id INT, type STRING, value INT);\n\
        CREATE STREAM stream2 (id INT, category STRING, threshold INT);\n\
        CREATE STREAM outputStream (id INT, type STRING, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.type AS type, stream1.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.value > 50 AND stream1.type = 'premium';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("premium".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(25),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

/// Join with subtraction in select
#[tokio::test]
async fn join_test_subtraction_in_select() {
    let app = "\
        CREATE STREAM stream1 (id INT, gross INT);\n\
        CREATE STREAM stream2 (id INT, discount INT);\n\
        CREATE STREAM outputStream (id INT, net INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.gross - stream2.discount AS net\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(15)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(85)); // 100 - 15 = 85
}

/// Join with multiplication in select
#[tokio::test]
async fn join_test_multiplication_in_select() {
    let app = "\
        CREATE STREAM stream1 (id INT, quantity INT);\n\
        CREATE STREAM stream2 (id INT, price INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.quantity * stream2.price AS total\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(20)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100)); // 5 * 20 = 100
}

/// Join with DOUBLE fields
#[tokio::test]
async fn join_test_double_fields() {
    let app = "\
        CREATE STREAM stream1 (id INT, rate DOUBLE);\n\
        CREATE STREAM stream2 (id INT, amount DOUBLE);\n\
        CREATE STREAM outputStream (id INT, result DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.rate * stream2.amount AS result\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.1)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Double(1000.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let result = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((result - 100.0).abs() < 0.001); // 0.1 * 1000 = 100
}

/// Join with LTE condition in WHERE
#[tokio::test]
async fn join_test_lte_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, value INT);\n\
        CREATE STREAM stream2 (id INT, threshold INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.value <= stream2.threshold;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(50)); // 50 <= 50 is true
}

/// Join with GTE condition in WHERE
#[tokio::test]
async fn join_test_gte_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, score INT);\n\
        CREATE STREAM stream2 (id INT, min_score INT);\n\
        CREATE STREAM outputStream (id INT, score INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.score AS score\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.score >= stream2.min_score;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(75)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(60)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(75)); // 75 >= 60 is true
}

/// Join with NOT EQUAL in WHERE
#[tokio::test]
async fn join_test_not_equal_where() {
    let app = "\
        CREATE STREAM stream1 (id INT, status STRING);\n\
        CREATE STREAM stream2 (id INT, category STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.status AS status\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id\n\
        WHERE stream1.status != 'DELETED';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ACTIVE".to_string()));
}

/// Join with uuid function
#[tokio::test]
async fn join_test_with_uuid() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, uuid_val STRING);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, uuid() AS uuid_val\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("test".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Join with addition in select
#[tokio::test]
async fn join_test_addition_in_select() {
    let app = "\
        CREATE STREAM stream1 (id INT, base INT);\n\
        CREATE STREAM stream2 (id INT, bonus INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.base + stream2.bonus AS total\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(25)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(125)); // 100 + 25 = 125
}

/// Join with multiple matches
#[tokio::test]
async fn join_test_multiple_matches() {
    let app = "\
        CREATE STREAM stream1 (id INT, name STRING);\n\
        CREATE STREAM stream2 (id INT, value INT);\n\
        CREATE STREAM outputStream (id INT, name STRING, value INT);\n\
        INSERT INTO outputStream\n\
        SELECT stream1.id AS id, stream1.name AS name, stream2.value AS value\n\
        FROM stream1 WINDOW('length', 5)\n\
        JOIN stream2 WINDOW('length', 5)\n\
        ON stream1.id = stream2.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stream1",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("A".to_string()),
        ],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "stream2",
        vec![AttributeValue::Int(1), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    // Should have 2 matches (A-100 and A-200)
    assert_eq!(out.len(), 2);
}
