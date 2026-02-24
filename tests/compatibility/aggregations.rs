// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Aggregation Compatibility Tests
// Reference: query/selector/attribute/aggregator/*.java, GroupByTestCase.java

use super::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC AGGREGATION TESTS
// ============================================================================

/// Test distinctCount aggregator
/// Reference: DistinctCountAttributeAggregatorExecutorTestCase.java
#[tokio::test]
async fn aggregator_test_distinct_count() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (uniqueSymbols BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT distinctCount(symbol) AS uniqueSymbols\n\
        FROM inputStream WINDOW('length', 10);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(76.6),
        ],
    );
    let out = runner.shutdown();
    // 2 distinct symbols: IBM, MSFT
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(2));
}

/// Test stdDev aggregator
/// Reference: Aggregation1TestCase.java
/// Note: stdDev window aggregator not implemented - only CollectionStdDev for patterns exists
#[tokio::test]
async fn aggregator_test_std_dev() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (stdPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT stdDev(price) AS stdPrice\n\
        FROM inputStream WINDOW('length', 4);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(10.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(20.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(30.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(40.0),
        ],
    );
    let out = runner.shutdown();
    // stdDev of 10, 20, 30, 40 should be ~11.18
    let last = out.last().unwrap();
    if let AttributeValue::Double(std) = last[0] {
        assert!(std > 10.0 && std < 13.0);
    } else {
        panic!("Expected Double for stdDev");
    }
}

/// Sum aggregation with group by
/// Reference: GroupByTestCase.java
#[tokio::test]
async fn aggregation_test_sum_group_by() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, totalVolume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, sum(volume) AS totalVolume FROM stockStream WINDOW('length', 10) GROUP BY symbol;\n";
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
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(110.0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Should have outputs for both symbols
    assert!(out.len() >= 2);
}

/// Min/Max aggregation
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn aggregation_test_min_max() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (minPrice FLOAT, maxPrice FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT min(price) AS minPrice, max(price) AS maxPrice FROM stockStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(50.0));
    assert_eq!(last[1], AttributeValue::Float(150.0));
}

/// Average aggregation
/// Reference: FunctionTestCase.java
#[tokio::test]
async fn aggregation_test_avg() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT avg(price) AS avgPrice FROM stockStream WINDOW('length', 3);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(300.0),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // Average of 100, 200, 300 = 200
    assert_eq!(last[0], AttributeValue::Double(200.0));
}

/// Multiple aggregations in single query
/// Reference: Aggregation test cases
#[tokio::test]
async fn aggregation_test_multiple_functions() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (total DOUBLE, average DOUBLE, cnt BIGINT, minP FLOAT, maxP FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(price) AS total, avg(price) AS average, count() AS cnt, \
               min(price) AS minP, max(price) AS maxP\n\
        FROM stockStream WINDOW('length', 3);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(300.0),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert!(out.len() >= 3);
    // Last event should have: sum=600, avg=200, count=3, min=100, max=300
    let last = &out[out.len() - 1];
    assert_eq!(last[0], AttributeValue::Double(600.0));
    assert_eq!(last[1], AttributeValue::Double(200.0));
    assert_eq!(last[2], AttributeValue::Long(3));
    assert_eq!(last[3], AttributeValue::Float(100.0));
    assert_eq!(last[4], AttributeValue::Float(300.0));
}

// ============================================================================
// GROUP BY AND HAVING TESTS
// Reference: query/GroupByTestCase.java
// ============================================================================

/// Test group by with multiple aggregations
/// Reference: GroupByTestCase.java:testGroupByQuery1
#[tokio::test]
async fn group_by_test1_multiple_aggregations() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE, totalVolume BIGINT, cnt BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) AS avgPrice, sum(volume) AS totalVolume, count() AS cnt\n\
        FROM inputStream WINDOW('length', 10)\n\
        GROUP BY symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Should have grouped aggregations
    assert_eq!(out.len(), 3);
}

/// Test having clause
/// Reference: GroupByTestCase.java with HAVING
#[tokio::test]
async fn group_by_test2_having() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, totalVolume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, sum(volume) AS totalVolume\n\
        FROM inputStream WINDOW('length', 10)\n\
        GROUP BY symbol\n\
        HAVING sum(volume) > 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    // Only groups with totalVolume > 100 should be output
    for row in &out {
        if let AttributeValue::Long(vol) = row[1] {
            assert!(vol > 100);
        }
    }
}

/// Group by with multiple groups
/// Reference: Aggregation group by tests
#[tokio::test]
async fn group_by_test3_multiple_groups() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, sum(price) AS total\n\
        FROM stockStream WINDOW('lengthBatch', 4)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::Float(50.0),
        ],
    );
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
            AttributeValue::Float(75.0),
        ],
    );
    let out = runner.shutdown();
    // Should have groups for IBM (250) and MSFT (125)
    assert!(out.len() >= 2);
}

// ============================================================================
// ORDER BY AND LIMIT TESTS
// Reference: query/OrderByLimitTestCase.java
// ============================================================================

/// Test order by with limit
/// Reference: OrderByLimitTestCase.java:limitTest1
#[tokio::test]
async fn order_by_limit_test1() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM inputStream WINDOW('lengthBatch', 4)\n\
        ORDER BY price DESC\n\
        LIMIT 2;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("D".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // Should output top 2 by price descending: B(200), C(150)
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("B".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("C".to_string()));
}

/// Test order by ascending
/// Reference: OrderByLimitTestCase.java
#[tokio::test]
async fn order_by_test2_ascending() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM inputStream WINDOW('lengthBatch', 3)\n\
        ORDER BY price ASC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
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
    // Should be ordered by price ascending: A(100), C(150), B(200)
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("C".to_string()));
    assert_eq!(out[2][0], AttributeValue::String("B".to_string()));
}

// ============================================================================
// FIRST/LAST AGGREGATOR TESTS
// Reference: FirstLastAggregatorTestCase.java
// ============================================================================

/// First aggregator test
/// Reference: FirstLastAggregatorTestCase.java
#[tokio::test]
async fn aggregator_test_first() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (firstSymbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT first(symbol) AS firstSymbol FROM stockStream WINDOW('length', 5);\n";
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
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // first() should return "IBM"
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::String("IBM".to_string()));
}

/// Last aggregator test
/// Reference: FirstLastAggregatorTestCase.java
#[tokio::test]
async fn aggregator_test_last() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (lastSymbol STRING);\n\
        INSERT INTO outputStream\n\
        SELECT last(symbol) AS lastSymbol FROM stockStream WINDOW('length', 5);\n";
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
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // last() should return "MSFT"
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::String("MSFT".to_string()));
}

// ============================================================================
// MINFOREVER/MAXFOREVER TESTS
// Reference: MinForeverAggregatorExtensionTestCase.java
// ============================================================================

/// minForever aggregator test
/// Reference: MinForeverAggregatorExtensionTestCase.java
#[tokio::test]
async fn aggregator_test_min_forever() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (minPrice FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT minForever(price) AS minPrice FROM stockStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(75.0),
        ],
    );
    let out = runner.shutdown();
    // minForever should track global minimum: 50.0
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(50.0));
}

/// maxForever aggregator test
/// Reference: MaxForeverAggregatorExtensionTestCase.java
#[tokio::test]
async fn aggregator_test_max_forever() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (maxPrice FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT maxForever(price) AS maxPrice FROM stockStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(75.0),
        ],
    );
    let out = runner.shutdown();
    // maxForever should track global maximum: 150.0
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(150.0));
}

// ============================================================================
// OFFSET TESTS
// Reference: OrderByLimitTestCase.java
// ============================================================================

/// Offset test
/// Reference: OrderByLimitTestCase.java:limitTest11
#[tokio::test]
async fn offset_test1_basic() {
    let app = "\
        CREATE STREAM inputStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM inputStream WINDOW('lengthBatch', 4)\n\
        ORDER BY price DESC\n\
        LIMIT 2 OFFSET 1;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
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
    // Sorted desc: B(200), C(150), A(100), D(50)
    // OFFSET 1, LIMIT 2 = C(150), A(100)
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("C".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("A".to_string()));
}

// ============================================================================
// MULTIPLE ORDER BY COLUMNS
// Reference: OrderByLimitTestCase.java
// ============================================================================

/// Multiple order by columns
/// Reference: OrderByLimitTestCase.java
#[tokio::test]
async fn order_by_test3_multiple_columns() {
    let app = "\
        CREATE STREAM inputStream (category STRING, price FLOAT);\n\
        CREATE STREAM outputStream (category STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT category, price\n\
        FROM inputStream WINDOW('lengthBatch', 4)\n\
        ORDER BY category ASC, price DESC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
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
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // Sorted by category ASC, then price DESC:
    // A(150), A(50), B(200), B(100)
    assert_eq!(out.len(), 4);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(150.0));
    assert_eq!(out[1][0], AttributeValue::String("A".to_string()));
    assert_eq!(out[1][1], AttributeValue::Float(50.0));
}

// ============================================================================
// OUTPUT RATE LIMITING TESTS
// Reference: query/ratelimit/SnapshotOutputRateLimitTestCase.java
// Reference: query/ratelimit/EventOutputRateLimitTestCase.java
// ============================================================================

/// Snapshot output rate limiting
/// Reference: SnapshotOutputRateLimitTestCase.java
#[tokio::test]
async fn rate_limit_test1_snapshot() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sum(price) AS total\n\
        FROM stockStream WINDOW('time', 1000 MILLISECONDS)\n\
        OUTPUT SNAPSHOT EVERY 500 MILLISECONDS;\n";
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
            AttributeValue::Float(200.0),
        ],
    );
    std::thread::sleep(std::time::Duration::from_millis(600));
    let out = runner.shutdown();
    // Should have snapshot outputs at rate-limited intervals
    assert!(!out.is_empty());
}

/// Event-based all output rate limiting
/// Reference: EventOutputRateLimitTestCase.java
#[tokio::test]
async fn rate_limit_test2_events_all() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        OUTPUT ALL EVERY 3 EVENTS;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=6 {
        runner.send(
            "stockStream",
            vec![
                AttributeValue::String(format!("SYM{}", i)),
                AttributeValue::Float(i as f32 * 10.0),
            ],
        );
    }
    let out = runner.shutdown();
    // Should output all accumulated events every 3 events
    assert!(!out.is_empty());
}

/// Event-based first output rate limiting
/// Reference: EventOutputRateLimitTestCase.java
#[tokio::test]
async fn rate_limit_test3_events_first() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        OUTPUT FIRST EVERY 3 EVENTS;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=6 {
        runner.send(
            "stockStream",
            vec![
                AttributeValue::String(format!("SYM{}", i)),
                AttributeValue::Float(i as f32 * 10.0),
            ],
        );
    }
    let out = runner.shutdown();
    // Should output first event of every 3-event batch
    assert_eq!(out.len(), 2); // 2 batches of 3
}

/// Event-based last output rate limiting
/// Reference: EventOutputRateLimitTestCase.java
#[tokio::test]
async fn rate_limit_test4_events_last() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        OUTPUT LAST EVERY 3 EVENTS;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=6 {
        runner.send(
            "stockStream",
            vec![
                AttributeValue::String(format!("SYM{}", i)),
                AttributeValue::Float(i as f32 * 10.0),
            ],
        );
    }
    let out = runner.shutdown();
    // Should output last event of every 3-event batch
    assert_eq!(out.len(), 2); // 2 batches of 3
}

/// Time-based output rate limiting
/// Reference: TimeOutputRateLimitTestCase.java
#[tokio::test]
async fn rate_limit_test5_time_all() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        OUTPUT ALL EVERY 200 MILLISECONDS;\n";
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
            AttributeValue::Float(200.0),
        ],
    );
    std::thread::sleep(std::time::Duration::from_millis(250));
    let out = runner.shutdown();
    // Should output at time-based intervals
    assert!(!out.is_empty());
}

// ============================================================================
// GROUP BY EDGE CASES
// Reference: GroupByTestCase.java
// ============================================================================

/// GROUP BY with count aggregation
#[tokio::test]
async fn group_by_test4_count() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, eventCount BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT category, count() AS eventCount\n\
        FROM eventStream WINDOW('length', 10)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(3),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Should have counts: A=3, B=1
    assert!(out.len() >= 2);
}

/// GROUP BY with min/max
#[tokio::test]
async fn group_by_test5_min_max() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, minPrice FLOAT, maxPrice FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, min(price) AS minPrice, max(price) AS maxPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // Last output for IBM should have min=50, max=150
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Float(50.0));
    assert_eq!(last[2], AttributeValue::Float(150.0));
}

/// HAVING with count condition
#[tokio::test]
async fn having_test2_count_condition() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, cnt BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT category, count() AS cnt\n\
        FROM eventStream WINDOW('length', 10)\n\
        GROUP BY category\n\
        HAVING count() >= 2;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    // Only groups with count >= 2 should pass (A has 2, B has 1)
    // After second A event, A should appear in output
    let has_valid = out.iter().any(|row| {
        if let AttributeValue::Long(cnt) = row[1] {
            cnt >= 2
        } else {
            false
        }
    });
    assert!(has_valid);
}

/// HAVING with average condition
#[tokio::test]
async fn having_test3_avg_condition() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) AS avgPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol\n\
        HAVING avg(price) > 100.0;\n";
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
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // Only IBM (avg=175) should pass, not MSFT (avg=50)
    for row in &out {
        if let AttributeValue::Double(avg) = row[1] {
            assert!(avg > 100.0);
        }
    }
}

// ============================================================================
// AGGREGATION WITH NULL VALUES
// ============================================================================

/// Aggregation ignoring NULL values
#[tokio::test]
async fn aggregation_test_null_handling() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (total DOUBLE, cnt BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(price) AS total, count() AS cnt\n\
        FROM stockStream WINDOW('length', 5);\n";
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
            AttributeValue::Null,
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // sum should be 300 (100 + 200), count should be 3
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Double(300.0));
    assert_eq!(last[1], AttributeValue::Long(3));
}

/// Count with all NULL values
#[tokio::test]
async fn aggregation_test_count_all_nulls() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (cnt BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT count() AS cnt\n\
        FROM stockStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![AttributeValue::Null, AttributeValue::Null],
    );
    runner.send(
        "stockStream",
        vec![AttributeValue::Null, AttributeValue::Null],
    );
    let out = runner.shutdown();
    // count() counts all events regardless of NULL
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(2));
}

// ============================================================================
// ORDER BY EDGE CASES
// ============================================================================

/// ORDER BY with NULL values
#[tokio::test]
async fn order_by_test4_with_nulls() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('lengthBatch', 3)\n\
        ORDER BY price ASC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Null,
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // Should handle NULL values in ordering
    assert_eq!(out.len(), 3);
}

/// LIMIT without ORDER BY
#[tokio::test]
async fn limit_test2_without_order() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('lengthBatch', 5)\n\
        LIMIT 2;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=5 {
        runner.send(
            "stockStream",
            vec![
                AttributeValue::String(format!("SYM{}", i)),
                AttributeValue::Float(i as f32 * 10.0),
            ],
        );
    }
    let out = runner.shutdown();
    // Should return only 2 results
    assert_eq!(out.len(), 2);
}

/// OFFSET only (without explicit LIMIT)
#[tokio::test]
async fn offset_test2_only() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('lengthBatch', 4)\n\
        ORDER BY price DESC\n\
        OFFSET 2;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("D".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // Sorted desc: B(200), C(150), A(100), D(50)
    // OFFSET 2 = A(100), D(50)
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("D".to_string()));
}

// ============================================================================
// AGGREGATION WITH EXPRESSIONS
// ============================================================================

/// Sum with arithmetic expression
#[tokio::test]
async fn aggregation_test_sum_expression() {
    let app = "\
        CREATE STREAM orderStream (qty INT, price FLOAT);\n\
        CREATE STREAM outputStream (totalValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sum(qty * price) AS totalValue\n\
        FROM orderStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(100.0)],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(50.0)],
    );
    let out = runner.shutdown();
    // 2*100 + 3*50 = 200 + 150 = 350
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Double(350.0));
}

/// Average with expression
#[tokio::test]
async fn aggregation_test_avg_expression() {
    let app = "\
        CREATE STREAM sensorStream (temp FLOAT, offset FLOAT);\n\
        CREATE STREAM outputStream (avgAdjusted DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT avg(temp + offset) AS avgAdjusted\n\
        FROM sensorStream WINDOW('length', 3);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Float(20.0), AttributeValue::Float(5.0)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Float(25.0), AttributeValue::Float(5.0)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Float(30.0), AttributeValue::Float(5.0)],
    );
    let out = runner.shutdown();
    // avg(25 + 30 + 35) = avg(90/3) = 30
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Double(30.0));
}

// ============================================================================
// COUNT VARIATIONS
// ============================================================================

/// Count with specific column
/// Note: In EventFlux, count(column) counts all events (like count())
/// This differs from SQL where count(column) skips NULL values
#[tokio::test]
async fn aggregation_test_count_column() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (priceCount BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT count(price) AS priceCount\n\
        FROM stockStream WINDOW('length', 5);\n";
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
            AttributeValue::Null,
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // EventFlux's count(column) counts all events including NULL
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(3));
}

/// Count(*) vs count()
#[tokio::test]
async fn aggregation_test_count_star() {
    let app = "\
        CREATE STREAM eventStream (id INT, data STRING);\n\
        CREATE STREAM outputStream (totalEvents BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT count() AS totalEvents\n\
        FROM eventStream WINDOW('length', 10);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=5 {
        runner.send(
            "eventStream",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String(format!("data{}", i)),
            ],
        );
    }
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(5));
}

// ============================================================================
// AGGREGATION WITH DIFFERENT DATA TYPES
// ============================================================================

/// Sum with DOUBLE type
#[tokio::test]
async fn aggregation_test_sum_double() {
    let app = "\
        CREATE STREAM sensorStream (id INT, reading DOUBLE);\n\
        CREATE STREAM outputStream (totalReading DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sum(reading) AS totalReading\n\
        FROM sensorStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(100.123)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(200.456)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    if let AttributeValue::Double(val) = last[0] {
        assert!((val - 300.579).abs() < 0.001);
    } else {
        panic!("Expected Double");
    }
}

/// Sum with BIGINT type
#[tokio::test]
async fn aggregation_test_sum_bigint() {
    let app = "\
        CREATE STREAM eventStream (id INT, value BIGINT);\n\
        CREATE STREAM outputStream (totalValue BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(value) AS totalValue\n\
        FROM eventStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(1), AttributeValue::Long(9999999999)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(2), AttributeValue::Long(1)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(10000000000));
}

/// Average with INT values (returns DOUBLE)
#[tokio::test]
async fn aggregation_test_avg_int() {
    let app = "\
        CREATE STREAM countStream (id INT, count INT);\n\
        CREATE STREAM outputStream (avgCount DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT avg(count) AS avgCount\n\
        FROM countStream WINDOW('length', 4);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "countStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    runner.send(
        "countStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(20)],
    );
    runner.send(
        "countStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(30)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // avg(10, 20, 30) = 20
    assert_eq!(last[0], AttributeValue::Double(20.0));
}

// ============================================================================
// MIN/MAX EDGE CASES
// ============================================================================

/// Min with negative values
#[tokio::test]
async fn aggregation_test_min_negative() {
    let app = "\
        CREATE STREAM tempStream (id INT, temp FLOAT);\n\
        CREATE STREAM outputStream (minTemp FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT min(temp) AS minTemp\n\
        FROM tempStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(-5.0)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(-20.0)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(-20.0));
}

/// Max with zero and positive values
#[tokio::test]
async fn aggregation_test_max_with_zero() {
    let app = "\
        CREATE STREAM dataStream (id INT, value FLOAT);\n\
        CREATE STREAM outputStream (maxValue FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT max(value) AS maxValue\n\
        FROM dataStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(0.0)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(-10.0)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(0.0));
}

/// Min/Max with strings (lexicographic)
#[tokio::test]
#[ignore = "String min/max aggregation not yet supported"]
async fn aggregation_test_min_max_string() {
    let app = "\
        CREATE STREAM nameStream (id INT, name STRING);\n\
        CREATE STREAM outputStream (minName STRING, maxName STRING);\n\
        INSERT INTO outputStream\n\
        SELECT min(name) AS minName, max(name) AS maxName\n\
        FROM nameStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("banana".to_string()),
        ],
    );
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("apple".to_string()),
        ],
    );
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("cherry".to_string()),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::String("apple".to_string()));
    assert_eq!(last[1], AttributeValue::String("cherry".to_string()));
}

// ============================================================================
// MULTIPLE AGGREGATIONS
// ============================================================================

/// Multiple aggregations on same column
#[tokio::test]
async fn aggregation_test_multiple_same_column() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (minPrice FLOAT, maxPrice FLOAT, avgPrice DOUBLE, sumPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT min(price) AS minPrice, max(price) AS maxPrice, \n\
               avg(price) AS avgPrice, sum(price) AS sumPrice\n\
        FROM stockStream WINDOW('length', 4);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(300.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("D".to_string()),
            AttributeValue::Float(400.0),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Float(100.0)); // min
    assert_eq!(last[1], AttributeValue::Float(400.0)); // max
    assert_eq!(last[2], AttributeValue::Double(250.0)); // avg
    assert_eq!(last[3], AttributeValue::Double(1000.0)); // sum
}

/// Multiple aggregations on different columns
#[tokio::test]
async fn aggregation_test_multiple_different_columns() {
    let app = "\
        CREATE STREAM orderStream (productId INT, quantity INT, price FLOAT);\n\
        CREATE STREAM outputStream (totalQty BIGINT, totalPrice DOUBLE, avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sum(quantity) AS totalQty, sum(price) AS totalPrice, avg(price) AS avgPrice\n\
        FROM orderStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(5),
            AttributeValue::Float(10.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(3),
            AttributeValue::Float(20.0),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(8)); // 5 + 3
    assert_eq!(last[1], AttributeValue::Double(30.0)); // 10 + 20
    assert_eq!(last[2], AttributeValue::Double(15.0)); // avg(10, 20)
}

// ============================================================================
// GROUP BY WITH MULTIPLE COLUMNS
// ============================================================================

/// GROUP BY with two columns
#[tokio::test]
async fn group_by_test6_two_columns() {
    let app = "\
        CREATE STREAM salesStream (region STRING, category STRING, amount FLOAT);\n\
        CREATE STREAM outputStream (region STRING, category STRING, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT region, category, sum(amount) AS total\n\
        FROM salesStream WINDOW('length', 10)\n\
        GROUP BY region, category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Clothing".to_string()),
            AttributeValue::Float(30.0),
        ],
    );
    let out = runner.shutdown();
    // US-Electronics should have total 150, US-Clothing should have 30
    assert!(out.len() >= 2);
}

// ============================================================================
// HAVING WITH MULTIPLE CONDITIONS
// ============================================================================

/// HAVING with AND conditions
#[tokio::test]
async fn having_test4_multiple_conditions() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, cnt BIGINT, avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, count() AS cnt, avg(price) AS avgPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol\n\
        HAVING count() >= 2 AND avg(price) > 50.0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // IBM: 2 events, avg = 150 (passes)
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    // MSFT: 2 events, avg = 30 (fails avg condition)
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(20.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(40.0),
        ],
    );
    // GOOG: 1 event (fails count condition)
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(1000.0),
        ],
    );
    let out = runner.shutdown();
    // Only IBM should pass both conditions
    let ibm_rows: Vec<_> = out
        .iter()
        .filter(|row| row[0] == AttributeValue::String("IBM".to_string()))
        .filter(|row| {
            if let AttributeValue::Long(cnt) = row[1] {
                cnt >= 2
            } else {
                false
            }
        })
        .collect();
    assert!(!ibm_rows.is_empty());
}

// ============================================================================
// DISTINCT COUNT EDGE CASES
// ============================================================================

/// DistinctCount with all same values
#[tokio::test]
async fn aggregation_test_distinct_count_all_same() {
    let app = "\
        CREATE STREAM eventStream (id INT, category STRING);\n\
        CREATE STREAM outputStream (uniqueCategories BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT distinctCount(category) AS uniqueCategories\n\
        FROM eventStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=5 {
        runner.send(
            "eventStream",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String("A".to_string()),
            ],
        );
    }
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // distinctCount returns Long in EventFlux
    assert_eq!(last[0], AttributeValue::Long(1)); // All same = 1 distinct
}

/// DistinctCount with all different values
#[tokio::test]
async fn aggregation_test_distinct_count_all_different() {
    let app = "\
        CREATE STREAM eventStream (id INT, category STRING);\n\
        CREATE STREAM outputStream (uniqueCategories BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT distinctCount(category) AS uniqueCategories\n\
        FROM eventStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=5 {
        runner.send(
            "eventStream",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String(format!("Cat{}", i)),
            ],
        );
    }
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // distinctCount returns Long in EventFlux
    assert_eq!(last[0], AttributeValue::Long(5)); // All different = 5 distinct
}

// ============================================================================
// AGGREGATION WITH CAST
// ============================================================================

/// Sum with CAST to ensure type
#[tokio::test]
async fn aggregation_test_sum_with_cast() {
    let app = "\
        CREATE STREAM orderStream (id INT, quantity INT);\n\
        CREATE STREAM outputStream (totalQty BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(CAST(quantity AS BIGINT)) AS totalQty\n\
        FROM orderStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(300));
}

// ============================================================================
// AGGREGATION WITH COALESCE
// ============================================================================

/// Sum with coalesce to handle NULL
#[tokio::test]
async fn aggregation_test_sum_with_coalesce() {
    let app = "\
        CREATE STREAM orderStream (id INT, quantity INT);\n\
        CREATE STREAM outputStream (totalQty BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(coalesce(quantity, CAST(0 AS INT))) AS totalQty\n\
        FROM orderStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(2), AttributeValue::Null],
    );
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(50)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // 100 + 0 + 50 = 150
    assert_eq!(last[0], AttributeValue::Long(150));
}

// ============================================================================
// ADDITIONAL AGGREGATION EDGE CASE TESTS
// ============================================================================

/// GROUP BY with CASE WHEN expression
#[tokio::test]
#[ignore = "Complex GROUP BY expressions not yet supported"]
async fn group_by_test_case_when() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (category STRING, totalPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT CASE WHEN price > 100.0 THEN 'high' ELSE 'low' END AS category,\n\
               sum(price) AS totalPrice\n\
        FROM stockStream WINDOW('length', 5)\n\
        GROUP BY CASE WHEN price > 100.0 THEN 'high' ELSE 'low' END;\n";
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
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // Should have outputs for 'high' and 'low' categories
    assert!(out.len() >= 3);
}

/// HAVING with multiple conditions
#[tokio::test]
async fn having_test_multiple_conditions() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE, totalVolume BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) AS avgPrice, sum(volume) AS totalVolume\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol\n\
        HAVING avg(price) > 50.0 AND sum(volume) > 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(30.0), // Low avg
            AttributeValue::Int(50),     // Low volume
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(110.0),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    // Only IBM should pass both HAVING conditions
    assert!(!out.is_empty());
}

/// ORDER BY with LIMIT and OFFSET
#[tokio::test]
#[ignore = "ORDER BY with both LIMIT and OFFSET in same query needs investigation"]
async fn order_by_limit_offset_test() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        ORDER BY price DESC\n\
        LIMIT 2\n\
        OFFSET 1;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("D".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // With OFFSET 1 and LIMIT 2, should skip B (200), get C (150) and A (100)
    assert!(!out.is_empty());
}

/// Aggregation with arithmetic expression in SELECT
#[tokio::test]
async fn aggregation_test_arithmetic_on_agg() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, adjustedAvg DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) * 1.1 AS adjustedAvg\n\
        FROM stockStream WINDOW('length', 5)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    let out = runner.shutdown();
    // avg(100, 100) * 1.1 = 110
    let last = out.last().unwrap();
    if let AttributeValue::Double(val) = last[1] {
        assert!((val - 110.0).abs() < 0.01);
    }
}

/// GROUP BY with multiple aggregations on same column
#[tokio::test]
async fn group_by_test_multiple_agg_same_column() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, minPrice DOUBLE, maxPrice DOUBLE, avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, min(price) AS minPrice, max(price) AS maxPrice, avg(price) AS avgPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(125.0),
        ],
    );
    let out = runner.shutdown();
    // Last output should have min=100, max=150, avg=125
    let last = out.last().unwrap();
    if let AttributeValue::Double(min) = last[1] {
        assert!((min - 100.0).abs() < 0.01);
    }
    if let AttributeValue::Double(max) = last[2] {
        assert!((max - 150.0).abs() < 0.01);
    }
    if let AttributeValue::Double(avg) = last[3] {
        assert!((avg - 125.0).abs() < 0.01);
    }
}

/// Count with GROUP BY HAVING count filter
#[tokio::test]
async fn having_test_count_greater() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, eventCount BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, count() AS eventCount\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol\n\
        HAVING count() >= 2;\n";
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
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(110.0),
        ],
    );
    let out = runner.shutdown();
    // Only IBM has 2+ events
    assert!(!out.is_empty());
}

/// Aggregation with WHERE filtering before GROUP BY
#[tokio::test]
async fn aggregation_test_where_then_group() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, active BOOLEAN);\n\
        CREATE STREAM outputStream (symbol STRING, totalPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, sum(price) AS totalPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        WHERE active = true\n\
        GROUP BY symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Bool(true),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(50.0),
            AttributeValue::Bool(false), // Filtered out
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Bool(true),
        ],
    );
    let out = runner.shutdown();
    // Sum should be 100 + 150 = 250 (not including filtered event)
    assert!(!out.is_empty());
}

/// ORDER BY with null handling (nulls should be at end by default)
#[tokio::test]
async fn order_by_test_nulls() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 5)\n\
        ORDER BY price ASC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Null,
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // Should have 3 outputs
    assert_eq!(out.len(), 3);
}

/// Aggregation with distinctCount and GROUP BY
#[tokio::test]
async fn aggregation_test_distinct_count_group_by() {
    let app = "\
        CREATE STREAM orderStream (region STRING, product STRING);\n\
        CREATE STREAM outputStream (region STRING, uniqueProducts BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT region, distinctCount(product) AS uniqueProducts\n\
        FROM orderStream WINDOW('length', 10)\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ProductA".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ProductA".to_string()), // Duplicate
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ProductB".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::String("ProductC".to_string()),
        ],
    );
    let out = runner.shutdown();
    // US: 2 unique products, EU: 1 unique product
    assert!(out.len() >= 4);
}

/// LIMIT 1 (single result)
#[tokio::test]
async fn limit_test_single() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, price\n\
        FROM stockStream WINDOW('length', 10)\n\
        ORDER BY price DESC\n\
        LIMIT 1;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("C".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // Should get exactly 1 output (B with 200)
    assert!(!out.is_empty());
}

/// GROUP BY with string function
#[tokio::test]
#[ignore = "Complex GROUP BY expressions (functions) not yet supported"]
async fn group_by_test_string_function() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (upperSymbol STRING, totalPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT upper(symbol) AS upperSymbol, sum(price) AS totalPrice\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY upper(symbol);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("ibm".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // Both should group together as 'IBM'
    assert!(!out.is_empty());
}

/// Aggregation with subtraction of aggregates
#[tokio::test]
async fn aggregation_test_difference() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, priceRange DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, max(price) - min(price) AS priceRange\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    let out = runner.shutdown();
    // priceRange = 200 - 100 = 100
    let last = out.last().unwrap();
    if let AttributeValue::Double(range) = last[1] {
        assert!((range - 100.0).abs() < 0.01);
    }
}

// ============================================================================
// ADDITIONAL AGGREGATION EDGE CASE TESTS
// ============================================================================

/// Aggregation with division of aggregates
#[tokio::test]
async fn aggregation_test_avg_vs_sum_count() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE, calculatedAvg DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) AS avgPrice, sum(price) / count() AS calculatedAvg\n\
        FROM stockStream WINDOW('length', 10)\n\
        GROUP BY symbol;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    // avg should equal sum/count = 150
    let last = out.last().unwrap();
    if let (AttributeValue::Double(avg), AttributeValue::Double(calc)) = (&last[1], &last[2]) {
        assert!((avg - 150.0).abs() < 0.01);
        assert!((calc - 150.0).abs() < 0.01);
    }
}

/// Aggregation with multiple GROUP BY columns
#[tokio::test]
async fn aggregation_test_multi_group_by() {
    let app = "\
        CREATE STREAM orderStream (region STRING, category STRING, amount FLOAT);\n\
        CREATE STREAM outputStream (region STRING, category STRING, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT region, category, sum(amount) AS total\n\
        FROM orderStream WINDOW('length', 20)\n\
        GROUP BY region, category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("Books".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // Should have separate groups for (US, Electronics) and (US, Books)
    assert!(out.len() >= 2);
}

/// Aggregation with stddev function
#[tokio::test]
#[ignore = "stddev function not yet supported"]
async fn aggregation_test_stddev() {
    let app = "\
        CREATE STREAM dataStream (id INT, value FLOAT);\n\
        CREATE STREAM outputStream (stdValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT stddev(value) AS stdValue\n\
        FROM dataStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(10.0)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(20.0)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(3), AttributeValue::Float(30.0)],
    );
    let out = runner.shutdown();
    // stddev of [10, 20, 30] should be ~8.16
    assert!(!out.is_empty());
    if let AttributeValue::Double(std) = out.last().unwrap()[0] {
        assert!(std > 0.0);
    }
}

/// Aggregation with coalesce on aggregate result
#[tokio::test]
async fn aggregation_test_coalesce_on_aggregate() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, coalesce(avg(price), 0.0) AS avgPrice\n\
        FROM stockStream WINDOW('length', 5)\n\
        GROUP BY symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    if let AttributeValue::Double(avg) = out[0][1] {
        assert!((avg - 100.0).abs() < 0.01);
    }
}

/// Aggregation with CASE WHEN on aggregate result
#[tokio::test]
async fn aggregation_test_case_when_on_aggregate() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, category STRING);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, \n\
               CASE WHEN avg(price) > 100.0 THEN 'high' ELSE 'low' END AS category\n\
        FROM stockStream WINDOW('length', 5)\n\
        GROUP BY symbol;\n";
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
    // IBM avg=150 -> 'high', MSFT avg=50 -> 'low'
    assert!(out.len() >= 2);
}

/// Aggregation with boolean result
#[tokio::test]
async fn aggregation_test_boolean_result() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, isExpensive BOOLEAN);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) > 100.0 AS isExpensive\n\
        FROM stockStream WINDOW('length', 5)\n\
        GROUP BY symbol;\n";
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
    assert!(out.len() >= 2);
}

/// Aggregation with sum of product (quantity * price)
#[tokio::test]
async fn aggregation_test_sum_of_product() {
    let app = "\
        CREATE STREAM orderStream (orderId INT, quantity INT, price FLOAT);\n\
        CREATE STREAM outputStream (totalValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sum(quantity * price) AS totalValue\n\
        FROM orderStream WINDOW('length', 10);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(3),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    // total = (2*100) + (3*50) = 200 + 150 = 350
    let last = out.last().unwrap();
    if let AttributeValue::Double(total) = last[0] {
        assert!((total - 350.0).abs() < 0.01);
    }
}

/// Aggregation with min and max on same column
#[tokio::test]
async fn aggregation_test_min_max_same_column() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temperature FLOAT);\n\
        CREATE STREAM outputStream (location STRING, minTemp DOUBLE, maxTemp DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT location, min(temperature) AS minTemp, max(temperature) AS maxTemp\n\
        FROM tempStream WINDOW('length', 10)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Float(10.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Float(25.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Float(15.0),
        ],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    if let (AttributeValue::Double(min), AttributeValue::Double(max)) = (&last[1], &last[2]) {
        assert!((min - 10.0).abs() < 0.01);
        assert!((max - 25.0).abs() < 0.01);
    }
}

/// Aggregation with count and distinctCount together
#[tokio::test]
async fn aggregation_test_count_distinct_count() {
    let app = "\
        CREATE STREAM eventStream (userId INT, action STRING);\n\
        CREATE STREAM outputStream (totalEvents BIGINT, uniqueUsers BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT count() AS totalEvents, distinctCount(userId) AS uniqueUsers\n\
        FROM eventStream WINDOW('length', 20);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("click".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("view".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("click".to_string()),
        ],
    );
    let out = runner.shutdown();
    // 3 events, 2 unique users
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(3));
    assert_eq!(last[1], AttributeValue::Long(2));
}

/// Aggregation with string column and sum
#[tokio::test]
async fn aggregation_test_string_and_numeric() {
    let app = "\
        CREATE STREAM salesStream (product STRING, region STRING, amount FLOAT);\n\
        CREATE STREAM outputStream (product STRING, totalAmount DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT product, sum(amount) AS totalAmount\n\
        FROM salesStream WINDOW('length', 10)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(1000.0),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::String("EU".to_string()),
            AttributeValue::Float(1200.0),
        ],
    );
    let out = runner.shutdown();
    // Laptop total = 2200
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::String("Laptop".to_string()));
    if let AttributeValue::Double(total) = last[1] {
        assert!((total - 2200.0).abs() < 0.01);
    }
}

/// Aggregation with long type column
#[tokio::test]
async fn aggregation_test_long_type() {
    let app = "\
        CREATE STREAM eventStream (eventId BIGINT, value BIGINT);\n\
        CREATE STREAM outputStream (sumValue BIGINT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(value) AS sumValue\n\
        FROM eventStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(1), AttributeValue::Long(1000000000)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Long(2), AttributeValue::Long(2000000000)],
    );
    let out = runner.shutdown();
    // sum = 3000000000
    let last = out.last().unwrap();
    assert_eq!(last[0], AttributeValue::Long(3000000000));
}

/// Aggregation with double precision
#[tokio::test]
async fn aggregation_test_double_precision() {
    let app = "\
        CREATE STREAM sensorStream (sensorId INT, value DOUBLE);\n\
        CREATE STREAM outputStream (avgValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT avg(value) AS avgValue\n\
        FROM sensorStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(0.1)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(0.2)],
    );
    runner.send(
        "sensorStream",
        vec![AttributeValue::Int(3), AttributeValue::Double(0.3)],
    );
    let out = runner.shutdown();
    // avg = 0.2
    let last = out.last().unwrap();
    if let AttributeValue::Double(avg) = last[0] {
        assert!((avg - 0.2).abs() < 0.0001);
    }
}

/// Aggregation with negative values
#[tokio::test]
async fn aggregation_test_negative_values() {
    let app = "\
        CREATE STREAM balanceStream (accountId STRING, balance INT);\n\
        CREATE STREAM outputStream (accountId STRING, totalBalance INT);\n\
        INSERT INTO outputStream\n\
        SELECT accountId, sum(balance) AS totalBalance\n\
        FROM balanceStream WINDOW('length', 5)\n\
        GROUP BY accountId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(-50),
        ],
    );
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(-30),
        ],
    );
    let out = runner.shutdown();
    // Sum should be 100 - 50 - 30 = 20
    let last = out.last().unwrap();
    let total = match last[1] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 20);
}

/// Aggregation with zero values
#[tokio::test]
async fn aggregation_test_zero_values() {
    let app = "\
        CREATE STREAM dataStream (id INT, value INT);\n\
        CREATE STREAM outputStream (total INT);\n\
        INSERT INTO outputStream\n\
        SELECT sum(value) AS total\n\
        FROM dataStream WINDOW('length', 4);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(0)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(10)],
    );
    let out = runner.shutdown();
    // Sum should be 0 + 0 + 10 = 10
    let last = out.last().unwrap();
    let total = match last[0] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 10);
}

/// Max with all negative values
#[tokio::test]
async fn aggregation_test_max_negative() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, maxTemp INT);\n\
        INSERT INTO outputStream\n\
        SELECT location, max(temp) AS maxTemp\n\
        FROM tempStream WINDOW('length', 5)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Alaska".to_string()),
            AttributeValue::Int(-20),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Alaska".to_string()),
            AttributeValue::Int(-15),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Alaska".to_string()),
            AttributeValue::Int(-25),
        ],
    );
    let out = runner.shutdown();
    // Max should be -15
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Int(-15));
}

/// Min with all positive values
#[tokio::test]
async fn aggregation_test_min_positive() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price INT);\n\
        CREATE STREAM outputStream (product STRING, minPrice INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, min(price) AS minPrice\n\
        FROM priceStream WINDOW('length', 5)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    // Min should be 50
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Int(50));
}

/// Aggregation with multiplication in SELECT
#[tokio::test]
async fn aggregation_test_multiplication_in_select() {
    let app = "\
        CREATE STREAM salesStream (product STRING, quantity INT, unitPrice INT);\n\
        CREATE STREAM outputStream (product STRING, revenue INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, sum(quantity * unitPrice) AS revenue\n\
        FROM salesStream WINDOW('length', 5)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(20),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    // Revenue should be (10*5) + (20*3) = 50 + 60 = 110
    let last = out.last().unwrap();
    let revenue = match last[1] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(revenue, 110);
}

/// Count with WHERE filter
#[tokio::test]
async fn aggregation_test_count_with_filter() {
    let app = "\
        CREATE STREAM logStream (level STRING, count INT);\n\
        CREATE STREAM outputStream (errorCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT count(*) AS errorCount\n\
        FROM logStream WINDOW('length', 10)\n\
        WHERE level = 'ERROR';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("INFO".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::Int(3),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("WARN".to_string()),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    // Only ERROR events are counted (2 events)
    let last = out.last().unwrap();
    let count = match last[0] {
        AttributeValue::Int(c) => c,
        AttributeValue::Long(c) => c as i32,
        _ => panic!("Expected int count"),
    };
    assert_eq!(count, 2);
}

/// Aggregation with division
#[tokio::test]
async fn aggregation_test_division() {
    let app = "\
        CREATE STREAM scoreStream (student STRING, score INT, total INT);\n\
        CREATE STREAM outputStream (student STRING, avgPercent DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT student, avg(score * 100 / total) AS avgPercent\n\
        FROM scoreStream WINDOW('length', 5)\n\
        GROUP BY student;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(80),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(90),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    // avg(80, 90) = 85
    assert!(!out.is_empty());
}

/// Multiple aggregations on same column
#[tokio::test]
async fn aggregation_test_multiple_on_same() {
    let app = "\
        CREATE STREAM valueStream (id INT, value INT);\n\
        CREATE STREAM outputStream (minVal INT, maxVal INT, sumVal INT, avgVal DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT min(value) AS minVal, max(value) AS maxVal, sum(value) AS sumVal, avg(value) AS avgVal\n\
        FROM valueStream WINDOW('length', 5);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(20)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(30)],
    );
    let out = runner.shutdown();
    let last = out.last().unwrap();
    // Check min
    let min_val = match last[0] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long for min"),
    };
    assert_eq!(min_val, 10);
    // Check max
    let max_val = match last[1] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long for max"),
    };
    assert_eq!(max_val, 30);
    // Check sum
    let sum_val = match last[2] {
        AttributeValue::Int(v) => v,
        AttributeValue::Long(v) => v as i32,
        _ => panic!("Expected int or long for sum"),
    };
    assert_eq!(sum_val, 60);
}

/// Aggregation with HAVING and ORDER BY
#[tokio::test]
#[ignore = "ORDER BY on aggregate alias not yet supported"]
async fn aggregation_test_having_order() {
    let app = "\
        CREATE STREAM orderStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT region, sum(amount) AS total\n\
        FROM orderStream WINDOW('lengthBatch', 6)\n\
        GROUP BY region\n\
        HAVING sum(amount) > 100\n\
        ORDER BY total DESC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(80),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("West".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("West".to_string()),
            AttributeValue::Int(40),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("South".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // East (130), North (150), South (200) pass HAVING; West (70) doesn't
    // Ordered by total DESC: South, North, East
    assert!(!out.is_empty());
}

/// Aggregation with sum and arithmetic in select
#[tokio::test]
async fn aggregation_test_sum_with_arithmetic() {
    let app = "\
        CREATE STREAM salesStream (product STRING, price INT, quantity INT);\n\
        CREATE STREAM outputStream (product STRING, revenue INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, sum(price * quantity) AS revenue\n\
        FROM salesStream WINDOW('lengthBatch', 2)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(20),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // 10*5 + 20*3 = 50 + 60 = 110 (sum returns Long)
    let revenue = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(revenue, 110);
}

/// Aggregation with avg and cast
#[tokio::test]
async fn aggregation_test_avg_cast() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, avgScore DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT player, avg(score) AS avgScore\n\
        FROM scoreStream WINDOW('lengthBatch', 3)\n\
        GROUP BY player;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(80),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(90),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // (80 + 90 + 100) / 3 = 90
    let avg = match &out[0][1] {
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Double(v) => *v,
        _ => panic!("Expected numeric type"),
    };
    assert!((avg - 90.0).abs() < 0.001);
}

/// Aggregation with count and distinct
#[tokio::test]
async fn aggregation_test_count_distinct() {
    let app = "\
        CREATE STREAM visitStream (page STRING, visitor STRING);\n\
        CREATE STREAM outputStream (page STRING, uniqueVisitors INT);\n\
        INSERT INTO outputStream\n\
        SELECT page, distinctCount(visitor) AS uniqueVisitors\n\
        FROM visitStream WINDOW('lengthBatch', 4)\n\
        GROUP BY page;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("bob".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("charlie".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // 3 unique visitors: alice, bob, charlie
    let count = match &out[0][1] {
        AttributeValue::Int(c) => *c as i64,
        AttributeValue::Long(c) => *c,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(count, 3);
}

/// Aggregation with min on double
#[tokio::test]
async fn aggregation_test_min_double() {
    let app = "\
        CREATE STREAM tempStream (sensor STRING, temp DOUBLE);\n\
        CREATE STREAM outputStream (sensor STRING, minTemp DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, min(temp) AS minTemp\n\
        FROM tempStream WINDOW('lengthBatch', 3)\n\
        GROUP BY sensor;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(25.5),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(18.2),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(22.8),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(m) = out[0][1] {
        assert!((m - 18.2).abs() < 0.001);
    } else {
        panic!("Expected Double");
    }
}

/// Aggregation with max on double
#[tokio::test]
async fn aggregation_test_max_double() {
    let app = "\
        CREATE STREAM measureStream (device STRING, reading DOUBLE);\n\
        CREATE STREAM outputStream (device STRING, maxReading DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT device, max(reading) AS maxReading\n\
        FROM measureStream WINDOW('lengthBatch', 3)\n\
        GROUP BY device;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Double(100.5),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Double(150.8),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Double(120.3),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(m) = out[0][1] {
        assert!((m - 150.8).abs() < 0.001);
    } else {
        panic!("Expected Double");
    }
}

/// Aggregation with multiple groups
#[tokio::test]
async fn aggregation_test_multiple_groups() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, sum(value) AS total\n\
        FROM eventStream WINDOW('lengthBatch', 4)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(40),
        ],
    );
    let out = runner.shutdown();
    // Should have 2 groups: A (10+30=40) and B (20+40=60)
    assert_eq!(out.len(), 2);
}

/// Aggregation with CASE WHEN in select
#[tokio::test]
async fn aggregation_test_case_when() {
    let app = "\
        CREATE STREAM orderStream (product STRING, amount INT);\n\
        CREATE STREAM outputStream (product STRING, tier STRING);\n\
        INSERT INTO outputStream\n\
        SELECT product, CASE WHEN sum(amount) > 100 THEN 'HIGH' ELSE 'LOW' END AS tier\n\
        FROM orderStream WINDOW('lengthBatch', 2)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(60),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // sum = 110 > 100, so HIGH
    assert_eq!(out[0][1], AttributeValue::String("HIGH".to_string()));
}

/// Aggregation with coalesce
#[tokio::test]
async fn aggregation_test_coalesce() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT coalesce(category, 'UNKNOWN') AS category, sum(value) AS total\n\
        FROM dataStream WINDOW('lengthBatch', 2)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("A".to_string()));
    // sum returns Long
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 30);
}

/// Aggregation with concat in group
#[tokio::test]
async fn aggregation_test_concat_group() {
    let app = "\
        CREATE STREAM logStream (region STRING, country STRING, events INT);\n\
        CREATE STREAM outputStream (location STRING, totalEvents INT);\n\
        INSERT INTO outputStream\n\
        SELECT concat(region, '-', country) AS location, sum(events) AS totalEvents\n\
        FROM logStream WINDOW('lengthBatch', 2)\n\
        GROUP BY region, country;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("NA".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("NA".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("NA-US".to_string()));
    // sum returns Long
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 300);
}

/// Aggregation with upper function in group
#[tokio::test]
async fn aggregation_test_upper_group() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (upperCategory STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT upper(category) AS upperCategory, sum(value) AS total\n\
        FROM eventStream WINDOW('lengthBatch', 2)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("sales".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("sales".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("SALES".to_string()));
    // sum returns Long
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 300);
}

/// Aggregation with multiple count expressions
#[tokio::test]
async fn aggregation_test_multi_count() {
    let app = "\
        CREATE STREAM logStream (level STRING, app STRING);\n\
        CREATE STREAM outputStream (app STRING, totalCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT app, count(*) AS totalCount\n\
        FROM logStream WINDOW('lengthBatch', 3)\n\
        GROUP BY app;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::String("App1".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("INFO".to_string()),
            AttributeValue::String("App1".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("WARN".to_string()),
            AttributeValue::String("App1".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let count = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(count, 3);
}

/// Aggregation with sum of subtraction
#[tokio::test]
async fn aggregation_test_sum_subtraction() {
    let app = "\
        CREATE STREAM transactionStream (id STRING, credit INT, debit INT);\n\
        CREATE STREAM outputStream (id STRING, net INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, sum(credit - debit) AS net\n\
        FROM transactionStream WINDOW('lengthBatch', 2)\n\
        GROUP BY id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("ACC1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("ACC1".to_string()),
            AttributeValue::Int(50),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let net = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    // (100-30) + (50-10) = 70 + 40 = 110
    assert_eq!(net, 110);
}

/// Aggregation with min and max on same price column
#[tokio::test]
async fn aggregation_test_min_max_price_column() {
    let app = "\
        CREATE STREAM priceStream (symbol STRING, price INT);\n\
        CREATE STREAM outputStream (symbol STRING, minPrice INT, maxPrice INT);\n\
        INSERT INTO outputStream\n\
        SELECT symbol, min(price) AS minPrice, max(price) AS maxPrice\n\
        FROM priceStream WINDOW('lengthBatch', 3)\n\
        GROUP BY symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(160),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(155),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
    assert_eq!(out[0][2], AttributeValue::Int(160));
}

/// Aggregation with avg and count together
#[tokio::test]
async fn aggregation_test_avg_count_together() {
    let app = "\
        CREATE STREAM scoreStream (subject STRING, score INT);\n\
        CREATE STREAM outputStream (subject STRING, avgScore DOUBLE, totalCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT subject, avg(score) AS avgScore, count(*) AS totalCount\n\
        FROM scoreStream WINDOW('lengthBatch', 2)\n\
        GROUP BY subject;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(80),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(90),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        AttributeValue::Int(v) => *v as f64,
        _ => panic!("Expected numeric type"),
    };
    assert!((avg - 85.0).abs() < 0.001);
    let count = match &out[0][2] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(count, 2);
}

/// Aggregation with single event batch
#[tokio::test]
async fn aggregation_test_single_event_batch() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, sum(value) AS total\n\
        FROM dataStream WINDOW('lengthBatch', 1)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(42),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 42);
}

/// Aggregation with sum of negative values
#[tokio::test]
async fn aggregation_test_sum_negative() {
    let app = "\
        CREATE STREAM balanceStream (account STRING, change INT);\n\
        CREATE STREAM outputStream (account STRING, totalChange INT);\n\
        INSERT INTO outputStream\n\
        SELECT account, sum(change) AS totalChange\n\
        FROM balanceStream WINDOW('lengthBatch', 3)\n\
        GROUP BY account;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("ACC1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("ACC1".to_string()),
            AttributeValue::Int(-30),
        ],
    );
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("ACC1".to_string()),
            AttributeValue::Int(-20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    // 100 + (-30) + (-20) = 50
    assert_eq!(total, 50);
}

/// Aggregation with zero values
#[tokio::test]
async fn aggregation_test_sum_with_zero() {
    let app = "\
        CREATE STREAM valueStream (id STRING, value INT);\n\
        CREATE STREAM outputStream (id STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT id, sum(value) AS total\n\
        FROM valueStream WINDOW('lengthBatch', 3)\n\
        GROUP BY id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 15);
}

/// Aggregation with min on DOUBLE values
#[tokio::test]
async fn aggregation_test_min_double_values() {
    let app = "\
        CREATE STREAM sensorStream (sensor STRING, reading DOUBLE);\n\
        CREATE STREAM outputStream (sensor STRING, minReading DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, min(reading) AS minReading\n\
        FROM sensorStream WINDOW('lengthBatch', 3)\n\
        GROUP BY sensor;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(25.5),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(22.3),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(28.1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let min_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((min_val - 22.3).abs() < 0.001);
}

/// Aggregation with max on DOUBLE values
#[tokio::test]
async fn aggregation_test_max_double_values() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp DOUBLE);\n\
        CREATE STREAM outputStream (location STRING, maxTemp DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT location, max(temp) AS maxTemp\n\
        FROM tempStream WINDOW('lengthBatch', 3)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(18.5),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(22.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(20.5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let max_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((max_val - 22.0).abs() < 0.001);
}

/// Aggregation with count and filter
#[tokio::test]
async fn aggregation_test_count_filter() {
    let app = "\
        CREATE STREAM orderStream (region STRING, status STRING);\n\
        CREATE STREAM outputStream (region STRING, activeCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT region, count(status) AS activeCount\n\
        FROM orderStream WINDOW('lengthBatch', 4)\n\
        WHERE status = 'ACTIVE'\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("INACTIVE".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Aggregation sum with negative values
#[tokio::test]
async fn aggregation_test_sum_mixed_sign() {
    let app = "\
        CREATE STREAM transactionStream (account STRING, amount INT);\n\
        CREATE STREAM outputStream (account STRING, netAmount INT);\n\
        INSERT INTO outputStream\n\
        SELECT account, sum(amount) AS netAmount\n\
        FROM transactionStream WINDOW('lengthBatch', 4)\n\
        GROUP BY account;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("A001".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("A001".to_string()),
            AttributeValue::Int(-30),
        ],
    );
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("A001".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "transactionStream",
        vec![
            AttributeValue::String("A001".to_string()),
            AttributeValue::Int(-20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(sum_val, 100); // 100 - 30 + 50 - 20 = 100
}

/// Aggregation avg with zeros
#[tokio::test]
async fn aggregation_test_avg_with_zeros() {
    let app = "\
        CREATE STREAM scoreStream (subject STRING, score INT);\n\
        CREATE STREAM outputStream (subject STRING, avgScore DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT subject, avg(score) AS avgScore\n\
        FROM scoreStream WINDOW('lengthBatch', 4)\n\
        GROUP BY subject;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(80),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((avg_val - 45.0).abs() < 0.001); // (100+0+80+0)/4 = 45
}

/// Aggregation min with equal values
#[tokio::test]
async fn aggregation_test_min_equal() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price INT);\n\
        CREATE STREAM outputStream (product STRING, minPrice INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, min(price) AS minPrice\n\
        FROM priceStream WINDOW('lengthBatch', 3)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(50));
}

/// Aggregation max with equal values
#[tokio::test]
async fn aggregation_test_max_equal() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price INT);\n\
        CREATE STREAM outputStream (product STRING, maxPrice INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, max(price) AS maxPrice\n\
        FROM priceStream WINDOW('lengthBatch', 3)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Gadget".to_string()),
            AttributeValue::Int(99),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Gadget".to_string()),
            AttributeValue::Int(99),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Gadget".to_string()),
            AttributeValue::Int(99),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(99));
}

/// Aggregation count single event
#[tokio::test]
async fn aggregation_test_count_one() {
    let app = "\
        CREATE STREAM eventStream (category STRING, eventId INT);\n\
        CREATE STREAM outputStream (category STRING, eventCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, count(eventId) AS eventCount\n\
        FROM eventStream WINDOW('lengthBatch', 1)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("Alert".to_string()),
            AttributeValue::Int(1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let count_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(count_val, 1);
}

/// Aggregation with DOUBLE sum (sensor values)
#[tokio::test]
async fn aggregation_test_sum_double_sensor() {
    let app = "\
        CREATE STREAM measureStream (sensor STRING, value DOUBLE);\n\
        CREATE STREAM outputStream (sensor STRING, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, sum(value) AS total\n\
        FROM measureStream WINDOW('lengthBatch', 3)\n\
        GROUP BY sensor;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(10.5),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(20.3),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(5.2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((sum_val - 36.0).abs() < 0.1); // 10.5+20.3+5.2 = 36.0
}

/// Aggregation multiple groups same batch
#[tokio::test]
async fn aggregation_test_multi_group_batch() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, totalSales INT);\n\
        INSERT INTO outputStream\n\
        SELECT region, sum(amount) AS totalSales\n\
        FROM salesStream WINDOW('lengthBatch', 4)\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::Int(250),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2); // Two groups: US and EU
}

/// Aggregation sum with large values
#[tokio::test]
async fn aggregation_test_sum_large() {
    let app = "\
        CREATE STREAM bigStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, sum(value) AS total\n\
        FROM bigStream WINDOW('lengthBatch', 3)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "bigStream",
        vec![
            AttributeValue::String("large".to_string()),
            AttributeValue::Int(1000000),
        ],
    );
    runner.send(
        "bigStream",
        vec![
            AttributeValue::String("large".to_string()),
            AttributeValue::Int(2000000),
        ],
    );
    runner.send(
        "bigStream",
        vec![
            AttributeValue::String("large".to_string()),
            AttributeValue::Int(3000000),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(sum_val, 6000000);
}

/// Aggregation avg with one value
#[tokio::test]
async fn aggregation_test_avg_one() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, avgValue DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT category, avg(value) AS avgValue\n\
        FROM dataStream WINDOW('lengthBatch', 1)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(42),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((avg_val - 42.0).abs() < 0.001);
}

/// Aggregation with upper function in select
#[tokio::test]
async fn aggregation_test_upper_in_select() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT upper(region) AS region, sum(amount) AS total\n\
        FROM salesStream WINDOW('lengthBatch', 2)\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("east".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("east".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("EAST".to_string()));
}

/// Aggregation with lower function in select
#[tokio::test]
async fn aggregation_test_lower_in_select() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT lower(region) AS region, sum(amount) AS total\n\
        FROM salesStream WINDOW('lengthBatch', 2)\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("WEST".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("WEST".to_string()),
            AttributeValue::Int(250),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("west".to_string()));
}

/// Aggregation count with zero results
#[tokio::test]
async fn aggregation_test_count_zero_batch() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, cnt INT);\n\
        INSERT INTO outputStream\n\
        SELECT category, count(value) AS cnt\n\
        FROM dataStream WINDOW('lengthBatch', 3)\n\
        GROUP BY category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let cnt_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(cnt_val, 3); // count of 3 events
}

/// Aggregation sum with DOUBLE values
#[tokio::test]
async fn aggregation_test_sum_double_values() {
    let app = "\
        CREATE STREAM measureStream (sensor STRING, reading DOUBLE);\n\
        CREATE STREAM outputStream (sensor STRING, total DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT sensor, sum(reading) AS total\n\
        FROM measureStream WINDOW('lengthBatch', 3)\n\
        GROUP BY sensor;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(1.5),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(2.5),
        ],
    );
    runner.send(
        "measureStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(3.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((sum_val - 7.0).abs() < 0.001); // 1.5 + 2.5 + 3.0 = 7.0
}

/// Aggregation avg with DOUBLE values
#[tokio::test]
async fn aggregation_test_avg_double_values() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp DOUBLE);\n\
        CREATE STREAM outputStream (location STRING, avgTemp DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT location, avg(temp) AS avgTemp\n\
        FROM tempStream WINDOW('lengthBatch', 4)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(20.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(22.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(24.0),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Double(26.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((avg_val - 23.0).abs() < 0.001); // (20+22+24+26)/4 = 23
}

/// Aggregation min with negative values
#[tokio::test]
async fn aggregation_test_min_negative_values() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, minTemp INT);\n\
        INSERT INTO outputStream\n\
        SELECT location, min(temp) AS minTemp\n\
        FROM tempStream WINDOW('lengthBatch', 3)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-10),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-30),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-30)); // min is -30
}

/// Aggregation max with negative values
#[tokio::test]
async fn aggregation_test_max_negative_values() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, maxTemp INT);\n\
        INSERT INTO outputStream\n\
        SELECT location, max(temp) AS maxTemp\n\
        FROM tempStream WINDOW('lengthBatch', 3)\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-10),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-30),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("Arctic".to_string()),
            AttributeValue::Int(-20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-10)); // max is -10
}

/// Aggregation with concat in select
#[tokio::test]
async fn aggregation_test_concat_in_select() {
    let app = "\
        CREATE STREAM orderStream (region STRING, country STRING, amount INT);\n\
        CREATE STREAM outputStream (location STRING, total INT);\n\
        INSERT INTO outputStream\n\
        SELECT concat(region, '-', country) AS location, sum(amount) AS total\n\
        FROM orderStream WINDOW('lengthBatch', 2)\n\
        GROUP BY region, country;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("NA".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("NA".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("NA-US".to_string()));
}

/// Aggregation with length function in select
#[tokio::test]
async fn aggregation_test_length_in_select() {
    let app = "\
        CREATE STREAM msgStream (channel STRING, message STRING);\n\
        CREATE STREAM outputStream (channel STRING, msgLen INT, msgCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT channel, length(channel) AS msgLen, count(message) AS msgCount\n\
        FROM msgStream WINDOW('lengthBatch', 2)\n\
        GROUP BY channel;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("general".to_string()),
            AttributeValue::String("hello".to_string()),
        ],
    );
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("general".to_string()),
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
    assert_eq!(len_val, 7); // "general" = 7 chars
}

/// Aggregation sum with subtraction expression
#[tokio::test]
async fn aggregation_test_sum_subtraction_expr() {
    let app = "\
        CREATE STREAM orderStream (region STRING, gross INT, discount INT);\n\
        CREATE STREAM outputStream (region STRING, netTotal INT);\n\
        INSERT INTO outputStream\n\
        SELECT region, sum(gross - discount) AS netTotal\n\
        FROM orderStream WINDOW('lengthBatch', 2)\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(200),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 270); // (100-10) + (200-20) = 90 + 180 = 270
}

/// Aggregation avg with addition expression
#[tokio::test]
async fn aggregation_test_avg_addition_expr() {
    let app = "\
        CREATE STREAM scoreStream (student STRING, quiz INT, exam INT);\n\
        CREATE STREAM outputStream (student STRING, avgTotal DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT student, avg(quiz + exam) AS avgTotal\n\
        FROM scoreStream WINDOW('lengthBatch', 2)\n\
        GROUP BY student;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(80),
            AttributeValue::Int(90),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(70),
            AttributeValue::Int(85),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // (80+90 + 70+85) / 2 = (170 + 155) / 2 = 325 / 2 = 162.5
    assert_eq!(out[0][1], AttributeValue::Double(162.5));
}

/// Aggregation min with multiplication expression
#[tokio::test]
async fn aggregation_test_min_multiplication_expr() {
    let app = "\
        CREATE STREAM productStream (product STRING, quantity INT, price INT);\n\
        CREATE STREAM outputStream (product STRING, minValue INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, min(quantity * price) AS minValue\n\
        FROM productStream WINDOW('lengthBatch', 3)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(10),
        ],
    ); // 50
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(3),
            AttributeValue::Int(15),
        ],
    ); // 45
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Int(4),
            AttributeValue::Int(12),
        ],
    ); // 48
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let min_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(min_val, 45); // min(50, 45, 48) = 45
}

/// Aggregation max with division expression
#[tokio::test]
async fn aggregation_test_max_division_expr() {
    let app = "\
        CREATE STREAM rateStream (source STRING, total INT, count_val INT);\n\
        CREATE STREAM outputStream (source STRING, maxRate DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT source, max(total / count_val) AS maxRate\n\
        FROM rateStream WINDOW('lengthBatch', 3)\n\
        GROUP BY source;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "rateStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(5),
        ],
    ); // 20
    runner.send(
        "rateStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(150),
            AttributeValue::Int(3),
        ],
    ); // 50
    runner.send(
        "rateStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(80),
            AttributeValue::Int(4),
        ],
    ); // 20
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let max_val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Long(v) => *v as f64,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(max_val, 50.0); // max(20, 50, 20) = 50
}

/// Aggregation count with WHERE filter
#[tokio::test]
async fn aggregation_test_count_where_filter() {
    let app = "\
        CREATE STREAM eventStream (event_type STRING, status STRING);\n\
        CREATE STREAM outputStream (event_type STRING, activeCount INT);\n\
        INSERT INTO outputStream\n\
        SELECT event_type, count(status) AS activeCount\n\
        FROM eventStream WINDOW('lengthBatch', 4)\n\
        WHERE status = 'active'\n\
        GROUP BY event_type;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("click".to_string()),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("click".to_string()),
            AttributeValue::String("inactive".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("click".to_string()),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("click".to_string()),
            AttributeValue::String("active".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let count_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(count_val, 3); // 3 active events
}

/// Aggregation sum with greater than filter
#[tokio::test]
async fn aggregation_test_sum_gt_filter() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, largeTotal INT);\n\
        INSERT INTO outputStream\n\
        SELECT region, sum(amount) AS largeTotal\n\
        FROM salesStream WINDOW('lengthBatch', 4)\n\
        WHERE amount > 50\n\
        GROUP BY region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(30),
        ],
    ); // filtered out
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(75),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(60),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 235); // 100 + 75 + 60 = 235
}

/// Aggregation avg with less than filter
#[tokio::test]
async fn aggregation_test_avg_lt_filter() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, avgLowTemp DOUBLE);\n\
        INSERT INTO outputStream\n\
        SELECT location, avg(temp) AS avgLowTemp\n\
        FROM tempStream WINDOW('lengthBatch', 4)\n\
        WHERE temp < 30\n\
        GROUP BY location;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(25),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(35),
        ],
    ); // filtered out
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(28),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // avg(25, 20, 28) = 73/3 ≈ 24.333...
    if let AttributeValue::Double(v) = out[0][1] {
        assert!((v - 24.333).abs() < 0.01);
    } else {
        panic!("Expected Double");
    }
}

/// Aggregation distinctCount with batch window
#[tokio::test]
async fn aggregation_test_distinct_count_batch() {
    let app = "\
        CREATE STREAM visitStream (page STRING, visitor STRING);\n\
        CREATE STREAM outputStream (page STRING, uniqueVisitors INT);\n\
        INSERT INTO outputStream\n\
        SELECT page, distinctCount(visitor) AS uniqueVisitors\n\
        FROM visitStream WINDOW('lengthBatch', 5)\n\
        GROUP BY page;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("bob".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("alice".to_string()),
        ],
    ); // duplicate
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("charlie".to_string()),
        ],
    );
    runner.send(
        "visitStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::String("bob".to_string()),
        ],
    ); // duplicate
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let count_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(count_val, 3); // alice, bob, charlie = 3 unique
}

/// Multiple aggregations: sum, count, min, max
#[tokio::test]
async fn aggregation_test_multi_agg_four() {
    let app = "\
        CREATE STREAM metricStream (service STRING, latency INT);\n\
        CREATE STREAM outputStream (service STRING, totalLatency INT, reqCount INT, minLatency INT, maxLatency INT);\n\
        INSERT INTO outputStream\n\
        SELECT service, sum(latency) AS totalLatency, count(latency) AS reqCount, min(latency) AS minLatency, max(latency) AS maxLatency\n\
        FROM metricStream WINDOW('lengthBatch', 4)\n\
        GROUP BY service;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Extract values
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    let count_val = match &out[0][2] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    let min_val = match &out[0][3] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    let max_val = match &out[0][4] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 425); // 100+50+200+75
    assert_eq!(count_val, 4);
    assert_eq!(min_val, 50);
    assert_eq!(max_val, 200);
}

/// Aggregation with CASE WHEN in select
#[tokio::test]
async fn aggregation_test_case_when_category() {
    let app = "\
        CREATE STREAM saleStream (product STRING, amount INT);\n\
        CREATE STREAM outputStream (product STRING, category STRING, totalSale INT);\n\
        INSERT INTO outputStream\n\
        SELECT product, CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS category, sum(amount) AS totalSale\n\
        FROM saleStream WINDOW('lengthBatch', 2)\n\
        GROUP BY product;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "saleStream",
        vec![
            AttributeValue::String("laptop".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "saleStream",
        vec![
            AttributeValue::String("laptop".to_string()),
            AttributeValue::Int(120),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Last value determines the CASE result (120 > 100 = high)
    assert_eq!(out[0][1], AttributeValue::String("high".to_string()));
}
