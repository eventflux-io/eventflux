// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Partition compatibility tests
// Reference: PartitionTestCase.java from the reference implementation

use super::common::AppRunner;
use eventflux::core::event::value::AttributeValue;

// ============================================================================
// BASIC PARTITION TESTS
// ============================================================================

/// Basic partition with simple passthrough
/// Reference: PartitionTestCase.java
#[tokio::test]
async fn partition_test1_basic_passthrough() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        PARTITION WITH (symbol OF stockStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT symbol, price FROM stockStream;\n\
        END;\n";
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
            AttributeValue::Float(105.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
}

/// Partition with sum aggregation - state should be isolated per partition
/// Reference: WindowPartitionTestCase.java shows per-partition isolation
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented - currently uses global state"]
async fn partition_test2_sum_aggregation() {
    let app = "\
        CREATE STREAM orderStream (customerId STRING, amount INT);\n\
        CREATE STREAM outputStream (total BIGINT);\n\
        PARTITION WITH (customerId OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT SUM(amount) AS total FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C2".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    // CORRECT per-partition SUM: C1=100, C2=50, C1=100+200=300
    assert_eq!(out[0][0], AttributeValue::Long(100)); // C1's first sum
    assert_eq!(out[1][0], AttributeValue::Long(50)); // C2's first sum (isolated)
    assert_eq!(out[2][0], AttributeValue::Long(300)); // C1's cumulative sum
}

/// Partition with count aggregation - state should be isolated per partition
/// Reference: WindowPartitionTestCase.java shows per-partition isolation
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented - currently uses global state"]
async fn partition_test3_count_aggregation() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (cnt BIGINT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT COUNT() AS cnt FROM eventStream;\n\
        END;\n";
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
    assert_eq!(out.len(), 4);
    // CORRECT per-partition count: A=1, B=1, A=2, A=3
    assert_eq!(out[0][0], AttributeValue::Long(1)); // A's first count
    assert_eq!(out[1][0], AttributeValue::Long(1)); // B's first count (isolated)
    assert_eq!(out[2][0], AttributeValue::Long(2)); // A's second count
    assert_eq!(out[3][0], AttributeValue::Long(3)); // A's third count
}

/// Partition with avg aggregation - state should be isolated per partition
/// Reference: WindowPartitionTestCase.java shows per-partition isolation
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented - currently uses global state"]
async fn partition_test4_avg_aggregation() {
    let app = "\
        CREATE STREAM sensorStream (sensorId STRING, reading FLOAT);\n\
        CREATE STREAM outputStream (avgReading DOUBLE);\n\
        PARTITION WITH (sensorId OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT AVG(reading) AS avgReading FROM sensorStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Float(10.0),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Float(20.0),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S2".to_string()),
            AttributeValue::Float(30.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    // CORRECT per-partition avg: S1=10, S1=(10+20)/2=15, S2=30 (isolated)
    assert_eq!(out[0][0], AttributeValue::Double(10.0)); // S1's first avg
    assert_eq!(out[1][0], AttributeValue::Double(15.0)); // S1's cumulative avg
    assert_eq!(out[2][0], AttributeValue::Double(30.0)); // S2's first avg (isolated)
}

/// Partition with filter
#[tokio::test]
async fn partition_test5_with_filter() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        PARTITION WITH (symbol OF stockStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT symbol, price FROM stockStream WHERE price > 100.0;\n\
        END;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Partition with window
/// Note: Length window includes expired events in output
#[tokio::test]
async fn partition_test6_with_window() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (total BIGINT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT SUM(value) AS total FROM eventStream WINDOW('length', 2);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    // Window includes expired events: 4 outputs (10, 30, expired-10, 50)
    assert!(out.len() >= 3);
}

/// Partition with multiple streams
#[tokio::test]
async fn partition_test7_multiple_streams() {
    let app = "\
        CREATE STREAM orders (productId STRING, quantity INT);\n\
        CREATE STREAM inventory (productId STRING, stock INT);\n\
        CREATE STREAM outputStream (ordQty INT, invStock INT);\n\
        PARTITION WITH (productId OF orders, productId OF inventory)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT orders.quantity AS ordQty, inventory.stock AS invStock\n\
            FROM orders JOIN inventory ON orders.productId = inventory.productId;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orders",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "inventory",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(5));
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Partition with min/max aggregation
#[tokio::test]
async fn partition_test8_min_max() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp FLOAT);\n\
        CREATE STREAM outputStream (minTemp FLOAT, maxTemp FLOAT);\n\
        PARTITION WITH (location OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT MIN(temp) AS minTemp, MAX(temp) AS maxTemp FROM tempStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Float(20.0),
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
    assert_eq!(out.len(), 3);
    // NYC: min=20, max=20 -> min=20, max=25 -> min=15, max=25
    assert_eq!(out[0][0], AttributeValue::Float(20.0));
    assert_eq!(out[0][1], AttributeValue::Float(20.0));
    assert_eq!(out[1][0], AttributeValue::Float(20.0));
    assert_eq!(out[1][1], AttributeValue::Float(25.0));
    assert_eq!(out[2][0], AttributeValue::Float(15.0));
    assert_eq!(out[2][1], AttributeValue::Float(25.0));
}

/// Partition with INT partition key - state should be isolated per partition
/// Reference: WindowPartitionTestCase.java shows per-partition isolation
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented - currently uses global state"]
async fn partition_test9_int_key() {
    let app = "\
        CREATE STREAM eventStream (regionId INT, value INT);\n\
        CREATE STREAM outputStream (total BIGINT);\n\
        PARTITION WITH (regionId OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT SUM(value) AS total FROM eventStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    runner.send(
        "eventStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    // CORRECT per-partition sum: Region1=100, Region2=50, Region1=300
    assert_eq!(out[0][0], AttributeValue::Long(100)); // Region 1's first sum
    assert_eq!(out[1][0], AttributeValue::Long(50)); // Region 2's first sum (isolated)
    assert_eq!(out[2][0], AttributeValue::Long(300)); // Region 1's cumulative sum
}

/// Partition with multiple queries in BEGIN block
#[tokio::test]
#[ignore = "Multiple queries in PARTITION BEGIN block not yet supported"]
async fn partition_test10_multiple_queries() {
    let app = "\
        CREATE STREAM inputStream (key STRING, value INT);\n\
        CREATE STREAM sumStream (total BIGINT);\n\
        CREATE STREAM countStream (cnt BIGINT);\n\
        PARTITION WITH (key OF inputStream)\n\
        BEGIN\n\
            INSERT INTO sumStream SELECT SUM(value) AS total FROM inputStream;\n\
            INSERT INTO countStream SELECT COUNT() AS cnt FROM inputStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "sumStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Partition with length batch window
#[tokio::test]
#[ignore = "Length batch window with partition has unexpected output count"]
async fn partition_test11_length_batch_window() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (total BIGINT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT SUM(value) AS total FROM eventStream WINDOW('lengthBatch', 2);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("X".to_string()),
            AttributeValue::Int(40),
        ],
    );
    let out = runner.shutdown();
    // Batch window outputs after every 2 events: 10+20=30, 30+40=70
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], AttributeValue::Long(30));
    assert_eq!(out[1][0], AttributeValue::Long(70));
}

/// Partition with distinctCount
#[tokio::test]
async fn partition_test12_distinct_count() {
    let app = "\
        CREATE STREAM clickStream (userId STRING, page STRING);\n\
        CREATE STREAM outputStream (uniquePages BIGINT);\n\
        PARTITION WITH (userId OF clickStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT distinctCount(page) AS uniquePages FROM clickStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("U1".to_string()),
            AttributeValue::String("home".to_string()),
        ],
    );
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("U1".to_string()),
            AttributeValue::String("products".to_string()),
        ],
    );
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("U1".to_string()),
            AttributeValue::String("home".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    // U1: 1 unique, 2 unique, still 2 unique (home repeated)
    assert_eq!(out[0][0], AttributeValue::Long(1));
    assert_eq!(out[1][0], AttributeValue::Long(2));
    assert_eq!(out[2][0], AttributeValue::Long(2));
}

/// Partition isolation test - partitions should not affect each other
/// Note: Currently aggregation state is global, not per-partition
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented"]
async fn partition_test13_isolation() {
    let app = "\
        CREATE STREAM dataStream (partition STRING, value INT);\n\
        CREATE STREAM outputStream (cnt BIGINT);\n\
        PARTITION WITH (partition OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT COUNT() AS cnt FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Interleave events from different partitions
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("P2".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("P2".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 5);
    // P1: 1, P2: 1, P1: 2, P2: 2, P1: 3
    assert_eq!(out[0][0], AttributeValue::Long(1));
    assert_eq!(out[1][0], AttributeValue::Long(1));
    assert_eq!(out[2][0], AttributeValue::Long(2));
    assert_eq!(out[3][0], AttributeValue::Long(2));
    assert_eq!(out[4][0], AttributeValue::Long(3));
}

/// Partition with arithmetic in SELECT
/// Note: Currently aggregation state is global, not per-partition
#[tokio::test]
#[ignore = "Per-partition aggregation isolation not yet implemented"]
async fn partition_test14_arithmetic() {
    let app = "\
        CREATE STREAM orderStream (customerId STRING, quantity INT, price FLOAT);\n\
        CREATE STREAM outputStream (totalValue FLOAT);\n\
        PARTITION WITH (customerId OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT SUM(quantity * price) AS totalValue FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(2),
            AttributeValue::Float(10.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(3),
            AttributeValue::Float(20.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    // C1: 2*10=20, C1: 20 + 3*20 = 80
    assert_eq!(out[0][0], AttributeValue::Float(20.0));
    assert_eq!(out[1][0], AttributeValue::Float(80.0));
}

/// Partition with CASE WHEN
#[tokio::test]
async fn partition_test15_case_when() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, level STRING);\n\
        PARTITION WITH (symbol OF stockStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT symbol, CASE WHEN price > 100.0 THEN 'HIGH' ELSE 'LOW' END AS level\n\
            FROM stockStream;\n\
        END;\n";
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
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(50.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("HIGH".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("LOW".to_string()));
}

/// Partition with string functions
#[tokio::test]
async fn partition_test16_string_functions() {
    let app = "\
        CREATE STREAM userStream (region STRING, name STRING);\n\
        CREATE STREAM outputStream (region STRING, upperName STRING);\n\
        PARTITION WITH (region OF userStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, upper(name) AS upperName\n\
            FROM userStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("alice".to_string()),
        ],
    );
    runner.send(
        "userStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::String("bob".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("ALICE".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("BOB".to_string()));
}

/// Partition with concat function
#[tokio::test]
async fn partition_test17_concat() {
    let app = "\
        CREATE STREAM personStream (country STRING, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (country STRING, fullName STRING);\n\
        PARTITION WITH (country OF personStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT country, concat(firstName, ' ', lastName) AS fullName\n\
            FROM personStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::String("USA".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Partition with coalesce function
#[tokio::test]
async fn partition_test18_coalesce() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value DOUBLE);\n\
        CREATE STREAM outputStream (category STRING, safeValue DOUBLE);\n\
        PARTITION WITH (category OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, coalesce(value, 0.0) AS safeValue\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(10.5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Partition with time window
#[tokio::test]
async fn partition_test19_time_window() {
    let app = "\
        CREATE STREAM eventStream (userId STRING, count INT);\n\
        CREATE STREAM outputStream (userId STRING, total INT);\n\
        PARTITION WITH (userId OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT userId, sum(count) AS total\n\
            FROM eventStream WINDOW('time', 5000 MILLISECONDS);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("user1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("user1".to_string()),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Partition with subtraction
#[tokio::test]
async fn partition_test20_subtraction() {
    let app = "\
        CREATE STREAM accountStream (accountId STRING, credit INT, debit INT);\n\
        CREATE STREAM outputStream (accountId STRING, balance INT);\n\
        PARTITION WITH (accountId OF accountStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT accountId, credit - debit AS balance\n\
            FROM accountStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "accountStream",
        vec![
            AttributeValue::String("A001".to_string()),
            AttributeValue::Int(1000),
            AttributeValue::Int(300),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(700));
}

/// Partition with multiplication
#[tokio::test]
async fn partition_test21_multiplication() {
    let app = "\
        CREATE STREAM salesStream (region STRING, quantity INT, price INT);\n\
        CREATE STREAM outputStream (region STRING, revenue INT);\n\
        PARTITION WITH (region OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, quantity * price AS revenue\n\
            FROM salesStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("North".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Partition with double type
#[tokio::test]
async fn partition_test22_double_type() {
    let app = "\
        CREATE STREAM sensorStream (sensorId STRING, reading DOUBLE);\n\
        CREATE STREAM outputStream (sensorId STRING, avgReading DOUBLE);\n\
        PARTITION WITH (sensorId OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensorId, avg(reading) AS avgReading\n\
            FROM sensorStream WINDOW('length', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(10.0),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Double(20.0),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Partition with long type
#[tokio::test]
async fn partition_test23_long_type() {
    let app = "\
        CREATE STREAM eventStream (source STRING, timestamp BIGINT);\n\
        CREATE STREAM outputStream (source STRING, latestTs BIGINT);\n\
        PARTITION WITH (source OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, max(timestamp) AS latestTs\n\
            FROM eventStream WINDOW('length', 5);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("app1".to_string()),
            AttributeValue::Long(1234567890123),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("app1".to_string()),
            AttributeValue::Long(1234567890456),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    let last_ts = match out.last().unwrap()[1] {
        AttributeValue::Long(v) => v,
        _ => panic!("Expected long"),
    };
    assert_eq!(last_ts, 1234567890456);
}

/// Partition with lower function
#[tokio::test]
async fn partition_test24_lower() {
    let app = "\
        CREATE STREAM inputStream (category STRING, text STRING);\n\
        CREATE STREAM outputStream (category STRING, lowerText STRING);\n\
        PARTITION WITH (category OF inputStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, lower(text) AS lowerText\n\
            FROM inputStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inputStream",
        vec![
            AttributeValue::String("CAT1".to_string()),
            AttributeValue::String("HELLO WORLD".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("hello world".to_string()));
}

/// Partition with length function
#[tokio::test]
async fn partition_test25_length() {
    let app = "\
        CREATE STREAM msgStream (channel STRING, message STRING);\n\
        CREATE STREAM outputStream (channel STRING, msgLen INT);\n\
        PARTITION WITH (channel OF msgStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT channel, length(message) AS msgLen\n\
            FROM msgStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("general".to_string()),
            AttributeValue::String("Hello!".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(6));
}

/// Partition with division
#[tokio::test]
async fn partition_test26_division() {
    let app = "\
        CREATE STREAM dataStream (group STRING, total INT, count INT);\n\
        CREATE STREAM outputStream (group STRING, avg DOUBLE);\n\
        PARTITION WITH (group OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group, total / count AS avg\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Partition with uuid function
#[tokio::test]
async fn partition_test27_uuid() {
    let app = "\
        CREATE STREAM eventStream (partition STRING, data STRING);\n\
        CREATE STREAM outputStream (partition STRING, eventId STRING);\n\
        PARTITION WITH (partition OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT partition, uuid() AS eventId\n\
            FROM eventStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::String("data1".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert_eq!(uuid.len(), 36);
    }
}

/// Partition with now() function
#[tokio::test]
async fn partition_test28_current_time() {
    let app = "\
        CREATE STREAM logStream (service STRING, message STRING);\n\
        CREATE STREAM outputStream (service STRING, timestamp BIGINT);\n\
        PARTITION WITH (service OF logStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT service, now() AS timestamp\n\
            FROM logStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::String("request".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Long(ts) = out[0][1] {
        assert!(ts > 1577836800000);
    }
}

/// Partition with float type
#[tokio::test]
async fn partition_test29_float_type() {
    let app = "\
        CREATE STREAM sensorStream (location STRING, reading FLOAT);\n\
        CREATE STREAM outputStream (location STRING, avgReading DOUBLE);\n\
        PARTITION WITH (location OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT location, avg(reading) AS avgReading\n\
            FROM sensorStream WINDOW('length', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("L1".to_string()),
            AttributeValue::Float(10.5),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("L1".to_string()),
            AttributeValue::Float(20.5),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Partition with nested concat
#[tokio::test]
async fn partition_test30_nested_concat() {
    let app = "\
        CREATE STREAM personStream (dept STRING, title STRING, name STRING);\n\
        CREATE STREAM outputStream (dept STRING, fullTitle STRING);\n\
        PARTITION WITH (dept OF personStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT dept, concat(title, ' ', name) AS fullTitle\n\
            FROM personStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::String("Engineering".to_string()),
            AttributeValue::String("Dr.".to_string()),
            AttributeValue::String("Smith".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("Dr. Smith".to_string()));
}

/// Partition with complex arithmetic
#[tokio::test]
async fn partition_test31_complex_arithmetic() {
    let app = "\
        CREATE STREAM orderStream (region STRING, quantity INT, price INT, discount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, quantity * price - discount AS total\n\
            FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("West".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(50),
            AttributeValue::Int(25),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(475));
}

/// Partition with multiple sum aggregations
#[tokio::test]
async fn partition_test32_multiple_sums() {
    let app = "\
        CREATE STREAM salesStream (store STRING, revenue INT, cost INT);\n\
        CREATE STREAM outputStream (store STRING, totalRevenue INT, totalCost INT);\n\
        PARTITION WITH (store OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT store, sum(revenue) AS totalRevenue, sum(cost) AS totalCost\n\
            FROM salesStream WINDOW('length', 5);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(60),
        ],
    );
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(150),
            AttributeValue::Int(90),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Partition with NOT operator in WHERE
#[tokio::test]
async fn partition_test33_not_where() {
    let app = "\
        CREATE STREAM statusStream (category STRING, active INT);\n\
        CREATE STREAM outputStream (category STRING, active INT);\n\
        PARTITION WITH (category OF statusStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, active\n\
            FROM statusStream\n\
            WHERE NOT (active = 0);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    // Only active=1 should pass
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(1));
}

/// Partition with AND in WHERE
#[tokio::test]
async fn partition_test34_and_where() {
    let app = "\
        CREATE STREAM productStream (category STRING, price INT, stock INT);\n\
        CREATE STREAM outputStream (category STRING, price INT);\n\
        PARTITION WITH (category OF productStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, price\n\
            FROM productStream\n\
            WHERE price > 50 AND stock > 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Books".to_string()),
            AttributeValue::Int(30),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    // Only Electronics (price=100 > 50, stock=10 > 0) should pass
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Electronics".to_string()));
}

/// Partition with OR in WHERE
#[tokio::test]
async fn partition_test35_or_where() {
    let app = "\
        CREATE STREAM itemStream (type STRING, priority INT, urgent INT);\n\
        CREATE STREAM outputStream (type STRING);\n\
        PARTITION WITH (type OF itemStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT type\n\
            FROM itemStream\n\
            WHERE priority = 1 OR urgent = 1;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "itemStream",
        vec![
            AttributeValue::String("Task".to_string()),
            AttributeValue::Int(1),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "itemStream",
        vec![
            AttributeValue::String("Bug".to_string()),
            AttributeValue::Int(0),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    // Only Task (priority=1) should pass
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Task".to_string()));
}

/// Partition with multiple partition keys
#[tokio::test]
#[ignore = "Multiple partition keys on same stream not yet fully supported"]
async fn partition_test36_multi_key() {
    let app = "\
        CREATE STREAM tradeStream (region STRING, product STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, product STRING, total INT);\n\
        PARTITION WITH (region OF tradeStream, product OF tradeStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, product, sum(amount) AS total\n\
            FROM tradeStream WINDOW('lengthBatch', 2);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][2], AttributeValue::Int(300));
}

/// Partition with length window (sliding)
#[tokio::test]
async fn partition_test37_length_window() {
    let app = "\
        CREATE STREAM sensorStream (sensor STRING, reading INT);\n\
        CREATE STREAM outputStream (sensor STRING, avgReading INT);\n\
        PARTITION WITH (sensor OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor, avg(reading) AS avgReading\n\
            FROM sensorStream WINDOW('length', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    // Each event outputs with current window average
    assert!(!out.is_empty());
}

/// Partition with count aggregation
#[tokio::test]
async fn partition_test38_count() {
    let app = "\
        CREATE STREAM clickStream (page STRING, userId INT);\n\
        CREATE STREAM outputStream (page STRING, clicks INT);\n\
        PARTITION WITH (page OF clickStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT page, count(*) AS clicks\n\
            FROM clickStream WINDOW('lengthBatch', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::Int(2),
        ],
    );
    runner.send(
        "clickStream",
        vec![
            AttributeValue::String("home".to_string()),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    // LengthBatch outputs once when batch is full
    assert!(!out.is_empty());
    // Last output should have count = 3 (count returns Long)
    let last = out.last().unwrap();
    let count = match &last[1] {
        AttributeValue::Int(c) => *c as i64,
        AttributeValue::Long(c) => *c,
        _ => panic!("Expected int or long count"),
    };
    assert_eq!(count, 3);
}

/// Partition with min aggregation
#[tokio::test]
async fn partition_test39_min() {
    let app = "\
        CREATE STREAM priceStream (category STRING, price INT);\n\
        CREATE STREAM outputStream (category STRING, minPrice INT);\n\
        PARTITION WITH (category OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, min(price) AS minPrice\n\
            FROM priceStream WINDOW('lengthBatch', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(500),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(800),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Last output should have min = 200
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Int(200));
}

/// Partition with max aggregation
#[tokio::test]
async fn partition_test40_max() {
    let app = "\
        CREATE STREAM scoreStream (game STRING, score INT);\n\
        CREATE STREAM outputStream (game STRING, maxScore INT);\n\
        PARTITION WITH (game OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT game, max(score) AS maxScore\n\
            FROM scoreStream WINDOW('lengthBatch', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Chess".to_string()),
            AttributeValue::Int(1200),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Chess".to_string()),
            AttributeValue::Int(1500),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Chess".to_string()),
            AttributeValue::Int(1100),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Last output should have max = 1500
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Int(1500));
}

/// Partition with avg aggregation
#[tokio::test]
async fn partition_test41_avg() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, avgTemp INT);\n\
        PARTITION WITH (location OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT location, avg(temp) AS avgTemp\n\
            FROM tempStream WINDOW('lengthBatch', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(25),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Last output should have avg = 25 (avg returns Double)
    let last = out.last().unwrap();
    let avg = match &last[1] {
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Double(v) => *v,
        _ => panic!("Expected int or double"),
    };
    assert!((avg - 25.0).abs() < 0.001);
}

/// Partition with CASE WHEN expression
#[tokio::test]
async fn partition_test42_case_when() {
    let app = "\
        CREATE STREAM orderStream (category STRING, amount INT);\n\
        CREATE STREAM outputStream (category STRING, tier STRING);\n\
        PARTITION WITH (category OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, CASE WHEN amount > 100 THEN 'HIGH' ELSE 'LOW' END AS tier\n\
            FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("Books".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("Books".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("HIGH".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("LOW".to_string()));
}

/// Partition with greater than comparison
#[tokio::test]
async fn partition_test43_greater_than() {
    let app = "\
        CREATE STREAM metricStream (service STRING, latency INT);\n\
        CREATE STREAM outputStream (service STRING, latency INT);\n\
        PARTITION WITH (service OF metricStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT service, latency\n\
            FROM metricStream\n\
            WHERE latency > 100;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
}

/// Partition with less than comparison
#[tokio::test]
async fn partition_test44_less_than() {
    let app = "\
        CREATE STREAM inventoryStream (warehouse STRING, stock INT);\n\
        CREATE STREAM outputStream (warehouse STRING, stock INT);\n\
        PARTITION WITH (warehouse OF inventoryStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT warehouse, stock\n\
            FROM inventoryStream\n\
            WHERE stock < 10;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::String("WH1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::String("WH1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(5));
}

/// Partition with equal comparison on string
#[tokio::test]
async fn partition_test45_string_equal() {
    let app = "\
        CREATE STREAM logStream (level STRING, message STRING);\n\
        CREATE STREAM outputStream (level STRING, message STRING);\n\
        PARTITION WITH (level OF logStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT level, message\n\
            FROM logStream\n\
            WHERE level = 'ERROR';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::String("Connection failed".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("INFO".to_string()),
            AttributeValue::String("Started".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ERROR".to_string()));
}

/// Partition with concat function in SELECT
#[tokio::test]
async fn partition_test46_concat_select() {
    let app = "\
        CREATE STREAM userStream (region STRING, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (region STRING, fullName STRING);\n\
        PARTITION WITH (region OF userStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, concat(firstName, ' ', lastName) AS fullName\n\
            FROM userStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Partition with upper function in SELECT
#[tokio::test]
async fn partition_test47_upper_select() {
    let app = "\
        CREATE STREAM productStream (category STRING, name STRING);\n\
        CREATE STREAM outputStream (category STRING, upperName STRING);\n\
        PARTITION WITH (category OF productStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, upper(name) AS upperName\n\
            FROM productStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::String("laptop".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("LAPTOP".to_string()));
}

/// Partition with lower function in SELECT
#[tokio::test]
async fn partition_test48_lower_select() {
    let app = "\
        CREATE STREAM eventStream (source STRING, eventType STRING);\n\
        CREATE STREAM outputStream (source STRING, lowerType STRING);\n\
        PARTITION WITH (source OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, lower(eventType) AS lowerType\n\
            FROM eventStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::String("REQUEST".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("request".to_string()));
}

/// Partition with addition in SELECT
#[tokio::test]
async fn partition_test49_addition_select() {
    let app = "\
        CREATE STREAM orderStream (region STRING, price INT, tax INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, price + tax AS total\n\
            FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(110));
}

/// Partition with subtraction in SELECT
#[tokio::test]
async fn partition_test50_subtraction_select() {
    let app = "\
        CREATE STREAM inventoryStream (warehouse STRING, stock INT, reserved INT);\n\
        CREATE STREAM outputStream (warehouse STRING, available INT);\n\
        PARTITION WITH (warehouse OF inventoryStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT warehouse, stock - reserved AS available\n\
            FROM inventoryStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::String("WH1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(70));
}

/// Partition with multiplication in SELECT
#[tokio::test]
async fn partition_test51_multiplication_select() {
    let app = "\
        CREATE STREAM salesStream (store STRING, qty INT, unitPrice INT);\n\
        CREATE STREAM outputStream (store STRING, revenue INT);\n\
        PARTITION WITH (store OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT store, qty * unitPrice AS revenue\n\
            FROM salesStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("Store1".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let revenue = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(revenue, 100);
}

/// Partition with coalesce in SELECT
#[tokio::test]
async fn partition_test52_coalesce_select() {
    let app = "\
        CREATE STREAM dataStream (category STRING, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (category STRING, result STRING);\n\
        PARTITION WITH (category OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, coalesce(primary_val, backup_val) AS result\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("primary".to_string()),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("primary".to_string()));
}

/// Partition with length function in SELECT
#[tokio::test]
async fn partition_test53_length_select() {
    let app = "\
        CREATE STREAM textStream (topic STRING, content STRING);\n\
        CREATE STREAM outputStream (topic STRING, contentLen INT);\n\
        PARTITION WITH (topic OF textStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT topic, length(content) AS contentLen\n\
            FROM textStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::String("News".to_string()),
            AttributeValue::String("Hello World".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len, 11);
}

/// Partition with DOUBLE field
#[tokio::test]
async fn partition_test54_double_field() {
    let app = "\
        CREATE STREAM sensorStream (location STRING, reading DOUBLE);\n\
        CREATE STREAM outputStream (location STRING, reading DOUBLE);\n\
        PARTITION WITH (location OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT location, reading\n\
            FROM sensorStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("Room1".to_string()),
            AttributeValue::Double(25.5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((val - 25.5).abs() < 0.001);
}

/// Partition with not equal comparison
#[tokio::test]
async fn partition_test55_not_equal() {
    let app = "\
        CREATE STREAM statusStream (service STRING, status STRING);\n\
        CREATE STREAM outputStream (service STRING, status STRING);\n\
        PARTITION WITH (service OF statusStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT service, status\n\
            FROM statusStream\n\
            WHERE status != 'OK';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::String("ERROR".to_string()),
        ],
    );
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::String("OK".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ERROR".to_string()));
}

/// Partition with uuid function in SELECT
#[tokio::test]
async fn partition_test56_uuid_select() {
    let app = "\
        CREATE STREAM requestStream (region STRING, path STRING);\n\
        CREATE STREAM outputStream (region STRING, requestId STRING);\n\
        PARTITION WITH (region OF requestStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, uuid() AS requestId\n\
            FROM requestStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "requestStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::String("/api/users".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Partition with greater than or equal comparison
#[tokio::test]
async fn partition_test57_gte_filter() {
    let app = "\
        CREATE STREAM orderStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, amount INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, amount\n\
            FROM orderStream\n\
            WHERE amount >= 100;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Partition with less than or equal comparison
#[tokio::test]
async fn partition_test58_lte_filter() {
    let app = "\
        CREATE STREAM inventoryStream (warehouse STRING, quantity INT);\n\
        CREATE STREAM outputStream (warehouse STRING, quantity INT);\n\
        PARTITION WITH (warehouse OF inventoryStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT warehouse, quantity\n\
            FROM inventoryStream\n\
            WHERE quantity <= 10;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::String("W1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::String("W1".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(5));
}

/// Partition with case when in SELECT
#[tokio::test]
async fn partition_test59_case_when() {
    let app = "\
        CREATE STREAM scoreStream (category STRING, score INT);\n\
        CREATE STREAM outputStream (category STRING, grade STRING);\n\
        PARTITION WITH (category OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade\n\
            FROM scoreStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Math".to_string()),
            AttributeValue::Int(95),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("A".to_string()));
}

/// Partition with division in SELECT
#[tokio::test]
async fn partition_test60_division() {
    let app = "\
        CREATE STREAM salesStream (region STRING, total INT, count INT);\n\
        CREATE STREAM outputStream (region STRING, average DOUBLE);\n\
        PARTITION WITH (region OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, total / count AS average\n\
            FROM salesStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "salesStream",
        vec![
            AttributeValue::String("West".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let avg = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Int(v) => *v as f64,
        _ => panic!("Expected double or int"),
    };
    assert!((avg - 20.0).abs() < 0.001); // 100 / 5 = 20
}

/// Partition with multiple arithmetic operations
#[tokio::test]
async fn partition_test61_complex_arithmetic() {
    let app = "\
        CREATE STREAM metricsStream (source STRING, a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (source STRING, result INT);\n\
        PARTITION WITH (source OF metricsStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, a + b * c AS result\n\
            FROM metricsStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricsStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(3),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(25)); // 10 + 3*5 = 25
}

/// Partition with DOUBLE arithmetic
#[tokio::test]
async fn partition_test62_double_arithmetic() {
    let app = "\
        CREATE STREAM priceStream (category STRING, price DOUBLE, discount DOUBLE);\n\
        CREATE STREAM outputStream (category STRING, finalPrice DOUBLE);\n\
        PARTITION WITH (category OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, price - discount AS finalPrice\n\
            FROM priceStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Double(99.99),
            AttributeValue::Double(10.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let price = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((price - 89.99).abs() < 0.01);
}

/// Partition with empty string field
#[tokio::test]
async fn partition_test63_empty_string() {
    let app = "\
        CREATE STREAM messageStream (channel STRING, content STRING);\n\
        CREATE STREAM outputStream (channel STRING, content STRING);\n\
        PARTITION WITH (channel OF messageStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT channel, content\n\
            FROM messageStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "messageStream",
        vec![
            AttributeValue::String("general".to_string()),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("".to_string()));
}

/// Partition with zero value
#[tokio::test]
async fn partition_test64_zero_value() {
    let app = "\
        CREATE STREAM counterStream (bucket STRING, count INT);\n\
        CREATE STREAM outputStream (bucket STRING, count INT);\n\
        PARTITION WITH (bucket OF counterStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT bucket, count\n\
            FROM counterStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "counterStream",
        vec![
            AttributeValue::String("errors".to_string()),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(0));
}

/// Partition with negative value
#[tokio::test]
async fn partition_test65_negative_value() {
    let app = "\
        CREATE STREAM balanceStream (account STRING, balance INT);\n\
        CREATE STREAM outputStream (account STRING, balance INT);\n\
        PARTITION WITH (account OF balanceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT account, balance\n\
            FROM balanceStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("checking".to_string()),
            AttributeValue::Int(-100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-100));
}

/// Partition with OR condition in WHERE
#[tokio::test]
async fn partition_test66_or_condition() {
    let app = "\
        CREATE STREAM eventStream (region STRING, priority INT, status STRING);\n\
        CREATE STREAM outputStream (region STRING, priority INT);\n\
        PARTITION WITH (region OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, priority\n\
            FROM eventStream\n\
            WHERE priority > 5 OR status = 'CRITICAL';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(3),
            AttributeValue::String("CRITICAL".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(3));
}

/// Partition with AND condition in WHERE
#[tokio::test]
async fn partition_test67_and_condition() {
    let app = "\
        CREATE STREAM orderStream (region STRING, amount INT, verified INT);\n\
        CREATE STREAM outputStream (region STRING, amount INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, amount\n\
            FROM orderStream\n\
            WHERE amount > 100 AND verified = 1;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("EU".to_string()),
            AttributeValue::Int(500),
            AttributeValue::Int(1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Partition with nested parentheses in WHERE
#[tokio::test]
async fn partition_test68_nested_parens() {
    let app = "\
        CREATE STREAM dataStream (bucket STRING, a INT, b INT);\n\
        CREATE STREAM outputStream (bucket STRING, result INT);\n\
        PARTITION WITH (bucket OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT bucket, (a + b) * 2 AS result\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("B1".to_string()),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let result = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(result, 30); // (10+5)*2 = 30
}

/// Partition with multiple string fields
#[tokio::test]
async fn partition_test69_multi_string() {
    let app = "\
        CREATE STREAM userStream (region STRING, firstName STRING, lastName STRING, email STRING);\n\
        CREATE STREAM outputStream (region STRING, firstName STRING, lastName STRING, email STRING);\n\
        PARTITION WITH (region OF userStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, firstName, lastName, email\n\
            FROM userStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "userStream",
        vec![
            AttributeValue::String("APAC".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
            AttributeValue::String("john@example.com".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John".to_string()));
    assert_eq!(
        out[0][3],
        AttributeValue::String("john@example.com".to_string())
    );
}

/// Partition with multiple INT fields
#[tokio::test]
async fn partition_test70_multi_int() {
    let app = "\
        CREATE STREAM metricsStream (source STRING, cpu INT, memory INT, disk INT);\n\
        CREATE STREAM outputStream (source STRING, cpu INT, memory INT, disk INT);\n\
        PARTITION WITH (source OF metricsStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, cpu, memory, disk\n\
            FROM metricsStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricsStream",
        vec![
            AttributeValue::String("server1".to_string()),
            AttributeValue::Int(80),
            AttributeValue::Int(70),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(80));
    assert_eq!(out[0][2], AttributeValue::Int(70));
    assert_eq!(out[0][3], AttributeValue::Int(50));
}

/// Partition with sum of three fields
#[tokio::test]
async fn partition_test71_sum_three() {
    let app = "\
        CREATE STREAM scoreStream (student STRING, math INT, science INT, english INT);\n\
        CREATE STREAM outputStream (student STRING, total INT);\n\
        PARTITION WITH (student OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT student, math + science + english AS total\n\
            FROM scoreStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(90),
            AttributeValue::Int(85),
            AttributeValue::Int(88),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(263)); // 90+85+88 = 263
}

/// Partition with DOUBLE multiplication
#[tokio::test]
async fn partition_test72_double_multiply() {
    let app = "\
        CREATE STREAM rateStream (currency STRING, amount DOUBLE, rate DOUBLE);\n\
        CREATE STREAM outputStream (currency STRING, converted DOUBLE);\n\
        PARTITION WITH (currency OF rateStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT currency, amount * rate AS converted\n\
            FROM rateStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "rateStream",
        vec![
            AttributeValue::String("USD".to_string()),
            AttributeValue::Double(100.0),
            AttributeValue::Double(1.25),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let converted = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((converted - 125.0).abs() < 0.001);
}

/// Partition with string length comparison
#[tokio::test]
async fn partition_test73_length_compare() {
    let app = "\
        CREATE STREAM textStream (category STRING, content STRING);\n\
        CREATE STREAM outputStream (category STRING, content STRING);\n\
        PARTITION WITH (category OF textStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, content\n\
            FROM textStream\n\
            WHERE length(content) > 5;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::String("news".to_string()),
            AttributeValue::String("Hello World".to_string()),
        ],
    );
    runner.send(
        "textStream",
        vec![
            AttributeValue::String("news".to_string()),
            AttributeValue::String("Hi".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("Hello World".to_string()));
}

/// Partition with boolean-like integer filter
#[tokio::test]
async fn partition_test74_bool_int() {
    let app = "\
        CREATE STREAM flagStream (group STRING, active INT, value INT);\n\
        CREATE STREAM outputStream (group STRING, value INT);\n\
        PARTITION WITH (group OF flagStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group, value\n\
            FROM flagStream\n\
            WHERE active = 1;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "flagStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(1),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "flagStream",
        vec![
            AttributeValue::String("B".to_string()),
            AttributeValue::Int(0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Partition with multiple partitions active
#[tokio::test]
async fn partition_test75_multi_partition() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, amount INT);\n\
        PARTITION WITH (region OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, amount\n\
            FROM salesStream;\n\
        END;\n";
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
            AttributeValue::String("APAC".to_string()),
            AttributeValue::Int(300),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
}

/// Partition with DOUBLE field
#[tokio::test]
async fn partition_test76_double_field() {
    let app = "\
        CREATE STREAM priceStream (category STRING, price DOUBLE);\n\
        CREATE STREAM outputStream (category STRING, price DOUBLE);\n\
        PARTITION WITH (category OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, price\n\
            FROM priceStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Double(19.99),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let price = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double"),
    };
    assert!((price - 19.99).abs() < 0.001);
}

/// Partition with concat function
#[tokio::test]
async fn partition_test77_concat() {
    let app = "\
        CREATE STREAM personStream (dept STRING, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (dept STRING, fullName STRING);\n\
        PARTITION WITH (dept OF personStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT dept, concat(firstName, ' ', lastName) AS fullName\n\
            FROM personStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::String("Engineering".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Partition with lower function
#[tokio::test]
async fn partition_test78_lower() {
    let app = "\
        CREATE STREAM msgStream (channel STRING, message STRING);\n\
        CREATE STREAM outputStream (channel STRING, lower_msg STRING);\n\
        PARTITION WITH (channel OF msgStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT channel, lower(message) AS lower_msg\n\
            FROM msgStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("chat".to_string()),
            AttributeValue::String("HELLO WORLD".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("hello world".to_string()));
}

/// Partition with coalesce function
#[tokio::test]
#[ignore = "coalesce with comma-separated columns parsing issue in partition"]
async fn partition_test79_coalesce() {
    let app = "\
        CREATE STREAM dataStream (key STRING, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (key STRING, result STRING);\n\
        PARTITION WITH (key OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT key, coalesce(primary_val, backup_val) AS result\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("K1".to_string()),
            AttributeValue::String("primary".to_string()),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("primary".to_string()));
}

/// Partition with subtraction
#[tokio::test]
async fn partition_test80_subtraction() {
    let app = "\
        CREATE STREAM orderStream (region STRING, gross INT, discount INT);\n\
        CREATE STREAM outputStream (region STRING, net INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, gross - discount AS net\n\
            FROM orderStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(15),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(85)); // 100 - 15 = 85
}

/// Partition with multiplication
#[tokio::test]
async fn partition_test81_multiplication() {
    let app = "\
        CREATE STREAM saleStream (store STRING, qty INT, price INT);\n\
        CREATE STREAM outputStream (store STRING, revenue INT);\n\
        PARTITION WITH (store OF saleStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT store, qty * price AS revenue\n\
            FROM saleStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "saleStream",
        vec![
            AttributeValue::String("Store1".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100)); // 5 * 20 = 100
}

/// Partition with length function
#[tokio::test]
async fn partition_test82_length() {
    let app = "\
        CREATE STREAM textStream (category STRING, content STRING);\n\
        CREATE STREAM outputStream (category STRING, content_len INT);\n\
        PARTITION WITH (category OF textStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, length(content) AS content_len\n\
            FROM textStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::String("news".to_string()),
            AttributeValue::String("hello world".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len_val, 11);
}

/// Partition with NOT EQUAL filter
#[tokio::test]
async fn partition_test83_not_equal() {
    let app = "\
        CREATE STREAM itemStream (category STRING, status STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, status STRING, value INT);\n\
        PARTITION WITH (category OF itemStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, status, value\n\
            FROM itemStream\n\
            WHERE status != 'DELETED';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "itemStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("ACTIVE".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ACTIVE".to_string()));
}

/// Partition with uuid function
#[tokio::test]
async fn partition_test84_uuid_function() {
    let app = "\
        CREATE STREAM triggerStream (partition_key STRING, data INT);\n\
        CREATE STREAM outputStream (partition_key STRING, uuid_val STRING);\n\
        PARTITION WITH (partition_key OF triggerStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT partition_key, uuid() AS uuid_val\n\
            FROM triggerStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "triggerStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(1),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Partition with CASE WHEN complex
#[tokio::test]
async fn partition_test85_case_when_complex() {
    let app = "\
        CREATE STREAM scoreStream (team STRING, score INT);\n\
        CREATE STREAM outputStream (team STRING, tier STRING);\n\
        PARTITION WITH (team OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT team, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS tier\n\
            FROM scoreStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("Team1".to_string()),
            AttributeValue::Int(85),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("B".to_string()));
}

/// Partition with window sum aggregation
#[tokio::test]
async fn partition_test86_window_sum() {
    let app = "\
        CREATE STREAM salesStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT);\n\
        PARTITION WITH (region OF salesStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, sum(amount) AS total\n\
            FROM salesStream WINDOW('lengthBatch', 2)\n\
            GROUP BY region;\n\
        END;\n";
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
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 250);
}

/// Partition with window count aggregation
#[tokio::test]
async fn partition_test87_window_count() {
    let app = "\
        CREATE STREAM eventStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, cnt INT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, count(value) AS cnt\n\
            FROM eventStream WINDOW('lengthBatch', 3)\n\
            GROUP BY category;\n\
        END;\n";
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
            AttributeValue::String("A".to_string()),
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
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let cnt = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(cnt, 3);
}

/// Partition with window avg aggregation
#[tokio::test]
async fn partition_test88_window_avg() {
    let app = "\
        CREATE STREAM dataStream (group_key STRING, value INT);\n\
        CREATE STREAM outputStream (group_key STRING, avg_val DOUBLE);\n\
        PARTITION WITH (group_key OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group_key, avg(value) AS avg_val\n\
            FROM dataStream WINDOW('lengthBatch', 2)\n\
            GROUP BY group_key;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(avg) = &out[0][1] {
        assert!((avg - 20.0).abs() < 0.01);
    } else {
        panic!("Expected double");
    }
}

/// Partition with window min aggregation
#[tokio::test]
async fn partition_test89_window_min() {
    let app = "\
        CREATE STREAM tempStream (sensor STRING, temp INT);\n\
        CREATE STREAM outputStream (sensor STRING, min_temp INT);\n\
        PARTITION WITH (sensor OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor, min(temp) AS min_temp\n\
            FROM tempStream WINDOW('lengthBatch', 3)\n\
            GROUP BY sensor;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(25),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(18),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(22),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(18));
}

/// Partition with window max aggregation
#[tokio::test]
async fn partition_test90_window_max() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, max_score INT);\n\
        PARTITION WITH (player OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT player, max(score) AS max_score\n\
            FROM scoreStream WINDOW('lengthBatch', 3)\n\
            GROUP BY player;\n\
        END;\n";
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
            AttributeValue::Int(95),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(88),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(95));
}

/// Partition with WHERE filter
#[tokio::test]
async fn partition_test91_where_filter() {
    let app = "\
        CREATE STREAM orderStream (customer STRING, amount INT);\n\
        CREATE STREAM outputStream (customer STRING, amount INT);\n\
        PARTITION WITH (customer OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT customer, amount\n\
            FROM orderStream\n\
            WHERE amount > 100;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(80),
        ],
    );
    let out = runner.shutdown();
    // Only amount=150 passes
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
}

/// Partition with OR condition in WHERE
#[tokio::test]
async fn partition_test92_or_condition() {
    let app = "\
        CREATE STREAM alertStream (device STRING, level INT, priority STRING);\n\
        CREATE STREAM outputStream (device STRING, level INT);\n\
        PARTITION WITH (device OF alertStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT device, level\n\
            FROM alertStream\n\
            WHERE level > 80 OR priority = 'high';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(50),
            AttributeValue::String("high".to_string()),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(90),
            AttributeValue::String("low".to_string()),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(30),
            AttributeValue::String("low".to_string()),
        ],
    );
    let out = runner.shutdown();
    // First (priority=high) and second (level>80) pass
    assert_eq!(out.len(), 2);
}

/// Partition with AND condition in WHERE
#[tokio::test]
async fn partition_test93_and_condition() {
    let app = "\
        CREATE STREAM tradeStream (symbol STRING, price INT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, value INT);\n\
        PARTITION WITH (symbol OF tradeStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT symbol, price * volume AS value\n\
            FROM tradeStream\n\
            WHERE price > 10 AND volume >= 100;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(15),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(300),
        ],
    );
    runner.send(
        "tradeStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Int(20),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    // Only first passes (price>10 AND volume>=100)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(3000)); // 15 * 200
}

/// Partition with arithmetic in select
#[tokio::test]
async fn partition_test94_arithmetic_select() {
    let app = "\
        CREATE STREAM invoiceStream (client STRING, subtotal INT, tax INT, discount INT);\n\
        CREATE STREAM outputStream (client STRING, total INT);\n\
        PARTITION WITH (client OF invoiceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT client, (subtotal + tax) - discount AS total\n\
            FROM invoiceStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "invoiceStream",
        vec![
            AttributeValue::String("Client1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(105)); // (100 + 10) - 5 = 105
}

/// Partition with LTE condition
#[tokio::test]
async fn partition_test95_lte_condition() {
    let app = "\
        CREATE STREAM tempStream (sensor STRING, temp INT);\n\
        CREATE STREAM outputStream (sensor STRING, temp INT);\n\
        PARTITION WITH (sensor OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor, temp\n\
            FROM tempStream\n\
            WHERE temp <= 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(-5),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    // Only temp=-5 passes
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-5));
}

/// Partition with GTE condition
#[tokio::test]
async fn partition_test96_gte_condition() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, score INT);\n\
        PARTITION WITH (player OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT player, score\n\
            FROM scoreStream\n\
            WHERE score >= 90;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(85),
        ],
    );
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(95),
        ],
    );
    let out = runner.shutdown();
    // Only score=95 passes
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(95));
}

/// Partition with string equality check
#[tokio::test]
async fn partition_test97_string_equal() {
    let app = "\
        CREATE STREAM eventStream (category STRING, type STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, value INT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, value\n\
            FROM eventStream\n\
            WHERE type = 'important';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("important".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("normal".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Partition with length window
#[tokio::test]
async fn partition_test98_length_window() {
    let app = "\
        CREATE STREAM dataStream (region STRING, value INT);\n\
        CREATE STREAM outputStream (region STRING, value INT);\n\
        PARTITION WITH (region OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, value\n\
            FROM dataStream WINDOW('length', 2);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(20),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Partition with division in select
#[tokio::test]
async fn partition_test99_division() {
    let app = "\
        CREATE STREAM ratioStream (category STRING, numerator INT, denominator INT);\n\
        CREATE STREAM outputStream (category STRING, ratio DOUBLE);\n\
        PARTITION WITH (category OF ratioStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, numerator / denominator AS ratio\n\
            FROM ratioStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "ratioStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // Division returns Double
    if let AttributeValue::Double(ratio) = &out[0][1] {
        assert!((ratio - 25.0).abs() < 0.01);
    }
}

/// Partition with arithmetic expression (addition and multiplication)
#[tokio::test]
async fn partition_test100_arithmetic_expr() {
    let app = "\
        CREATE STREAM calcStream (partition_key STRING, val1 INT, val2 INT, val3 INT);\n\
        CREATE STREAM outputStream (partition_key STRING, result INT);\n\
        PARTITION WITH (partition_key OF calcStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT partition_key, val1 + val2 * val3 AS result\n\
            FROM calcStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "calcStream",
        vec![
            AttributeValue::String("K1".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(3),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(11)); // 5 + (3 * 2) = 11 (multiplication first)
}

/// Partition with range check (simulating BETWEEN)
#[tokio::test]
async fn partition_test101_range_check() {
    let app = "\
        CREATE STREAM valueStream (id STRING, value INT);\n\
        CREATE STREAM outputStream (id STRING, value INT);\n\
        PARTITION WITH (id OF valueStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT id, value\n\
            FROM valueStream\n\
            WHERE value >= 10 AND value <= 50;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("V1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("V1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("V1".to_string()),
            AttributeValue::Int(60),
        ],
    );
    let out = runner.shutdown();
    // Only value=30 passes the range
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(30));
}

/// Partition with upper function
#[tokio::test]
async fn partition_test102_upper_function() {
    let app = "\
        CREATE STREAM nameStream (category STRING, name STRING);\n\
        CREATE STREAM outputStream (category STRING, upper_name STRING);\n\
        PARTITION WITH (category OF nameStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, upper(name) AS upper_name\n\
            FROM nameStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("HELLO".to_string()));
}

/// Partition with multiple aggregations
#[tokio::test]
async fn partition_test103_multi_agg() {
    let app = "\
        CREATE STREAM metricStream (device STRING, value INT);\n\
        CREATE STREAM outputStream (device STRING, total INT, cnt INT);\n\
        PARTITION WITH (device OF metricStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT device, sum(value) AS total, count(value) AS cnt\n\
            FROM metricStream WINDOW('lengthBatch', 3)\n\
            GROUP BY device;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 60);
}

/// Partition with CASE WHEN
#[tokio::test]
async fn partition_test104_case_when_filter() {
    let app = "\
        CREATE STREAM gradeStream (student STRING, score INT);\n\
        CREATE STREAM outputStream (student STRING, grade STRING);\n\
        PARTITION WITH (student OF gradeStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT student, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade\n\
            FROM gradeStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "gradeStream",
        vec![
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("B".to_string()));
}

/// Partition with lower function
#[tokio::test]
async fn partition_test105_lower_function() {
    let app = "\
        CREATE STREAM msgStream (category STRING, message STRING);\n\
        CREATE STREAM outputStream (category STRING, lower_msg STRING);\n\
        PARTITION WITH (category OF msgStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, lower(message) AS lower_msg\n\
            FROM msgStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("HELLO WORLD".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("hello world".to_string()));
}

/// Partition with concat function
#[tokio::test]
async fn partition_test106_concat_function() {
    let app = "\
        CREATE STREAM personStream (region STRING, first_name STRING, last_name STRING);\n\
        CREATE STREAM outputStream (region STRING, full_name STRING);\n\
        PARTITION WITH (region OF personStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, concat(first_name, ' ', last_name) AS full_name\n\
            FROM personStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Partition with length function
#[tokio::test]
async fn partition_test107_length_function() {
    let app = "\
        CREATE STREAM textStream (category STRING, content STRING);\n\
        CREATE STREAM outputStream (category STRING, content_len INT);\n\
        PARTITION WITH (category OF textStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, length(content) AS content_len\n\
            FROM textStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len, 5);
}

/// Partition with not equal filter
#[tokio::test]
async fn partition_test108_not_equal_filter() {
    let app = "\
        CREATE STREAM statusStream (device STRING, status STRING);\n\
        CREATE STREAM outputStream (device STRING, status STRING);\n\
        PARTITION WITH (device OF statusStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT device, status\n\
            FROM statusStream\n\
            WHERE status != 'offline';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::String("online".to_string()),
        ],
    );
    runner.send(
        "statusStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::String("offline".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("online".to_string()));
}

/// Partition with greater than filter
#[tokio::test]
async fn partition_test109_greater_than() {
    let app = "\
        CREATE STREAM metricStream (sensor STRING, value INT);\n\
        CREATE STREAM outputStream (sensor STRING, value INT);\n\
        PARTITION WITH (sensor OF metricStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor, value\n\
            FROM metricStream\n\
            WHERE value > 100;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
}

/// Partition with less than filter
#[tokio::test]
async fn partition_test110_less_than() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price INT);\n\
        CREATE STREAM outputStream (product STRING, price INT);\n\
        PARTITION WITH (product OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT product, price\n\
            FROM priceStream\n\
            WHERE price < 50;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(30));
}

/// Partition with zero value check
#[tokio::test]
async fn partition_test111_zero_check() {
    let app = "\
        CREATE STREAM counterStream (counter_id STRING, count_val INT);\n\
        CREATE STREAM outputStream (counter_id STRING, count_val INT);\n\
        PARTITION WITH (counter_id OF counterStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT counter_id, count_val\n\
            FROM counterStream\n\
            WHERE count_val = 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "counterStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "counterStream",
        vec![
            AttributeValue::String("C1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(0));
}

/// Partition with negative value check
#[tokio::test]
async fn partition_test112_negative_check() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, temp INT);\n\
        PARTITION WITH (location OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT location, temp\n\
            FROM tempStream\n\
            WHERE temp < 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("L1".to_string()),
            AttributeValue::Int(-10),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("L1".to_string()),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-10));
}

/// Partition with addition in select
#[tokio::test]
async fn partition_test113_addition() {
    let app = "\
        CREATE STREAM valueStream (group_id STRING, val1 INT, val2 INT);\n\
        CREATE STREAM outputStream (group_id STRING, total INT);\n\
        PARTITION WITH (group_id OF valueStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group_id, val1 + val2 AS total\n\
            FROM valueStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(150));
}

/// Partition with distinctCount aggregation
#[tokio::test]
async fn partition_test114_distinct_count() {
    let app = "\
        CREATE STREAM eventStream (category STRING, event_type STRING);\n\
        CREATE STREAM outputStream (category STRING, unique_count INT);\n\
        PARTITION WITH (category OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, distinctCount(event_type) AS unique_count\n\
            FROM eventStream WINDOW('lengthBatch', 4)\n\
            GROUP BY category;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("click".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("view".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("click".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::String("purchase".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let count = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(count, 3); // click, view, purchase
}

/// Partition with coalesce function
#[tokio::test]
async fn partition_test115_coalesce() {
    let app = "\
        CREATE STREAM dataStream (region STRING, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (region STRING, result STRING);\n\
        PARTITION WITH (region OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, coalesce(primary_val, backup_val) AS result\n\
            FROM dataStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("primary".to_string()),
            AttributeValue::String("backup".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("primary".to_string()));
}

/// Partition with uuid function
#[tokio::test]
async fn partition_test116_uuid() {
    let app = "\
        CREATE STREAM eventStream (source STRING, event_name STRING);\n\
        CREATE STREAM outputStream (source STRING, event_id STRING);\n\
        PARTITION WITH (source OF eventStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, uuid() AS event_id\n\
            FROM eventStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::String("web".to_string()),
            AttributeValue::String("click".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(uuid.len() > 0);
    } else {
        panic!("Expected string UUID");
    }
}

/// Partition with multiple conditions in WHERE (AND)
#[tokio::test]
async fn partition_test117_multi_and_where() {
    let app = "\
        CREATE STREAM sensorStream (device STRING, temp INT, humidity INT);\n\
        CREATE STREAM outputStream (device STRING, temp INT, humidity INT);\n\
        PARTITION WITH (device OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT device, temp, humidity\n\
            FROM sensorStream\n\
            WHERE temp > 20 AND humidity > 50;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(25),
            AttributeValue::Int(60),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(15),
            AttributeValue::Int(60),
        ],
    ); // filtered
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("D1".to_string()),
            AttributeValue::Int(25),
            AttributeValue::Int(40),
        ],
    ); // filtered
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(25));
}

/// Partition with multiple conditions in WHERE (OR)
#[tokio::test]
async fn partition_test118_multi_or_where() {
    let app = "\
        CREATE STREAM alertStream (zone STRING, severity INT, priority STRING);\n\
        CREATE STREAM outputStream (zone STRING, severity INT);\n\
        PARTITION WITH (zone OF alertStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT zone, severity\n\
            FROM alertStream\n\
            WHERE severity > 8 OR priority = 'critical';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(9),
            AttributeValue::String("normal".to_string()),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(3),
            AttributeValue::String("critical".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Partition with subtraction in WHERE
#[tokio::test]
async fn partition_test119_subtraction_where() {
    let app = "\
        CREATE STREAM accountStream (account_id STRING, balance INT, threshold INT);\n\
        CREATE STREAM outputStream (account_id STRING, margin INT);\n\
        PARTITION WITH (account_id OF accountStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT account_id, balance - threshold AS margin\n\
            FROM accountStream\n\
            WHERE balance - threshold > 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "accountStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(1000),
            AttributeValue::Int(500),
        ],
    );
    runner.send(
        "accountStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(400),
            AttributeValue::Int(500),
        ],
    ); // filtered
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Partition with sum and count together
#[tokio::test]
async fn partition_test120_sum_count_together() {
    let app = "\
        CREATE STREAM orderStream (store STRING, amount INT);\n\
        CREATE STREAM outputStream (store STRING, total INT, order_count INT);\n\
        PARTITION WITH (store OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT store, sum(amount) AS total, count(amount) AS order_count\n\
            FROM orderStream WINDOW('lengthBatch', 3)\n\
            GROUP BY store;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
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
    assert_eq!(sum_val, 450);
    assert_eq!(count_val, 3);
}

/// Partition with min and max together
#[tokio::test]
async fn partition_test121_min_max_together() {
    let app = "\
        CREATE STREAM metricStream (service STRING, latency INT);\n\
        CREATE STREAM outputStream (service STRING, min_lat INT, max_lat INT);\n\
        PARTITION WITH (service OF metricStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT service, min(latency) AS min_lat, max(latency) AS max_lat\n\
            FROM metricStream WINDOW('lengthBatch', 4)\n\
            GROUP BY service;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
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
            AttributeValue::Int(120),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(30),
        ],
    );
    runner.send(
        "metricStream",
        vec![
            AttributeValue::String("api".to_string()),
            AttributeValue::Int(80),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let min_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    let max_val = match &out[0][2] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(min_val, 30);
    assert_eq!(max_val, 120);
}

/// Partition with double type values
#[tokio::test]
async fn partition_test122_double_values() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price DOUBLE);\n\
        CREATE STREAM outputStream (product STRING, avg_price DOUBLE);\n\
        PARTITION WITH (product OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT product, avg(price) AS avg_price\n\
            FROM priceStream WINDOW('lengthBatch', 2)\n\
            GROUP BY product;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(10.5),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(20.5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Double(15.5));
}

/// Partition with string equality in WHERE
#[tokio::test]
async fn partition_test123_string_equality() {
    let app = "\
        CREATE STREAM logStream (app_name STRING, level STRING, message STRING);\n\
        CREATE STREAM outputStream (app_name STRING, message STRING);\n\
        PARTITION WITH (app_name OF logStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT app_name, message\n\
            FROM logStream\n\
            WHERE level = 'ERROR';\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("myapp".to_string()),
            AttributeValue::String("ERROR".to_string()),
            AttributeValue::String("Failed".to_string()),
        ],
    );
    runner.send(
        "logStream",
        vec![
            AttributeValue::String("myapp".to_string()),
            AttributeValue::String("INFO".to_string()),
            AttributeValue::String("Started".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("Failed".to_string()));
}

/// Partition with multiplication in select
#[tokio::test]
async fn partition_test124_multiplication() {
    let app = "\
        CREATE STREAM itemStream (category STRING, quantity INT, unit_price INT);\n\
        CREATE STREAM outputStream (category STRING, total_price INT);\n\
        PARTITION WITH (category OF itemStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, quantity * unit_price AS total_price\n\
            FROM itemStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "itemStream",
        vec![
            AttributeValue::String("electronics".to_string()),
            AttributeValue::Int(5),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Partition with CASE WHEN expression
#[tokio::test]
async fn partition_test125_case_when() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, grade STRING);\n\
        PARTITION WITH (player OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT player, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade\n\
            FROM scoreStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(95),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("A".to_string()));
}

/// Partition with length window and filter
#[tokio::test]
async fn partition_test126_length_window_filter() {
    let app = "\
        CREATE STREAM readingStream (sensor STRING, value INT);\n\
        CREATE STREAM outputStream (sensor STRING, avg_value DOUBLE);\n\
        PARTITION WITH (sensor OF readingStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor, avg(value) AS avg_value\n\
            FROM readingStream WINDOW('length', 3)\n\
            WHERE value > 0\n\
            GROUP BY sensor;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "readingStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "readingStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "readingStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Partition with modulo-like check using subtraction
#[tokio::test]
async fn partition_test127_even_odd_check() {
    let app = "\
        CREATE STREAM numStream (batch STRING, num INT);\n\
        CREATE STREAM outputStream (batch STRING, num INT);\n\
        PARTITION WITH (batch OF numStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT batch, num\n\
            FROM numStream\n\
            WHERE num > 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "numStream",
        vec![
            AttributeValue::String("B1".to_string()),
            AttributeValue::Int(4),
        ],
    );
    runner.send(
        "numStream",
        vec![
            AttributeValue::String("B1".to_string()),
            AttributeValue::Int(-2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(4));
}

/// Partition with sqrt function
#[tokio::test]
async fn partition_test128_sqrt() {
    let app = "\
        CREATE STREAM mathStream (group_name STRING, value DOUBLE);\n\
        CREATE STREAM outputStream (group_name STRING, sqrt_val DOUBLE);\n\
        PARTITION WITH (group_name OF mathStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group_name, sqrt(value) AS sqrt_val\n\
            FROM mathStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "mathStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Double(16.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Double(4.0));
}

/// Partition with round function
#[tokio::test]
async fn partition_test129_round() {
    let app = "\
        CREATE STREAM valStream (group_id STRING, value DOUBLE);\n\
        CREATE STREAM outputStream (group_id STRING, rounded DOUBLE);\n\
        PARTITION WITH (group_id OF valStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT group_id, round(value) AS rounded\n\
            FROM valStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valStream",
        vec![
            AttributeValue::String("G1".to_string()),
            AttributeValue::Double(3.7),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Double(4.0));
}

/// Partition with nested concat upper
#[tokio::test]
async fn partition_test130_nested_concat_upper() {
    let app = "\
        CREATE STREAM nameStream (region STRING, first_name STRING, last_name STRING);\n\
        CREATE STREAM outputStream (region STRING, full_name STRING);\n\
        PARTITION WITH (region OF nameStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, upper(concat(first_name, last_name)) AS full_name\n\
            FROM nameStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("JOHNDOE".to_string()));
}

/// Partition with lengthBatch window size 5
#[tokio::test]
async fn partition_test131_length_batch_5() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, total INT);\n\
        PARTITION WITH (category OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, sum(value) AS total\n\
            FROM dataStream WINDOW('lengthBatch', 5)\n\
            GROUP BY category;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 1..=5 {
        runner.send(
            "dataStream",
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(i * 10),
            ],
        );
    }
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 150); // 10+20+30+40+50 = 150
}

/// Partition with multiple string functions
#[tokio::test]
async fn partition_test132_string_functions() {
    let app = "\
        CREATE STREAM msgStream (channel STRING, message STRING);\n\
        CREATE STREAM outputStream (channel STRING, msg_len INT, msg_upper STRING);\n\
        PARTITION WITH (channel OF msgStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT channel, length(message) AS msg_len, upper(message) AS msg_upper\n\
            FROM msgStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::String("general".to_string()),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len_val = match &out[0][1] {
        AttributeValue::Int(v) => *v,
        AttributeValue::Long(v) => *v as i32,
        _ => panic!("Expected int"),
    };
    assert_eq!(len_val, 5);
    assert_eq!(out[0][2], AttributeValue::String("HELLO".to_string()));
}

/// Partition with range filter using AND
#[tokio::test]
async fn partition_test133_range_filter() {
    let app = "\
        CREATE STREAM sensorStream (sensor_id STRING, temp INT);\n\
        CREATE STREAM outputStream (sensor_id STRING, temp INT);\n\
        PARTITION WITH (sensor_id OF sensorStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT sensor_id, temp\n\
            FROM sensorStream\n\
            WHERE temp >= 20 AND temp <= 30;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(25),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(15),
        ],
    );
    runner.send(
        "sensorStream",
        vec![
            AttributeValue::String("S1".to_string()),
            AttributeValue::Int(35),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(25));
}

/// Partition with multiple aggregations and filter
#[tokio::test]
async fn partition_test134_multi_agg_filter() {
    let app = "\
        CREATE STREAM orderStream (region STRING, amount INT);\n\
        CREATE STREAM outputStream (region STRING, total INT, avg_amt DOUBLE, count_amt INT);\n\
        PARTITION WITH (region OF orderStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, sum(amount) AS total, avg(amount) AS avg_amt, count(amount) AS count_amt\n\
            FROM orderStream WINDOW('lengthBatch', 4)\n\
            WHERE amount > 0\n\
            GROUP BY region;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(200),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(300),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::String("East".to_string()),
            AttributeValue::Int(400),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let sum_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 1000);
}

/// Partition with double type sum
#[tokio::test]
async fn partition_test135_double_sum() {
    let app = "\
        CREATE STREAM priceStream (product STRING, price DOUBLE);\n\
        CREATE STREAM outputStream (product STRING, total_price DOUBLE);\n\
        PARTITION WITH (product OF priceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT product, sum(price) AS total_price\n\
            FROM priceStream WINDOW('lengthBatch', 3)\n\
            GROUP BY product;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(10.5),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(20.25),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("Widget".to_string()),
            AttributeValue::Double(30.75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(v) = out[0][1] {
        assert!((v - 61.5).abs() < 0.01);
    } else {
        panic!("Expected Double");
    }
}

/// Partition with integer division
#[tokio::test]
async fn partition_test136_division() {
    let app = "\
        CREATE STREAM rateStream (source STRING, total INT, count_val INT);\n\
        CREATE STREAM outputStream (source STRING, rate DOUBLE);\n\
        PARTITION WITH (source OF rateStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT source, total / count_val AS rate\n\
            FROM rateStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "rateStream",
        vec![
            AttributeValue::String("API".to_string()),
            AttributeValue::Int(100),
            AttributeValue::Int(4),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let rate = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Long(v) => *v as f64,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(rate, 25.0);
}

/// Partition with string concatenation three args
#[tokio::test]
async fn partition_test137_concat_three() {
    let app = "\
        CREATE STREAM nameStream (region STRING, first STRING, middle STRING, last STRING);\n\
        CREATE STREAM outputStream (region STRING, full_name STRING);\n\
        PARTITION WITH (region OF nameStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT region, concat(first, middle, last) AS full_name\n\
            FROM nameStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::String("US".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String(" ".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Partition with minForever aggregation
#[ignore = "minForever aggregation not yet supported in SQL parser"]
#[tokio::test]
async fn partition_test138_min_forever() {
    let app = "\
        CREATE STREAM tempStream (location STRING, temp INT);\n\
        CREATE STREAM outputStream (location STRING, min_temp INT);\n\
        PARTITION WITH (location OF tempStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT location, minForever(temp) AS min_temp\n\
            FROM tempStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(15),
        ],
    );
    runner.send(
        "tempStream",
        vec![
            AttributeValue::String("NYC".to_string()),
            AttributeValue::Int(25),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // Last output should have min=15
    let last = out.last().unwrap();
    let min_val = match &last[1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(min_val, 15);
}

/// Partition with maxForever aggregation
#[ignore = "maxForever aggregation not yet supported in SQL parser"]
#[tokio::test]
async fn partition_test139_max_forever() {
    let app = "\
        CREATE STREAM scoreStream (player STRING, score INT);\n\
        CREATE STREAM outputStream (player STRING, max_score INT);\n\
        PARTITION WITH (player OF scoreStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT player, maxForever(score) AS max_score\n\
            FROM scoreStream;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![
            AttributeValue::String("P1".to_string()),
            AttributeValue::Int(50),
        ],
    );
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
            AttributeValue::Int(60),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    let last = out.last().unwrap();
    let max_val = match &last[1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(max_val, 80);
}

/// Partition with length window sum
#[tokio::test]
async fn partition_test140_length_sum() {
    let app = "\
        CREATE STREAM dataStream (category STRING, value INT);\n\
        CREATE STREAM outputStream (category STRING, rolling_sum INT);\n\
        PARTITION WITH (category OF dataStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT category, sum(value) AS rolling_sum\n\
            FROM dataStream WINDOW('length', 3)\n\
            GROUP BY category;\n\
        END;\n";
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
    runner.send(
        "dataStream",
        vec![
            AttributeValue::String("A".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // After 3 events, sum = 10+20+30 = 60
    let last = out.last().unwrap();
    let sum_val = match &last[1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected numeric"),
    };
    assert_eq!(sum_val, 60);
}

/// Partition with boolean-like condition
#[tokio::test]
async fn partition_test141_bool_condition() {
    let app = "\
        CREATE STREAM flagStream (id STRING, active INT);\n\
        CREATE STREAM outputStream (id STRING, active INT);\n\
        PARTITION WITH (id OF flagStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT id, active\n\
            FROM flagStream\n\
            WHERE active = 1;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "flagStream",
        vec![
            AttributeValue::String("F1".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "flagStream",
        vec![
            AttributeValue::String("F1".to_string()),
            AttributeValue::Int(0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(1));
}

/// Partition with negative number handling
#[tokio::test]
async fn partition_test142_negative_handling() {
    let app = "\
        CREATE STREAM balanceStream (account STRING, balance INT);\n\
        CREATE STREAM outputStream (account STRING, balance INT);\n\
        PARTITION WITH (account OF balanceStream)\n\
        BEGIN\n\
            INSERT INTO outputStream\n\
            SELECT account, balance\n\
            FROM balanceStream\n\
            WHERE balance < 0;\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(-100),
        ],
    );
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::String("A1".to_string()),
            AttributeValue::Int(50),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-100));
}
