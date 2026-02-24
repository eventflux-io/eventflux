// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Table Compatibility Tests
// Reference: query/table/*.java, InsertIntoTableTestCase.java

use super::common::AppRunner;
use eventflux::core::event::value::AttributeValue;
use std::thread::sleep;
use std::time::Duration;

// ============================================================================
// TABLE INSERT AND QUERY TESTS
// Reference: query/table/InsertIntoTableTestCase.java
// ============================================================================

/// Test table insert and query
/// Reference: InsertIntoTableTestCase.java:insertIntoTableTest1
#[tokio::test]
async fn table_test1_insert_and_query() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM queryStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price, stockTable.volume\n\
        FROM queryStream JOIN stockTable ON queryStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert into table
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Int(50),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query the table
    runner.send(
        "queryStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

// ============================================================================
// PARTITION TESTS
// Reference: query/partition/PartitionTestCase1.java
// ============================================================================

/// Test value-based partition
/// Reference: PartitionTestCase1.java:testPartitionQuery1
/// Note: PARTITION BY syntax not yet supported
#[tokio::test]
#[ignore = "PARTITION BY syntax not yet supported"]
async fn partition_test1_value_based() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, totalVolume BIGINT);\n\
        \n\
        PARTITION BY symbol OF stockStream\n\
        BEGIN\n\
        INSERT INTO outputStream\n\
        SELECT symbol, sum(volume) AS totalVolume\n\
        FROM stockStream WINDOW('length', 2);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(75.6),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(55.6),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(76.6),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Each symbol should have its own partition with separate aggregation
    assert!(!out.is_empty());
}

// ============================================================================
// TRIGGER TESTS
// Reference: query/trigger/TriggerTestCase.java
// ============================================================================

/// Periodic trigger test
/// Reference: TriggerTestCase.java:triggerTest1
#[tokio::test]
#[ignore = "Trigger syntax not yet fully supported in SQL converter"]
async fn trigger_test1_periodic() {
    let app = "\
        CREATE TRIGGER FiveSecTrigger AT EVERY 5000;\n\
        CREATE STREAM outputStream (triggered BOOLEAN);\n\
        INSERT INTO outputStream\n\
        SELECT true AS triggered FROM FiveSecTrigger;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Wait for trigger to fire
    sleep(Duration::from_millis(6000));
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Start trigger test
/// Reference: TriggerTestCase.java:triggerTest2
#[tokio::test]
#[ignore = "Trigger syntax not yet fully supported in SQL converter"]
async fn trigger_test2_start() {
    let app = "\
        CREATE TRIGGER StartTrigger AT 'start';\n\
        CREATE STREAM outputStream (triggered BOOLEAN);\n\
        INSERT INTO outputStream\n\
        SELECT true AS triggered FROM StartTrigger;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    // Start trigger should fire once at startup
    assert_eq!(out.len(), 1);
}

// ============================================================================
// TABLE UPDATE/DELETE TESTS
// Reference: query/table/UpdateTableTestCase.java, DeleteFromTableTestCase.java
// ============================================================================

/// Table update test - verify lookup works without UPDATE first
#[tokio::test]
async fn table_test2a_lookup_without_update() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert initial data
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(100));
    // Lookup without update
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    // Table should return original values
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(100.0)); // Original price
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

/// Table update test
/// Reference: UpdateTableTestCase.java:updateTableTest1
#[tokio::test]
async fn table_test2_update() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM updateStream (symbol STRING, newPrice FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert initial data
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(100));
    // Update the price
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(100));
    // Lookup to verify the update
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    // Table should have updated price
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(150.0)); // Updated price
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

/// Table delete test
/// Reference: DeleteFromTableTestCase.java:deleteFromTableTest1
#[tokio::test]
async fn table_test3_delete() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM deleteStream (symbol STRING);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        DELETE FROM stockTable\n\
        USING deleteStream\n\
        WHERE stockTable.symbol = deleteStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert initial data
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(50));
    // Delete the row
    runner.send(
        "deleteStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    // Lookup to verify the delete - should find nothing
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    let out = runner.shutdown();
    // Row should be deleted - lookup returns empty
    assert!(out.is_empty());
}

/// Table with primary key
/// Reference: DefineTableTestCase.java
#[tokio::test]
#[ignore = "Primary key syntax not yet supported"]
async fn table_test4_primary_key() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING PRIMARY KEY, price FLOAT, volume INT);\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    // Insert duplicate key - should update or error
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Table should have only one IBM entry
    assert!(!out.is_empty());
}

// ============================================================================
// TABLE WITH MULTIPLE QUERIES
// Reference: query/table/InsertIntoTableTestCase.java
// ============================================================================

/// Test table with multiple queries
/// Reference: InsertIntoTableTestCase.java:insertIntoTableTest3
#[tokio::test]
async fn table_test5_multiple_queries() {
    let app = "\
        CREATE TABLE userTable (userId INT, name STRING, score INT);\n\
        CREATE STREAM insertStream (userId INT, name STRING, score INT);\n\
        CREATE STREAM lookupStream (userId INT);\n\
        CREATE STREAM outputStream (name STRING, score INT);\n\
        \n\
        INSERT INTO userTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT userTable.name AS name, userTable.score AS score\n\
        FROM lookupStream JOIN userTable\n\
        ON lookupStream.userId = userTable.userId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert multiple users
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Bob".to_string()),
            AttributeValue::Int(85),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("Charlie".to_string()),
            AttributeValue::Int(92),
        ],
    );
    sleep(Duration::from_millis(50));
    // Lookup user 2
    runner.send("lookupStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("Bob".to_string()));
    assert_eq!(out[0][1], AttributeValue::Int(85));
}

/// Test table with aggregation query
/// Reference: InsertIntoTableTestCase.java:insertIntoTableTest5
#[tokio::test]
#[ignore = "Complex GROUP BY with table join not yet supported"]
async fn table_test6_with_aggregation() {
    let app = "\
        CREATE TABLE salesTable (productId INT, region STRING, amount FLOAT);\n\
        CREATE STREAM insertStream (productId INT, region STRING, amount FLOAT);\n\
        CREATE STREAM queryStream (region STRING);\n\
        CREATE STREAM outputStream (region STRING, total DOUBLE);\n\
        \n\
        INSERT INTO salesTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT salesTable.region AS region, sum(salesTable.amount) AS total\n\
        FROM queryStream JOIN salesTable\n\
        ON queryStream.region = salesTable.region\n\
        GROUP BY salesTable.region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert sales records
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("EU".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query US region
    runner.send(
        "queryStream",
        vec![AttributeValue::String("US".to_string())],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // US total should be 300.0
    assert_eq!(out[0][0], AttributeValue::String("US".to_string()));
}

// ============================================================================
// UPDATE OR INSERT (UPSERT) TESTS
// Reference: query/table/UpdateOrInsertTableTestCase.java
// ============================================================================

/// Test update or insert into table
/// Reference: UpdateOrInsertTableTestCase.java:updateOrInsertTableTest1
#[tokio::test]
async fn table_test7_upsert() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        UPSERT INTO stockTable\n\
        SELECT symbol, price, volume\n\
        FROM stockStream\n\
        ON stockTable.symbol = stockStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // First insert - should insert new row
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(50));
    // Second upsert - should update existing row
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(200),
        ],
    );
    sleep(Duration::from_millis(50));
    // Lookup to verify upsert
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    let out = runner.shutdown();
    // Should have exactly one row with updated values
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(150.0)); // Updated price
    assert_eq!(out[0][2], AttributeValue::Int(200)); // Updated volume
}

// ============================================================================
// COMPREHENSIVE MUTATION TESTS - EDGE CASES
// ============================================================================

/// UPDATE with no matching rows - should not affect table
#[tokio::test]
async fn table_mutation_update_no_match() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM updateStream (symbol STRING, newPrice FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(50));

    // Try to update AAPL (doesn't exist) - should have no effect
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(999.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Lookup IBM - should still have original values
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1, "IBM should still exist");
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Float(100.0),
        "Price should be unchanged"
    );
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

/// UPDATE multiple rows matching same condition
#[tokio::test]
async fn table_mutation_update_multiple_rows() {
    let app = "\
        CREATE TABLE stockTable (category STRING, symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (category STRING, symbol STRING, price FLOAT);\n\
        CREATE STREAM updateStream (category STRING, newPrice FLOAT);\n\
        CREATE STREAM lookupStream (category STRING);\n\
        CREATE STREAM outputStream (category STRING, symbol STRING, price FLOAT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice\n\
        FROM updateStream\n\
        WHERE stockTable.category = updateStream.category;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.category, stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.category = stockTable.category;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert multiple tech stocks
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Update all TECH stocks to same price
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::Float(500.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Lookup TECH category - should find both with updated price
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("TECH".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 2, "Both TECH stocks should be returned");
    // Both should have updated price
    for row in &out {
        assert_eq!(row[0], AttributeValue::String("TECH".to_string()));
        assert_eq!(
            row[2],
            AttributeValue::Float(500.0),
            "Price should be updated to 500.0"
        );
    }
}

/// UPDATE with multiple SET columns
#[tokio::test]
async fn table_mutation_update_multiple_columns() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM updateStream (symbol STRING, newPrice FLOAT, newVolume INT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice, volume = updateStream.newVolume\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price, stockTable.volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert initial data
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(1000),
        ],
    );
    sleep(Duration::from_millis(50));

    // Update both price and volume
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(2000),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify both columns updated
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Float(150.0),
        "Price should be updated"
    );
    assert_eq!(
        out[0][2],
        AttributeValue::Int(2000),
        "Volume should be updated"
    );
}

/// DELETE with no matching rows - should not affect table
#[tokio::test]
async fn table_mutation_delete_no_match() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM deleteStream (symbol STRING);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        DELETE FROM stockTable\n\
        USING deleteStream\n\
        WHERE stockTable.symbol = deleteStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price, stockTable.volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(100),
        ],
    );
    sleep(Duration::from_millis(50));

    // Try to delete AAPL (doesn't exist) - should have no effect
    runner.send(
        "deleteStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(50));

    // Lookup IBM - should still exist
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1, "IBM should still exist after failed delete");
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
}

/// DELETE multiple rows matching same condition
#[tokio::test]
async fn table_mutation_delete_multiple_rows() {
    let app = "\
        CREATE TABLE stockTable (category STRING, symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (category STRING, symbol STRING, price FLOAT);\n\
        CREATE STREAM deleteStream (category STRING);\n\
        CREATE STREAM lookupStream (category STRING);\n\
        CREATE STREAM outputStream (category STRING, symbol STRING, price FLOAT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        DELETE FROM stockTable\n\
        USING deleteStream\n\
        WHERE stockTable.category = deleteStream.category;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.category, stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.category = stockTable.category;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert TECH stocks
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(30));
    // Insert FINANCE stock
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("FINANCE".to_string()),
            AttributeValue::String("JPM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Delete all TECH stocks
    runner.send(
        "deleteStream",
        vec![AttributeValue::String("TECH".to_string())],
    );
    sleep(Duration::from_millis(50));

    // Lookup TECH - should find nothing
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("TECH".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert!(out.is_empty(), "All TECH stocks should be deleted");
}

/// DELETE preserves non-matching rows
#[tokio::test]
async fn table_mutation_delete_preserves_others() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM deleteStream (symbol STRING);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        DELETE FROM stockTable\n\
        USING deleteStream\n\
        WHERE stockTable.symbol = deleteStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert two stocks
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Delete only IBM
    runner.send(
        "deleteStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));

    // Verify AAPL still exists
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1, "AAPL should still exist");
    assert_eq!(out[0][0], AttributeValue::String("AAPL".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(200.0));
}

/// UPSERT pure insert - first event with no existing match
#[tokio::test]
async fn table_mutation_upsert_pure_insert() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT, volume INT);\n\
        \n\
        UPSERT INTO stockTable\n\
        SELECT symbol, price, volume\n\
        FROM stockStream\n\
        ON stockTable.symbol = stockStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price, stockTable.volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // UPSERT into empty table - should insert
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Int(1000),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify the insert
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1, "Row should be inserted");
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(100.0));
    assert_eq!(out[0][2], AttributeValue::Int(1000));
}

/// UPSERT multiple distinct inserts
#[tokio::test]
async fn table_mutation_upsert_multiple_inserts() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        \n\
        UPSERT INTO stockTable\n\
        SELECT symbol, price\n\
        FROM stockStream\n\
        ON stockTable.symbol = stockStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert three different stocks
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(300.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify all three exist
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("MSFT".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 3, "All three stocks should exist");
}

/// UPSERT alternating insert and update
#[tokio::test]
async fn table_mutation_upsert_alternating() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        \n\
        UPSERT INTO stockTable\n\
        SELECT symbol, price\n\
        FROM stockStream\n\
        ON stockTable.symbol = stockStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Insert AAPL (new)
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Update IBM (existing)
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Insert MSFT (new)
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(300.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Lookup IBM - should have updated value
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Float(150.0),
        "IBM should have updated price"
    );
}

/// Interleaved mutations - INSERT, UPDATE, DELETE sequence
#[tokio::test]
async fn table_mutation_interleaved_operations() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM insertStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM updateStream (symbol STRING, newPrice FLOAT);\n\
        CREATE STREAM deleteStream (symbol STRING);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM insertStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        DELETE FROM stockTable\n\
        USING deleteStream\n\
        WHERE stockTable.symbol = deleteStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Insert AAPL
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Update IBM
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(30));

    // Delete AAPL
    runner.send(
        "deleteStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(50));

    // Lookup IBM - should exist with updated price
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(30));

    // Lookup AAPL - should not exist
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1, "Only IBM should exist");
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Float(150.0),
        "IBM should have updated price"
    );
}

/// UPDATE preserves non-matching rows
#[tokio::test]
async fn table_mutation_update_preserves_others() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM updateStream (symbol STRING, newPrice FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price FLOAT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.newPrice\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert two stocks
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(30));
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Float(200.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Update only IBM
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(150.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify AAPL is unchanged
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("AAPL".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("AAPL".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Float(200.0),
        "AAPL should be unchanged"
    );
}

/// UPDATE with expression in SET clause (arithmetic)
#[tokio::test]
async fn table_mutation_update_with_expression() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price DOUBLE) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM updateStream (symbol STRING, multiplier DOUBLE);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price DOUBLE);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = updateStream.multiplier * 100.0\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol, stockTable.price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    sleep(Duration::from_millis(50));

    // Update with expression: price = 1.5 * 100.0 = 150.0
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(1.5),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify the expression was evaluated
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(
        out[0][1],
        AttributeValue::Double(150.0),
        "Price should be multiplier * 100.0"
    );
}

/// UPSERT with different data types
#[tokio::test]
async fn table_mutation_upsert_different_types() {
    let app = "\
        CREATE TABLE dataTable (id INT, name STRING, value DOUBLE, active BOOL) WITH (extension = 'inMemory');\n\
        CREATE STREAM dataStream (id INT, name STRING, value DOUBLE, active BOOL);\n\
        CREATE STREAM lookupStream (id INT);\n\
        CREATE STREAM outputStream (id INT, name STRING, value DOUBLE, active BOOL);\n\
        \n\
        UPSERT INTO dataTable\n\
        SELECT id, name, value, active\n\
        FROM dataStream\n\
        ON dataTable.id = dataStream.id;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT dataTable.id, dataTable.name, dataTable.value, dataTable.active\n\
        FROM lookupStream JOIN dataTable\n\
        ON lookupStream.id = dataTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert with various types
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("test".to_string()),
            AttributeValue::Double(3.14159),
            AttributeValue::Bool(true),
        ],
    );
    sleep(Duration::from_millis(50));

    // Update with different values
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("updated".to_string()),
            AttributeValue::Double(2.71828),
            AttributeValue::Bool(false),
        ],
    );
    sleep(Duration::from_millis(50));

    // Verify updated values
    runner.send("lookupStream", vec![AttributeValue::Int(1)]);
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::String("updated".to_string()));
    assert_eq!(out[0][2], AttributeValue::Double(2.71828));
    assert_eq!(out[0][3], AttributeValue::Bool(false));
}

/// UPDATE with SET expression referencing BOTH table and stream columns
/// This tests that SET expressions are evaluated per-row with proper table context
#[tokio::test]
async fn table_mutation_update_set_references_table_column() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price DOUBLE, volume INT) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price DOUBLE, volume INT);\n\
        CREATE STREAM updateStream (symbol STRING, delta DOUBLE);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price DOUBLE, volume INT);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = stockTable.price + updateStream.delta\n\
        FROM updateStream\n\
        WHERE stockTable.symbol = updateStream.symbol;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price, stockTable.volume AS volume\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert IBM at price 100.0 and AAPL at price 200.0
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(100.0),
            AttributeValue::Int(1000),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("AAPL".to_string()),
            AttributeValue::Double(200.0),
            AttributeValue::Int(2000),
        ],
    );
    sleep(Duration::from_millis(100));

    // Update IBM: price = stockTable.price + delta = 100.0 + 25.0 = 125.0
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Double(25.0),
        ],
    );
    sleep(Duration::from_millis(100));

    // Lookup IBM - should have updated price
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    // The key assertion: price should be 100.0 + 25.0 = 125.0
    // NOT just 25.0 (if table column was ignored) or some other wrong value
    assert_eq!(
        out[0][1],
        AttributeValue::Double(125.0),
        "SET price = stockTable.price + updateStream.delta should compute 100.0 + 25.0 = 125.0"
    );
    assert_eq!(out[0][2], AttributeValue::Int(1000));
}

/// UPDATE with SET expression referencing table columns - multiple rows with different values
/// Verifies each row gets its own computed value, not the same value for all
#[tokio::test]
async fn table_mutation_update_set_per_row_evaluation() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price DOUBLE) WITH (extension = 'inMemory');\n\
        CREATE STREAM stockStream (symbol STRING, price DOUBLE);\n\
        CREATE STREAM updateStream (category STRING, multiplier DOUBLE);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, price DOUBLE);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM stockStream;\n\
        \n\
        UPDATE stockTable SET price = stockTable.price * updateStream.multiplier\n\
        FROM updateStream\n\
        WHERE stockTable.symbol LIKE 'TECH%';\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT stockTable.symbol AS symbol, stockTable.price AS price\n\
        FROM lookupStream JOIN stockTable\n\
        ON lookupStream.symbol = stockTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;

    // Insert two TECH stocks with different prices
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH1".to_string()),
            AttributeValue::Double(100.0),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("TECH2".to_string()),
            AttributeValue::Double(50.0),
        ],
    );
    sleep(Duration::from_millis(100));

    // Update all TECH stocks: multiply each by 2.0
    // TECH1: 100.0 * 2.0 = 200.0
    // TECH2: 50.0 * 2.0 = 100.0
    runner.send(
        "updateStream",
        vec![
            AttributeValue::String("TECH".to_string()),
            AttributeValue::Double(2.0),
        ],
    );
    sleep(Duration::from_millis(100));

    // Lookup both
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("TECH1".to_string())],
    );
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("TECH2".to_string())],
    );
    sleep(Duration::from_millis(50));
    let out = runner.shutdown();

    assert_eq!(out.len(), 2, "Should have 2 results");

    // Find TECH1 and TECH2 in results
    let tech1 = out
        .iter()
        .find(|r| r[0] == AttributeValue::String("TECH1".to_string()));
    let tech2 = out
        .iter()
        .find(|r| r[0] == AttributeValue::String("TECH2".to_string()));

    assert!(tech1.is_some(), "TECH1 should be in results");
    assert!(tech2.is_some(), "TECH2 should be in results");

    // Each row should have its own computed value
    assert_eq!(
        tech1.unwrap()[1],
        AttributeValue::Double(200.0),
        "TECH1: 100.0 * 2.0 = 200.0"
    );
    assert_eq!(
        tech2.unwrap()[1],
        AttributeValue::Double(100.0),
        "TECH2: 50.0 * 2.0 = 100.0"
    );
}

// ============================================================================
// TABLE CONTAINS TESTS
// Reference: query/table/ContainsInTableTestCase.java
// ============================================================================

/// Test contains in table query
/// Reference: ContainsInTableTestCase.java:containsInTableTest1
#[tokio::test]
#[ignore = "CONTAINS IN syntax not yet supported"]
async fn table_test8_contains() {
    let app = "\
        CREATE TABLE stockTable (symbol STRING, price FLOAT);\n\
        CREATE STREAM insertStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM checkStream (symbol STRING);\n\
        CREATE STREAM outputStream (exists BOOLEAN);\n\
        \n\
        INSERT INTO stockTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT (checkStream.symbol CONTAINS IN stockTable) AS exists\n\
        FROM checkStream;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "checkStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

// ============================================================================
// TABLE WITH WINDOW JOIN
// Reference: query/join/JoinTestCase.java
// ============================================================================

/// Test table join with windowed stream
#[tokio::test]
async fn table_test9_window_stream_join() {
    let app = "\
        CREATE TABLE categoryTable (categoryId INT, categoryName STRING);\n\
        CREATE STREAM insertStream (categoryId INT, categoryName STRING);\n\
        CREATE STREAM productStream (productName STRING, categoryId INT, price FLOAT);\n\
        CREATE STREAM outputStream (productName STRING, categoryName STRING, price FLOAT);\n\
        \n\
        INSERT INTO categoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT productStream.productName AS productName, \n\
               categoryTable.categoryName AS categoryName, \n\
               productStream.price AS price\n\
        FROM productStream WINDOW('length', 10)\n\
        JOIN categoryTable\n\
        ON productStream.categoryId = categoryTable.categoryId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert categories
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Books".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Send products
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::Int(1),
            AttributeValue::Float(1200.0),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::String("Novel".to_string()),
            AttributeValue::Int(2),
            AttributeValue::Float(15.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

// ============================================================================
// RANGE-BASED PARTITION TESTS
// Reference: query/partition/PartitionTestCase1.java
// ============================================================================

/// Test range-based partition
/// Reference: PartitionTestCase1.java:testPartitionQuery2
#[tokio::test]
#[ignore = "Range partition syntax not yet supported"]
async fn partition_test2_range_based() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE);\n\
        \n\
        PARTITION BY\n\
            price < 50.0 AS 'cheap' OR\n\
            price >= 50.0 AND price < 100.0 AS 'medium' OR\n\
            price >= 100.0 AS 'expensive'\n\
        OF stockStream\n\
        BEGIN\n\
        INSERT INTO outputStream\n\
        SELECT symbol, avg(price) AS avgPrice\n\
        FROM stockStream WINDOW('length', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(25.0),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("MSFT".to_string()),
            AttributeValue::Float(75.0),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("GOOG".to_string()),
            AttributeValue::Float(150.0),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Each price range should have its own partition
    assert!(!out.is_empty());
}

// ============================================================================
// MULTI-VALUE PARTITION TESTS
// Reference: query/partition/PartitionTestCase1.java
// ============================================================================

/// Test partition with multiple values
/// Reference: PartitionTestCase1.java:testPartitionQuery3
#[tokio::test]
#[ignore = "PARTITION BY with multiple values syntax not yet supported"]
async fn partition_test3_multiple_values() {
    let app = "\
        CREATE STREAM stockStream (symbol STRING, region STRING, price FLOAT, volume INT);\n\
        CREATE STREAM outputStream (symbol STRING, region STRING, totalVolume BIGINT);\n\
        \n\
        PARTITION BY symbol, region OF stockStream\n\
        BEGIN\n\
        INSERT INTO outputStream\n\
        SELECT symbol, region, sum(volume) AS totalVolume\n\
        FROM stockStream WINDOW('length', 3);\n\
        END;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(75.6),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("EU".to_string()),
            AttributeValue::Float(75.6),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(76.6),
            AttributeValue::Int(200),
        ],
    );
    let out = runner.shutdown();
    // Each (symbol, region) combination should have its own partition
    assert!(!out.is_empty());
}

// ============================================================================
// TABLE LOOKUP TESTS
// Reference: query/table/JoinTableTestCase.java
// ============================================================================

/// Test table lookup with multiple matching rows
#[tokio::test]
async fn table_test11_lookup_multiple_matches() {
    let app = "\
        CREATE TABLE orderTable (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM insertStream (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM lookupStream (productId INT);\n\
        CREATE STREAM outputStream (orderId INT, quantity INT);\n\
        \n\
        INSERT INTO orderTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderTable.orderId AS orderId, orderTable.quantity AS quantity\n\
        FROM lookupStream JOIN orderTable\n\
        ON lookupStream.productId = orderTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert multiple orders for the same product
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(5),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(200),
            AttributeValue::Int(3),
        ],
    );
    sleep(Duration::from_millis(50));
    // Lookup product 100 - should return 2 matches
    runner.send("lookupStream", vec![AttributeValue::Int(100)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Test table lookup with no match
#[tokio::test]
async fn table_test12_lookup_no_match() {
    let app = "\
        CREATE TABLE userTable (userId INT, name STRING);\n\
        CREATE STREAM insertStream (userId INT, name STRING);\n\
        CREATE STREAM lookupStream (userId INT);\n\
        CREATE STREAM outputStream (name STRING);\n\
        \n\
        INSERT INTO userTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT userTable.name AS name\n\
        FROM lookupStream JOIN userTable\n\
        ON lookupStream.userId = userTable.userId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Lookup non-existent user
    runner.send("lookupStream", vec![AttributeValue::Int(999)]);
    let out = runner.shutdown();
    // Inner join should return no results for non-matching key
    assert_eq!(out.len(), 0);
}

/// Test table with left outer join lookup
#[tokio::test]
async fn table_test13_left_outer_join_lookup() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM orderStream (orderId INT, productId INT, qty INT);\n\
        CREATE STREAM outputStream (orderId INT, price FLOAT, qty INT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.orderId AS orderId, priceTable.price AS price, orderStream.qty AS qty\n\
        FROM orderStream LEFT OUTER JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(100), AttributeValue::Float(25.0)],
    );
    sleep(Duration::from_millis(50));
    // Order for existing product
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(2),
        ],
    );
    // Order for non-existing product
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(999),
            AttributeValue::Int(3),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    // First order should have price
    assert_eq!(out[0][1], AttributeValue::Float(25.0));
    // Second order should have null price
    assert_eq!(out[1][1], AttributeValue::Null);
}

/// Test table with sequential inserts and lookups
#[tokio::test]
async fn table_test14_sequential_operations() {
    let app = "\
        CREATE TABLE inventoryTable (itemId INT, stock INT);\n\
        CREATE STREAM insertStream (itemId INT, stock INT);\n\
        CREATE STREAM checkStream (itemId INT);\n\
        CREATE STREAM outputStream (itemId INT, stock INT);\n\
        \n\
        INSERT INTO inventoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT inventoryTable.itemId AS itemId, inventoryTable.stock AS stock\n\
        FROM checkStream JOIN inventoryTable\n\
        ON checkStream.itemId = inventoryTable.itemId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Initial insert
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    sleep(Duration::from_millis(50));
    runner.send("checkStream", vec![AttributeValue::Int(1)]);
    sleep(Duration::from_millis(50));
    // Add more inventory
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    sleep(Duration::from_millis(50));
    runner.send("checkStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::Int(100));
    assert_eq!(out[1][1], AttributeValue::Int(50));
}

/// Test table with multiple columns in join condition
#[tokio::test]
async fn table_test15_multi_column_join() {
    let app = "\
        CREATE TABLE priceTable (category STRING, region STRING, price FLOAT);\n\
        CREATE STREAM insertStream (category STRING, region STRING, price FLOAT);\n\
        CREATE STREAM queryStream (category STRING, region STRING);\n\
        CREATE STREAM outputStream (price FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceTable.price AS price\n\
        FROM queryStream JOIN priceTable\n\
        ON queryStream.category = priceTable.category AND queryStream.region = priceTable.region;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::String("US".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::String("EU".to_string()),
            AttributeValue::Float(120.0),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query specific combination
    runner.send(
        "queryStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::String("EU".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Float(120.0));
}

/// Test table with count of matching records
/// Note: count() in table join counts per-row output, not total matches
#[tokio::test]
async fn table_test16_count_matches() {
    let app = "\
        CREATE TABLE logTable (userId INT, action STRING);\n\
        CREATE STREAM insertStream (userId INT, action STRING);\n\
        CREATE STREAM countStream (userId INT);\n\
        CREATE STREAM outputStream (userId INT, action STRING);\n\
        \n\
        INSERT INTO logTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT logTable.userId AS userId, logTable.action AS action\n\
        FROM countStream JOIN logTable\n\
        ON countStream.userId = logTable.userId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert multiple actions for user 1
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("login".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("view".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("purchase".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("login".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query actions for user 1 - should return all 3 matching rows
    runner.send("countStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    // Should have 3 outputs for user 1's 3 actions
    assert_eq!(out.len(), 3);
}

// ============================================================================
// TABLE WITH EXPRESSIONS
// ============================================================================

/// Test table lookup with arithmetic in SELECT
#[tokio::test]
async fn table_test17_select_with_arithmetic() {
    let app = "\
        CREATE TABLE productTable (productId INT, basePrice FLOAT, taxRate FLOAT);\n\
        CREATE STREAM insertStream (productId INT, basePrice FLOAT, taxRate FLOAT);\n\
        CREATE STREAM orderStream (productId INT, quantity INT);\n\
        CREATE STREAM outputStream (productId INT, totalPrice DOUBLE);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.productId AS productId, \n\
               productTable.basePrice * (1.0 + productTable.taxRate) * orderStream.quantity AS totalPrice\n\
        FROM orderStream JOIN productTable\n\
        ON orderStream.productId = productTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Product with 10% tax
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(100.0),
            AttributeValue::Float(0.1),
        ],
    );
    sleep(Duration::from_millis(50));
    // Order 2 units
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(2)],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    // 100 * 1.1 * 2 = 220
    if let AttributeValue::Double(val) = out[0][1] {
        assert!((val - 220.0).abs() < 0.01);
    } else {
        panic!("Expected Double value");
    }
}

/// Test table with CASE WHEN in SELECT
#[tokio::test]
async fn table_test18_select_with_case() {
    let app = "\
        CREATE TABLE statusTable (itemId INT, status STRING);\n\
        CREATE STREAM insertStream (itemId INT, status STRING);\n\
        CREATE STREAM queryStream (itemId INT);\n\
        CREATE STREAM outputStream (itemId INT, isActive BOOLEAN);\n\
        \n\
        INSERT INTO statusTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT queryStream.itemId AS itemId, \n\
               CASE WHEN statusTable.status = 'active' THEN true ELSE false END AS isActive\n\
        FROM queryStream JOIN statusTable\n\
        ON queryStream.itemId = statusTable.itemId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("inactive".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::Bool(true));
    assert_eq!(out[1][1], AttributeValue::Bool(false));
}

// ============================================================================
// TABLE INSERT WITH SELECT TRANSFORMATION
// ============================================================================

/// Test table insert with SELECT transformation
#[tokio::test]
async fn table_test10_insert_with_transform() {
    let app = "\
        CREATE TABLE summaryTable (symbol STRING, adjustedPrice FLOAT);\n\
        CREATE STREAM stockStream (symbol STRING, price FLOAT, adjustment FLOAT);\n\
        CREATE STREAM lookupStream (symbol STRING);\n\
        CREATE STREAM outputStream (symbol STRING, adjustedPrice FLOAT);\n\
        \n\
        INSERT INTO summaryTable\n\
        SELECT symbol, price + adjustment AS adjustedPrice\n\
        FROM stockStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT summaryTable.symbol AS symbol, summaryTable.adjustedPrice AS adjustedPrice\n\
        FROM lookupStream JOIN summaryTable\n\
        ON lookupStream.symbol = summaryTable.symbol;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "stockStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
            AttributeValue::Float(5.0),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "lookupStream",
        vec![AttributeValue::String("IBM".to_string())],
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
    assert_eq!(out[0][0], AttributeValue::String("IBM".to_string()));
    assert_eq!(out[0][1], AttributeValue::Float(105.0));
}

// ============================================================================
// TABLE WITH NULL VALUES
// ============================================================================

/// Test table with NULL values in columns
#[tokio::test]
async fn table_test19_null_values() {
    let app = "\
        CREATE TABLE dataTable (id INT, value FLOAT);\n\
        CREATE STREAM insertStream (id INT, value FLOAT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, value FLOAT);\n\
        \n\
        INSERT INTO dataTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT dataTable.id AS id, dataTable.value AS value\n\
        FROM queryStream JOIN dataTable\n\
        ON queryStream.id = dataTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Null],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(50.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::Null);
    assert_eq!(out[1][1], AttributeValue::Float(50.0));
}

/// Test table lookup with coalesce for NULL handling
#[tokio::test]
async fn table_test20_lookup_with_coalesce() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM orderStream (productId INT);\n\
        CREATE STREAM outputStream (productId INT, finalPrice FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.productId AS productId, \n\
               coalesce(priceTable.price, CAST(0.0 AS FLOAT)) AS finalPrice\n\
        FROM orderStream LEFT OUTER JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send("orderStream", vec![AttributeValue::Int(1)]); // Has price
    runner.send("orderStream", vec![AttributeValue::Int(999)]); // No price, uses default
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::Float(100.0));
    assert_eq!(out[1][1], AttributeValue::Float(0.0));
}

// ============================================================================
// TABLE WITH DIFFERENT DATA TYPES
// ============================================================================

/// Test table with BIGINT type
#[tokio::test]
async fn table_test21_bigint_type() {
    let app = "\
        CREATE TABLE eventTable (eventId BIGINT, eventName STRING);\n\
        CREATE STREAM insertStream (eventId BIGINT, eventName STRING);\n\
        CREATE STREAM queryStream (eventId BIGINT);\n\
        CREATE STREAM outputStream (eventId BIGINT, eventName STRING);\n\
        \n\
        INSERT INTO eventTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT eventTable.eventId AS eventId, eventTable.eventName AS eventName\n\
        FROM queryStream JOIN eventTable\n\
        ON queryStream.eventId = eventTable.eventId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Long(9999999999999),
            AttributeValue::String("BigEvent".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Long(9999999999999)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Long(9999999999999));
    assert_eq!(out[0][1], AttributeValue::String("BigEvent".to_string()));
}

/// Test table with DOUBLE type
#[tokio::test]
async fn table_test22_double_type() {
    let app = "\
        CREATE TABLE measurementTable (id INT, reading DOUBLE);\n\
        CREATE STREAM insertStream (id INT, reading DOUBLE);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, reading DOUBLE);\n\
        \n\
        INSERT INTO measurementTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT measurementTable.id AS id, measurementTable.reading AS reading\n\
        FROM queryStream JOIN measurementTable\n\
        ON queryStream.id = measurementTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Double(1.234567890123456),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(val) = out[0][1] {
        assert!((val - 1.234567890123456).abs() < 0.0000001);
    } else {
        panic!("Expected Double value");
    }
}

/// Test table with BOOLEAN type
#[tokio::test]
async fn table_test23_boolean_type() {
    let app = "\
        CREATE TABLE flagTable (id INT, isActive BOOLEAN);\n\
        CREATE STREAM insertStream (id INT, isActive BOOLEAN);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, isActive BOOLEAN);\n\
        \n\
        INSERT INTO flagTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT flagTable.id AS id, flagTable.isActive AS isActive\n\
        FROM queryStream JOIN flagTable\n\
        ON queryStream.id = flagTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::Bool(true));
    assert_eq!(out[1][1], AttributeValue::Bool(false));
}

// ============================================================================
// TABLE WITH STRING KEY
// ============================================================================

/// Test table with STRING as primary key
#[tokio::test]
async fn table_test24_string_key() {
    let app = "\
        CREATE TABLE configTable (configKey STRING, configValue STRING);\n\
        CREATE STREAM insertStream (configKey STRING, configValue STRING);\n\
        CREATE STREAM queryStream (configKey STRING);\n\
        CREATE STREAM outputStream (configKey STRING, configValue STRING);\n\
        \n\
        INSERT INTO configTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT configTable.configKey AS configKey, configTable.configValue AS configValue\n\
        FROM queryStream JOIN configTable\n\
        ON queryStream.configKey = configTable.configKey;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("db.host".to_string()),
            AttributeValue::String("localhost".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("db.port".to_string()),
            AttributeValue::String("5432".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "queryStream",
        vec![AttributeValue::String("db.host".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("db.host".to_string()));
    assert_eq!(out[0][1], AttributeValue::String("localhost".to_string()));
}

// ============================================================================
// TABLE WITH FUNCTION IN SELECT
// ============================================================================

/// Test table lookup with upper() function
#[tokio::test]
async fn table_test25_function_in_select() {
    let app = "\
        CREATE TABLE nameTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, upperName STRING);\n\
        \n\
        INSERT INTO nameTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT nameTable.id AS id, upper(nameTable.name) AS upperName\n\
        FROM queryStream JOIN nameTable\n\
        ON queryStream.id = nameTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("alice".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ALICE".to_string()));
}

/// Test table lookup with concat() function
#[tokio::test]
async fn table_test26_concat_in_select() {
    let app = "\
        CREATE TABLE personTable (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM insertStream (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, fullName STRING);\n\
        \n\
        INSERT INTO personTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT personTable.id AS id, concat(personTable.firstName, ' ', personTable.lastName) AS fullName\n\
        FROM queryStream JOIN personTable\n\
        ON queryStream.id = personTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

// ============================================================================
// TABLE UPDATE SCENARIOS
// ============================================================================

/// Test table row update (same key, new value)
#[tokio::test]
async fn table_test27_row_update() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM queryStream (productId INT);\n\
        CREATE STREAM outputStream (productId INT, price FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceTable.productId AS productId, priceTable.price AS price\n\
        FROM queryStream JOIN priceTable\n\
        ON queryStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Initial price
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    sleep(Duration::from_millis(50));
    // Update price
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(150.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    // Table stores multiple entries with same key, returns all matches
    assert!(!out.is_empty());
}

// ============================================================================
// TABLE WITH FILTER ON LOOKUP
// ============================================================================

/// Test table lookup with additional WHERE filter
#[tokio::test]
async fn table_test28_lookup_with_filter() {
    let app = "\
        CREATE TABLE productTable (productId INT, name STRING, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, name STRING, price FLOAT);\n\
        CREATE STREAM queryStream (productId INT, maxPrice FLOAT);\n\
        CREATE STREAM outputStream (productId INT, name STRING, price FLOAT);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT productTable.productId AS productId, productTable.name AS name, productTable.price AS price\n\
        FROM queryStream JOIN productTable\n\
        ON queryStream.productId = productTable.productId\n\
        WHERE productTable.price <= queryStream.maxPrice;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::Float(1000.0),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Mouse".to_string()),
            AttributeValue::Float(25.0),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query with max price 500 - only Mouse should match
    runner.send(
        "queryStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(500.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("Mouse".to_string()));
}

// ============================================================================
// TABLE EMPTY RESULT
// ============================================================================

/// Test table lookup with no match
#[tokio::test]
async fn table_test29_no_match() {
    let app = "\
        CREATE TABLE dataTable (id INT, value INT);\n\
        CREATE STREAM insertStream (id INT, value INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        \n\
        INSERT INTO dataTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT dataTable.id AS id, dataTable.value AS value\n\
        FROM queryStream JOIN dataTable\n\
        ON queryStream.id = dataTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    sleep(Duration::from_millis(50));
    // Query for non-existent ID
    runner.send("queryStream", vec![AttributeValue::Int(999)]);
    let out = runner.shutdown();
    // Inner join with no match produces no output
    assert!(out.is_empty());
}

// ============================================================================
// ADDITIONAL TABLE EDGE CASES
// ============================================================================

/// Test empty table query - inner join returns no results
#[tokio::test]
async fn table_test_empty_table() {
    let app = "\
        CREATE TABLE emptyTable (id INT, value STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, value STRING);\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT emptyTable.id AS id, emptyTable.value AS value\n\
        FROM queryStream JOIN emptyTable\n\
        ON queryStream.id = emptyTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    sleep(Duration::from_millis(50));
    // Query empty table
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert!(out.is_empty());
}

/// Test table with many rows - stress test
#[tokio::test]
async fn table_test_many_rows() {
    let app = "\
        CREATE TABLE largeTable (id INT, value INT);\n\
        CREATE STREAM insertStream (id INT, value INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        \n\
        INSERT INTO largeTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT largeTable.id AS id, largeTable.value AS value\n\
        FROM queryStream JOIN largeTable\n\
        ON queryStream.id = largeTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert 100 rows
    for i in 0..100 {
        runner.send(
            "insertStream",
            vec![AttributeValue::Int(i), AttributeValue::Int(i * 10)],
        );
    }
    sleep(Duration::from_millis(100));
    // Query specific row
    runner.send("queryStream", vec![AttributeValue::Int(50)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Test table with insert from windowed aggregation
#[tokio::test]
async fn table_test_insert_from_window() {
    let app = "\
        CREATE TABLE statsTable (symbol STRING, avgPrice DOUBLE);\n\
        CREATE STREAM priceStream (symbol STRING, price FLOAT);\n\
        CREATE STREAM outputStream (symbol STRING, avgPrice DOUBLE);\n\
        \n\
        INSERT INTO statsTable\n\
        SELECT symbol, avg(price) AS avgPrice\n\
        FROM priceStream WINDOW('length', 3);\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT symbol AS symbol, avgPrice AS avgPrice FROM statsTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(100.0),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(110.0),
        ],
    );
    runner.send(
        "priceStream",
        vec![
            AttributeValue::String("IBM".to_string()),
            AttributeValue::Float(120.0),
        ],
    );
    sleep(Duration::from_millis(100));
    let out = runner.shutdown();
    // Each window emission inserts into table AND emits to outputStream
    assert!(!out.is_empty());
}

/// Test table with right outer join
#[tokio::test]
#[ignore = "RIGHT OUTER JOIN on table not yet supported"]
async fn table_test_right_outer_join() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM orderStream (productId INT, qty INT);\n\
        CREATE STREAM outputStream (productId INT, price FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceTable.productId AS productId, priceTable.price AS price\n\
        FROM orderStream RIGHT OUTER JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(200.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    // Right outer join includes all table rows
    assert!(!out.is_empty());
}

/// Test table with full outer join
#[tokio::test]
#[ignore = "FULL OUTER JOIN on table not yet supported"]
async fn table_test_full_outer_join() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM orderStream (productId INT, qty INT);\n\
        CREATE STREAM outputStream (productId INT, price FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceTable.productId AS productId, priceTable.price AS price\n\
        FROM orderStream FULL OUTER JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(999), AttributeValue::Int(5)], // No matching product
    );
    let out = runner.shutdown();
    assert!(!out.is_empty());
}

/// Test table with ORDER BY in query
#[tokio::test]
#[ignore = "ORDER BY with table join not yet supported"]
async fn table_test_order_by() {
    let app = "\
        CREATE TABLE scoreTable (playerId INT, score INT);\n\
        CREATE STREAM insertStream (playerId INT, score INT);\n\
        CREATE STREAM triggerStream (dummy INT);\n\
        CREATE STREAM outputStream (playerId INT, score INT);\n\
        \n\
        INSERT INTO scoreTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT scoreTable.playerId AS playerId, scoreTable.score AS score\n\
        FROM triggerStream JOIN scoreTable\n\
        ORDER BY scoreTable.score DESC;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(100)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(75)],
    );
    sleep(Duration::from_millis(50));
    runner.send("triggerStream", vec![AttributeValue::Int(0)]);
    let out = runner.shutdown();
    // Should be ordered by score DESC: 100, 75, 50
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][1], AttributeValue::Int(100));
}

/// Test table with LIMIT clause
#[tokio::test]
#[ignore = "LIMIT with table join not yet supported"]
async fn table_test_limit() {
    let app = "\
        CREATE TABLE itemTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM triggerStream (dummy INT);\n\
        CREATE STREAM outputStream (id INT, name STRING);\n\
        \n\
        INSERT INTO itemTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT itemTable.id AS id, itemTable.name AS name\n\
        FROM triggerStream JOIN itemTable\n\
        LIMIT 2;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    for i in 0..5 {
        runner.send(
            "insertStream",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String(format!("item{}", i)),
            ],
        );
    }
    sleep(Duration::from_millis(50));
    runner.send("triggerStream", vec![AttributeValue::Int(0)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
}

/// Test table insert with filter
#[tokio::test]
async fn table_test_insert_with_filter() {
    let app = "\
        CREATE TABLE validOrderTable (orderId INT, amount FLOAT);\n\
        CREATE STREAM orderStream (orderId INT, amount FLOAT);\n\
        CREATE STREAM queryStream (orderId INT);\n\
        CREATE STREAM outputStream (orderId INT, amount FLOAT);\n\
        \n\
        INSERT INTO validOrderTable\n\
        SELECT orderId, amount\n\
        FROM orderStream\n\
        WHERE amount > 0;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT validOrderTable.orderId AS orderId, validOrderTable.amount AS amount\n\
        FROM queryStream JOIN validOrderTable\n\
        ON queryStream.orderId = validOrderTable.orderId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Valid order
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    // Invalid order (should be filtered out)
    runner.send(
        "orderStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(-50.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    // Only valid order should be in table
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Test table with type coercion in join
#[tokio::test]
async fn table_test_type_coercion_join() {
    let app = "\
        CREATE TABLE intTable (id INT, value STRING);\n\
        CREATE STREAM insertStream (id INT, value STRING);\n\
        CREATE STREAM queryStream (id BIGINT);\n\
        CREATE STREAM outputStream (id INT, value STRING);\n\
        \n\
        INSERT INTO intTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT intTable.id AS id, intTable.value AS value\n\
        FROM queryStream JOIN intTable\n\
        ON queryStream.id = intTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(42),
            AttributeValue::String("answer".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query with BIGINT that matches INT
    runner.send("queryStream", vec![AttributeValue::Long(42)]);
    let out = runner.shutdown();
    // Type coercion should allow match
    assert_eq!(out.len(), 1);
}

/// Test table after multiple lookups
#[tokio::test]
async fn table_test_multiple_lookups() {
    let app = "\
        CREATE TABLE userTable (userId INT, name STRING);\n\
        CREATE STREAM insertStream (userId INT, name STRING);\n\
        CREATE STREAM lookupStream (userId INT);\n\
        CREATE STREAM outputStream (name STRING);\n\
        \n\
        INSERT INTO userTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT userTable.name AS name\n\
        FROM lookupStream JOIN userTable\n\
        ON lookupStream.userId = userTable.userId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Multiple lookups of same record
    runner.send("lookupStream", vec![AttributeValue::Int(1)]);
    runner.send("lookupStream", vec![AttributeValue::Int(1)]);
    runner.send("lookupStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0][0], AttributeValue::String("Alice".to_string()));
    assert_eq!(out[1][0], AttributeValue::String("Alice".to_string()));
    assert_eq!(out[2][0], AttributeValue::String("Alice".to_string()));
}

// ============================================================================
// TABLE WITH MULTIPLE COLUMNS IN JOIN
// ============================================================================

/// Test table with composite key (multiple columns in ON clause)
#[tokio::test]
async fn table_test30_composite_key() {
    let app = "\
        CREATE TABLE inventoryTable (productId INT, warehouseId INT, quantity INT);\n\
        CREATE STREAM insertStream (productId INT, warehouseId INT, quantity INT);\n\
        CREATE STREAM queryStream (productId INT, warehouseId INT);\n\
        CREATE STREAM outputStream (productId INT, warehouseId INT, quantity INT);\n\
        \n\
        INSERT INTO inventoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT inventoryTable.productId AS productId, inventoryTable.warehouseId AS warehouseId, \n\
               inventoryTable.quantity AS quantity\n\
        FROM queryStream JOIN inventoryTable\n\
        ON queryStream.productId = inventoryTable.productId \n\
           AND queryStream.warehouseId = inventoryTable.warehouseId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(1),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(2),
            AttributeValue::Int(50),
        ],
    );
    sleep(Duration::from_millis(50));
    // Query for product 1 in warehouse 2
    runner.send(
        "queryStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(2)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][2], AttributeValue::Int(50));
}

// ============================================================================
// ADDITIONAL TABLE EDGE CASE TESTS
// ============================================================================

/// Table with CASE WHEN in SELECT during join
#[tokio::test]
async fn table_test_case_when_in_select() {
    let app = "\
        CREATE TABLE customerTable (customerId INT, tier STRING);\n\
        CREATE STREAM insertStream (customerId INT, tier STRING);\n\
        CREATE STREAM orderStream (orderId INT, customerId INT, amount FLOAT);\n\
        CREATE STREAM outputStream (orderId INT, discount STRING);\n\
        \n\
        INSERT INTO customerTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.orderId AS orderId,\n\
               CASE WHEN customerTable.tier = 'gold' THEN 'high'\n\
                    WHEN customerTable.tier = 'silver' THEN 'medium'\n\
                    ELSE 'low' END AS discount\n\
        FROM orderStream JOIN customerTable\n\
        ON orderStream.customerId = customerTable.customerId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("gold".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("silver".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(100),
            AttributeValue::Int(1),
            AttributeValue::Float(500.0),
        ],
    );
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(101),
            AttributeValue::Int(2),
            AttributeValue::Float(300.0),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("high".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("medium".to_string()));
}

/// Table with string functions in SELECT
#[tokio::test]
async fn table_test_string_functions() {
    let app = "\
        CREATE TABLE userTable (userId INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM insertStream (userId INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM lookupStream (userId INT);\n\
        CREATE STREAM outputStream (fullName STRING);\n\
        \n\
        INSERT INTO userTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT concat(upper(userTable.firstName), ' ', upper(userTable.lastName)) AS fullName\n\
        FROM lookupStream JOIN userTable\n\
        ON lookupStream.userId = userTable.userId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("john".to_string()),
            AttributeValue::String("doe".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("lookupStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("JOHN DOE".to_string()));
}

/// Table with arithmetic operations on lookup result
#[tokio::test]
#[ignore = "Complex arithmetic with type coercion on table join not yet fully supported"]
async fn table_test_arithmetic_on_lookup() {
    let app = "\
        CREATE TABLE priceTable (productId INT, basePrice FLOAT, taxRate FLOAT);\n\
        CREATE STREAM insertStream (productId INT, basePrice FLOAT, taxRate FLOAT);\n\
        CREATE STREAM orderStream (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM outputStream (orderId INT, totalPrice FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.orderId AS orderId,\n\
               priceTable.basePrice * (1.0 + priceTable.taxRate) * orderStream.quantity AS totalPrice\n\
        FROM orderStream JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Float(100.0),
            AttributeValue::Float(0.1),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1001),
            AttributeValue::Int(1),
            AttributeValue::Int(2),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // 100 * (1 + 0.1) * 2 = 220
    if let AttributeValue::Float(price) = out[0][1] {
        assert!((price - 220.0).abs() < 0.01);
    } else {
        panic!("Expected Float");
    }
}

/// Table with coalesce function
#[tokio::test]
#[ignore = "LEFT OUTER JOIN on table with coalesce not yet fully supported"]
async fn table_test_coalesce() {
    let app = "\
        CREATE TABLE configTable (key STRING, value STRING);\n\
        CREATE STREAM insertStream (key STRING, value STRING);\n\
        CREATE STREAM lookupStream (key STRING, defaultValue STRING);\n\
        CREATE STREAM outputStream (result STRING);\n\
        \n\
        INSERT INTO configTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT coalesce(configTable.value, lookupStream.defaultValue) AS result\n\
        FROM lookupStream LEFT OUTER JOIN configTable\n\
        ON lookupStream.key = configTable.key;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("theme".to_string()),
            AttributeValue::String("dark".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    // Lookup existing key
    runner.send(
        "lookupStream",
        vec![
            AttributeValue::String("theme".to_string()),
            AttributeValue::String("light".to_string()),
        ],
    );
    // Lookup non-existing key
    runner.send(
        "lookupStream",
        vec![
            AttributeValue::String("color".to_string()),
            AttributeValue::String("blue".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert!(out.len() >= 1);
    // First lookup returns table value "dark"
    assert_eq!(out[0][0], AttributeValue::String("dark".to_string()));
}

/// Table with boolean comparison
#[tokio::test]
async fn table_test_boolean_comparison() {
    let app = "\
        CREATE TABLE featureTable (featureId INT, enabled BOOLEAN);\n\
        CREATE STREAM insertStream (featureId INT, enabled BOOLEAN);\n\
        CREATE STREAM checkStream (featureId INT);\n\
        CREATE STREAM outputStream (featureId INT, status STRING);\n\
        \n\
        INSERT INTO featureTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT featureTable.featureId AS featureId,\n\
               CASE WHEN featureTable.enabled = true THEN 'active' ELSE 'inactive' END AS status\n\
        FROM checkStream JOIN featureTable\n\
        ON checkStream.featureId = featureTable.featureId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Bool(true)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Bool(false)],
    );
    sleep(Duration::from_millis(50));
    runner.send("checkStream", vec![AttributeValue::Int(1)]);
    runner.send("checkStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][1], AttributeValue::String("active".to_string()));
    assert_eq!(out[1][1], AttributeValue::String("inactive".to_string()));
}

/// Table with multiple inserts followed by query
#[tokio::test]
async fn table_test_multiple_inserts() {
    let app = "\
        CREATE TABLE logTable (id INT, message STRING);\n\
        CREATE STREAM insertStream (id INT, message STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, message STRING);\n\
        \n\
        INSERT INTO logTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT logTable.id AS id, logTable.message AS message\n\
        FROM queryStream JOIN logTable\n\
        ON queryStream.id = logTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert multiple records
    for i in 1..=5 {
        runner.send(
            "insertStream",
            vec![
                AttributeValue::Int(i),
                AttributeValue::String(format!("log_{}", i)),
            ],
        );
    }
    sleep(Duration::from_millis(50));
    // Query for specific id
    runner.send("queryStream", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(3));
    assert_eq!(out[0][1], AttributeValue::String("log_3".to_string()));
}

/// Table with double type columns
#[tokio::test]
async fn table_test_double_type() {
    let app = "\
        CREATE TABLE measurementTable (sensorId INT, value DOUBLE);\n\
        CREATE STREAM insertStream (sensorId INT, value DOUBLE);\n\
        CREATE STREAM queryStream (sensorId INT);\n\
        CREATE STREAM outputStream (sensorId INT, value DOUBLE);\n\
        \n\
        INSERT INTO measurementTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT measurementTable.sensorId AS sensorId, measurementTable.value AS value\n\
        FROM queryStream JOIN measurementTable\n\
        ON queryStream.sensorId = measurementTable.sensorId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Double(9.876543210987654),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(val) = out[0][1] {
        assert!((val - 9.876543210987654).abs() < 0.0000001);
    } else {
        panic!("Expected Double");
    }
}

/// Table with long type columns
#[tokio::test]
async fn table_test_long_type() {
    let app = "\
        CREATE TABLE timestampTable (eventId INT, timestamp BIGINT);\n\
        CREATE STREAM insertStream (eventId INT, timestamp BIGINT);\n\
        CREATE STREAM queryStream (eventId INT);\n\
        CREATE STREAM outputStream (eventId INT, timestamp BIGINT);\n\
        \n\
        INSERT INTO timestampTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT timestampTable.eventId AS eventId, timestampTable.timestamp AS timestamp\n\
        FROM queryStream JOIN timestampTable\n\
        ON queryStream.eventId = timestampTable.eventId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Long(1609459200000)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Long(1609459200000));
}

/// Table with length function on column
#[tokio::test]
async fn table_test_length_function() {
    let app = "\
        CREATE TABLE productTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, nameLength INT);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT productTable.id AS id, length(productTable.name) AS nameLength\n\
        FROM queryStream JOIN productTable\n\
        ON queryStream.id = productTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(6));
}

/// Table with filtering using stream value
#[tokio::test]
#[ignore = "WHERE clause filtering during table join not yet fully supported"]
async fn table_test_filter_with_stream_value() {
    let app = "\
        CREATE TABLE priceTable (productId INT, price FLOAT);\n\
        CREATE STREAM insertStream (productId INT, price FLOAT);\n\
        CREATE STREAM budgetStream (productId INT, maxBudget FLOAT);\n\
        CREATE STREAM outputStream (productId INT, price FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceTable.productId AS productId, priceTable.price AS price\n\
        FROM budgetStream JOIN priceTable\n\
        ON budgetStream.productId = priceTable.productId\n\
        WHERE priceTable.price <= budgetStream.maxBudget;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(200.0)],
    );
    sleep(Duration::from_millis(50));
    // Budget too low for product 1
    runner.send(
        "budgetStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(50.0)],
    );
    // Budget enough for product 2
    runner.send(
        "budgetStream",
        vec![AttributeValue::Int(2), AttributeValue::Float(250.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

// ============================================================================
// MORE TABLE EDGE CASE TESTS
// ============================================================================

/// Table with concat on lookup columns
#[tokio::test]
async fn table_test_concat_lookup() {
    let app = "\
        CREATE TABLE nameTable (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM insertStream (id INT, firstName STRING, lastName STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (fullName STRING);\n\
        \n\
        INSERT INTO nameTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT concat(nameTable.firstName, ' ', nameTable.lastName) AS fullName\n\
        FROM queryStream JOIN nameTable\n\
        ON queryStream.id = nameTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("John Doe".to_string()));
}

/// Table with multiple columns selected
#[tokio::test]
async fn table_test_multi_column_select() {
    let app = "\
        CREATE TABLE productTable (id INT, name STRING, price FLOAT, category STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING, price FLOAT, category STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (name STRING, price FLOAT, category STRING);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT productTable.name AS name, productTable.price AS price, productTable.category AS category\n\
        FROM queryStream JOIN productTable\n\
        ON queryStream.id = productTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Laptop".to_string()),
            AttributeValue::Float(999.99),
            AttributeValue::String("Electronics".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Laptop".to_string()));
}

/// Table with stream column in output
#[tokio::test]
async fn table_test_stream_column_output() {
    let app = "\
        CREATE TABLE priceTable (productId INT, basePrice FLOAT);\n\
        CREATE STREAM insertStream (productId INT, basePrice FLOAT);\n\
        CREATE STREAM orderStream (orderId INT, productId INT, quantity INT);\n\
        CREATE STREAM outputStream (orderId INT, quantity INT, basePrice FLOAT);\n\
        \n\
        INSERT INTO priceTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT orderStream.orderId AS orderId, orderStream.quantity AS quantity, priceTable.basePrice AS basePrice\n\
        FROM orderStream JOIN priceTable\n\
        ON orderStream.productId = priceTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(50.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1001),
            AttributeValue::Int(1),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1001));
    assert_eq!(out[0][1], AttributeValue::Int(5));
}

/// Table with upper function on lookup
#[tokio::test]
async fn table_test_upper_lookup() {
    let app = "\
        CREATE TABLE categoryTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (upperName STRING);\n\
        \n\
        INSERT INTO categoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT upper(categoryTable.name) AS upperName\n\
        FROM queryStream JOIN categoryTable\n\
        ON queryStream.id = categoryTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("electronics".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("ELECTRONICS".to_string()));
}

/// Table with lower function on lookup
#[tokio::test]
async fn table_test_lower_lookup() {
    let app = "\
        CREATE TABLE categoryTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (lowerName STRING);\n\
        \n\
        INSERT INTO categoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT lower(categoryTable.name) AS lowerName\n\
        FROM queryStream JOIN categoryTable\n\
        ON queryStream.id = categoryTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ELECTRONICS".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("electronics".to_string()));
}

/// Table with multiple updates to same key
#[tokio::test]
#[ignore = "STRING primary key in tables has SQL parse issues"]
async fn table_test_update_same_key() {
    let app = "\
        CREATE TABLE configTable (key STRING, value STRING);\n\
        CREATE STREAM insertStream (key STRING, value STRING);\n\
        CREATE STREAM queryStream (key STRING);\n\
        CREATE STREAM outputStream (value STRING);\n\
        \n\
        INSERT INTO configTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT configTable.value AS value\n\
        FROM queryStream JOIN configTable\n\
        ON queryStream.key = configTable.key;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    // Insert initial value
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("theme".to_string()),
            AttributeValue::String("light".to_string()),
        ],
    );
    // Update with new value
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("theme".to_string()),
            AttributeValue::String("dark".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "queryStream",
        vec![AttributeValue::String("theme".to_string())],
    );
    let out = runner.shutdown();
    // Should get at least one result
    assert!(out.len() >= 1);
}

/// Table with integer multiplication
#[tokio::test]
async fn table_test_int_multiplication() {
    let app = "\
        CREATE TABLE rateTable (id INT, rate INT);\n\
        CREATE STREAM insertStream (id INT, rate INT);\n\
        CREATE STREAM quantityStream (id INT, quantity INT);\n\
        CREATE STREAM outputStream (total INT);\n\
        \n\
        INSERT INTO rateTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT rateTable.rate * quantityStream.quantity AS total\n\
        FROM quantityStream JOIN rateTable\n\
        ON quantityStream.id = rateTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "quantityStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(50));
}

/// Table with string key lookup
#[tokio::test]
async fn table_test_string_key() {
    let app = "\
        CREATE TABLE lookupTable (code STRING, description STRING);\n\
        CREATE STREAM insertStream (code STRING, description STRING);\n\
        CREATE STREAM queryStream (code STRING);\n\
        CREATE STREAM outputStream (description STRING);\n\
        \n\
        INSERT INTO lookupTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT lookupTable.description AS description\n\
        FROM queryStream JOIN lookupTable\n\
        ON queryStream.code = lookupTable.code;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("ERR001".to_string()),
            AttributeValue::String("Connection failed".to_string()),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::String("ERR002".to_string()),
            AttributeValue::String("Timeout occurred".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "queryStream",
        vec![AttributeValue::String("ERR001".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][0],
        AttributeValue::String("Connection failed".to_string())
    );
}

/// Table with float addition
#[tokio::test]
async fn table_test_float_addition() {
    let app = "\
        CREATE TABLE discountTable (productId INT, discount FLOAT);\n\
        CREATE STREAM insertStream (productId INT, discount FLOAT);\n\
        CREATE STREAM priceStream (productId INT, price FLOAT);\n\
        CREATE STREAM outputStream (finalPrice DOUBLE);\n\
        \n\
        INSERT INTO discountTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT priceStream.price + discountTable.discount AS finalPrice\n\
        FROM priceStream JOIN discountTable\n\
        ON priceStream.productId = discountTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(5.0)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(price) = out[0][0] {
        assert!((price - 105.0).abs() < 0.01);
    }
}

/// Table with uuid in output
#[tokio::test]
async fn table_test_uuid_output() {
    let app = "\
        CREATE TABLE userTable (id INT, name STRING);\n\
        CREATE STREAM insertStream (id INT, name STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (name STRING, eventId STRING);\n\
        \n\
        INSERT INTO userTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT userTable.name AS name, uuid() AS eventId\n\
        FROM queryStream JOIN userTable\n\
        ON queryStream.id = userTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Alice".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("Alice".to_string()));
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert_eq!(uuid.len(), 36);
    }
}

/// Table with subtraction on lookup
#[tokio::test]
async fn table_test_subtraction() {
    let app = "\
        CREATE TABLE inventoryTable (productId INT, quantity INT, reserved INT);\n\
        CREATE STREAM insertStream (productId INT, quantity INT, reserved INT);\n\
        CREATE STREAM queryStream (productId INT);\n\
        CREATE STREAM outputStream (productId INT, available INT);\n\
        \n\
        INSERT INTO inventoryTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT inventoryTable.productId AS productId, inventoryTable.quantity - inventoryTable.reserved AS available\n\
        FROM queryStream JOIN inventoryTable\n\
        ON queryStream.productId = inventoryTable.productId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(30),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(70));
}

/// Table with division
#[tokio::test]
async fn table_test_division() {
    let app = "\
        CREATE TABLE statsTable (id INT, total INT, count INT);\n\
        CREATE STREAM insertStream (id INT, total INT, count INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, average DOUBLE);\n\
        \n\
        INSERT INTO statsTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT statsTable.id AS id, statsTable.total / statsTable.count AS average\n\
        FROM queryStream JOIN statsTable\n\
        ON queryStream.id = statsTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(4),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Table with now() function
#[tokio::test]
async fn table_test_current_time() {
    let app = "\
        CREATE TABLE dataTable (id INT, value INT);\n\
        CREATE STREAM insertStream (id INT, value INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (value INT, queriedAt BIGINT);\n\
        \n\
        INSERT INTO dataTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT dataTable.value AS value, now() AS queriedAt\n\
        FROM queryStream JOIN dataTable\n\
        ON queryStream.id = dataTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(42)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Long(ts) = out[0][1] {
        assert!(ts > 1577836800000); // After 2020
    }
}

/// Table with NOT in WHERE
#[tokio::test]
#[ignore = "WHERE filter with table JOIN not yet supported"]
async fn table_test_not_filter() {
    let app = "\
        CREATE TABLE statusTable (id INT, active INT);\n\
        CREATE STREAM insertStream (id INT, active INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT);\n\
        \n\
        INSERT INTO statusTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT statusTable.id AS id\n\
        FROM queryStream JOIN statusTable\n\
        ON queryStream.id = statusTable.id\n\
        WHERE NOT (statusTable.active = 0);\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(1)],
    );
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    // Only id=1 should pass (active=1, which is NOT 0)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with complex WHERE
#[tokio::test]
#[ignore = "WHERE filter with table JOIN not yet supported"]
async fn table_test_complex_where() {
    let app = "\
        CREATE TABLE productTable (id INT, price INT, stock INT);\n\
        CREATE STREAM insertStream (id INT, price INT, stock INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, price INT);\n\
        \n\
        INSERT INTO productTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT productTable.id AS id, productTable.price AS price\n\
        FROM queryStream JOIN productTable\n\
        ON queryStream.id = productTable.id\n\
        WHERE productTable.price > 50 AND productTable.stock > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(30),
            AttributeValue::Int(5),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    let out = runner.shutdown();
    // Only id=1 passes (price=100 > 50 AND stock=10 > 0)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with OR in WHERE
#[tokio::test]
#[ignore = "WHERE filter with table JOIN not yet supported"]
async fn table_test_or_where() {
    let app = "\
        CREATE TABLE itemTable (id INT, category STRING, featured INT);\n\
        CREATE STREAM insertStream (id INT, category STRING, featured INT);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (id INT, category STRING);\n\
        \n\
        INSERT INTO itemTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT itemTable.id AS id, itemTable.category AS category\n\
        FROM queryStream JOIN itemTable\n\
        ON queryStream.id = itemTable.id\n\
        WHERE itemTable.category = 'premium' OR itemTable.featured = 1;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("premium".to_string()),
            AttributeValue::Int(0),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("basic".to_string()),
            AttributeValue::Int(1),
        ],
    );
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("basic".to_string()),
            AttributeValue::Int(0),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    runner.send("queryStream", vec![AttributeValue::Int(2)]);
    runner.send("queryStream", vec![AttributeValue::Int(3)]);
    let out = runner.shutdown();
    // id=1 (premium) and id=2 (featured=1) pass, id=3 doesn't
    assert_eq!(out.len(), 2);
}

/// Table with double precision
#[tokio::test]
async fn table_test_double_arithmetic() {
    let app = "\
        CREATE TABLE rateTable (id INT, rate DOUBLE);\n\
        CREATE STREAM insertStream (id INT, rate DOUBLE);\n\
        CREATE STREAM queryStream (id INT, amount DOUBLE);\n\
        CREATE STREAM outputStream (id INT, result DOUBLE);\n\
        \n\
        INSERT INTO rateTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT rateTable.id AS id, queryStream.amount * rateTable.rate AS result\n\
        FROM queryStream JOIN rateTable\n\
        ON queryStream.id = rateTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(1.5)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "queryStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(100.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(result) = out[0][1] {
        assert!((result - 150.0).abs() < 0.0001);
    }
}

/// Table with nested concat
#[tokio::test]
async fn table_test_nested_concat() {
    let app = "\
        CREATE TABLE personTable (id INT, title STRING, firstName STRING, lastName STRING);\n\
        CREATE STREAM insertStream (id INT, title STRING, firstName STRING, lastName STRING);\n\
        CREATE STREAM queryStream (id INT);\n\
        CREATE STREAM outputStream (fullName STRING);\n\
        \n\
        INSERT INTO personTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT concat(personTable.title, ' ', concat(personTable.firstName, ' ', personTable.lastName)) AS fullName\n\
        FROM queryStream JOIN personTable\n\
        ON queryStream.id = personTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Dr.".to_string()),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Smith".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][0],
        AttributeValue::String("Dr. John Smith".to_string())
    );
}

/// Table with multiple arithmetic operations
#[tokio::test]
async fn table_test_multiple_arithmetic() {
    let app = "\
        CREATE TABLE taxTable (id INT, rate INT);\n\
        CREATE STREAM insertStream (id INT, rate INT);\n\
        CREATE STREAM queryStream (id INT, subtotal INT);\n\
        CREATE STREAM outputStream (subtotal INT, tax INT, total INT);\n\
        \n\
        INSERT INTO taxTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT queryStream.subtotal AS subtotal, \n\
               queryStream.subtotal * taxTable.rate / 100 AS tax,\n\
               queryStream.subtotal + queryStream.subtotal * taxTable.rate / 100 AS total\n\
        FROM queryStream JOIN taxTable\n\
        ON queryStream.id = taxTable.id;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    sleep(Duration::from_millis(50));
    runner.send(
        "queryStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
}

/// Table with bigint key
#[tokio::test]
async fn table_test_bigint_key() {
    let app = "\
        CREATE TABLE eventTable (eventId BIGINT, data STRING);\n\
        CREATE STREAM insertStream (eventId BIGINT, data STRING);\n\
        CREATE STREAM queryStream (eventId BIGINT);\n\
        CREATE STREAM outputStream (data STRING);\n\
        \n\
        INSERT INTO eventTable SELECT * FROM insertStream;\n\
        \n\
        INSERT INTO outputStream\n\
        SELECT eventTable.data AS data\n\
        FROM queryStream JOIN eventTable\n\
        ON queryStream.eventId = eventTable.eventId;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Long(9999999999999),
            AttributeValue::String("event data".to_string()),
        ],
    );
    sleep(Duration::from_millis(50));
    runner.send("queryStream", vec![AttributeValue::Long(9999999999999)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::String("event data".to_string()));
}

/// Table with UPDATE query
#[tokio::test]
async fn table_test_update() {
    let app = "\
        CREATE STREAM updateStream (id INT, value INT);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY, value INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO dataTable\n\
        SELECT id, value FROM updateStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM dataTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "updateStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "updateStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(200)],
    );
    let out = runner.shutdown();
    // Second insert should update the value
    assert!(out.len() >= 1);
    let last = out.last().unwrap();
    assert_eq!(last[1], AttributeValue::Int(200));
}

/// Table with FLOAT type columns
#[tokio::test]
async fn table_test_float_column() {
    let app = "\
        CREATE STREAM priceStream (id INT, price FLOAT);\n\
        CREATE TABLE priceTable (id INT PRIMARY KEY, price FLOAT);\n\
        CREATE STREAM outputStream (id INT, price FLOAT);\n\
        INSERT INTO priceTable\n\
        SELECT id, price FROM priceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, price FROM priceTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Float(99.99)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    if let AttributeValue::Float(p) = out[0][1] {
        assert!((p - 99.99).abs() < 0.001);
    } else {
        panic!("Expected Float");
    }
}

/// Table with DOUBLE type columns
#[tokio::test]
async fn table_test_double_column() {
    let app = "\
        CREATE STREAM measureStream (id INT, measurement DOUBLE);\n\
        CREATE TABLE measureTable (id INT PRIMARY KEY, measurement DOUBLE);\n\
        CREATE STREAM outputStream (id INT, measurement DOUBLE);\n\
        INSERT INTO measureTable\n\
        SELECT id, measurement FROM measureStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, measurement FROM measureTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(1.23456)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::Double(m) = out[0][1] {
        assert!((m - 1.23456).abs() < 0.00001);
    } else {
        panic!("Expected Double");
    }
}

/// Table lookup with greater than in stream filter
#[tokio::test]
#[ignore = "Table alias resolution in SELECT not yet supported"]
async fn table_test_stream_filter_gt() {
    let app = "\
        CREATE STREAM queryStream (category STRING, minPrice INT);\n\
        CREATE TABLE productTable (id INT PRIMARY KEY, category STRING, price INT);\n\
        CREATE STREAM outputStream (id INT, price INT);\n\
        CREATE STREAM insertStream (id INT, category STRING, price INT);\n\
        INSERT INTO productTable\n\
        SELECT id, category, price FROM insertStream;\n\
        INSERT INTO outputStream\n\
        SELECT t.id, t.price\n\
        FROM queryStream AS q JOIN productTable AS t\n\
        ON q.category = t.category;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "insertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(500),
        ],
    );
    runner.send(
        "queryStream",
        vec![
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
    assert_eq!(out[0][1], AttributeValue::Int(500));
}

/// Table with coalesce in select
#[tokio::test]
async fn table_test_coalesce_select() {
    let app = "\
        CREATE STREAM dataStream (id INT, value STRING);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY, value STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO dataTable\n\
        SELECT id, value FROM dataStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, coalesce(value, 'DEFAULT') AS result FROM dataTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("actual".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("actual".to_string()));
}

/// Table with upper function in select
#[tokio::test]
async fn table_test_upper_select() {
    let app = "\
        CREATE STREAM nameStream (id INT, name STRING);\n\
        CREATE TABLE nameTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, upperName STRING);\n\
        INSERT INTO nameTable\n\
        SELECT id, name FROM nameStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(name) AS upperName FROM nameTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("alice".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ALICE".to_string()));
}

/// Table with lower function in select
#[tokio::test]
async fn table_test_lower_select() {
    let app = "\
        CREATE STREAM codeStream (id INT, code STRING);\n\
        CREATE TABLE codeTable (id INT PRIMARY KEY, code STRING);\n\
        CREATE STREAM outputStream (id INT, lowerCode STRING);\n\
        INSERT INTO codeTable\n\
        SELECT id, code FROM codeStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, lower(code) AS lowerCode FROM codeTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "codeStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ABC123".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("abc123".to_string()));
}

/// Table with concat function in select
#[tokio::test]
async fn table_test_concat_select() {
    let app = "\
        CREATE STREAM personStream (id INT, firstName STRING, lastName STRING);\n\
        CREATE TABLE personTable (id INT PRIMARY KEY, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (id INT, fullName STRING);\n\
        INSERT INTO personTable\n\
        SELECT id, firstName, lastName FROM personStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, concat(firstName, ' ', lastName) AS fullName FROM personTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Table with length function in select
#[tokio::test]
async fn table_test_length_select() {
    let app = "\
        CREATE STREAM textStream (id INT, text STRING);\n\
        CREATE TABLE textTable (id INT PRIMARY KEY, text STRING);\n\
        CREATE STREAM outputStream (id INT, textLen INT);\n\
        INSERT INTO textTable\n\
        SELECT id, text FROM textStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, length(text) AS textLen FROM textTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello World".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // "Hello World" = 11 characters
    let len = match &out[0][1] {
        AttributeValue::Int(l) => *l as i64,
        AttributeValue::Long(l) => *l,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len, 11);
}

/// Table with arithmetic in select (addition)
#[tokio::test]
async fn table_test_arithmetic_select() {
    let app = "\
        CREATE STREAM priceStream (id INT, basePrice INT, tax INT);\n\
        CREATE TABLE priceTable (id INT PRIMARY KEY, basePrice INT, tax INT);\n\
        CREATE STREAM outputStream (id INT, totalPrice INT);\n\
        INSERT INTO priceTable\n\
        SELECT id, basePrice, tax FROM priceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, basePrice + tax AS totalPrice FROM priceTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(110));
}

/// Table with multiplication in select
#[tokio::test]
async fn table_test_multiplication_select() {
    let app = "\
        CREATE STREAM orderStream (id INT, qty INT, unitPrice INT);\n\
        CREATE TABLE orderTable (id INT PRIMARY KEY, qty INT, unitPrice INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO orderTable\n\
        SELECT id, qty, unitPrice FROM orderStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, qty * unitPrice AS total FROM orderTable;\n";
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
    let total = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(total, 100);
}

/// Table with subtraction in select
#[tokio::test]
async fn table_test_subtraction_select() {
    let app = "\
        CREATE STREAM inventoryStream (id INT, inStock INT, reserved INT);\n\
        CREATE TABLE inventoryTable (id INT PRIMARY KEY, inStock INT, reserved INT);\n\
        CREATE STREAM outputStream (id INT, available INT);\n\
        INSERT INTO inventoryTable\n\
        SELECT id, inStock, reserved FROM inventoryStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, inStock - reserved AS available FROM inventoryTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inventoryStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(25),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(75));
}

/// Table with CASE WHEN in select
#[tokio::test]
async fn table_test_case_when_select() {
    let app = "\
        CREATE STREAM scoreStream (id INT, score INT);\n\
        CREATE TABLE scoreTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, grade STRING);\n\
        INSERT INTO scoreTable\n\
        SELECT id, score FROM scoreStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM scoreTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(95)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("A".to_string()));
}

/// Table with multiple rows and WHERE filter
#[tokio::test]
async fn table_test_multiple_rows_where() {
    let app = "\
        CREATE STREAM productStream (id INT, category STRING, price INT);\n\
        CREATE TABLE productTable (id INT PRIMARY KEY, category STRING, price INT);\n\
        CREATE STREAM outputStream (id INT, category STRING, price INT);\n\
        INSERT INTO productTable\n\
        SELECT id, category, price FROM productStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, category, price FROM productTable WHERE price > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(100),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("Books".to_string()),
            AttributeValue::Int(20),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("Toys".to_string()),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    // Only id 1 and 3 have price > 50
    assert_eq!(out.len(), 2);
}

/// Table with string comparison in WHERE
#[tokio::test]
async fn table_test_string_where() {
    let app = "\
        CREATE STREAM userStream (id INT, status STRING);\n\
        CREATE TABLE userTable (id INT PRIMARY KEY, status STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO userTable\n\
        SELECT id, status FROM userStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, status FROM userTable WHERE status = 'active';\n";
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
            AttributeValue::String("inactive".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with nested concat for full address
#[tokio::test]
async fn table_test_nested_concat_address() {
    let app = "\
        CREATE STREAM addressStream (id INT, street STRING, city STRING, country STRING);\n\
        CREATE TABLE addressTable (id INT PRIMARY KEY, street STRING, city STRING, country STRING);\n\
        CREATE STREAM outputStream (id INT, address STRING);\n\
        INSERT INTO addressTable\n\
        SELECT id, street, city, country FROM addressStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, concat(concat(street, ', '), concat(city, ', ', country)) AS address FROM addressTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "addressStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("123 Main St".to_string()),
            AttributeValue::String("NYC".to_string()),
            AttributeValue::String("USA".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0][1],
        AttributeValue::String("123 Main St, NYC, USA".to_string())
    );
}

/// Table with multiple non-null string columns
#[tokio::test]
async fn table_test_multi_string_columns() {
    let app = "\
        CREATE STREAM personStream (id INT, firstName STRING, lastName STRING, email STRING);\n\
        CREATE TABLE personTable (id INT PRIMARY KEY, firstName STRING, lastName STRING, email STRING);\n\
        CREATE STREAM outputStream (id INT, firstName STRING, lastName STRING, email STRING);\n\
        INSERT INTO personTable\n\
        SELECT id, firstName, lastName, email FROM personStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, firstName, lastName, email FROM personTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
            AttributeValue::String("john@example.com".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John".to_string()));
    assert_eq!(out[0][2], AttributeValue::String("Doe".to_string()));
    assert_eq!(
        out[0][3],
        AttributeValue::String("john@example.com".to_string())
    );
}

/// Table with DOUBLE precision field
#[tokio::test]
async fn table_test_double_precision_field() {
    let app = "\
        CREATE STREAM measureStream (id INT, value DOUBLE);\n\
        CREATE TABLE measureTable (id INT PRIMARY KEY, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT, value DOUBLE);\n\
        INSERT INTO measureTable\n\
        SELECT id, value FROM measureStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM measureTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "measureStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Double(7.654321098765432),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let val = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        _ => panic!("Expected double or float"),
    };
    assert!((val - 7.654321098765432).abs() < 0.0000001);
}

/// Table with comparison to zero
#[tokio::test]
async fn table_test_comparison_zero() {
    let app = "\
        CREATE STREAM balanceStream (id INT, balance INT);\n\
        CREATE TABLE balanceTable (id INT PRIMARY KEY, balance INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO balanceTable\n\
        SELECT id, balance FROM balanceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, balance FROM balanceTable WHERE balance > 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(100)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(0)],
    );
    runner.send(
        "balanceStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(-50)],
    );
    let out = runner.shutdown();
    // Only id 1 has balance > 0
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with uuid function in insert
#[tokio::test]
async fn table_test_uuid_insert() {
    let app = "\
        CREATE STREAM eventStream (name STRING);\n\
        CREATE TABLE eventTable (eventId STRING PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (eventId STRING, name STRING);\n\
        INSERT INTO eventTable\n\
        SELECT uuid() AS eventId, name FROM eventStream;\n\
        INSERT INTO outputStream\n\
        SELECT eventId, name FROM eventTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![AttributeValue::String("TestEvent".to_string())],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][0] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
    assert_eq!(out[0][1], AttributeValue::String("TestEvent".to_string()));
}

/// Table with greater than or equal filter
#[tokio::test]
async fn table_test_gte_filter() {
    let app = "\
        CREATE STREAM scoreStream (id INT, score INT);\n\
        CREATE TABLE scoreTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, score INT);\n\
        INSERT INTO scoreTable\n\
        SELECT id, score FROM scoreStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, score FROM scoreTable WHERE score >= 80;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(85)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(70)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(80)],
    );
    let out = runner.shutdown();
    // id 1 (85) and id 3 (80) pass the filter
    assert_eq!(out.len(), 2);
}

/// Table with less than or equal filter
#[tokio::test]
async fn table_test_lte_filter() {
    let app = "\
        CREATE STREAM inventoryStream (id INT, quantity INT);\n\
        CREATE TABLE inventoryTable (id INT PRIMARY KEY, quantity INT);\n\
        CREATE STREAM outputStream (id INT, quantity INT);\n\
        INSERT INTO inventoryTable\n\
        SELECT id, quantity FROM inventoryStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, quantity FROM inventoryTable WHERE quantity <= 10;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "inventoryStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(5)],
    );
    runner.send(
        "inventoryStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    runner.send(
        "inventoryStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(10)],
    );
    let out = runner.shutdown();
    // id 1 (5) and id 3 (10) pass the filter
    assert_eq!(out.len(), 2);
}

/// Table with concat function in select (fullname)
#[tokio::test]
async fn table_test_concat_fullname() {
    let app = "\
        CREATE STREAM personStream (id INT, firstName STRING, lastName STRING);\n\
        CREATE TABLE personTable (id INT PRIMARY KEY, firstName STRING, lastName STRING);\n\
        CREATE STREAM outputStream (id INT, fullName STRING);\n\
        INSERT INTO personTable\n\
        SELECT id, firstName, lastName FROM personStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, concat(firstName, ' ', lastName) AS fullName FROM personTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Table with upper function in select (product)
#[tokio::test]
async fn table_test_upper_product() {
    let app = "\
        CREATE STREAM productStream (id INT, name STRING);\n\
        CREATE TABLE productTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, upperName STRING);\n\
        INSERT INTO productTable\n\
        SELECT id, name FROM productStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(name) AS upperName FROM productTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("laptop".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("LAPTOP".to_string()));
}

/// Table with lower function in select (category)
#[tokio::test]
async fn table_test_lower_category() {
    let app = "\
        CREATE STREAM categoryStream (id INT, name STRING);\n\
        CREATE TABLE categoryTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, lowerName STRING);\n\
        INSERT INTO categoryTable\n\
        SELECT id, name FROM categoryStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, lower(name) AS lowerName FROM categoryTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "categoryStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ELECTRONICS".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("electronics".to_string()));
}

/// Table with arithmetic multiplication
#[tokio::test]
async fn table_test_arithmetic_multiply() {
    let app = "\
        CREATE STREAM orderStream (id INT, quantity INT, price INT);\n\
        CREATE TABLE orderTable (id INT PRIMARY KEY, quantity INT, price INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO orderTable\n\
        SELECT id, quantity, price FROM orderStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, quantity * price AS total FROM orderTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(5),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(500)); // 5 * 100 = 500
}

/// Table with empty string content
#[tokio::test]
async fn table_test_empty_string_content() {
    let app = "\
        CREATE STREAM messageStream (id INT, content STRING);\n\
        CREATE TABLE messageTable (id INT PRIMARY KEY, content STRING);\n\
        CREATE STREAM outputStream (id INT, content STRING);\n\
        INSERT INTO messageTable\n\
        SELECT id, content FROM messageStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, content FROM messageTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "messageStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("".to_string()));
}

/// Table with negative balance
#[tokio::test]
async fn table_test_negative_balance() {
    let app = "\
        CREATE STREAM accountStream (id INT, balance INT);\n\
        CREATE TABLE accountTable (id INT PRIMARY KEY, balance INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO accountTable\n\
        SELECT id, balance FROM accountStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, balance FROM accountTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "accountStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(-500)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-500));
}

/// Table with coalesce function (primary)
#[tokio::test]
async fn table_test_coalesce_primary() {
    let app = "\
        CREATE STREAM dataStream (id INT, primary_val STRING, backup_val STRING);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO dataTable\n\
        SELECT id, primary_val, backup_val FROM dataStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, coalesce(primary_val, backup_val) AS result FROM dataTable;\n";
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

/// Table with zero value storage
#[tokio::test]
async fn table_test_zero_value_storage() {
    let app = "\
        CREATE STREAM dataStream (id INT, count INT);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY, count INT);\n\
        CREATE STREAM outputStream (id INT, count INT);\n\
        INSERT INTO dataTable\n\
        SELECT id, count FROM dataStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, count FROM dataTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "dataStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(0));
}

/// Table with DOUBLE field type
#[tokio::test]
async fn table_test_double_field() {
    let app = "\
        CREATE STREAM priceStream (id INT, price DOUBLE);\n\
        CREATE TABLE priceTable (id INT PRIMARY KEY, price DOUBLE);\n\
        CREATE STREAM outputStream (id INT, price DOUBLE);\n\
        INSERT INTO priceTable\n\
        SELECT id, price FROM priceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, price FROM priceTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(19.99)],
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

/// Table with length function in select
#[tokio::test]
async fn table_test_length_in_select() {
    let app = "\
        CREATE STREAM textStream (id INT, content STRING);\n\
        CREATE TABLE textTable (id INT PRIMARY KEY, content STRING);\n\
        CREATE STREAM outputStream (id INT, content_len INT);\n\
        INSERT INTO textTable\n\
        SELECT id, content FROM textStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, length(content) AS content_len FROM textTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len_val = match &out[0][1] {
        AttributeValue::Int(v) => *v as i64,
        AttributeValue::Long(v) => *v,
        _ => panic!("Expected int or long"),
    };
    assert_eq!(len_val, 5);
}

/// Table with division in select
#[tokio::test]
async fn table_test_division_in_select() {
    let app = "\
        CREATE STREAM salesStream (id INT, total INT, quantity INT);\n\
        CREATE TABLE salesTable (id INT PRIMARY KEY, total INT, quantity INT);\n\
        CREATE STREAM outputStream (id INT, unit_price DOUBLE);\n\
        INSERT INTO salesTable\n\
        SELECT id, total, quantity FROM salesStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, total / quantity AS unit_price FROM salesTable;\n";
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
    let unit_price = match &out[0][1] {
        AttributeValue::Double(v) => *v,
        AttributeValue::Float(v) => *v as f64,
        AttributeValue::Int(v) => *v as f64,
        AttributeValue::Long(v) => *v as f64,
        _ => panic!("Expected numeric"),
    };
    assert!((unit_price - 25.0).abs() < 0.001); // 100 / 4 = 25
}

/// Table with subtraction in select
#[tokio::test]
async fn table_test_subtraction_in_select() {
    let app = "\
        CREATE STREAM orderStream (id INT, gross INT, discount INT);\n\
        CREATE TABLE orderTable (id INT PRIMARY KEY, gross INT, discount INT);\n\
        CREATE STREAM outputStream (id INT, net INT);\n\
        INSERT INTO orderTable\n\
        SELECT id, gross, discount FROM orderStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, gross - discount AS net FROM orderTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "orderStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(15),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(85)); // 100 - 15 = 85
}

/// Table with AND condition in WHERE
#[tokio::test]
async fn table_test_and_where() {
    let app = "\
        CREATE STREAM productStream (id INT, category STRING, price INT);\n\
        CREATE TABLE productTable (id INT PRIMARY KEY, category STRING, price INT);\n\
        CREATE STREAM outputStream (id INT, category STRING, price INT);\n\
        INSERT INTO productTable\n\
        SELECT id, category, price FROM productStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, category, price FROM productTable\n\
        WHERE category = 'Electronics' AND price > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Electronics".to_string()),
            AttributeValue::Int(100),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][2], AttributeValue::Int(100));
}

/// Table with OR condition in WHERE (priority match)
#[tokio::test]
async fn table_test_or_where_priority() {
    let app = "\
        CREATE STREAM statusStream (id INT, status STRING, priority INT);\n\
        CREATE TABLE statusTable (id INT PRIMARY KEY, status STRING, priority INT);\n\
        CREATE STREAM outputStream (id INT, status STRING, priority INT);\n\
        INSERT INTO statusTable\n\
        SELECT id, status, priority FROM statusStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, status, priority FROM statusTable\n\
        WHERE status = 'CRITICAL' OR priority > 8;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "statusStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("NORMAL".to_string()),
            AttributeValue::Int(9),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][2], AttributeValue::Int(9)); // priority > 8 matches
}

/// Table with multiple inserts (update behavior)
#[tokio::test]
async fn table_test_update_on_duplicate() {
    let app = "\
        CREATE STREAM userStream (id INT, name STRING);\n\
        CREATE TABLE userTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, name STRING);\n\
        INSERT INTO userTable\n\
        SELECT id, name FROM userStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, name FROM userTable;\n";
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
            AttributeValue::Int(1),
            AttributeValue::String("Bob".to_string()),
        ],
    );
    let out = runner.shutdown();
    // Second insert should update the row
    assert!(out.len() >= 1);
    // The last output should be "Bob"
    let last_name = &out[out.len() - 1][1];
    assert_eq!(last_name, &AttributeValue::String("Bob".to_string()));
}

/// Table with uuid function in select
#[tokio::test]
async fn table_test_uuid_in_select() {
    let app = "\
        CREATE STREAM triggerStream (id INT);\n\
        CREATE TABLE triggerTable (id INT PRIMARY KEY);\n\
        CREATE STREAM outputStream (id INT, uuid_val STRING);\n\
        INSERT INTO triggerTable\n\
        SELECT id FROM triggerStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, uuid() AS uuid_val FROM triggerTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("triggerStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    // UUID should be a non-empty string
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Table with NOT EQUAL condition
#[tokio::test]
async fn table_test_not_equal_where() {
    let app = "\
        CREATE STREAM itemStream (id INT, status STRING);\n\
        CREATE TABLE itemTable (id INT PRIMARY KEY, status STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO itemTable\n\
        SELECT id, status FROM itemStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, status FROM itemTable\n\
        WHERE status != 'DELETED';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "itemStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("ACTIVE".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("ACTIVE".to_string()));
}

/// Table with LTE condition
#[tokio::test]
async fn table_test_lte_condition() {
    let app = "\
        CREATE STREAM priceStream (id INT, price INT);\n\
        CREATE TABLE priceTable (id INT PRIMARY KEY, price INT);\n\
        CREATE STREAM outputStream (id INT, price INT);\n\
        INSERT INTO priceTable\n\
        SELECT id, price FROM priceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, price FROM priceTable\n\
        WHERE price <= 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(30)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(75)],
    );
    let out = runner.shutdown();
    // id=1 (30) and id=2 (50) match
    assert_eq!(out.len(), 2);
}

/// Table with GTE condition
#[tokio::test]
async fn table_test_gte_condition() {
    let app = "\
        CREATE STREAM scoreStream (id INT, score INT);\n\
        CREATE TABLE scoreTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, score INT);\n\
        INSERT INTO scoreTable\n\
        SELECT id, score FROM scoreStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, score FROM scoreTable\n\
        WHERE score >= 60;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(55)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(60)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(80)],
    );
    let out = runner.shutdown();
    // id=2 (60) and id=3 (80) match
    assert_eq!(out.len(), 2);
}

/// Table with AND condition combining multiple filters
#[tokio::test]
async fn table_test_and_multi_filter() {
    let app = "\
        CREATE STREAM productStream (id INT, category STRING, price INT);\n\
        CREATE TABLE productTable (id INT PRIMARY KEY, category STRING, price INT);\n\
        CREATE STREAM outputStream (id INT, category STRING, price INT);\n\
        INSERT INTO productTable\n\
        SELECT id, category, price FROM productStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, category, price FROM productTable\n\
        WHERE category = 'electronics' AND price < 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("electronics".to_string()),
            AttributeValue::Int(50),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::String("electronics".to_string()),
            AttributeValue::Int(150),
        ],
    );
    runner.send(
        "productStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::String("clothing".to_string()),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    // Only id=1 (electronics, price<100) matches
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with OR condition
#[tokio::test]
async fn table_test_or_multi_filter() {
    let app = "\
        CREATE STREAM alertStream (id INT, level INT, priority STRING);\n\
        CREATE TABLE alertTable (id INT PRIMARY KEY, level INT, priority STRING);\n\
        CREATE STREAM outputStream (id INT, level INT, priority STRING);\n\
        INSERT INTO alertTable\n\
        SELECT id, level, priority FROM alertStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, level, priority FROM alertTable\n\
        WHERE level > 80 OR priority = 'critical';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "alertStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(50),
            AttributeValue::String("critical".to_string()),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(90),
            AttributeValue::String("low".to_string()),
        ],
    );
    runner.send(
        "alertStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(30),
            AttributeValue::String("medium".to_string()),
        ],
    );
    let out = runner.shutdown();
    // id=1 (critical) and id=2 (level>80) match
    assert_eq!(out.len(), 2);
}

/// Table with CASE WHEN expression
#[tokio::test]
async fn table_test_case_when() {
    let app = "\
        CREATE STREAM gradeStream (id INT, score INT);\n\
        CREATE TABLE gradeTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, grade STRING);\n\
        INSERT INTO gradeTable\n\
        SELECT id, score FROM gradeStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade\n\
        FROM gradeTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "gradeStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(85)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("B".to_string()));
}

/// Table with nested arithmetic
#[tokio::test]
async fn table_test_nested_arithmetic() {
    let app = "\
        CREATE STREAM invoiceStream (id INT, base INT, tax INT, discount INT);\n\
        CREATE TABLE invoiceTable (id INT PRIMARY KEY, base INT, tax INT, discount INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO invoiceTable\n\
        SELECT id, base, tax, discount FROM invoiceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, (base + tax) - discount AS total FROM invoiceTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "invoiceStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(10),
            AttributeValue::Int(5),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(105)); // (100 + 10) - 5 = 105
}

/// Table with multiplication for order total
#[tokio::test]
async fn table_test_multiplication_order_total() {
    let app = "\
        CREATE STREAM orderStream (id INT, qty INT, unit_price INT);\n\
        CREATE TABLE orderTable (id INT PRIMARY KEY, qty INT, unit_price INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO orderTable\n\
        SELECT id, qty, unit_price FROM orderStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, qty * unit_price AS total FROM orderTable;\n";
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

/// Table with concat for full name
#[tokio::test]
async fn table_test_concat_full_name() {
    let app = "\
        CREATE STREAM personStream (id INT, first_name STRING, last_name STRING);\n\
        CREATE TABLE personTable (id INT PRIMARY KEY, first_name STRING, last_name STRING);\n\
        CREATE STREAM outputStream (id INT, full_name STRING);\n\
        INSERT INTO personTable\n\
        SELECT id, first_name, last_name FROM personStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, concat(first_name, ' ', last_name) AS full_name FROM personTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "personStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("John Doe".to_string()));
}

/// Table with upper and lower functions
#[tokio::test]
async fn table_test_upper_lower() {
    let app = "\
        CREATE STREAM nameStream (id INT, name STRING);\n\
        CREATE TABLE nameTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, upper_name STRING, lower_name STRING);\n\
        INSERT INTO nameTable\n\
        SELECT id, name FROM nameStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(name) AS upper_name, lower(name) AS lower_name FROM nameTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("HELLO".to_string()));
    assert_eq!(out[0][2], AttributeValue::String("hello".to_string()));
}

/// Table with length function (content length)
#[tokio::test]
async fn table_test_length_content() {
    let app = "\
        CREATE STREAM textStream (id INT, content STRING);\n\
        CREATE TABLE textTable (id INT PRIMARY KEY, content STRING);\n\
        CREATE STREAM outputStream (id INT, len INT);\n\
        INSERT INTO textTable\n\
        SELECT id, content FROM textStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, length(content) AS len FROM textTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "textStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("hello world".to_string()),
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

/// Table with range filter
#[tokio::test]
async fn table_test_range_filter() {
    let app = "\
        CREATE STREAM valueStream (id INT, value INT);\n\
        CREATE TABLE valueTable (id INT PRIMARY KEY, value INT);\n\
        CREATE STREAM outputStream (id INT, value INT);\n\
        INSERT INTO valueTable\n\
        SELECT id, value FROM valueStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, value FROM valueTable\n\
        WHERE value >= 20 AND value <= 80;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(10)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(50)],
    );
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(90)],
    );
    let out = runner.shutdown();
    // Only id=2 (50) is in range
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Table with subtraction for balance calculation
#[tokio::test]
async fn table_test_subtraction_balance() {
    let app = "\
        CREATE STREAM balanceStream (id INT, credit INT, debit INT);\n\
        CREATE TABLE balanceTable (id INT PRIMARY KEY, credit INT, debit INT);\n\
        CREATE STREAM outputStream (id INT, balance INT);\n\
        INSERT INTO balanceTable\n\
        SELECT id, credit, debit FROM balanceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, credit - debit AS balance FROM balanceTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "balanceStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(500),
            AttributeValue::Int(150),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(350)); // 500 - 150 = 350
}

/// Table with addition in select
#[tokio::test]
async fn table_test_addition_select() {
    let app = "\
        CREATE STREAM componentStream (id INT, val1 INT, val2 INT);\n\
        CREATE TABLE componentTable (id INT PRIMARY KEY, val1 INT, val2 INT);\n\
        CREATE STREAM outputStream (id INT, total INT);\n\
        INSERT INTO componentTable\n\
        SELECT id, val1, val2 FROM componentStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, val1 + val2 AS total FROM componentTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "componentStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(75),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(175)); // 100 + 75 = 175
}

/// Table with uuid function
#[tokio::test]
async fn table_test_uuid_function() {
    let app = "\
        CREATE STREAM dataStream (id INT);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY);\n\
        CREATE STREAM outputStream (id INT, uuid_val STRING);\n\
        INSERT INTO dataTable\n\
        SELECT id FROM dataStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, uuid() AS uuid_val FROM dataTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send("dataStream", vec![AttributeValue::Int(1)]);
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    if let AttributeValue::String(uuid) = &out[0][1] {
        assert!(!uuid.is_empty());
    } else {
        panic!("Expected UUID string");
    }
}

/// Table with string comparison
#[tokio::test]
async fn table_test_string_comparison() {
    let app = "\
        CREATE STREAM statusStream (id INT, status STRING);\n\
        CREATE TABLE statusTable (id INT PRIMARY KEY, status STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO statusTable\n\
        SELECT id, status FROM statusStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, status FROM statusTable\n\
        WHERE status = 'active';\n";
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
            AttributeValue::String("inactive".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with negative value filter
#[tokio::test]
async fn table_test_negative_filter() {
    let app = "\
        CREATE STREAM tempStream (id INT, temp INT);\n\
        CREATE TABLE tempTable (id INT PRIMARY KEY, temp INT);\n\
        CREATE STREAM outputStream (id INT, temp INT);\n\
        INSERT INTO tempTable\n\
        SELECT id, temp FROM tempStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, temp FROM tempTable\n\
        WHERE temp < 0;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(-10)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(5)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(-10));
}

/// Table with zero value
#[tokio::test]
async fn table_test_zero_value() {
    let app = "\
        CREATE STREAM countStream (id INT, count_val INT);\n\
        CREATE TABLE countTable (id INT PRIMARY KEY, count_val INT);\n\
        CREATE STREAM outputStream (id INT, count_val INT);\n\
        INSERT INTO countTable\n\
        SELECT id, count_val FROM countStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, count_val FROM countTable\n\
        WHERE count_val = 0;\n";
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

/// Table with greater than filter
#[tokio::test]
async fn table_test_greater_than() {
    let app = "\
        CREATE STREAM scoreStream (id INT, score INT);\n\
        CREATE TABLE scoreTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, score INT);\n\
        INSERT INTO scoreTable\n\
        SELECT id, score FROM scoreStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, score FROM scoreTable\n\
        WHERE score > 50;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(40)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(60)],
    );
    runner.send(
        "scoreStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(50)],
    );
    let out = runner.shutdown();
    // Only id=2 (60 > 50)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(2));
}

/// Table with less than filter
#[tokio::test]
async fn table_test_less_than() {
    let app = "\
        CREATE STREAM priceStream (id INT, price INT);\n\
        CREATE TABLE priceTable (id INT PRIMARY KEY, price INT);\n\
        CREATE STREAM outputStream (id INT, price INT);\n\
        INSERT INTO priceTable\n\
        SELECT id, price FROM priceStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, price FROM priceTable\n\
        WHERE price < 100;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(50)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(2), AttributeValue::Int(150)],
    );
    runner.send(
        "priceStream",
        vec![AttributeValue::Int(3), AttributeValue::Int(100)],
    );
    let out = runner.shutdown();
    // Only id=1 (50 < 100)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with coalesce function primary backup
#[tokio::test]
async fn table_test_coalesce_primary_backup() {
    let app = "\
        CREATE STREAM dataStream (id INT, primary_val STRING, backup_val STRING);\n\
        CREATE TABLE dataTable (id INT PRIMARY KEY, primary_val STRING, backup_val STRING);\n\
        CREATE STREAM outputStream (id INT, result STRING);\n\
        INSERT INTO dataTable\n\
        SELECT id, primary_val, backup_val FROM dataStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, coalesce(primary_val, backup_val) AS result FROM dataTable;\n";
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

/// Table with uuid function
#[tokio::test]
async fn table_test_uuid_select() {
    let app = "\
        CREATE STREAM eventStream (id INT, name STRING);\n\
        CREATE TABLE eventTable (id INT PRIMARY KEY, name STRING);\n\
        CREATE STREAM outputStream (id INT, event_id STRING);\n\
        INSERT INTO eventTable\n\
        SELECT id, name FROM eventStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, uuid() AS event_id FROM eventTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("test".to_string()),
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

/// Table with sqrt function
#[tokio::test]
async fn table_test_sqrt() {
    let app = "\
        CREATE STREAM mathStream (id INT, value DOUBLE);\n\
        CREATE TABLE mathTable (id INT PRIMARY KEY, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT, sqrt_val DOUBLE);\n\
        INSERT INTO mathTable\n\
        SELECT id, value FROM mathStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, sqrt(value) AS sqrt_val FROM mathTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "mathStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(25.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Double(5.0));
}

/// Table with round function
#[tokio::test]
async fn table_test_round() {
    let app = "\
        CREATE STREAM valueStream (id INT, value DOUBLE);\n\
        CREATE TABLE valueTable (id INT PRIMARY KEY, value DOUBLE);\n\
        CREATE STREAM outputStream (id INT, rounded DOUBLE);\n\
        INSERT INTO valueTable\n\
        SELECT id, value FROM valueStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, round(value) AS rounded FROM valueTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "valueStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(3.7)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Double(4.0));
}

/// Table with multiple arithmetic operations
#[tokio::test]
async fn table_test_multi_arithmetic() {
    let app = "\
        CREATE STREAM calcStream (id INT, a INT, b INT, c INT);\n\
        CREATE TABLE calcTable (id INT PRIMARY KEY, a INT, b INT, c INT);\n\
        CREATE STREAM outputStream (id INT, result INT);\n\
        INSERT INTO calcTable\n\
        SELECT id, a, b, c FROM calcStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, a + b - c AS result FROM calcTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "calcStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(100),
            AttributeValue::Int(50),
            AttributeValue::Int(30),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::Int(120)); // 100 + 50 - 30 = 120
}

/// Table with division returning double
#[tokio::test]
async fn table_test_division_double() {
    let app = "\
        CREATE STREAM rateStream (id INT, total INT, count_val INT);\n\
        CREATE TABLE rateTable (id INT PRIMARY KEY, total INT, count_val INT);\n\
        CREATE STREAM outputStream (id INT, rate DOUBLE);\n\
        INSERT INTO rateTable\n\
        SELECT id, total, count_val FROM rateStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, total / count_val AS rate FROM rateTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "rateStream",
        vec![
            AttributeValue::Int(1),
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

/// Table with CASE WHEN multiple branches
#[tokio::test]
async fn table_test_case_when_multi() {
    let app = "\
        CREATE STREAM gradeStream (id INT, score INT);\n\
        CREATE TABLE gradeTable (id INT PRIMARY KEY, score INT);\n\
        CREATE STREAM outputStream (id INT, grade STRING);\n\
        INSERT INTO gradeTable\n\
        SELECT id, score FROM gradeStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END AS grade FROM gradeTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "gradeStream",
        vec![AttributeValue::Int(1), AttributeValue::Int(85)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("B".to_string()));
}

/// Table with string not equal
#[tokio::test]
async fn table_test_string_neq() {
    let app = "\
        CREATE STREAM statusStream (id INT, status STRING);\n\
        CREATE TABLE statusTable (id INT PRIMARY KEY, status STRING);\n\
        CREATE STREAM outputStream (id INT, status STRING);\n\
        INSERT INTO statusTable\n\
        SELECT id, status FROM statusStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, status FROM statusTable\n\
        WHERE status != 'inactive';\n";
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
            AttributeValue::String("inactive".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with double comparison
#[tokio::test]
async fn table_test_double_compare() {
    let app = "\
        CREATE STREAM tempStream (id INT, temp DOUBLE);\n\
        CREATE TABLE tempTable (id INT PRIMARY KEY, temp DOUBLE);\n\
        CREATE STREAM outputStream (id INT, temp DOUBLE);\n\
        INSERT INTO tempTable\n\
        SELECT id, temp FROM tempStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, temp FROM tempTable\n\
        WHERE temp > 36.5;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(1), AttributeValue::Double(37.2)],
    );
    runner.send(
        "tempStream",
        vec![AttributeValue::Int(2), AttributeValue::Double(36.0)],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with combined AND OR conditions
#[tokio::test]
async fn table_test_and_or_combined() {
    let app = "\
        CREATE STREAM eventStream (id INT, priority INT, status STRING);\n\
        CREATE TABLE eventTable (id INT PRIMARY KEY, priority INT, status STRING);\n\
        CREATE STREAM outputStream (id INT, priority INT);\n\
        INSERT INTO eventTable\n\
        SELECT id, priority, status FROM eventStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, priority FROM eventTable\n\
        WHERE priority > 5 AND status = 'active';\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::Int(8),
            AttributeValue::String("active".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(2),
            AttributeValue::Int(8),
            AttributeValue::String("inactive".to_string()),
        ],
    );
    runner.send(
        "eventStream",
        vec![
            AttributeValue::Int(3),
            AttributeValue::Int(3),
            AttributeValue::String("active".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], AttributeValue::Int(1));
}

/// Table with nested function upper(concat)
#[tokio::test]
async fn table_test_upper_concat() {
    let app = "\
        CREATE STREAM nameStream (id INT, first STRING, last STRING);\n\
        CREATE TABLE nameTable (id INT PRIMARY KEY, first STRING, last STRING);\n\
        CREATE STREAM outputStream (id INT, full_name STRING);\n\
        INSERT INTO nameTable\n\
        SELECT id, first, last FROM nameStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, upper(concat(first, last)) AS full_name FROM nameTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "nameStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("John".to_string()),
            AttributeValue::String("Doe".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], AttributeValue::String("JOHNDOE".to_string()));
}

/// Table with length(concat)
#[tokio::test]
async fn table_test_length_concat() {
    let app = "\
        CREATE STREAM msgStream (id INT, prefix STRING, suffix STRING);\n\
        CREATE TABLE msgTable (id INT PRIMARY KEY, prefix STRING, suffix STRING);\n\
        CREATE STREAM outputStream (id INT, total_len INT);\n\
        INSERT INTO msgTable\n\
        SELECT id, prefix, suffix FROM msgStream;\n\
        INSERT INTO outputStream\n\
        SELECT id, length(concat(prefix, suffix)) AS total_len FROM msgTable;\n";
    let runner = AppRunner::new(app, "outputStream").await;
    runner.send(
        "msgStream",
        vec![
            AttributeValue::Int(1),
            AttributeValue::String("Hello".to_string()),
            AttributeValue::String("World".to_string()),
        ],
    );
    let out = runner.shutdown();
    assert_eq!(out.len(), 1);
    let len = match &out[0][1] {
        AttributeValue::Int(v) => *v,
        AttributeValue::Long(v) => *v as i32,
        _ => panic!("Expected int"),
    };
    assert_eq!(len, 10); // "HelloWorld" = 10 chars
}
