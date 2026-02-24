// Type Validation Tests
//
// Tests for all 10 type validation features from SIDDHI_ONLY.md Priority 1:
// 1.1 Boolean Comparison Type Validation
// 1.2 String Comparison Type Validation
// 1.3 Non-Existing Attribute Validation
// 1.4 Duplicate Stream Definition Validation
// 1.5 Invalid Filter Condition Validation
// 1.6 Non-Boolean Filter Expression Validation (NOT)
// 1.7 Direct Table Query Prevention
// 1.8 Direct Aggregation Query Prevention (infrastructure ready)
// 1.9 Every Keyword with Table Prevention
// 1.10 Every Keyword with Aggregation Prevention (infrastructure ready)

use eventflux::core::event::value::AttributeValue;
use eventflux::core::executor::condition::compare_expression_executor::CompareExpressionExecutor;
use eventflux::core::executor::condition::not_expression_executor::NotExpressionExecutor;
use eventflux::core::executor::constant_expression_executor::ConstantExpressionExecutor;
use eventflux::query_api::definition::attribute::Type as AttributeType;
use eventflux::query_api::expression::condition::CompareOperator;
use eventflux::sql_compiler::parse;

// ============================================================================
// 1.1 Boolean Comparison Type Validation
// Prevents boolean comparison with numeric types using >, <, >=, <=
// ============================================================================

#[test]
fn test_1_1_boolean_greater_than_rejected() {
    // BOOL > BOOL should be rejected (only == and != allowed for BOOL)
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::GreaterThan);
    assert!(result.is_err(), "BOOL > BOOL should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.contains("Only == and != supported for BOOL"),
        "Error message should mention only == and != supported: {}",
        err
    );
}

#[test]
fn test_1_1_boolean_less_than_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::LessThan);
    assert!(result.is_err(), "BOOL < BOOL should be rejected");
}

#[test]
fn test_1_1_boolean_greater_equal_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::GreaterThanEqual);
    assert!(result.is_err(), "BOOL >= BOOL should be rejected");
}

#[test]
fn test_1_1_boolean_less_equal_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::LessThanEqual);
    assert!(result.is_err(), "BOOL <= BOOL should be rejected");
}

#[test]
fn test_1_1_boolean_equal_allowed() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::Equal);
    assert!(result.is_ok(), "BOOL == BOOL should be allowed");
}

#[test]
fn test_1_1_boolean_not_equal_allowed() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(false),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::NotEqual);
    assert!(result.is_ok(), "BOOL != BOOL should be allowed");
}

// ============================================================================
// 1.2 String Comparison Type Validation
// Prevents string comparison with numeric types
// ============================================================================

#[test]
fn test_1_2_string_int_comparison_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("test".to_string()),
        AttributeType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(42),
        AttributeType::INT,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::Equal);
    assert!(result.is_err(), "STRING = INT should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.contains("Cannot compare"),
        "Error should mention cannot compare: {}",
        err
    );
}

#[test]
fn test_1_2_string_double_comparison_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("test".to_string()),
        AttributeType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Double(3.14),
        AttributeType::DOUBLE,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::GreaterThan);
    assert!(result.is_err(), "STRING > DOUBLE should be rejected");
}

#[test]
fn test_1_2_string_bool_comparison_rejected() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("test".to_string()),
        AttributeType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));

    let result = CompareExpressionExecutor::new(left, right, CompareOperator::Equal);
    assert!(result.is_err(), "STRING = BOOL should be rejected");
}

#[test]
fn test_1_2_string_string_comparison_allowed() {
    let left = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("apple".to_string()),
        AttributeType::STRING,
    ));
    let right = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("banana".to_string()),
        AttributeType::STRING,
    ));

    // String-to-string comparison is allowed (lexicographic ordering)
    let result = CompareExpressionExecutor::new(left, right, CompareOperator::LessThan);
    assert!(
        result.is_ok(),
        "STRING < STRING should be allowed for lexicographic comparison"
    );
}

// ============================================================================
// 1.3 Non-Existing Attribute Validation
// Throws exception when query references undefined attribute
// ============================================================================

#[test]
fn test_1_3_undefined_attribute_rejected() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol, undefined_column\n\
        FROM Stock;\n";

    let result = parse(sql);
    assert!(result.is_err(), "Undefined attribute should be rejected");

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("undefined_column") || err.contains("not found") || err.contains("Unknown"),
        "Error should mention the undefined column: {}",
        err
    );
}

#[test]
fn test_1_3_undefined_attribute_in_where_rejected() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock\n\
        WHERE nonexistent > 100;\n";

    let result = parse(sql);
    // Undefined attribute should be detected at parse time
    assert!(
        result.is_err(),
        "Undefined attribute 'nonexistent' in WHERE should be rejected. Got: {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("nonexistent") || err.contains("not found") || err.contains("Unknown"),
        "Error should mention the undefined column: {}",
        err
    );
}

#[test]
fn test_1_3_valid_attributes_allowed() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol, price\n\
        FROM Stock\n\
        WHERE price > 100.0;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Valid attributes should be allowed: {:?}",
        result.err()
    );
}

// ============================================================================
// 1.4 Duplicate Stream Definition Validation
// Throws exception for duplicate stream definition
// ============================================================================

#[test]
fn test_1_4_duplicate_stream_rejected() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        CREATE STREAM Stock (symbol STRING, volume INT);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock;\n";

    let result = parse(sql);
    assert!(
        result.is_err(),
        "Duplicate stream definition should be rejected"
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Duplicate") || err.contains("Stock"),
        "Error should mention duplicate stream: {}",
        err
    );
}

#[test]
fn test_1_4_different_streams_allowed() {
    let sql = "\
        CREATE STREAM Stock1 (symbol STRING, price DOUBLE);\n\
        CREATE STREAM Stock2 (symbol STRING, volume INT);\n\
        INSERT INTO Out\n\
        SELECT Stock1.symbol\n\
        FROM Stock1 JOIN Stock2 ON Stock1.symbol = Stock2.symbol;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Different stream names should be allowed: {:?}",
        result.err()
    );
}

// ============================================================================
// 1.5 Invalid Filter Condition Validation
// Throws exception when filter condition contains non-boolean expression
// ============================================================================

#[test]
fn test_1_5_non_boolean_where_rejected() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock\n\
        WHERE price;\n";

    let result = parse(sql);
    // This should be rejected because price is DOUBLE, not BOOL
    assert!(
        result.is_err(),
        "Non-boolean expression 'price' (DOUBLE) in WHERE should be rejected. Got: {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BOOL") || err.contains("boolean") || err.contains("filter"),
        "Error should mention boolean type expected: {}",
        err
    );
}

#[test]
fn test_1_5_arithmetic_in_where_rejected() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock\n\
        WHERE price + 10;\n";

    let result = parse(sql);
    // Arithmetic expression in WHERE should be rejected
    assert!(
        result.is_err(),
        "Arithmetic expression 'price + 10' in WHERE should be rejected. Got: {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("BOOL") || err.contains("boolean") || err.contains("comparison"),
        "Error should mention boolean expected: {}",
        err
    );
}

#[test]
fn test_1_5_boolean_where_allowed() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE, active BOOL);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock\n\
        WHERE active;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Boolean attribute in WHERE should be allowed: {:?}",
        result.err()
    );
}

#[test]
fn test_1_5_comparison_where_allowed() {
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE);\n\
        INSERT INTO Out\n\
        SELECT symbol\n\
        FROM Stock\n\
        WHERE price > 100.0;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Comparison in WHERE should be allowed: {:?}",
        result.err()
    );
}

// ============================================================================
// 1.6 Non-Boolean Filter Expression Validation (NOT)
// Throws exception for `not(price)` where price is non-boolean
// ============================================================================

#[test]
fn test_1_6_not_non_boolean_rejected() {
    // NOT operator requires BOOL operand
    let int_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(42),
        AttributeType::INT,
    ));

    let result = NotExpressionExecutor::new(int_exec);
    assert!(result.is_err(), "NOT(INT) should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.contains("BOOL") || err.contains("NOT"),
        "Error should mention BOOL type required: {}",
        err
    );
}

#[test]
fn test_1_6_not_double_rejected() {
    let double_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Double(3.14),
        AttributeType::DOUBLE,
    ));

    let result = NotExpressionExecutor::new(double_exec);
    assert!(result.is_err(), "NOT(DOUBLE) should be rejected");
}

#[test]
fn test_1_6_not_string_rejected() {
    let string_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::String("test".to_string()),
        AttributeType::STRING,
    ));

    let result = NotExpressionExecutor::new(string_exec);
    assert!(result.is_err(), "NOT(STRING) should be rejected");
}

#[test]
fn test_1_6_not_boolean_allowed() {
    let bool_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Bool(true),
        AttributeType::BOOL,
    ));

    let result = NotExpressionExecutor::new(bool_exec);
    assert!(result.is_ok(), "NOT(BOOL) should be allowed");
}

// ============================================================================
// 1.7 Direct Table Query Prevention
// Throws error for `FROM TableName SELECT *` without JOIN
// ============================================================================

#[test]
fn test_1_7_direct_table_query_rejected() {
    // Tables are identified by having 'extension' in WITH clause
    let sql = "\
        CREATE STREAM Events (id INT, data STRING);\n\
        CREATE TABLE Lookup (id INT, name STRING) WITH ('extension' = 'cache');\n\
        INSERT INTO Out\n\
        SELECT id, name\n\
        FROM Lookup;\n";

    let result = parse(sql);
    assert!(
        result.is_err(),
        "Direct table query without JOIN should be rejected"
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("table")
            || err.contains("Table")
            || err.contains("JOIN")
            || err.contains("Direct"),
        "Error should mention table needs JOIN: {}",
        err
    );
}

#[test]
fn test_1_7_table_with_join_allowed() {
    // Tables are identified by having 'extension' in WITH clause
    let sql = "\
        CREATE STREAM Events (id INT, data STRING);\n\
        CREATE TABLE Lookup (id INT, name STRING) WITH ('extension' = 'cache');\n\
        INSERT INTO Out\n\
        SELECT Events.id, Lookup.name\n\
        FROM Events JOIN Lookup ON Events.id = Lookup.id;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Table with JOIN should be allowed: {:?}",
        result.err()
    );
}

#[test]
fn test_1_7_stream_direct_query_allowed() {
    let sql = "\
        CREATE STREAM Events (id INT, data STRING);\n\
        INSERT INTO Out\n\
        SELECT id, data\n\
        FROM Events;\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Direct stream query should be allowed: {:?}",
        result.err()
    );
}

// ============================================================================
// 1.8 Direct Aggregation Query Prevention
// Note: Aggregation support is not yet fully implemented in SQL compiler.
// The error type is defined but validation will be added when aggregations
// are supported. This test documents the expected behavior.
// ============================================================================

#[test]
#[ignore = "Aggregation definitions not yet supported in SQL compiler"]
fn test_1_8_direct_aggregation_query_rejected() {
    // When aggregation definitions are supported, this should be rejected
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE, timestamp LONG);\n\
        CREATE AGGREGATION StockAgg\n\
        SELECT symbol, AVG(price) as avgPrice\n\
        FROM Stock\n\
        GROUP BY symbol\n\
        AGGREGATE BY timestamp EVERY sec...min;\n\
        INSERT INTO Out\n\
        SELECT symbol, avgPrice\n\
        FROM StockAgg;\n";

    let result = parse(sql);
    // Expected: Direct aggregation query should be rejected
    if result.is_err() {
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("aggregation") || err.contains("Aggregation"),
            "Error should mention aggregation: {}",
            err
        );
    }
}

// ============================================================================
// 1.9 Every Keyword with Table Prevention
// Tables cannot be used with EVERY keyword in patterns
// ============================================================================

#[test]
fn test_1_9_every_table_in_pattern_rejected() {
    // Tables are identified by having 'extension' in WITH clause
    let sql = "\
        CREATE STREAM Events (id INT, data STRING);\n\
        CREATE TABLE Lookup (id INT, name STRING) WITH ('extension' = 'cache');\n\
        INSERT INTO Out\n\
        SELECT e.id, l.name\n\
        FROM PATTERN (EVERY e=Events -> l=Lookup);\n";

    let result = parse(sql);
    // Table in pattern should be rejected
    assert!(
        result.is_err(),
        "Table 'Lookup' in PATTERN should be rejected. Got: {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Table") || err.contains("table") || err.contains("pattern"),
        "Error should mention table in pattern: {}",
        err
    );
}

#[test]
fn test_1_9_stream_in_pattern_allowed() {
    // EVERY must wrap the entire pattern expression
    let sql = "\
        CREATE STREAM EventA (id INT, data STRING);\n\
        CREATE STREAM EventB (id INT, result STRING);\n\
        INSERT INTO Out\n\
        SELECT a.id, b.result\n\
        FROM PATTERN (EVERY (a=EventA -> b=EventB));\n";

    let result = parse(sql);
    assert!(
        result.is_ok(),
        "Streams in pattern should be allowed: {:?}",
        result.err()
    );
}

// ============================================================================
// 1.10 Every Keyword with Aggregation Prevention
// Note: Aggregation support not yet implemented in SQL compiler.
// ============================================================================

#[test]
#[ignore = "Aggregation definitions not yet supported in SQL compiler"]
fn test_1_10_every_aggregation_in_pattern_rejected() {
    // When aggregation definitions are supported, this should be rejected
    let sql = "\
        CREATE STREAM Stock (symbol STRING, price DOUBLE, timestamp LONG);\n\
        CREATE AGGREGATION StockAgg ...;\n\
        INSERT INTO Out\n\
        FROM PATTERN (EVERY agg=StockAgg)\n\
        SELECT agg.symbol;\n";

    let result = parse(sql);
    // Expected: Aggregation in pattern should be rejected
    if result.is_err() {
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("aggregation") || err.contains("pattern"),
            "Error should mention aggregation in pattern: {}",
            err
        );
    }
}

// ============================================================================
// Additional edge case tests
// ============================================================================

#[test]
fn test_numeric_type_comparison_allowed() {
    // Different numeric types can be compared (with type promotion)
    let int_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(42),
        AttributeType::INT,
    ));
    let double_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Double(42.0),
        AttributeType::DOUBLE,
    ));

    let result = CompareExpressionExecutor::new(int_exec, double_exec, CompareOperator::Equal);
    assert!(
        result.is_ok(),
        "INT = DOUBLE should be allowed with type promotion"
    );
}

#[test]
fn test_int_long_comparison_allowed() {
    let int_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Int(42),
        AttributeType::INT,
    ));
    let long_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Long(42),
        AttributeType::LONG,
    ));

    let result = CompareExpressionExecutor::new(int_exec, long_exec, CompareOperator::LessThan);
    assert!(result.is_ok(), "INT < LONG should be allowed");
}

#[test]
fn test_float_double_comparison_allowed() {
    let float_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Float(3.14),
        AttributeType::FLOAT,
    ));
    let double_exec = Box::new(ConstantExpressionExecutor::new(
        AttributeValue::Double(3.14),
        AttributeType::DOUBLE,
    ));

    let result =
        CompareExpressionExecutor::new(float_exec, double_exec, CompareOperator::GreaterThan);
    assert!(result.is_ok(), "FLOAT > DOUBLE should be allowed");
}
