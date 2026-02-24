// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pattern SQL Integration Tests
//!
//! Tests the complete pipeline: SQL pattern parsing → conversion → Query API
//! These tests verify that patterns parsed from SQL correctly produce
//! StateInputStream structures that can be used by the pattern runtime.

use eventflux::query_api::definition::attribute::Type as AttrType;
use eventflux::query_api::definition::StreamDefinition;
use eventflux::query_api::execution::query::input::state::StateElement;
use eventflux::query_api::execution::query::input::stream::input_stream::InputStream;
use eventflux::query_api::execution::query::input::stream::state_input_stream::Type as StateType;
use eventflux::sql_compiler::{SqlCatalog, SqlConverter};
use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, PatternExpression, PatternMode};

fn setup_catalog() -> SqlCatalog {
    let mut catalog = SqlCatalog::new();

    let stream_a = StreamDefinition::new("AStream".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("volume".to_string(), AttrType::INT);
    catalog
        .register_stream("AStream".to_string(), stream_a)
        .unwrap();

    let stream_b = StreamDefinition::new("BStream".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("volume".to_string(), AttrType::INT);
    catalog
        .register_stream("BStream".to_string(), stream_b)
        .unwrap();

    let stream_c = StreamDefinition::new("CStream".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("price".to_string(), AttrType::DOUBLE);
    catalog
        .register_stream("CStream".to_string(), stream_c)
        .unwrap();

    catalog
}

// Helper to create stream pattern
fn stream(alias: &str, name: &str) -> PatternExpression {
    PatternExpression::Stream {
        alias: Some(Ident::new(alias)),
        stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        filter: None,
    }
}

// Helper to create sequence pattern
fn sequence(first: PatternExpression, second: PatternExpression) -> PatternExpression {
    PatternExpression::Sequence {
        first: Box::new(first),
        second: Box::new(second),
    }
}

// Helper to create count pattern
fn count(pattern: PatternExpression, min: u32, max: u32) -> PatternExpression {
    PatternExpression::Count {
        pattern: Box::new(pattern),
        min_count: min,
        max_count: max,
    }
}

// Helper to create every pattern
fn every(pattern: PatternExpression) -> PatternExpression {
    PatternExpression::Every {
        pattern: Box::new(pattern),
    }
}

// ============================================================================
// PHASE 5.1: Basic Sequence Tests
// ============================================================================

#[test]
fn test_e2e_basic_sequence_a_then_b() {
    let catalog = setup_catalog();

    // Pattern: e1=AStream -> e2=BStream
    let pattern = sequence(stream("e1", "AStream"), stream("e2", "BStream"));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Sequence, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert basic sequence");
    let input_stream = result.unwrap();

    // Verify it's a State input stream
    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Sequence);

        // Verify the state element is a Next (sequence)
        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                // First should be a Stream element
                assert!(
                    matches!(next.state_element.as_ref(), StateElement::Stream(_)),
                    "Expected Stream for first element"
                );
                // Second should be a Stream element
                assert!(
                    matches!(next.next_state_element.as_ref(), StateElement::Stream(_)),
                    "Expected Stream for second element"
                );
            }
            _ => panic!("Expected Next state element for sequence"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_three_way_sequence() {
    let catalog = setup_catalog();

    // Pattern: e1=AStream -> e2=BStream -> e3=CStream
    let pattern = sequence(
        stream("e1", "AStream"),
        sequence(stream("e2", "BStream"), stream("e3", "CStream")),
    );

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Sequence, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert three-way sequence");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Sequence);

        // Verify nested structure: Next(A, Next(B, C))
        match state.state_element.as_ref() {
            StateElement::Next(outer) => {
                // First is A
                assert!(matches!(
                    outer.state_element.as_ref(),
                    StateElement::Stream(_)
                ));

                // Second is Next(B, C)
                match outer.next_state_element.as_ref() {
                    StateElement::Next(inner) => {
                        assert!(matches!(
                            inner.state_element.as_ref(),
                            StateElement::Stream(_)
                        ));
                        assert!(matches!(
                            inner.next_state_element.as_ref(),
                            StateElement::Stream(_)
                        ));
                    }
                    _ => panic!("Expected nested Next"),
                }
            }
            _ => panic!("Expected Next state element"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

// ============================================================================
// PHASE 5.2: Count Quantifier Tests
// ============================================================================

#[test]
fn test_e2e_count_exact() {
    let catalog = setup_catalog();

    // Pattern: e1=AStream{3} -> e2=BStream
    let pattern = sequence(
        count(stream("e1", "AStream"), 3, 3),
        stream("e2", "BStream"),
    );

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert count quantifier");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                // First should be Count
                match next.state_element.as_ref() {
                    StateElement::Count(c) => {
                        assert_eq!(c.min_count, 3);
                        assert_eq!(c.max_count, 3);
                    }
                    _ => panic!("Expected Count for first element"),
                }
            }
            _ => panic!("Expected Next state element"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_count_range() {
    let catalog = setup_catalog();

    // Pattern: e1=AStream{2,5} -> e2=BStream
    let pattern = sequence(
        count(stream("e1", "AStream"), 2, 5),
        stream("e2", "BStream"),
    );

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert count range");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        match state.state_element.as_ref() {
            StateElement::Next(next) => match next.state_element.as_ref() {
                StateElement::Count(c) => {
                    assert_eq!(c.min_count, 2);
                    assert_eq!(c.max_count, 5);
                }
                _ => panic!("Expected Count"),
            },
            _ => panic!("Expected Next"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

// ============================================================================
// PHASE 5.3: EVERY Pattern Tests
// ============================================================================

#[test]
fn test_e2e_every_sequence() {
    let catalog = setup_catalog();

    // Pattern: EVERY(e1=AStream -> e2=BStream)
    let pattern = every(sequence(stream("e1", "AStream"), stream("e2", "BStream")));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert EVERY pattern");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        match state.state_element.as_ref() {
            StateElement::Every(every) => {
                // Inner should be a sequence
                assert!(matches!(
                    every.state_element.as_ref(),
                    StateElement::Next(_)
                ));
            }
            _ => panic!("Expected Every state element"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_every_with_count() {
    let catalog = setup_catalog();

    // Pattern: EVERY(e1=AStream{2,3} -> e2=BStream)
    let pattern = every(sequence(
        count(stream("e1", "AStream"), 2, 3),
        stream("e2", "BStream"),
    ));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert EVERY with count");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        match state.state_element.as_ref() {
            StateElement::Every(every) => match every.state_element.as_ref() {
                StateElement::Next(next) => {
                    assert!(matches!(
                        next.state_element.as_ref(),
                        StateElement::Count(_)
                    ));
                }
                _ => panic!("Expected Next inside Every"),
            },
            _ => panic!("Expected Every"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

// ============================================================================
// PHASE 5.4: Logical Pattern Tests
// ============================================================================

#[test]
fn test_e2e_logical_and() {
    use sqlparser::ast::PatternLogicalOp;

    let catalog = setup_catalog();

    // Pattern: (e1=AStream AND e2=BStream) -> e3=CStream
    let and_pattern = PatternExpression::Logical {
        left: Box::new(stream("e1", "AStream")),
        op: PatternLogicalOp::And,
        right: Box::new(stream("e2", "BStream")),
    };
    let pattern = sequence(and_pattern, stream("e3", "CStream"));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert logical AND pattern");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                // First should be Logical
                assert!(matches!(
                    next.state_element.as_ref(),
                    StateElement::Logical(_)
                ));
                // Second should be Stream
                assert!(matches!(
                    next.next_state_element.as_ref(),
                    StateElement::Stream(_)
                ));
            }
            _ => panic!("Expected Next"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_logical_or() {
    use sqlparser::ast::PatternLogicalOp;

    let catalog = setup_catalog();

    // Pattern: (e1=AStream OR e2=BStream) -> e3=CStream
    let or_pattern = PatternExpression::Logical {
        left: Box::new(stream("e1", "AStream")),
        op: PatternLogicalOp::Or,
        right: Box::new(stream("e2", "BStream")),
    };
    let pattern = sequence(or_pattern, stream("e3", "CStream"));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert logical OR pattern");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                assert!(matches!(
                    next.state_element.as_ref(),
                    StateElement::Logical(_)
                ));
            }
            _ => panic!("Expected Next"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

// ============================================================================
// PHASE 5.5: WITHIN Constraint Tests
// ============================================================================

#[test]
fn test_e2e_within_events_unsupported() {
    use sqlparser::ast::WithinConstraint;

    let catalog = setup_catalog();

    // Pattern: e1=AStream -> e2=BStream WITHIN 100 EVENTS
    // Note: WITHIN EVENTS is not yet supported - should return error
    let pattern = sequence(stream("e1", "AStream"), stream("e2", "BStream"));
    let within = WithinConstraint::EventCount(100);

    let result = SqlConverter::convert_pattern_input(
        &PatternMode::Pattern,
        &pattern,
        &Some(within),
        &catalog,
    );

    // WITHIN EVENTS is not yet supported - should return error
    assert!(result.is_err(), "WITHIN EVENTS should be unsupported");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("WITHIN") && err.contains("EVENTS"),
        "Error should mention WITHIN EVENTS: {}",
        err
    );
}

// ============================================================================
// PHASE 5.6: Validation Error Tests
// ============================================================================

#[test]
fn test_e2e_rejects_every_in_sequence_mode() {
    let catalog = setup_catalog();

    // EVERY in SEQUENCE mode should fail validation
    let pattern = every(sequence(stream("e1", "AStream"), stream("e2", "BStream")));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Sequence, &pattern, &None, &catalog);

    assert!(result.is_err(), "EVERY in SEQUENCE mode should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("EVERY not allowed in SEQUENCE mode"));
}

#[test]
fn test_e2e_rejects_zero_count() {
    let catalog = setup_catalog();

    // A{0,5} should fail validation
    let pattern = count(stream("e1", "AStream"), 0, 5);

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_err(), "Zero count should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("min_count must be >= 1"));
}

#[test]
fn test_e2e_rejects_nested_every() {
    let catalog = setup_catalog();

    // (EVERY A) -> B should fail validation
    let pattern = sequence(every(stream("e1", "AStream")), stream("e2", "BStream"));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_err(), "Nested EVERY should be rejected");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("EVERY must be at top level"));
}

// ============================================================================
// PHASE 5.7: Complex Pattern Tests
// ============================================================================

#[test]
fn test_e2e_complex_pattern() {
    use sqlparser::ast::PatternLogicalOp;

    let catalog = setup_catalog();

    // Complex pattern: EVERY((e1=A AND e2=B) -> e3=C{2,3})
    // Note: Logical operators require Stream operands (not Count)
    let and_pattern = PatternExpression::Logical {
        left: Box::new(stream("e1", "AStream")),
        op: PatternLogicalOp::And,
        right: Box::new(stream("e2", "BStream")),
    };
    let pattern = every(sequence(and_pattern, count(stream("e3", "CStream"), 2, 3)));

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(result.is_ok(), "Should convert complex pattern");
    let input_stream = result.unwrap();

    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        // Verify structure: Every(Next(Logical(Stream, Stream), Count))
        match state.state_element.as_ref() {
            StateElement::Every(every) => {
                match every.state_element.as_ref() {
                    StateElement::Next(next) => {
                        // First is Logical
                        assert!(
                            matches!(next.state_element.as_ref(), StateElement::Logical(_)),
                            "Expected Logical element"
                        );
                        // Second is Count
                        assert!(
                            matches!(next.next_state_element.as_ref(), StateElement::Count(_)),
                            "Expected Count element"
                        );
                    }
                    _ => panic!("Expected Next inside Every"),
                }
            }
            _ => panic!("Expected Every"),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_pattern_vs_sequence_mode() {
    let catalog = setup_catalog();

    let pattern = sequence(stream("e1", "AStream"), stream("e2", "BStream"));

    // Test PATTERN mode
    let pattern_result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);
    assert!(pattern_result.is_ok());
    if let InputStream::State(state) = pattern_result.unwrap() {
        assert_eq!(state.state_type, StateType::Pattern);
    }

    // Test SEQUENCE mode
    let sequence_result =
        SqlConverter::convert_pattern_input(&PatternMode::Sequence, &pattern, &None, &catalog);
    assert!(sequence_result.is_ok());
    if let InputStream::State(state) = sequence_result.unwrap() {
        assert_eq!(state.state_type, StateType::Sequence);
    }
}

// ============================================================================
// PHASE 5.8: Array Access Expression Tests
// ============================================================================

#[test]
fn test_e2e_array_access_numeric_index() {
    use eventflux::query_api::expression::expression::Expression;
    use eventflux::query_api::expression::indexed_variable::EventIndex;
    use sqlparser::ast::{AccessExpr, Expr as SqlExpr, Subscript};

    let catalog = setup_catalog();

    // Construct e1[0].price manually
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::value(sqlparser::ast::Value::Number("0".to_string(), false)),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("price"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_ok(),
        "Should convert array access e1[0].price: {:?}",
        result
    );

    let converted = result.unwrap();
    match converted {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e1".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "price");
            assert_eq!(indexed_var.get_index(), &EventIndex::Numeric(0));
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }
}

#[test]
fn test_e2e_array_access_last_index() {
    use eventflux::query_api::expression::expression::Expression;
    use eventflux::query_api::expression::indexed_variable::EventIndex;
    use sqlparser::ast::{AccessExpr, Expr as SqlExpr, Subscript};

    let catalog = setup_catalog();

    // Construct e2[last].symbol manually
    let root = SqlExpr::Identifier(Ident::new("e2"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::Identifier(Ident::new("last")),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("symbol"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_ok(),
        "Should convert array access e2[last].symbol: {:?}",
        result
    );

    let converted = result.unwrap();
    match converted {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e2".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "symbol");
            assert_eq!(indexed_var.get_index(), &EventIndex::Last);
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }
}

#[test]
fn test_e2e_array_access_invalid_format() {
    use sqlparser::ast::{AccessExpr, Expr as SqlExpr, Subscript};

    let catalog = setup_catalog();

    // Missing attribute after subscript: e1[0] (without .price)
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::value(sqlparser::ast::Value::Number("0".to_string(), false)),
        }),
        // Missing AccessExpr::Dot for attribute
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_err(),
        "Should reject array access without attribute"
    );
    let err = result.unwrap_err().to_string();
    assert!(err.contains("[index].attribute format"));
}

// ============================================================================
// PHASE 5.9: Real-World Pattern Scenarios
// ============================================================================

/// Setup catalog with security-related streams for brute force detection
fn setup_security_catalog() -> SqlCatalog {
    let mut catalog = SqlCatalog::new();

    let failed_login = StreamDefinition::new("FailedLoginStream".to_string())
        .attribute("userId".to_string(), AttrType::STRING)
        .attribute("ipAddress".to_string(), AttrType::STRING)
        .attribute("timestamp".to_string(), AttrType::LONG);
    catalog
        .register_stream("FailedLoginStream".to_string(), failed_login)
        .unwrap();

    let success_login = StreamDefinition::new("SuccessLoginStream".to_string())
        .attribute("userId".to_string(), AttrType::STRING)
        .attribute("ipAddress".to_string(), AttrType::STRING)
        .attribute("timestamp".to_string(), AttrType::LONG);
    catalog
        .register_stream("SuccessLoginStream".to_string(), success_login)
        .unwrap();

    let alerts = StreamDefinition::new("BruteForceAlerts".to_string())
        .attribute("userId".to_string(), AttrType::STRING)
        .attribute("firstAttemptTime".to_string(), AttrType::LONG)
        .attribute("lastAttemptTime".to_string(), AttrType::LONG)
        .attribute("successTime".to_string(), AttrType::LONG)
        .attribute("attemptCount".to_string(), AttrType::LONG)
        .attribute("ipAddress".to_string(), AttrType::STRING);
    catalog
        .register_stream("BruteForceAlerts".to_string(), alerts)
        .unwrap();

    catalog
}

/// Setup catalog with trading-related streams
fn setup_trading_catalog() -> SqlCatalog {
    let mut catalog = SqlCatalog::new();

    let stock = StreamDefinition::new("StockStream".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("price".to_string(), AttrType::DOUBLE)
        .attribute("volume".to_string(), AttrType::LONG);
    catalog
        .register_stream("StockStream".to_string(), stock)
        .unwrap();

    let alert = StreamDefinition::new("AlertStream".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("triggered".to_string(), AttrType::BOOL);
    catalog
        .register_stream("AlertStream".to_string(), alert)
        .unwrap();

    let signals = StreamDefinition::new("TradingSignals".to_string())
        .attribute("symbol".to_string(), AttrType::STRING)
        .attribute("entryPrice".to_string(), AttrType::DOUBLE)
        .attribute("exitPrice".to_string(), AttrType::DOUBLE)
        .attribute("priceChange".to_string(), AttrType::DOUBLE)
        .attribute("eventCount".to_string(), AttrType::LONG);
    catalog
        .register_stream("TradingSignals".to_string(), signals)
        .unwrap();

    catalog
}

#[test]
fn test_e2e_brute_force_login_detection_pattern() {
    // Pattern: e1=FailedLoginStream{3,5} -> e2=SuccessLoginStream
    // This tests: count quantifiers + sequence + multi-stream pattern

    let catalog = setup_security_catalog();

    // Build the pattern: FailedLogin{3,5} -> SuccessLogin
    let failed_logins = count(
        stream("e1", "FailedLoginStream"),
        3, // min 3 failed attempts
        5, // max 5 failed attempts
    );
    let success_login = stream("e2", "SuccessLoginStream");
    let pattern = sequence(failed_logins, success_login);

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(
        result.is_ok(),
        "Should convert brute force detection pattern: {:?}",
        result
    );

    let input_stream = result.unwrap();
    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        // Verify structure: Next(Count(Stream), Stream)
        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                // First should be Count(FailedLoginStream)
                match next.state_element.as_ref() {
                    StateElement::Count(count_elem) => {
                        assert_eq!(count_elem.min_count, 3);
                        assert_eq!(count_elem.max_count, 5);
                        // Count element exists with correct bounds - stream is embedded in it
                    }
                    other => panic!("Expected Count state element, got {:?}", other),
                }

                // Second should be Stream(SuccessLoginStream)
                assert!(
                    matches!(next.next_state_element.as_ref(), StateElement::Stream(_)),
                    "Expected Stream state element for SuccessLoginStream"
                );
            }
            other => panic!("Expected Next state element, got {:?}", other),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_brute_force_with_within_time() {
    use sqlparser::ast::{DateTimeField, Expr as SqlExpr, Interval, Value, WithinConstraint};

    let catalog = setup_security_catalog();

    // Pattern: FailedLogin{3,5} -> SuccessLogin WITHIN 5 minutes
    let failed_logins = count(stream("e1", "FailedLoginStream"), 3, 5);
    let success_login = stream("e2", "SuccessLoginStream");
    let pattern = sequence(failed_logins, success_login);

    // Create WITHIN 5 MINUTE constraint
    let within = WithinConstraint::Time(Box::new(SqlExpr::Interval(Interval {
        value: Box::new(SqlExpr::Value(
            Value::SingleQuotedString("5".to_string()).into(),
        )),
        leading_field: Some(DateTimeField::Minute),
        leading_precision: None,
        last_field: None,
        fractional_seconds_precision: None,
    })));

    let result = SqlConverter::convert_pattern_input(
        &PatternMode::Pattern,
        &pattern,
        &Some(within),
        &catalog,
    );

    assert!(
        result.is_ok(),
        "Should convert brute force pattern with WITHIN: {:?}",
        result
    );

    let input_stream = result.unwrap();
    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);
        assert!(
            state.within_time.is_some(),
            "WITHIN time constraint should be set"
        );
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_trading_signal_pattern() {
    // Pattern: e1=StockStream{2,4} -> e2=AlertStream
    // This tests: count quantifiers on stock events followed by alert

    let catalog = setup_trading_catalog();

    // Build the pattern: StockStream{2,4} -> AlertStream
    let stock_events = count(
        stream("e1", "StockStream"),
        2, // min 2 stock events
        4, // max 4 stock events
    );
    let alert_event = stream("e2", "AlertStream");
    let pattern = sequence(stock_events, alert_event);

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(
        result.is_ok(),
        "Should convert trading signal pattern: {:?}",
        result
    );

    let input_stream = result.unwrap();
    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        // Verify structure: Next(Count(Stream), Stream)
        match state.state_element.as_ref() {
            StateElement::Next(next) => {
                // First should be Count(StockStream)
                match next.state_element.as_ref() {
                    StateElement::Count(count_elem) => {
                        assert_eq!(count_elem.min_count, 2);
                        assert_eq!(count_elem.max_count, 4);
                        // Count element exists with correct bounds - stream is embedded in it
                    }
                    other => panic!("Expected Count state element, got {:?}", other),
                }

                // Second should be Stream(AlertStream)
                assert!(
                    matches!(next.next_state_element.as_ref(), StateElement::Stream(_)),
                    "Expected Stream state element for AlertStream"
                );
            }
            other => panic!("Expected Next state element, got {:?}", other),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_trading_signal_with_every() {
    // Pattern: EVERY(StockStream{2,4} -> AlertStream)
    // This tests: EVERY wrapper around the trading pattern

    let catalog = setup_trading_catalog();

    // Build the pattern: EVERY(StockStream{2,4} -> AlertStream)
    let stock_events = count(stream("e1", "StockStream"), 2, 4);
    let alert_event = stream("e2", "AlertStream");
    let inner_pattern = sequence(stock_events, alert_event);
    let pattern = every(inner_pattern);

    let result =
        SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

    assert!(
        result.is_ok(),
        "Should convert EVERY trading signal pattern: {:?}",
        result
    );

    let input_stream = result.unwrap();
    if let InputStream::State(state) = input_stream {
        assert_eq!(state.state_type, StateType::Pattern);

        // Verify structure: Every(Next(Count, Stream))
        match state.state_element.as_ref() {
            StateElement::Every(every_elem) => match every_elem.state_element.as_ref() {
                StateElement::Next(next) => {
                    assert!(matches!(
                        next.state_element.as_ref(),
                        StateElement::Count(_)
                    ));
                    assert!(matches!(
                        next.next_state_element.as_ref(),
                        StateElement::Stream(_)
                    ));
                }
                other => panic!("Expected Next inside Every, got {:?}", other),
            },
            other => panic!("Expected Every state element, got {:?}", other),
        }
    } else {
        panic!("Expected State input stream");
    }
}

#[test]
fn test_e2e_indexed_access_in_brute_force_pattern() {
    use eventflux::query_api::expression::expression::Expression;
    use eventflux::query_api::expression::indexed_variable::EventIndex;
    use sqlparser::ast::{AccessExpr, Expr as SqlExpr, Subscript};

    let catalog = setup_security_catalog();

    // Test e1[0].timestamp - first failed login timestamp
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::value(sqlparser::ast::Value::Number("0".to_string(), false)),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("timestamp"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_ok(),
        "Should convert e1[0].timestamp: {:?}",
        result
    );

    match result.unwrap() {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e1".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "timestamp");
            assert_eq!(indexed_var.get_index(), &EventIndex::Numeric(0));
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }

    // Test e1[last].timestamp - last failed login timestamp
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::Identifier(Ident::new("last")),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("timestamp"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_ok(),
        "Should convert e1[last].timestamp: {:?}",
        result
    );

    match result.unwrap() {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e1".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "timestamp");
            assert_eq!(indexed_var.get_index(), &EventIndex::Last);
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }
}

#[test]
fn test_e2e_indexed_access_in_trading_pattern() {
    use eventflux::query_api::expression::expression::Expression;
    use eventflux::query_api::expression::indexed_variable::EventIndex;
    use sqlparser::ast::{AccessExpr, Expr as SqlExpr, Subscript};

    let catalog = setup_trading_catalog();

    // Test e1[0].price - entry price
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::value(sqlparser::ast::Value::Number("0".to_string(), false)),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("price"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(result.is_ok(), "Should convert e1[0].price: {:?}", result);

    match result.unwrap() {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e1".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "price");
            assert_eq!(indexed_var.get_index(), &EventIndex::Numeric(0));
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }

    // Test e1[last].price - exit price
    let root = SqlExpr::Identifier(Ident::new("e1"));
    let access_chain = vec![
        AccessExpr::Subscript(Subscript::Index {
            index: SqlExpr::Identifier(Ident::new("last")),
        }),
        AccessExpr::Dot(SqlExpr::Identifier(Ident::new("price"))),
    ];

    let expr = SqlExpr::CompoundFieldAccess {
        root: Box::new(root),
        access_chain,
    };

    let result = SqlConverter::convert_expression(&expr, &catalog);
    assert!(
        result.is_ok(),
        "Should convert e1[last].price: {:?}",
        result
    );

    match result.unwrap() {
        Expression::IndexedVariable(indexed_var) => {
            assert_eq!(indexed_var.get_stream_id(), Some(&"e1".to_string()));
            assert_eq!(indexed_var.get_attribute_name(), "price");
            assert_eq!(indexed_var.get_index(), &EventIndex::Last);
        }
        other => panic!("Expected IndexedVariable, got {:?}", other),
    }
}
