// SPDX-License-Identifier: MIT OR Apache-2.0

//! # M5 Error Handling Integration Tests
//!
//! Comprehensive integration tests for the error handling and DLQ system.
//!
//! ## Test Coverage
//!
//! 1. **Retry Strategy Tests**
//!    - Exponential backoff calculation
//!    - Consecutive error tracking
//!    - Max attempts enforcement
//!    - Successful retry after transient error
//!
//! 2. **DLQ Strategy Tests**
//!    - DLQ event schema compliance
//!    - DLQ event delivery
//!    - Original event serialization
//!    - Metadata accuracy
//!
//! 3. **DLQ Fallback Tests**
//!    - Log fallback when DLQ unavailable
//!    - Retry fallback when DLQ unavailable
//!    - Fail fallback when DLQ unavailable
//!
//! 4. **End-to-End Integration Tests**
//!    - Source with retry strategy
//!    - Source with DLQ strategy
//!    - Source with fail strategy
//!    - Error recovery after successful event

use eventflux::core::error::{
    create_dlq_event, BackoffStrategy, DlqConfig, DlqFallbackStrategy, ErrorAction, ErrorConfig,
    ErrorHandler, ErrorStrategy, FailConfig, LogLevel, RetryConfig, SourceErrorContext,
};
use eventflux::core::event::{AttributeValue, Event};
use eventflux::core::exception::EventFluxError;
use std::time::{Duration, Instant};

// ============================================================================
// Test 1: Retry Strategy with Exponential Backoff
// ============================================================================

#[test]
fn test_retry_exponential_backoff_calculation() {
    let retry_config = RetryConfig {
        max_attempts: 5,
        backoff: BackoffStrategy::Exponential,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
    };

    // Test exponential growth: 100ms, 200ms, 400ms, 800ms, 1600ms
    assert_eq!(retry_config.calculate_delay(1), Duration::from_millis(100));
    assert_eq!(retry_config.calculate_delay(2), Duration::from_millis(200));
    assert_eq!(retry_config.calculate_delay(3), Duration::from_millis(400));
    assert_eq!(retry_config.calculate_delay(4), Duration::from_millis(800));
    assert_eq!(retry_config.calculate_delay(5), Duration::from_millis(1600));
}

#[test]
fn test_retry_exponential_backoff_max_delay_cap() {
    let retry_config = RetryConfig {
        max_attempts: 100,
        backoff: BackoffStrategy::Exponential,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(5),
    };

    // After many attempts, should cap at max_delay (5 seconds)
    let delay = retry_config.calculate_delay(50);
    assert!(
        delay <= Duration::from_secs(5),
        "Delay should be capped at max_delay"
    );
    assert!(
        delay >= Duration::from_secs(4),
        "Delay should approach max_delay"
    );
}

#[test]
fn test_retry_handler_consecutive_errors() {
    let retry_config = RetryConfig {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed,
        initial_delay: Duration::from_millis(1), // Short for testing
        max_delay: Duration::from_secs(1),
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Retry,
        LogLevel::Warn,
        Some(retry_config),
        None,
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::ConnectionUnavailable {
        message: "Test error".to_string(),
        source: None,
    };

    // First 2 errors should retry
    for i in 1..=2 {
        let action = handler.handle_error(Some(&event), &error);
        assert!(
            matches!(action, ErrorAction::Retry { .. }),
            "Attempt {} should retry",
            i
        );
        assert_eq!(
            handler.consecutive_error_count(),
            i,
            "Should track error count"
        );
    }

    // Third error should drop (max attempts reached - retry strategy drops after exhausting retries)
    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Drop),
        "Should drop after max attempts in retry strategy"
    );
}

#[test]
fn test_retry_handler_reset_on_success() {
    let retry_config = RetryConfig {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_secs(1),
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Retry,
        LogLevel::Warn,
        Some(retry_config),
        None,
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::ConnectionUnavailable {
        message: "Test error".to_string(),
        source: None,
    };

    // Trigger error
    handler.handle_error(Some(&event), &error);
    assert_eq!(handler.consecutive_error_count(), 1);

    // Reset on success
    handler.reset_consecutive_errors();
    assert_eq!(handler.consecutive_error_count(), 0);

    // Next error should be treated as first attempt
    let action = handler.handle_error(Some(&event), &error);
    assert!(matches!(action, ErrorAction::Retry { .. }));
    assert_eq!(handler.consecutive_error_count(), 1);
}

#[test]
fn test_retry_timing_validation() {
    let retry_config = RetryConfig {
        max_attempts: 3,
        backoff: BackoffStrategy::Exponential,
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_secs(1),
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Retry,
        LogLevel::Warn,
        Some(retry_config),
        None,
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::ConnectionUnavailable {
        message: "Test error".to_string(),
        source: None,
    };

    // First retry: 50ms delay
    let start = Instant::now();
    match handler.handle_error(Some(&event), &error) {
        ErrorAction::Retry { delay } => {
            assert_eq!(delay, Duration::from_millis(50));
            std::thread::sleep(delay);
        }
        _ => panic!("Expected retry action"),
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(45),
        "Should wait at least 45ms"
    );

    // Second retry: 100ms delay
    match handler.handle_error(Some(&event), &error) {
        ErrorAction::Retry { delay } => {
            assert_eq!(delay, Duration::from_millis(100));
        }
        _ => panic!("Expected retry action"),
    }
}

// ============================================================================
// Test 2: DLQ Strategy - Event Delivery and Schema
// ============================================================================

#[test]
fn test_dlq_event_schema_compliance() {
    let original_event = Event::new_with_data(
        12345,
        vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(42),
            AttributeValue::Long(999),
        ],
    );

    let error = EventFluxError::MappingFailed {
        message: "Test mapping error".to_string(),
        source: None,
    };

    let dlq_event = create_dlq_event(&original_event, &error, 3, "InputStream");

    // Verify schema: 6 attributes in specific order
    let data = dlq_event.get_data();
    assert_eq!(data.len(), 6, "DLQ event should have exactly 6 attributes");

    // Verify attribute types and order
    match &data[0] {
        AttributeValue::String(s) => {
            assert!(s.contains("test"), "Should serialize original event");
            assert!(s.contains("42"), "Should include all original data");
        }
        _ => panic!("First attribute (originalEvent) should be String"),
    }

    match &data[1] {
        AttributeValue::String(s) => {
            assert!(
                s.contains("mapping"),
                "Error message should describe the error"
            );
        }
        _ => panic!("Second attribute (errorMessage) should be String"),
    }

    match &data[2] {
        AttributeValue::String(s) => {
            assert_eq!(s, "MappingFailed", "Should include error type");
        }
        _ => panic!("Third attribute (errorType) should be String"),
    }

    match &data[3] {
        AttributeValue::Long(ts) => {
            assert!(ts > &0, "Timestamp should be positive");
        }
        _ => panic!("Fourth attribute (timestamp) should be BIGINT (Long)"),
    }

    match &data[4] {
        AttributeValue::Int(count) => {
            assert_eq!(count, &3, "Attempt count should match input");
        }
        _ => panic!("Fifth attribute (attemptCount) should be INT"),
    }

    match &data[5] {
        AttributeValue::String(s) => {
            assert_eq!(s, "InputStream", "Stream name should match input");
        }
        _ => panic!("Sixth attribute (streamName) should be String"),
    }
}

// Note: Full DLQ event delivery tests are in timer_source.rs tests
// These tests focus on DLQ event creation and schema compliance

#[test]
fn test_dlq_event_delivery_action() {
    // Test that DLQ strategy returns correct action when DLQ is unavailable
    // (actual delivery tested in source integration tests)
    let dlq_config = DlqConfig {
        stream: "ErrorStream".to_string(),
        fallback_strategy: DlqFallbackStrategy::Log,
        fallback_retry: None,
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Dlq,
        LogLevel::Error,
        None,
        Some(dlq_config),
        None,
    )
    .unwrap();

    // No DLQ junction provided - should use fallback
    let mut handler = ErrorHandler::new(error_config, None, "InputStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::String("data".to_string())]);
    let error = EventFluxError::MappingFailed {
        message: "Mapping failed".to_string(),
        source: None,
    };

    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Drop),
        "Should drop (log fallback) when DLQ unavailable"
    );
}

#[test]
fn test_dlq_preserves_original_event_data() {
    // Create event with complex data
    let original_event = Event::new_with_data(
        9999,
        vec![
            AttributeValue::String("complex_value".to_string()),
            AttributeValue::Int(-42),
            AttributeValue::Long(1234567890),
            AttributeValue::Float(3.14),
            AttributeValue::Double(2.718281828),
            AttributeValue::Bool(true),
        ],
    );

    let error = EventFluxError::Other("Test error".to_string());

    // Create DLQ event
    let dlq_event = create_dlq_event(&original_event, &error, 1, "InputStream");

    // Extract serialized original event
    if let AttributeValue::String(serialized) = &dlq_event.get_data()[0] {
        assert!(
            serialized.contains("complex_value"),
            "Should preserve string data"
        );
        assert!(serialized.contains("-42"), "Should preserve int data");
        assert!(
            serialized.contains("1234567890"),
            "Should preserve long data"
        );
        assert!(serialized.contains("3.14"), "Should preserve float data");
        assert!(serialized.contains("true"), "Should preserve bool data");
    } else {
        panic!("First field should be serialized original event");
    }
}

// ============================================================================
// Test 3: DLQ Fallback Strategies
// ============================================================================

#[test]
fn test_dlq_fallback_log_when_dlq_unavailable() {
    // No DLQ junction provided (simulating unavailable DLQ stream)
    let dlq_config = DlqConfig {
        stream: "ErrorStream".to_string(),
        fallback_strategy: DlqFallbackStrategy::Log,
        fallback_retry: None,
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Dlq,
        LogLevel::Error,
        None,
        Some(dlq_config),
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "InputStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::Other("Test error".to_string());

    // Should fall back to log (drop) strategy
    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Drop),
        "Should drop when DLQ unavailable with Log fallback"
    );
}

#[test]
fn test_dlq_fallback_fail_when_dlq_unavailable() {
    let dlq_config = DlqConfig {
        stream: "ErrorStream".to_string(),
        fallback_strategy: DlqFallbackStrategy::Fail,
        fallback_retry: None,
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Dlq,
        LogLevel::Error,
        None,
        Some(dlq_config),
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "InputStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::Other("Test error".to_string());

    // Should fall back to fail strategy
    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Fail),
        "Should fail when DLQ unavailable with Fail fallback"
    );
}

#[test]
fn test_dlq_fallback_retry_when_dlq_unavailable() {
    let fallback_retry = RetryConfig {
        max_attempts: 2,
        backoff: BackoffStrategy::Fixed,
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_secs(1),
    };

    let dlq_config = DlqConfig {
        stream: "ErrorStream".to_string(),
        fallback_strategy: DlqFallbackStrategy::Retry,
        fallback_retry: Some(fallback_retry),
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Dlq,
        LogLevel::Error,
        None,
        Some(dlq_config),
        None,
    )
    .unwrap();

    let mut handler = ErrorHandler::new(error_config, None, "InputStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::Other("Test error".to_string());

    // First attempt should retry DLQ delivery
    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Retry { .. }),
        "Should retry DLQ delivery (attempt 1)"
    );

    // Second attempt should drop (max fallback retries of 2 exceeded)
    let action = handler.handle_error(Some(&event), &error);
    assert!(
        matches!(action, ErrorAction::Drop),
        "Should drop after max fallback retry attempts"
    );
}

// ============================================================================
// Test 4: End-to-End Integration with SourceErrorContext
// ============================================================================

#[test]
fn test_source_error_context_drop_strategy() {
    let error_config = ErrorConfig::default(); // Default is Drop
    let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::Other("Test error".to_string());

    // Should continue processing (drop)
    assert!(
        ctx.handle_error(Some(&event), &error),
        "Should continue with drop strategy"
    );
    assert_eq!(ctx.error_count(), 0, "Drop strategy doesn't track errors");
}

#[test]
fn test_source_error_context_fail_strategy() {
    let error_config = ErrorConfig::new(
        ErrorStrategy::Fail,
        LogLevel::Error,
        None,
        None,
        Some(FailConfig::default()),
    )
    .unwrap();

    let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::Other("Fatal error".to_string());

    // Should stop processing (fail)
    assert!(
        !ctx.handle_error(Some(&event), &error),
        "Should stop with fail strategy"
    );
}

#[test]
fn test_source_error_context_retry_and_reset() {
    let retry_config = RetryConfig {
        max_attempts: 3,
        backoff: BackoffStrategy::Fixed,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_secs(1),
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Retry,
        LogLevel::Warn,
        Some(retry_config),
        None,
        None,
    )
    .unwrap();

    let mut ctx = SourceErrorContext::new(error_config, None, "TestStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::Int(42)]);
    let error = EventFluxError::ConnectionUnavailable {
        message: "Transient error".to_string(),
        source: None,
    };

    // Trigger errors
    assert!(ctx.handle_error(Some(&event), &error));
    assert_eq!(ctx.error_count(), 1);

    assert!(ctx.handle_error(Some(&event), &error));
    assert_eq!(ctx.error_count(), 2);

    // Reset on success
    ctx.reset_errors();
    assert_eq!(ctx.error_count(), 0);

    // Next error is treated as first attempt
    assert!(ctx.handle_error(Some(&event), &error));
    assert_eq!(ctx.error_count(), 1);
}

// Note: Full DLQ delivery with SourceErrorContext tested in timer_source.rs

#[test]
fn test_source_error_context_with_dlq_fallback() {
    let dlq_config = DlqConfig {
        stream: "ErrorStream".to_string(),
        fallback_strategy: DlqFallbackStrategy::Log,
        fallback_retry: None,
    };

    let error_config = ErrorConfig::new(
        ErrorStrategy::Dlq,
        LogLevel::Error,
        None,
        Some(dlq_config),
        None,
    )
    .unwrap();

    // No DLQ junction - should use fallback
    let mut ctx = SourceErrorContext::new(error_config, None, "InputStream".to_string());

    let event = Event::new_with_data(123, vec![AttributeValue::String("data".to_string())]);
    let error = EventFluxError::MappingFailed {
        message: "Mapping error".to_string(),
        source: None,
    };

    // Should continue processing (fallback handles error)
    assert!(
        ctx.handle_error(Some(&event), &error),
        "Should continue with fallback strategy"
    );
}

// ============================================================================
// Test 5: Error Retriability Classification
// ============================================================================

#[test]
fn test_error_retriability_classification() {
    // Retriable errors (transient)
    assert!(EventFluxError::ConnectionUnavailable {
        message: "".to_string(),
        source: None
    }
    .is_retriable());

    assert!(EventFluxError::DatabaseRuntime {
        message: "".to_string(),
        source: None
    }
    .is_retriable());

    assert!(EventFluxError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error {
            code: rusqlite::ErrorCode::DatabaseBusy,
            extended_code: 5
        },
        None
    ))
    .is_retriable());

    // Non-retriable errors (permanent/configuration)
    assert!(!EventFluxError::Configuration {
        message: "".to_string(),
        config_key: None,
    }
    .is_retriable());

    assert!(!EventFluxError::MappingFailed {
        message: "".to_string(),
        source: None
    }
    .is_retriable());

    assert!(!EventFluxError::TypeError {
        message: "".to_string(),
        expected: Some("".to_string()),
        actual: Some("".to_string()),
    }
    .is_retriable());

    assert!(!EventFluxError::InvalidParameter {
        message: "".to_string(),
        parameter: None,
        expected: None
    }
    .is_retriable());
}
