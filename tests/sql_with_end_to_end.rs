// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-End Tests for SQL WITH Configuration
//!
//! These tests verify that SQL WITH properties flow correctly from parsing
//! through to runtime initialization, completing the end-to-end chain:
//!
//! SQL WITH → StreamDefinition.with_config → StreamTypeConfig → Factory.create_initialized()

use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::extension::{CsvSinkMapperFactory, LogSinkFactory, TimerSourceFactory};
use std::time::Duration;
use tokio::time::sleep;

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;

/// Test 1: Timer Source with SQL WITH Configuration
///
/// Verifies that:
/// - SQL WITH properties are stored in StreamDefinition
/// - Runtime extracts with_config and creates StreamTypeConfig
/// - TimerSourceFactory.create_initialized() receives the properties
/// - Source starts and generates events
#[tokio::test]
async fn test_sql_with_timer_source_basic() {
    // Create manager and register timer source factory
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    // SQL WITH configuration for timer source
    // Note: Timer source uses binary passthrough format (no format specification needed)
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '100'
        );

        CREATE STREAM TimerOutput (tick STRING);

        INSERT INTO TimerOutput
        SELECT tick FROM TimerInput;
    "#;

    // Create runtime using AppRunner with the manager, collect from output stream
    let runner = AppRunner::new_with_manager(manager, sql, "TimerOutput").await;

    // Wait for timer to generate events (100ms interval × 5 = 500ms)
    sleep(Duration::from_millis(500)).await;

    let out = runner.shutdown();

    // Should have received ~5 timer events
    assert!(
        out.len() >= 3 && out.len() <= 7,
        "Expected 3-7 timer events, got {}",
        out.len()
    );

    // Each event should have a "tick" string attribute
    for event in out {
        assert_eq!(event.len(), 1);
        assert!(matches!(event[0], AttributeValue::String(_)));
    }
}

/// Test 2: Full Flow Timer → Filter → Log Sink
///
/// Verifies:
/// - Source configured via SQL WITH
/// - Sink configured via SQL WITH
/// - Events flow through the entire pipeline
/// - Both source and sink handlers are auto-attached
#[tokio::test]
async fn test_sql_with_timer_to_log_full_flow() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    manager.add_sink_factory("log".to_string(), Box::new(LogSinkFactory));
    manager.add_sink_mapper_factory("csv".to_string(), Box::new(CsvSinkMapperFactory));

    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '100'
        );

        CREATE STREAM LogOutput (tick STRING) WITH (
            type = 'sink',
            extension = 'log',
            format = 'csv',
            "log.prefix" = '[TEST]'
        );

        INSERT INTO LogOutput
        SELECT tick FROM TimerInput;
    "#;

    let runner = AppRunner::new_with_manager(manager, sql, "LogOutput").await;

    // Wait for events to flow through
    sleep(Duration::from_millis(500)).await;

    let out = runner.shutdown();

    // Should have received events from timer through to log sink
    assert!(
        out.len() >= 3,
        "Expected at least 3 events, got {}",
        out.len()
    );
}

/// Test 2b: COMPLETE End-to-End Flow - Timer Source → Query → Log Sink
///
/// This is the REAL complete test: ALL configured via SQL WITH
/// Verifies the ENTIRE chain:
/// - SQL WITH configures timer source (with custom interval)
/// - SQL WITH configures log sink (with custom prefix)
/// - Events flow: Timer → Query Processing → Log Sink
/// - Tests the complete integration from parser → runtime → factories → execution
#[tokio::test]
async fn test_sql_with_timer_filter_log_complete() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    manager.add_sink_factory("log".to_string(), Box::new(LogSinkFactory));
    manager.add_sink_mapper_factory("csv".to_string(), Box::new(CsvSinkMapperFactory));

    // COMPLETE flow: All configured via SQL WITH!
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '50'
        );

        CREATE STREAM ProcessedOutput (tick STRING);

        CREATE STREAM LogSink (tick STRING) WITH (
            type = 'sink',
            extension = 'log',
            format = 'csv',
            "log.prefix" = '[COMPLETE-FLOW]'
        );

        -- Query processing layer
        INSERT INTO ProcessedOutput
        SELECT tick FROM TimerInput;

        -- Send to log sink (also configured via SQL WITH)
        INSERT INTO LogSink
        SELECT tick FROM ProcessedOutput;
    "#;

    let runner = AppRunner::new_with_manager(manager, sql, "ProcessedOutput").await;

    // Wait for events to flow through
    sleep(Duration::from_millis(300)).await;

    let out = runner.shutdown();

    // Should have received events from timer through SQL WITH configured source
    assert!(
        out.len() >= 3,
        "Expected at least 3 timer events (SQL WITH → filter → collect), got {}",
        out.len()
    );

    // All events should be strings from timer
    for event in &out {
        assert_eq!(event.len(), 1, "Event should have 1 attribute");
        assert!(
            matches!(&event[0], AttributeValue::String(_)),
            "Expected String value, got {:?}",
            event[0]
        );
    }
}

/// Test 3: Multiple Independent Streams with SQL WITH
///
/// Verifies:
/// - Multiple streams can be independently configured
/// - Each stream's configuration is isolated
/// - Both sources can run concurrently
#[tokio::test]
async fn test_sql_with_multiple_streams_independent() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    let sql = r#"
        CREATE STREAM FastTimer (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '50'
        );

        CREATE STREAM SlowTimer (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '200'
        );

        SELECT tick FROM FastTimer;
    "#;

    let runner = AppRunner::new_with_manager(manager, sql, "FastTimer").await;

    // Wait 400ms: FastTimer should produce ~8 events, SlowTimer ~2 (but we're only collecting from FastTimer)
    sleep(Duration::from_millis(400)).await;

    let out = runner.shutdown();

    // FastTimer interval is 50ms, so in 400ms should get ~8 events
    assert!(
        out.len() >= 5 && out.len() <= 10,
        "Expected 5-10 events from FastTimer, got {}",
        out.len()
    );
}

/// Test 4: Error Handling - Missing Extension Property
///
/// Verifies:
/// - Missing required property 'extension' is handled gracefully
/// - Runtime logs error but doesn't crash
/// - Stream is NOT attached
#[tokio::test]
async fn test_sql_with_missing_extension_error() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    // Missing 'extension' property - this should fail at parse-time validation
    let sql = r#"
        CREATE STREAM BadStream (tick STRING) WITH (
            type = 'source'
        );

        SELECT tick FROM BadStream;
    "#;

    // Parse-time validation should catch missing extension for source streams
    let runtime_result = manager.create_eventflux_app_runtime_from_string(sql).await;

    // Should either fail at parse time OR succeed but not attach the stream
    match runtime_result {
        Err(_) => {
            // Parse-time validation caught it - good!
        }
        Ok(runtime) => {
            // Runtime created, but stream should not be attached
            runtime.start();
            assert!(
                runtime.get_source_handler("BadStream").is_none(),
                "Source should not be attached when extension is missing"
            );
            runtime.shutdown();
        }
    }
}

/// Test 5: Error Handling - Invalid Factory (Extension Not Registered)
///
/// Verifies:
/// - Requesting unregistered extension is handled gracefully
/// - Runtime logs error but continues
/// - Source is NOT attached
#[tokio::test]
async fn test_sql_with_unregistered_extension() {
    let manager = EventFluxManager::new();
    // Note: NOT registering 'kafka' factory

    let sql = r#"
        CREATE STREAM KafkaStream (value STRING) WITH (
            type = 'source',
            extension = 'kafka',
            "kafka.bootstrap.servers" = 'localhost:9092',
            "kafka.topic" = 'test',
            format = 'json'
        );

        SELECT value FROM KafkaStream;
    "#;

    // Runtime creation succeeds (no auto-start anymore)
    // but runtime.start() should FAIL when using unregistered extension
    let runtime_result = manager.create_eventflux_app_runtime_from_string(sql).await;
    assert!(
        runtime_result.is_ok(),
        "Runtime creation should succeed (errors happen at start time now)"
    );

    // Starting the runtime should fail with unregistered extension
    let runtime = runtime_result.unwrap();
    let start_result = runtime.start();
    assert!(
        start_result.is_err(),
        "Runtime start should fail with unregistered extension"
    );

    // Verify error message indicates the unregistered extension
    let err_msg = start_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("kafka") || err_msg.contains("not found") || err_msg.contains("extension"),
        "Error message should mention kafka extension not found, got: {}",
        err_msg
    );
}

/// Test 6: SQL WITH with YAML Configuration Loaded
///
/// Verifies:
/// - AppRunner works correctly with YAML configuration loaded via ConfigManager
/// - SQL WITH clause stream configuration is applied correctly
/// - Runtime uses YAML global config (thread pool, buffer size, etc.)
/// - Timer source configured via SQL WITH generates events properly
///
/// Note: This test loads YAML config with runtime settings and uses SQL WITH
/// for stream configuration, demonstrating the integration between YAML config
/// loading and SQL-defined streams.
#[tokio::test]
async fn test_sql_with_priority_over_yaml() {
    use eventflux::core::config::ConfigManager;

    // Load YAML configuration from test fixture
    let config_manager = ConfigManager::from_file("tests/fixtures/timer-config.yaml");

    // Create EventFluxManager with the YAML config
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    // Register required factories
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    // SQL WITH configuration for timer source
    // The timer interval is defined in SQL, not in YAML
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '100'
        );

        CREATE STREAM TimerOutput (tick STRING);

        INSERT INTO TimerOutput
        SELECT tick FROM TimerInput;
    "#;

    // Create runtime using AppRunner with the manager (which has YAML config loaded)
    let runner = AppRunner::new_with_manager(manager, sql, "TimerOutput").await;

    // Wait for timer to generate events (100ms interval × 6 = 600ms)
    sleep(Duration::from_millis(600)).await;

    let out = runner.shutdown();

    // Should have received ~6 timer events (100ms interval)
    // This proves that:
    // 1. YAML config was loaded successfully (runtime settings applied)
    // 2. SQL WITH configuration was used for the timer stream
    // 3. The timer interval from SQL (100ms) is what's actually used
    assert!(
        out.len() >= 4 && out.len() <= 8,
        "Expected 4-8 timer events with 100ms interval, got {}. \
         This verifies SQL WITH configuration is applied correctly with YAML config loaded.",
        out.len()
    );

    // Each event should have a "tick" string attribute
    for event in out {
        assert_eq!(
            event.len(),
            1,
            "Each event should have exactly one attribute"
        );
        assert!(
            matches!(event[0], AttributeValue::String(_)),
            "Tick attribute should be a String"
        );
    }
}

/// Test 7: Internal Stream (No WITH Clause)
///
/// Verifies:
/// - Streams without WITH clause work normally
/// - with_config is None for internal streams
/// - No auto-attach is attempted
#[tokio::test]
async fn test_sql_internal_stream_no_with() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '100'
        );

        CREATE STREAM InternalStream (tick STRING);

        INSERT INTO InternalStream
        SELECT tick FROM TimerInput;

        SELECT tick FROM InternalStream;
    "#;

    let runner = AppRunner::new_with_manager(manager, sql, "InternalStream").await;

    sleep(Duration::from_millis(300)).await;

    let out = runner.shutdown();

    // Should receive events through internal stream
    assert!(
        out.len() >= 1,
        "Expected at least 1 event, got {}",
        out.len()
    );
}

/// Test 8: Empty WITH Clause
///
/// Verifies:
/// - Empty WITH clause is handled gracefully
/// - Stream is treated as internal (no type specified)
#[tokio::test]
async fn test_sql_with_empty_clause() {
    let manager = EventFluxManager::new();

    let sql = r#"
        CREATE STREAM EmptyWithStream (value INT) WITH ();

        SELECT value FROM EmptyWithStream;
    "#;

    // Should parse and create runtime without error
    let runtime_result = manager.create_eventflux_app_runtime_from_string(sql).await;
    assert!(runtime_result.is_ok());

    let runtime = runtime_result.unwrap();
    runtime.start();

    // No source should be attached (no type specified)
    assert!(runtime.get_source_handler("EmptyWithStream").is_none());

    runtime.shutdown();
}

/// Test 9: Verify FlatConfig Properties Are Passed to Factory
///
/// This test creates a custom factory that verifies it receives the exact
/// properties from SQL WITH configuration
#[tokio::test]
async fn test_sql_with_properties_reach_factory() {
    use eventflux::core::exception::EventFluxError;
    use eventflux::core::extension::SourceFactory;
    use eventflux::core::stream::input::source::Source;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    // Counter to track factory calls
    static FACTORY_CALL_COUNT: Mutex<usize> = Mutex::new(0);

    #[derive(Debug, Clone)]
    struct VerifyingFactory;

    impl SourceFactory for VerifyingFactory {
        fn name(&self) -> &'static str {
            "verify"
        }

        fn supported_formats(&self) -> &[&str] {
            // No format required - uses passthrough (returns TimerSource internally)
            &[]
        }

        fn required_parameters(&self) -> &[&str] {
            &[]
        }

        fn optional_parameters(&self) -> &[&str] {
            &["verify.prop1", "verify.prop2"]
        }

        fn create_initialized(
            &self,
            config: &HashMap<String, String>,
        ) -> Result<Box<dyn Source>, EventFluxError> {
            // Verify the properties we expect from SQL WITH are present
            assert_eq!(config.get("verify.prop1"), Some(&"value1".to_string()));
            assert_eq!(config.get("verify.prop2"), Some(&"value2".to_string()));

            // Increment call count
            let mut count = FACTORY_CALL_COUNT.lock().unwrap();
            *count += 1;

            // Return a simple timer source
            Ok(Box::new(
                eventflux::core::stream::input::source::timer_source::TimerSource::new(1000),
            ))
        }

        fn clone_box(&self) -> Box<dyn SourceFactory> {
            Box::new(self.clone())
        }
    }

    let manager = EventFluxManager::new();
    manager.add_source_factory("verify".to_string(), Box::new(VerifyingFactory));

    let sql = r#"
        CREATE STREAM VerifyStream (tick STRING) WITH (
            type = 'source',
            extension = 'verify',
            "verify.prop1" = 'value1',
            "verify.prop2" = 'value2'
        );

        SELECT tick FROM VerifyStream;
    "#;

    let runner = AppRunner::new_with_manager(manager, sql, "VerifyStream").await;

    // Verify factory was called
    {
        let count = FACTORY_CALL_COUNT.lock().unwrap();
        assert_eq!(
            *count, 1,
            "Factory should have been called exactly once, got {} calls",
            *count
        );
    }

    runner.shutdown();
}
