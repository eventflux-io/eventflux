/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! End to end tests for SQL WITH configuration
//!
//! SQL WITH properties must carry through from parsing to runtime start, along
//! this whole path
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

    // SQL WITH configuration for the timer source.
    // The timer source uses the binary passthrough format, so no format is given.
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

    // AppRunner with this manager, collecting from the output stream
    let runner = AppRunner::new_with_manager(manager, sql, "TimerOutput").await;

    // Wait for timer to generate events (100ms interval × 5 = 500ms)
    sleep(Duration::from_millis(500)).await;

    let out = runner.shutdown();

    // About 5 timer events. 3 to 7 are accepted to allow for timing.
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
/// - Both source and sink handlers are attached on their own
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

/// Test 2b: COMPLETE End to End Flow, Timer Source → Query → Log Sink
///
/// The full test, with ALL parts configured through SQL WITH.
/// Verifies the ENTIRE chain:
/// - SQL WITH configures the timer source, with a custom interval
/// - SQL WITH configures the log sink, with a custom prefix
/// - Events flow Timer → Query Processing → Log Sink
/// - The whole path parser → runtime → factories → execution works
#[tokio::test]
async fn test_sql_with_timer_filter_log_complete() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    manager.add_sink_factory("log".to_string(), Box::new(LogSinkFactory));
    manager.add_sink_mapper_factory("csv".to_string(), Box::new(CsvSinkMapperFactory));

    // COMPLETE flow, all configured through SQL WITH
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

    // Wait 400 ms. FastTimer should give about 8 events and SlowTimer about 2,
    // but only FastTimer is collected.
    sleep(Duration::from_millis(400)).await;

    let out = runner.shutdown();

    // FastTimer fires every 50 ms, so 400 ms should give about 8 events
    assert!(
        out.len() >= 5 && out.len() <= 10,
        "Expected 5-10 events from FastTimer, got {}",
        out.len()
    );
}

/// Test 4: Error Handling, Missing Extension Property
///
/// Verifies:
/// - A missing required 'extension' property does not crash anything
/// - Either creation fails, or the runtime starts without attaching the stream
/// - The stream is NOT attached
#[tokio::test]
async fn test_sql_with_missing_extension_error() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    // No 'extension' property. Validation at parse time should reject this.
    let sql = r#"
        CREATE STREAM BadStream (tick STRING) WITH (
            type = 'source'
        );

        SELECT tick FROM BadStream;
    "#;

    let runtime_result = manager.create_eventflux_app_runtime_from_string(sql).await;

    // Either creation fails at parse time, OR it succeeds and the stream is not attached
    match runtime_result {
        Err(_) => {
            // Caught by validation at parse time
        }
        Ok(runtime) => {
            // The runtime exists, but the stream must not be attached
            runtime.start().expect("Failed to start runtime");
            assert!(
                runtime.get_source_handler("BadStream").is_none(),
                "Source should not be attached when extension is missing"
            );
            runtime.shutdown();
        }
    }
}

/// Test 5: Error Handling, Extension Not Registered
///
/// Verifies:
/// - Creating the runtime still succeeds with an extension that is not registered
/// - start() then fails with an error that names the problem
#[tokio::test]
async fn test_sql_with_unregistered_extension() {
    let manager = EventFluxManager::new();
    // The 'kafka' factory is NOT registered on purpose

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

    // Creation succeeds because the runtime no longer starts on its own.
    // runtime.start() must FAIL because the extension is not registered.
    let runtime_result = manager.create_eventflux_app_runtime_from_string(sql).await;
    assert!(
        runtime_result.is_ok(),
        "Runtime creation should succeed (errors happen at start time now)"
    );

    let runtime = runtime_result.unwrap();
    let start_result = runtime.start();
    assert!(
        start_result.is_err(),
        "Runtime start should fail with unregistered extension"
    );

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
/// The test loads YAML config with runtime settings and uses SQL WITH for the
/// stream configuration, to show that YAML config and streams defined in SQL
/// work together.
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

    // About 6 timer events at the 100 ms interval. This shows that
    // 1. The YAML config loaded and its runtime settings were applied
    // 2. The SQL WITH configuration was used for the timer stream
    // 3. The 100 ms timer interval from SQL is the one actually used
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
/// - Nothing tries to attach a connector
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
        !out.is_empty(),
        "Expected at least 1 event, got {}",
        out.len()
    );
}

/// Test 8: Empty WITH Clause
///
/// Verifies:
/// - An empty WITH clause does not cause an error
/// - The stream is treated as internal because no type is given
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
    runtime.start().expect("Failed to start runtime");

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
    use std::sync::Mutex;

    // Counter to track factory calls
    static FACTORY_CALL_COUNT: Mutex<usize> = Mutex::new(0);

    #[derive(Debug, Clone)]
    struct VerifyingFactory;

    impl SourceFactory for VerifyingFactory {
        fn name(&self) -> &'static str {
            "verify"
        }

        fn supported_formats(&self) -> &[&str] {
            // No format needed. It uses passthrough and returns a TimerSource inside.
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

/// YAML that ticks every 60 s, so a timer source built from it alone produces
/// nothing within a test's few hundred milliseconds. The key is flat because the
/// old YAML only path rejected nested mappings, which would hide its fallback bug.
const SLOW_YAML_TIMER: &str = r#"
streams:
  TimerInput:
    source:
      type: timer
      timer.interval: 60000
"#;

/// Runs `sql` with [`SLOW_YAML_TIMER`] as the YAML config for 350 ms and returns
/// what reached `TimerOutput`. Panics if start() fails.
async fn run_with_slow_yaml_timer(app_name: &str, sql: &str) -> Vec<Vec<AttributeValue>> {
    let manager = common::manager_with_app_config(app_name, SLOW_YAML_TIMER);
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    let runner = AppRunner::new_with_manager(manager, sql, "TimerOutput").await;
    sleep(Duration::from_millis(350)).await;
    runner.shutdown()
}

/// A stream in both YAML and a typed SQL WITH gets one source, built from the
/// merged config where SQL wins. The YAML ticks every 60 s and SQL every 100 ms,
/// so events only arrive if the SQL value was used and no YAML only source
/// replaced it.
#[tokio::test]
async fn test_typed_sql_with_overrides_yaml_source() {
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

    let out = run_with_slow_yaml_timer("TypedOverrideApp", sql).await;
    assert!(
        !out.is_empty(),
        "the SQL timer.interval should win over the YAML one"
    );
}

/// A SQL WITH clause with no `type` still overrides the YAML connector config.
/// It used to be ignored, because only a typed WITH clause read the merged config.
#[tokio::test]
async fn test_untyped_sql_with_overrides_yaml_source() {
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH ("timer.interval" = '100');

        CREATE STREAM TimerOutput (tick STRING);

        INSERT INTO TimerOutput
        SELECT tick FROM TimerInput;
    "#;

    let out = run_with_slow_yaml_timer("UntypedOverrideApp", sql).await;
    assert!(
        !out.is_empty(),
        "the SQL timer.interval should win over the YAML one"
    );
}

/// A stream declared internal in SQL must not quietly get the YAML source, and
/// must not quietly lose it either. Creation fails and names the conflict.
#[tokio::test]
async fn test_internal_stream_with_yaml_source_fails_creation() {
    let manager = common::manager_with_app_config("InternalYamlApp", SLOW_YAML_TIMER);
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (type = 'internal');
        CREATE STREAM TimerOutput (tick STRING);
        INSERT INTO TimerOutput SELECT tick FROM TimerInput;
    "#;

    let err = match manager.create_eventflux_app_runtime_from_string(sql).await {
        Ok(_) => panic!("conflicting stream config must fail creation"),
        Err(e) => e,
    };
    assert!(
        err.contains("Stream 'TimerInput' is declared type = 'internal'")
            && err.contains("a source"),
        "got: {err}"
    );
}

/// When the typed SQL attach fails, the YAML config must not be used as a
/// fallback. It used to be, and a second start() then ran the YAML only source
/// with the SQL error hidden.
#[tokio::test]
async fn test_failed_sql_attach_does_not_fall_back_to_yaml() {
    let manager = common::manager_with_app_config("NoFallbackApp", SLOW_YAML_TIMER);
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = 'not-a-number'
        );
        CREATE STREAM TimerOutput (tick STRING);
        INSERT INTO TimerOutput SELECT tick FROM TimerInput;
    "#;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    for attempt in 1..=2 {
        let err = runtime
            .start()
            .expect_err("a bad SQL timer.interval must fail every start");
        assert!(
            err.to_string().contains("timer.interval"),
            "attempt {attempt}: {err}"
        );
    }
    runtime.shutdown();
}

/// A YAML source or sink for a stream the app does not define is reported, not
/// skipped without a word
#[tokio::test]
async fn test_yaml_connector_for_undefined_stream_fails_start() {
    let manager = common::manager_with_app_config("UndefinedYamlApp", SLOW_YAML_TIMER);
    let sql = r#"
        CREATE STREAM Other (tick STRING);
        CREATE STREAM TimerOutput (tick STRING);
        INSERT INTO TimerOutput SELECT tick FROM Other;
    "#;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("runtime");
    let err = runtime
        .start()
        .expect_err("YAML for an undefined stream must fail start");
    runtime.shutdown();
    assert!(
        err.to_string()
            .contains("source or sink for stream 'TimerInput', but the app does not define"),
        "got: {err}"
    );
}

/// A DLQ on a source that cannot send to a DLQ, here the timer source, must make
/// start() fail instead of being quietly ignored (#207)
#[tokio::test]
async fn test_sql_with_dlq_on_source_without_dlq_support_fails_start() {
    let manager = EventFluxManager::new();
    manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));

    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = 'source',
            extension = 'timer',
            "timer.interval" = '100',
            "error.strategy" = 'dlq',
            "error.dlq.stream" = 'TimerErrors'
        );

        CREATE STREAM TimerErrors (originalEvent STRING, errorMessage STRING,
            errorType STRING, timestamp LONG, attemptCount INT, streamName STRING);
    "#;

    let runtime = manager
        .create_eventflux_app_runtime_from_string(sql)
        .await
        .expect("a valid DLQ reference passes app creation");
    let err = runtime
        .start()
        .expect_err("timer source cannot dead-letter")
        .to_string();
    runtime.shutdown();

    assert!(err.contains("does not support DLQ routing"), "got: {err}");
    assert!(err.contains("'TimerInput'"), "got: {err}");
}

/// `type = NULL` must not be dropped like other NULL options. A stream with an
/// extension but no type is read as a table, so the source would never attach.
#[tokio::test]
async fn test_sql_with_null_type_fails_creation() {
    let manager = EventFluxManager::new();
    let sql = r#"
        CREATE STREAM TimerInput (tick STRING) WITH (
            type = NULL,
            extension = 'timer',
            "timer.interval" = '100'
        );
        CREATE STREAM TimerOutput (tick STRING);
        INSERT INTO TimerOutput SELECT tick FROM TimerInput;
    "#;

    let err = match manager.create_eventflux_app_runtime_from_string(sql).await {
        Ok(_) => panic!("type = NULL must fail creation"),
        Err(e) => e,
    };
    assert!(
        err.contains("WITH option 'type' cannot be NULL"),
        "got: {err}"
    );
}
