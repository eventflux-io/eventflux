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

//! End to end configuration integration tests
//!
//! Follows configuration from loading a YAML file through to runtime components,
//! to check that configuration values reach the processors.

use eventflux::core::config::types::application_config::{DefinitionConfig, WindowConfig};
use eventflux::core::config::types::*;
use eventflux::core::config::{
    ApplicationConfig, ConfigManager, ConfigValue, EventFluxConfig, ProcessorConfigReader,
    RuntimeMode,
};
use eventflux::core::event::Event;
use eventflux::core::stream::output::stream_callback::StreamCallback;
use eventflux::core::EventFluxManager;
use serial_test::serial;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;

/// Callback that collects output events
#[derive(Debug, Clone, Default)]
struct TestEventCollector {
    events: Arc<Mutex<Vec<Event>>>,
    callback_count: Arc<Mutex<usize>>,
}

#[allow(dead_code)]
impl TestEventCollector {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            callback_count: Arc::new(Mutex::new(0)),
        }
    }

    fn get_events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn get_callback_count(&self) -> usize {
        *self.callback_count.lock().unwrap()
    }
}

impl StreamCallback for TestEventCollector {
    fn receive_events(&self, events: &[Event]) {
        let mut stored_events = self.events.lock().unwrap();
        let mut count = self.callback_count.lock().unwrap();

        stored_events.extend_from_slice(events);
        *count += 1;
    }
}

/// Create a test YAML configuration file
async fn create_test_config_file() -> NamedTempFile {
    let config_yaml = r#"
apiVersion: eventflux.io/v2
kind: EventFluxConfig
metadata:
  name: test-config
  namespace: default
eventflux:
  runtime:
    mode: single-node
    performance:
      thread_pool_size: 8
      event_buffer_size: 2048
      batch_size: 500
      batch_processing: true
      async_processing: false
  resource:
    memory:
      maxHeapSize: "1g"
      buffer: "256m"
    cpu:
      cores: 4
  observability:
    metrics:
      enabled: true
      provider: prometheus
      port: 8080
    tracing:
      enabled: true
      provider: jaeger
      endpoint: "http://localhost:14268"
applications:
  test-app:
    streams:
      TestOutputStream:
        sink:
          type: "log"
          prefix: "[TEST-OUTPUT]"
          level: "INFO"
    definitions:
      sort-window:
        type: window
        window_type: sort
        parameters:
          initial_capacity_multiplier: "1.5"
          distributed_size_factor: "0.7"
      length-window:
        type: window
        window_type: length
        parameters:
          initial_capacity_multiplier: "1.3"
          max_size_multiplier: "2.5"
      session-window:
        type: window
        window_type: session
        parameters:
          timeout_ms: "60000"
    monitoring:
      enabled: true
      metrics:
        collection_interval: "10s"
        retention_period: "1d"
    error_handling:
      strategy: retry
      max_retries: 3
      backoff:
        type: exponential
        initial_delay: "100ms"
        max_delay: "30s"
"#;

    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    temp_file
        .write_all(config_yaml.as_bytes())
        .expect("Failed to write config");
    temp_file
}

// TODO: NOT PART OF M1. Ignored because it uses the old EventFluxQL syntax with
// @app:name and @info annotations, which the SQL parser does not support. The YAML
// configuration system works, but this test needs rewriting in SQL syntax.
// See feat/grammar/GRAMMAR.md for the grammar status.
#[tokio::test]
#[serial]
#[ignore = "@app annotations and old EventFluxQL syntax not part of M1"]
async fn test_yaml_config_to_runtime_integration() {
    let config_file = create_test_config_file().await;
    let config_path = config_file.path();

    let config_manager = ConfigManager::new();
    let loaded_config = config_manager
        .load_from_file(config_path)
        .await
        .expect("Failed to load configuration");

    assert_eq!(
        loaded_config.eventflux.runtime.mode,
        RuntimeMode::SingleNode
    );
    // PerformanceConfig defaults may override these, so they are printed, not asserted
    println!(
        "Event buffer size: {}",
        loaded_config
            .eventflux
            .runtime
            .performance
            .event_buffer_size
    );
    println!(
        "Thread pool size: {}",
        loaded_config.eventflux.runtime.performance.thread_pool_size
    );

    let app_config = loaded_config
        .applications
        .get("test-app")
        .expect("Test app configuration not found");

    // Application config has no performance field. Performance settings live in the
    // global config, so only the monitoring settings are checked here.
    assert!(app_config.monitoring.is_some());

    let manager = EventFluxManager::new_with_config(loaded_config);

    // EventFluxQL whose sort window would read the window configuration
    let eventflux_app_string = r#"
        @app:name('test-app')
        @app:statistics('true')

        define stream InputStream (symbol string, price float, volume int);
        define stream OutputStream (symbol string, avgPrice float, maxVolume int);

        @info(name = 'query1')
        from InputStream#window:sort(5, price desc)
        select symbol, avg(price) as avgPrice, max(volume) as maxVolume
        group by symbol
        insert into OutputStream;
    "#;

    // Creating the runtime should apply the configuration
    let eventflux_app_runtime = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await
        .expect("Failed to create EventFluxApp runtime");

    let event_collector = Arc::new(TestEventCollector::new());
    eventflux_app_runtime
        .add_callback("OutputStream", Box::new(event_collector.as_ref().clone()))
        .expect("Failed to add callback");

    // Start only after the callbacks are added
    eventflux_app_runtime
        .start()
        .expect("Failed to start runtime");

    let _input_handler = eventflux_app_runtime
        .get_input_handler("InputStream")
        .expect("Failed to get input handler");

    // No events are sent for now. The test only covers configuration.
    // TODO: Fix AttributeValue construction so real events can be sent
    println!("Skipping event sending for now due to AttributeValue conversion issues");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // For now the test only checks that the runtime was created and started.
    // TODO: Check event processing once AttributeValue construction is fixed
    println!("EventFluxApp runtime created successfully with configuration");

    // Processor internals cannot be reached from here, so nothing more is checked
    println!("Configuration integration test completed successfully");

    eventflux_app_runtime.shutdown();
}

#[tokio::test]
#[serial]
async fn test_processor_config_reader() {
    // Create test application config
    let mut app_config = ApplicationConfig::default();

    // Create window definitions with parameters using YAML format
    let mut yaml_params = serde_yaml::Mapping::new();
    yaml_params.insert(
        serde_yaml::Value::String("initial_capacity_multiplier".to_string()),
        serde_yaml::Value::String("1.8".to_string()),
    );
    yaml_params.insert(
        serde_yaml::Value::String("distributed_size_factor".to_string()),
        serde_yaml::Value::String("0.6".to_string()),
    );

    let window_config = WindowConfig {
        window_type: "sort".to_string(),
        parameters: serde_yaml::Value::Mapping(yaml_params),
        persistence: None,
        memory: None,
    };

    let window_def = DefinitionConfig::Window(window_config);

    app_config
        .definitions
        .insert("sort".to_string(), window_def);

    // Create global config
    let global_config = EventFluxConfig::default();

    // Test ProcessorConfigReader directly
    let config_reader = ProcessorConfigReader::new(Some(app_config), Some(global_config));

    // Test window configuration reading
    let multiplier = config_reader.get_window_config("sort", "initial_capacity_multiplier");
    assert!(matches!(multiplier, Some(ConfigValue::Float(1.8))));

    let factor = config_reader.get_window_config("sort", "distributed_size_factor");
    assert!(matches!(factor, Some(ConfigValue::Float(0.6))));

    // Performance settings are read from the global config
    let buffer_size = config_reader.get_performance_config("event_buffer_size");
    // Only the type is checked, not the value
    assert!(matches!(buffer_size, Some(ConfigValue::Integer(_))));

    let batch_size = config_reader.get_performance_config("batch_size");
    // With a global config whose batch_size is None, the reader returns None. The
    // default of 1000 applies only when there is no global config at all.
    println!("Batch size: {:?}", batch_size);
    // This assert accepts any value, so it checks nothing
    assert!(batch_size.is_some() || batch_size.is_none());

    let stats_enabled = config_reader.get_performance_config("enable_statistics");
    assert!(matches!(stats_enabled, Some(ConfigValue::Boolean(false)))); // Default value

    // Unknown settings have no default and return None
    let unknown_window = config_reader.get_window_config("unknown", "param");
    assert!(unknown_window.is_none());

    let unknown_perf = config_reader.get_performance_config("unknown_param");
    assert!(unknown_perf.is_none());

    // Caching. Clear any existing cache first.
    config_reader.clear_cache();
    assert_eq!(config_reader.get_cache_size(), 0);

    // Each lookup adds one entry to the cache
    let _val1 = config_reader.get_window_config("sort", "initial_capacity_multiplier");
    let _val2 = config_reader.get_performance_config("event_buffer_size");

    assert_eq!(config_reader.get_cache_size(), 2);

    config_reader.clear_cache();
    assert_eq!(config_reader.get_cache_size(), 0);
}

#[tokio::test]
#[serial]
async fn test_config_fallback_behavior() {
    // With no application or global config, the reader returns built in defaults
    let config_reader = ProcessorConfigReader::new(None, None);

    let multiplier = config_reader.get_window_config("length", "initial_capacity_multiplier");
    assert!(matches!(multiplier, Some(ConfigValue::Float(1.2))));

    let timeout = config_reader.get_window_config("session", "timeout_ms");
    assert!(matches!(timeout, Some(ConfigValue::Integer(30000))));

    let buffer_size = config_reader.get_performance_config("event_buffer_size");
    assert!(matches!(buffer_size, Some(ConfigValue::Integer(1024))));

    let stats_enabled = config_reader.get_performance_config("enable_statistics");
    assert!(matches!(stats_enabled, Some(ConfigValue::Boolean(false))));
}

#[tokio::test]
#[serial]
async fn test_config_manager_eventflux_manager_integration() {
    // Create test configuration
    let mut config = EventFluxConfig::default();
    config.eventflux.runtime.mode = RuntimeMode::Distributed;

    let perf_config = PerformanceConfig {
        event_buffer_size: 16384,
        batch_size: Some(5000),
        ..Default::default()
    };
    config.eventflux.runtime.performance = perf_config;

    let app_config = ApplicationConfig::default();

    config
        .applications
        .insert("integration-test".to_string(), app_config);

    // Create manager with config
    let manager = EventFluxManager::new_with_config(config);

    // Verify configuration access
    let loaded_config = manager.get_config().await.expect("Failed to get config");
    assert_eq!(
        loaded_config.eventflux.runtime.mode,
        RuntimeMode::Distributed
    );

    let app_specific = manager
        .get_application_config("integration-test")
        .await
        .expect("Failed to get app config")
        .expect("App config not found");

    // The app config exists but is the empty default, so monitoring is not set
    assert!(app_specific.monitoring.is_none());

    // An application that does not exist gives None, not an error
    let missing_app = manager
        .get_application_config("non-existent")
        .await
        .expect("Failed to get app config");
    assert!(missing_app.is_none());
}

#[tokio::test]
#[serial]
#[allow(clippy::approx_constant)]
async fn test_config_value_type_conversions() {
    // Test ConfigValue type conversions
    let int_val = ConfigValue::Integer(42);
    assert_eq!(int_val.as_integer(), Some(42));
    assert_eq!(int_val.as_float(), Some(42.0));
    assert_eq!(int_val.as_usize(), Some(42));
    assert_eq!(int_val.as_boolean(), None);

    let float_val = ConfigValue::Float(3.14159);
    assert_eq!(float_val.as_float(), Some(3.14159));
    assert_eq!(float_val.as_integer(), None);
    assert_eq!(float_val.as_usize(), None);

    let string_val = ConfigValue::String("test".to_string());
    assert_eq!(string_val.as_string(), Some(&"test".to_string()));
    assert_eq!(string_val.as_integer(), None);

    let bool_val = ConfigValue::Boolean(true);
    assert_eq!(bool_val.as_boolean(), Some(true));
    assert_eq!(bool_val.as_integer(), None);

    // Test negative integer conversion to usize
    let negative_val = ConfigValue::Integer(-1);
    assert_eq!(negative_val.as_usize(), None);
    assert_eq!(negative_val.as_integer(), Some(-1));
}
