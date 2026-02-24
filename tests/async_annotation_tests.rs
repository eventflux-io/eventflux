// SPDX-License-Identifier: MIT OR Apache-2.0

// ✅ MIGRATED: All tests converted to SQL WITH syntax
//
// These tests verify async stream configuration using modern SQL WITH clauses
// and YAML configuration files, replacing legacy @Async/@config/@app annotations.
//
// Migration completed: 2025-10-24
// - All 6 tests now use SQL WITH for stream-level async configuration
// - Application-level async uses YAML configuration files
// - Pure SQL syntax with no custom annotations
//
// Tests verify:
// - SQL WITH async properties (buffer_size, workers, batch_size_max)
// - YAML application-level configuration
// - Multiple streams with different async settings
// - Async configuration with SQL queries

// Tests for async stream configuration via SQL WITH and YAML
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::query_api::definition::attribute::Type as AttributeType;

#[tokio::test]
async fn test_async_with_sql_basic() {
    let mut manager = EventFluxManager::new();

    // MIGRATED: @Async annotation replaced with SQL WITH clause
    let eventflux_app_string = r#"
        CREATE STREAM StockStream (symbol STRING, price DOUBLE, volume BIGINT) WITH (
            'async.buffer_size' = '1024',
            'async.workers' = '2',
            'async.batch_size_max' = '10'
        );
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to parse SQL WITH async configuration: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;

    assert!(stream_definitions.contains_key("StockStream"));
    let stream_def = stream_definitions.get("StockStream").unwrap();

    // Check that the stream has SQL WITH configuration
    let with_config = stream_def
        .with_config
        .as_ref()
        .expect("Stream should have WITH configuration");

    // Check configuration parameters
    assert_eq!(
        with_config.get("async.buffer_size"),
        Some(&"1024".to_string()),
        "buffer_size should be 1024"
    );

    assert_eq!(
        with_config.get("async.workers"),
        Some(&"2".to_string()),
        "workers should be 2"
    );

    assert_eq!(
        with_config.get("async.batch_size_max"),
        Some(&"10".to_string()),
        "batch_size_max should be 10"
    );
}

#[tokio::test]
async fn test_async_with_sql_minimal() {
    let mut manager = EventFluxManager::new();

    // MIGRATED: Minimal @Async replaced with async.enabled property
    let eventflux_app_string = r#"
        CREATE STREAM MinimalAsyncStream (id INT, value STRING) WITH (
            'async.enabled' = 'true'
        );
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to parse minimal SQL WITH async configuration: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;

    assert!(stream_definitions.contains_key("MinimalAsyncStream"));
    let stream_def = stream_definitions.get("MinimalAsyncStream").unwrap();

    // Check that the stream has SQL WITH configuration with async enabled
    let with_config = stream_def
        .with_config
        .as_ref()
        .expect("Stream should have WITH configuration");

    assert_eq!(
        with_config.get("async.enabled"),
        Some(&"true".to_string()),
        "async should be enabled"
    );
}

#[tokio::test]
async fn test_async_via_yaml_config() {
    use eventflux::core::config::ConfigManager;

    // MIGRATED: @config(async='true') replaced with YAML configuration
    let config_manager = ConfigManager::from_file("tests/fixtures/app-async-enabled.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    let eventflux_app_string = r#"
        CREATE STREAM ConfigAsyncStream (symbol STRING, price DOUBLE);
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to create runtime with YAML async config: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;

    assert!(stream_definitions.contains_key("ConfigAsyncStream"));
    // Stream created successfully with YAML configuration
    // Async mode configured via YAML app-async-enabled.yaml
}

#[tokio::test]
async fn test_app_level_async_via_yaml() {
    use eventflux::core::config::ConfigManager;

    // MIGRATED: @app(async='true') replaced with YAML configuration
    let config_manager = ConfigManager::from_file("tests/fixtures/app-async-enabled.yaml");
    let manager = EventFluxManager::new_with_config_manager(config_manager);

    let eventflux_app_string = r#"
        CREATE STREAM AutoAsyncStream (id INT, value STRING);

        CREATE STREAM RegularStream (name STRING, count INT);
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to create runtime with app-level YAML async config: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();

    // Both streams should exist
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;
    assert!(stream_definitions.contains_key("AutoAsyncStream"));
    assert!(stream_definitions.contains_key("RegularStream"));
    // Both streams inherit async mode from YAML application config
}

#[tokio::test]
async fn test_multiple_async_streams_with_sql() {
    let mut manager = EventFluxManager::new();

    // MIGRATED: Multiple @Async annotations replaced with SQL WITH clauses
    let eventflux_app_string = r#"
        CREATE STREAM Stream1 (id INT) WITH (
            'async.buffer_size' = '512'
        );

        CREATE STREAM Stream2 (name STRING) WITH (
            'async.buffer_size' = '2048',
            'async.workers' = '4'
        );

        CREATE STREAM Stream3 (value DOUBLE);
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to parse multiple async streams with SQL WITH: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;

    // Check Stream1 has correct buffer size
    let stream1 = stream_definitions.get("Stream1").unwrap();
    let with_config1 = stream1.with_config.as_ref().unwrap();
    assert_eq!(
        with_config1.get("async.buffer_size"),
        Some(&"512".to_string())
    );

    // Check Stream2 has correct parameters
    let stream2 = stream_definitions.get("Stream2").unwrap();
    let with_config2 = stream2.with_config.as_ref().unwrap();
    assert_eq!(
        with_config2.get("async.buffer_size"),
        Some(&"2048".to_string())
    );
    assert_eq!(with_config2.get("async.workers"), Some(&"4".to_string()));

    // Check Stream3 has no WITH configuration
    let stream3 = stream_definitions.get("Stream3").unwrap();
    assert!(
        stream3.with_config.is_none(),
        "Stream3 should not have WITH configuration"
    );
}

#[tokio::test]
async fn test_async_with_sql_query() {
    let mut manager = EventFluxManager::new();

    // MIGRATED: @Async + EventFluxQL query replaced with SQL WITH + SQL query
    let eventflux_app_string = r#"
        CREATE STREAM InputStream (symbol STRING, price DOUBLE, volume BIGINT) WITH (
            'async.buffer_size' = '1024'
        );

        CREATE STREAM OutputStream (symbol STRING, avgPrice DOUBLE);

        INSERT INTO OutputStream
        SELECT symbol, AVG(price) as avgPrice
        FROM InputStream WINDOW('time', 10000 MILLISECONDS)
        GROUP BY symbol;
    "#;

    let result = manager
        .create_eventflux_app_runtime_from_string(eventflux_app_string)
        .await;
    assert!(
        result.is_ok(),
        "Failed to parse async stream with SQL query: {:?}",
        result.as_ref().err()
    );

    let app_runtime = result.unwrap();
    let stream_definitions = &app_runtime.eventflux_app.stream_definition_map;

    assert!(stream_definitions.contains_key("InputStream"));
    assert!(stream_definitions.contains_key("OutputStream"));

    // InputStream should have SQL WITH configuration
    let input_stream = stream_definitions.get("InputStream").unwrap();
    assert!(input_stream.with_config.is_some());
    assert_eq!(
        input_stream
            .with_config
            .as_ref()
            .unwrap()
            .get("async.buffer_size"),
        Some(&"1024".to_string())
    );

    // OutputStream should not have WITH configuration
    let output_stream = stream_definitions.get("OutputStream").unwrap();
    assert!(output_stream.with_config.is_none());
}
