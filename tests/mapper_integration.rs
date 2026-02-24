// SPDX-License-Identifier: MIT OR Apache-2.0

//! Integration Tests for Data Mapping System
//!
//! These tests verify the complete mapper system including:
//! - Source and sink mappers
//! - Auto-mapping and explicit mapping
//! - Factory system
//! - Validation
//! - End-to-end JSON and CSV workflows

use eventflux::core::event::{AttributeValue, Event};
use eventflux::core::stream::mapper::{
    factory::{
        JsonSinkMapperFactory, JsonSourceMapperFactory, MapperFactoryRegistry, SinkMapperFactory,
        SourceMapperFactory,
    },
    validation::{validate_sink_mapper_config, validate_source_mapper_config},
    SinkMapper, SourceMapper,
};
use std::collections::HashMap;

// ============================================================================
// JSON Auto-Mapping Tests
// ============================================================================

#[test]
fn test_json_auto_mapping_end_to_end() {
    // Source: Auto-map all top-level fields
    // JSON keys are preserved in insertion order: orderId, amount, customer
    let json_input = r#"{"orderId": "123", "amount": 100.0, "customer": "Alice"}"#;

    let config = HashMap::new();
    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let events = mapper.map(json_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data.len(), 3);

    // Verify data types (insertion order: orderId, amount, customer)
    assert!(matches!(events[0].data[0], AttributeValue::String(_))); // orderId
    assert!(matches!(events[0].data[1], AttributeValue::Double(_))); // amount
    assert!(matches!(events[0].data[2], AttributeValue::String(_))); // customer
}

#[test]
fn test_json_auto_mapping_preserves_types() {
    // JSON keys preserved in insertion order: stringField, intField, doubleField, boolField, nullField
    let json_input = r#"{
        "stringField": "test",
        "intField": 42,
        "doubleField": 99.5,
        "boolField": true,
        "nullField": null
    }"#;

    let config = HashMap::new();
    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let events = mapper.map(json_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data.len(), 5);

    // Verify type preservation (insertion order)
    assert!(matches!(events[0].data[0], AttributeValue::String(_))); // stringField
    assert!(matches!(events[0].data[1], AttributeValue::Int(42))); // intField
    assert!(matches!(events[0].data[2], AttributeValue::Double(_))); // doubleField
    assert!(matches!(events[0].data[3], AttributeValue::Bool(true))); // boolField
    assert!(matches!(events[0].data[4], AttributeValue::Null)); // nullField
}

// ============================================================================
// JSON Explicit Mapping Tests
// ============================================================================

#[test]
fn test_json_explicit_mapping_nested_fields() {
    let json_input = r#"{
        "order": {
            "id": "ORDER-123",
            "details": {
                "amount": 250.75,
                "currency": "USD"
            }
        }
    }"#;

    let mut config = HashMap::new();
    config.insert("json.mapping.orderId".to_string(), "$.order.id".to_string());
    config.insert(
        "json.mapping.amount".to_string(),
        "$.order.details.amount".to_string(),
    );
    config.insert(
        "json.mapping.currency".to_string(),
        "$.order.details.currency".to_string(),
    );

    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let events = mapper.map(json_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data.len(), 3);

    // Verify extracted values (explicit mappings are sorted alphabetically: amount, currency, orderId)
    if let AttributeValue::Double(d) = events[0].data[0] {
        assert_eq!(d, 250.75);
    } else {
        panic!("Expected Double for amount");
    }

    if let AttributeValue::String(s) = &events[0].data[1] {
        assert_eq!(s, "USD");
    } else {
        panic!("Expected String for currency");
    }

    if let AttributeValue::String(s) = &events[0].data[2] {
        assert_eq!(s, "ORDER-123");
    } else {
        panic!("Expected String for orderId");
    }
}

#[test]
fn test_json_explicit_mapping_array_access() {
    let json_input = r#"{
        "order": {
            "items": [
                {"name": "Widget", "price": 10.0},
                {"name": "Gadget", "price": 20.0}
            ]
        }
    }"#;

    let mut config = HashMap::new();
    config.insert(
        "json.mapping.firstItem".to_string(),
        "$.order.items[0].name".to_string(),
    );
    config.insert(
        "json.mapping.firstPrice".to_string(),
        "$.order.items[0].price".to_string(),
    );

    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let events = mapper.map(json_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data.len(), 2);

    if let AttributeValue::String(s) = &events[0].data[0] {
        assert_eq!(s, "Widget");
    } else {
        panic!("Expected String for firstItem");
    }
}

// ============================================================================
// JSON Sink Mapping Tests
// ============================================================================

#[test]
fn test_json_sink_simple_serialization() {
    let event = Event::new_with_data(
        123456789,
        vec![
            AttributeValue::String("ORDER-123".to_string()),
            AttributeValue::Double(250.75),
            AttributeValue::Bool(true),
        ],
    );

    let config = HashMap::new();
    let factory = JsonSinkMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let result = mapper.map(&[event]).unwrap();
    let json_str = String::from_utf8(result).unwrap();

    // Verify it's valid JSON
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    assert!(parsed.is_object());
    assert!(parsed.get("_timestamp").is_some());
}

#[test]
fn test_json_sink_template_rendering() {
    let event = Event::new_with_data(
        123456789,
        vec![
            AttributeValue::String("ORDER-123".to_string()),
            AttributeValue::Double(250.75),
            AttributeValue::String("USD".to_string()),
        ],
    );

    let mut config = HashMap::new();
    config.insert(
        "json.template".to_string(),
        r#"{"orderId":"{{field_0}}","amount":{{field_1}},"currency":"{{field_2}}"}"#.to_string(),
    );

    let factory = JsonSinkMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let result = mapper.map(&[event]).unwrap();
    let json_str = String::from_utf8(result).unwrap();

    assert!(json_str.contains("\"orderId\":\"ORDER-123\""));
    assert!(json_str.contains("\"amount\":250.75"));
    assert!(json_str.contains("\"currency\":\"USD\""));

    // Verify it's valid JSON
    let _: serde_json::Value = serde_json::from_str(&json_str).unwrap();
}

#[test]
fn test_json_sink_pretty_print() {
    let event = Event::new_with_data(
        123,
        vec![
            AttributeValue::String("test".to_string()),
            AttributeValue::Int(42),
        ],
    );

    let mut config = HashMap::new();
    config.insert("json.pretty-print".to_string(), "true".to_string());

    let factory = JsonSinkMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let result = mapper.map(&[event]).unwrap();
    let json_str = String::from_utf8(result).unwrap();

    // Pretty-printed JSON should have newlines
    assert!(json_str.contains('\n'));
}

// ============================================================================
// CSV Mapping Tests
// ============================================================================

#[test]
fn test_csv_auto_mapping() {
    let csv_input = "123,250.75,Alice\n456,150.25,Bob\n789,99.99,Charlie";

    let config = HashMap::new();
    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_source_mapper("csv", &config).unwrap();

    let events = mapper.map(csv_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].data.len(), 3);
    assert_eq!(events[1].data.len(), 3);
    assert_eq!(events[2].data.len(), 3);

    // Verify type inference
    assert!(matches!(events[0].data[0], AttributeValue::Int(123)));
    assert!(matches!(events[0].data[1], AttributeValue::Double(_)));
    assert!(matches!(events[0].data[2], AttributeValue::String(_)));
}

#[test]
fn test_csv_with_header() {
    let csv_input = "orderId,amount,customer\n123,250.75,Alice\n456,150.25,Bob";

    let mut config = HashMap::new();
    config.insert("csv.has-header".to_string(), "true".to_string());

    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_source_mapper("csv", &config).unwrap();

    let events = mapper.map(csv_input.as_bytes()).unwrap();

    // Should skip header row
    assert_eq!(events.len(), 2);
}

#[test]
fn test_csv_explicit_column_mapping() {
    let csv_input = "Alice,123,250.75\nBob,456,150.25";

    let mut config = HashMap::new();
    config.insert("csv.mapping.orderId".to_string(), "1".to_string());
    config.insert("csv.mapping.amount".to_string(), "2".to_string());

    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_source_mapper("csv", &config).unwrap();

    let events = mapper.map(csv_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].data.len(), 2); // Only mapped columns
}

#[test]
fn test_csv_sink_with_header() {
    let events = vec![
        Event::new_with_data(
            123,
            vec![
                AttributeValue::Int(1),
                AttributeValue::String("Alice".to_string()),
                AttributeValue::Double(250.75),
            ],
        ),
        Event::new_with_data(
            124,
            vec![
                AttributeValue::Int(2),
                AttributeValue::String("Bob".to_string()),
                AttributeValue::Double(150.25),
            ],
        ),
    ];

    let mut config = HashMap::new();
    config.insert("csv.include-header".to_string(), "true".to_string());

    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_sink_mapper("csv", &config).unwrap();

    let result = mapper.map(&events).unwrap();
    let csv_str = String::from_utf8(result).unwrap();

    let lines: Vec<&str> = csv_str.lines().collect();
    assert_eq!(lines.len(), 3); // Header + 2 data rows
    assert!(lines[0].contains("field_0"));
    assert!(lines[1].contains("Alice"));
    assert!(lines[2].contains("Bob"));
}

#[test]
fn test_csv_custom_delimiter() {
    let csv_input = "123|250.75|Alice\n456|150.25|Bob";

    let mut config = HashMap::new();
    config.insert("csv.delimiter".to_string(), "|".to_string());

    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_source_mapper("csv", &config).unwrap();

    let events = mapper.map(csv_input.as_bytes()).unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].data.len(), 3);
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_source_rejects_template_property() {
    let mut config = HashMap::new();
    config.insert("json.template".to_string(), "{...}".to_string());

    let result = validate_source_mapper_config(&config, "json");
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(err_msg.contains("template"));
    assert!(err_msg.contains("sink-only"));
}

#[test]
fn test_sink_rejects_mapping_property() {
    let mut config = HashMap::new();
    config.insert("json.mapping.id".to_string(), "$.id".to_string());

    let result = validate_sink_mapper_config(&config, "json");
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(err_msg.contains("mapping"));
    assert!(err_msg.contains("source-only"));
}

#[test]
fn test_validation_all_or_nothing_policy() {
    // Source with mappings is valid
    let mut source_config = HashMap::new();
    source_config.insert("json.mapping.id".to_string(), "$.id".to_string());
    source_config.insert("json.mapping.amount".to_string(), "$.amount".to_string());
    assert!(validate_source_mapper_config(&source_config, "json").is_ok());

    // Source without mappings is valid
    let empty_config = HashMap::new();
    assert!(validate_source_mapper_config(&empty_config, "json").is_ok());

    // Sink with template is valid
    let mut sink_config = HashMap::new();
    sink_config.insert("json.template".to_string(), "{...}".to_string());
    assert!(validate_sink_mapper_config(&sink_config, "json").is_ok());
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_json_malformed_input() {
    let invalid_json = r#"{"invalid": json syntax"#;

    let config = HashMap::new();
    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let result = mapper.map(invalid_json.as_bytes());
    assert!(result.is_err());
}

#[test]
fn test_json_invalid_jsonpath() {
    let json_input = r#"{"order": {"id": "123"}}"#;

    let mut config = HashMap::new();
    config.insert(
        "json.mapping.orderId".to_string(),
        "$.nonexistent.field".to_string(),
    );

    let factory = JsonSourceMapperFactory;
    let mapper = factory.create_initialized(&config).unwrap();

    let result = mapper.map(json_input.as_bytes());
    assert!(result.is_err());
}

#[test]
fn test_csv_malformed_input() {
    // UTF-8 validation test
    let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];

    let config = HashMap::new();
    let registry = MapperFactoryRegistry::new();
    let mapper = registry.create_source_mapper("csv", &config).unwrap();

    let result = mapper.map(&invalid_utf8);
    assert!(result.is_err());
}

// ============================================================================
// Registry Tests
// ============================================================================

#[test]
fn test_registry_supports_all_formats() {
    let registry = MapperFactoryRegistry::new();
    let config = HashMap::new();

    // JSON source and sink
    assert!(registry.create_source_mapper("json", &config).is_ok());
    assert!(registry.create_sink_mapper("json", &config).is_ok());

    // CSV source and sink
    assert!(registry.create_source_mapper("csv", &config).is_ok());
    assert!(registry.create_sink_mapper("csv", &config).is_ok());

    // Unknown format
    assert!(registry.create_source_mapper("unknown", &config).is_err());
    assert!(registry.create_sink_mapper("unknown", &config).is_err());
}

#[test]
fn test_end_to_end_json_workflow() {
    // Step 1: Parse JSON input (fields sorted alphabetically: amount, orderId)
    let json_input = r#"{"orderId": "ORDER-123", "amount": 250.75}"#;

    let mut source_config = HashMap::new();
    source_config.insert("json.mapping.orderId".to_string(), "$.orderId".to_string());
    source_config.insert("json.mapping.amount".to_string(), "$.amount".to_string());

    let source_factory = JsonSourceMapperFactory;
    let source_mapper = source_factory.create_initialized(&source_config).unwrap();

    let events = source_mapper.map(json_input.as_bytes()).unwrap();
    assert_eq!(events.len(), 1);

    // Step 2: Process event and serialize to JSON output
    // Fields are sorted alphabetically: amount=field_0, orderId=field_1
    let mut sink_config = HashMap::new();
    sink_config.insert(
        "json.template".to_string(),
        r#"{"id":"{{field_1}}","total":{{field_0}}}"#.to_string(),
    );

    let sink_factory = JsonSinkMapperFactory;
    let sink_mapper = sink_factory.create_initialized(&sink_config).unwrap();

    let output = sink_mapper.map(&events).unwrap();
    let output_str = String::from_utf8(output).unwrap();

    // Verify output
    assert!(output_str.contains("\"id\":\"ORDER-123\""));
    assert!(output_str.contains("\"total\":250.75"));

    // Verify it's valid JSON
    let _: serde_json::Value = serde_json::from_str(&output_str).unwrap();
}

#[test]
fn test_end_to_end_csv_workflow() {
    // Step 1: Parse CSV input
    let csv_input = "123,250.75,Alice";

    let config = HashMap::new();
    let registry = MapperFactoryRegistry::new();

    let source_mapper = registry.create_source_mapper("csv", &config).unwrap();
    let events = source_mapper.map(csv_input.as_bytes()).unwrap();
    assert_eq!(events.len(), 1);

    // Step 2: Serialize to CSV output with header
    let mut sink_config = HashMap::new();
    sink_config.insert("csv.include-header".to_string(), "true".to_string());

    let sink_mapper = registry.create_sink_mapper("csv", &sink_config).unwrap();
    let output = sink_mapper.map(&events).unwrap();
    let output_str = String::from_utf8(output).unwrap();

    // Verify output
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 2); // Header + data
    assert!(lines[0].contains("field_0"));
    assert!(lines[1].contains("123"));
    assert!(lines[1].contains("250.75"));
    assert!(lines[1].contains("Alice"));
}
