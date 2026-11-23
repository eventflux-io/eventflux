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

//! DLQ (Dead Letter Queue) Validation
//!
//! Implements Phase 1 and Phase 2 validation for DLQ configurations:
//! - Phase 1: DLQ stream name existence validation
//! - Phase 2: DLQ schema validation and recursive restriction validation

use crate::core::config::{ElementType, ResolvedElementConfig};
use crate::core::error::DLQ_STREAM_KEY;
use crate::core::exception::{EventFluxError, EventFluxResult};
use crate::query_api::definition::attribute::Type as AttributeType;
use crate::query_api::definition::StreamDefinition;
use std::collections::HashMap;
use std::sync::Arc;

/// Required DLQ stream schema fields
///
/// DLQ streams MUST have exactly these fields with exact types:
/// - originalEvent: STRING (serialized original event)
/// - errorMessage: STRING (error description)
/// - errorType: STRING (error classification)
/// - timestamp: LONG (error occurrence time)
/// - attemptCount: INT (number of retry attempts)
/// - streamName: STRING (source stream name)
const REQUIRED_DLQ_FIELDS: &[(&str, AttributeType)] = &[
    ("originalEvent", AttributeType::STRING),
    ("errorMessage", AttributeType::STRING),
    ("errorType", AttributeType::STRING),
    ("timestamp", AttributeType::LONG),
    ("attemptCount", AttributeType::INT),
    ("streamName", AttributeType::STRING),
];

/// Validate DLQ stream schema against required fields
///
/// **Phase 2 Validation** (Application Initialization)
///
/// Validates that the DLQ stream has the exact required schema:
/// - Exact field count (no more, no less)
/// - All required fields present with correct types
/// - No extra fields beyond requirements
///
/// # Arguments
/// * `stream_name` - Name of the stream using the DLQ
/// * `dlq_stream_name` - Name of the DLQ stream
/// * `parsed_streams` - Map of all parsed stream definitions
///
/// # Returns
/// * `Ok(())` if schema is valid
/// * `Err(EventFluxError)` if schema validation fails
///
/// # Example Valid DLQ Stream
/// ```sql
/// CREATE STREAM OrderErrors (
///     originalEvent STRING,
///     errorMessage STRING,
///     errorType STRING,
///     timestamp LONG,
///     attemptCount INT,
///     streamName STRING
/// );
/// ```
pub fn validate_dlq_schema(
    stream_name: &str,
    dlq_stream_name: &str,
    parsed_streams: &HashMap<String, Arc<StreamDefinition>>,
) -> EventFluxResult<()> {
    // 1. Look up DLQ stream schema from parsed CREATE STREAM definition
    let dlq_stream = parsed_streams.get(dlq_stream_name).ok_or_else(|| {
        EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{}' not found for stream '{}'",
                dlq_stream_name, stream_name
            ),
            DLQ_STREAM_KEY,
        )
    })?;

    // 2. Extract actual schema from DLQ stream definition. A duplicated field
    // would collapse in the map and slip past the count check, while DLQ
    // events always carry exactly one value per required field.
    let mut actual_fields: HashMap<String, AttributeType> = HashMap::new();
    for attr in &dlq_stream.abstract_definition.attribute_list {
        if actual_fields
            .insert(attr.name.clone(), attr.attribute_type)
            .is_some()
        {
            return Err(EventFluxError::configuration_with_key(
                format!(
                    "DLQ stream '{}' declares field '{}' more than once",
                    dlq_stream_name, attr.name
                ),
                DLQ_STREAM_KEY,
            ));
        }
    }

    // 3. Validate exact field count (no more, no less)
    if actual_fields.len() != REQUIRED_DLQ_FIELDS.len() {
        return Err(EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{}' has {} fields, but exactly {} required (originalEvent, errorMessage, errorType, timestamp, attemptCount, streamName)",
                dlq_stream_name,
                actual_fields.len(),
                REQUIRED_DLQ_FIELDS.len()
            ),
            DLQ_STREAM_KEY,
        ));
    }

    // 4. Validate each required field present with correct type
    for (required_name, required_type) in REQUIRED_DLQ_FIELDS {
        match actual_fields.get(*required_name) {
            Some(actual_type) if actual_type == required_type => {
                // ✅ Field present with correct type
            }
            Some(actual_type) => {
                return Err(EventFluxError::configuration_with_key(
                    format!(
                        "DLQ stream '{}' field '{}' has type {:?}, expected {:?}",
                        dlq_stream_name, required_name, actual_type, required_type
                    ),
                    DLQ_STREAM_KEY,
                ));
            }
            None => {
                return Err(EventFluxError::configuration_with_key(
                    format!(
                        "DLQ stream '{}' missing required field '{} {:?}'",
                        dlq_stream_name, required_name, required_type
                    ),
                    DLQ_STREAM_KEY,
                ));
            }
        }
    }

    // 5. Check for extra fields not in required schema
    for actual_name in actual_fields.keys() {
        if !REQUIRED_DLQ_FIELDS
            .iter()
            .any(|(req_name, _)| req_name == actual_name)
        {
            return Err(EventFluxError::configuration_with_key(
                format!(
                    "DLQ stream '{}' has extra field '{}' not in required schema",
                    dlq_stream_name, actual_name
                ),
                DLQ_STREAM_KEY,
            ));
        }
    }

    // ✅ Schema validation passed
    Ok(())
}

/// Validate DLQ stream name exists in parsed streams
///
/// **Phase 1 Validation** (Parse-Time)
///
/// Checks that the referenced DLQ stream has been defined.
/// Schema validation happens in Phase 2.
///
/// # Arguments
/// * `stream_name` - Name of the stream using the DLQ
/// * `dlq_stream_name` - Name of the DLQ stream to validate
/// * `parsed_streams` - Map of all parsed stream definitions
///
/// # Returns
/// * `Ok(())` if DLQ stream exists
/// * `Err(EventFluxError)` if DLQ stream not found
pub fn validate_dlq_stream_name(
    stream_name: &str,
    dlq_stream_name: &str,
    parsed_streams: &HashMap<String, Arc<StreamDefinition>>,
) -> EventFluxResult<()> {
    if !parsed_streams.contains_key(dlq_stream_name) {
        return Err(EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{}' referenced by stream '{}' does not exist",
                dlq_stream_name, stream_name
            ),
            DLQ_STREAM_KEY,
        ));
    }

    Ok(())
}

/// Validate DLQ stream does not have its own DLQ (no recursive error handling)
///
/// **Phase 2 Validation** (Application Initialization)
///
/// DLQ streams cannot have their own DLQ to prevent infinite error loops.
///
/// # Arguments
/// * `stream_name` - Name of the stream being validated
/// * `dlq_config_key` - The stream's own `error.dlq.stream` value, if configured
/// * `is_dlq_stream` - Whether this stream is itself a DLQ for another stream
///
/// # Returns
/// * `Ok(())` if validation passes
/// * `Err(EventFluxError)` if DLQ stream has its own DLQ
pub fn validate_no_recursive_dlq(
    stream_name: &str,
    dlq_config_key: Option<&str>,
    is_dlq_stream: bool,
) -> EventFluxResult<()> {
    if is_dlq_stream && dlq_config_key.is_some() {
        return Err(EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{}' cannot have its own DLQ (recursive error handling not allowed)",
                stream_name
            ),
            DLQ_STREAM_KEY,
        ));
    }

    Ok(())
}

/// Validate one source stream's DLQ reference
///
/// **Phase 2 Validation** (Application Initialization)
///
/// Runs every DLQ rule for `stream_name -> dlq_stream_name`: the DLQ stream
/// exists, has the exact DLQ schema, and does not route its own failures to
/// another DLQ. A stream naming itself as its DLQ fails the recursion rule.
///
/// # Arguments
/// * `stream_name` - Name of the stream using the DLQ
/// * `dlq_stream_name` - Name of the DLQ stream
/// * `dlq_stream_own_dlq` - The DLQ stream's own `error.dlq.stream`, if any
/// * `stream_definitions` - Map of all stream definitions in the app
pub fn validate_dlq_reference(
    stream_name: &str,
    dlq_stream_name: &str,
    dlq_stream_own_dlq: Option<&str>,
    stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
) -> EventFluxResult<()> {
    validate_dlq_stream_name(stream_name, dlq_stream_name, stream_definitions)?;
    validate_dlq_schema(stream_name, dlq_stream_name, stream_definitions)?;
    validate_no_recursive_dlq(dlq_stream_name, dlq_stream_own_dlq, true)
}

/// The DLQ stream that `stream`'s source routes failures to, if its resolved
/// configuration sets `error.dlq.stream` (a source-only property)
pub fn source_dlq_stream<'a>(
    resolved_configs: &'a HashMap<String, ResolvedElementConfig>,
    stream: &str,
) -> Option<&'a str> {
    resolved_configs
        .get(&ElementType::Source.config_key(stream))
        .and_then(|config| config.get(DLQ_STREAM_KEY))
        .map(String::as_str)
}

/// Validate the DLQ reference of every source stream in an app
///
/// **Phase 2 Validation** (Application Initialization)
///
/// Checks each source whose resolved configuration (YAML + SQL WITH) sets
/// `error.dlq.stream`, using [`validate_dlq_reference`]. Runs whatever the
/// error strategy, since an explicit stream reference that is wrong is a
/// configuration mistake either way. Every violation is reported, not just
/// the first, in stream-name order.
///
/// # Arguments
/// * `resolved_configs` - Resolved element configs keyed by
///   [`ElementType::config_key`]
/// * `stream_definitions` - Map of all stream definitions in the app
pub fn validate_source_dlq_configs(
    resolved_configs: &HashMap<String, ResolvedElementConfig>,
    stream_definitions: &HashMap<String, Arc<StreamDefinition>>,
) -> Result<(), Vec<EventFluxError>> {
    let dlq_of = |stream: &str| source_dlq_stream(resolved_configs, stream);

    let mut stream_names: Vec<&String> = stream_definitions.keys().collect();
    stream_names.sort();

    let errors: Vec<EventFluxError> = stream_names
        .into_iter()
        .filter_map(|stream| {
            let dlq_stream = dlq_of(stream)?;
            validate_dlq_reference(stream, dlq_stream, dlq_of(dlq_stream), stream_definitions).err()
        })
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::definition::StreamDefinition;

    fn create_valid_dlq_stream() -> StreamDefinition {
        StreamDefinition::new("ValidDLQ".to_string())
            .attribute("originalEvent".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("timestamp".to_string(), AttributeType::LONG)
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("streamName".to_string(), AttributeType::STRING)
    }

    #[test]
    fn test_validate_dlq_schema_valid() {
        let mut parsed_streams = HashMap::new();
        parsed_streams.insert(
            "OrderErrors".to_string(),
            Arc::new(create_valid_dlq_stream()),
        );

        let result = validate_dlq_schema("Orders", "OrderErrors", &parsed_streams);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_dlq_schema_missing_field() {
        let stream_def = StreamDefinition::new("IncompleteDLQ".to_string())
            .attribute("originalEvent".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING);
        // Missing other required fields

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("IncompleteDLQ".to_string(), Arc::new(stream_def));

        let result = validate_dlq_schema("Orders", "IncompleteDLQ", &parsed_streams);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("has 2 fields"));
        assert!(err_msg.contains("exactly 6 required"));
    }

    #[test]
    fn test_validate_dlq_schema_wrong_type() {
        let stream_def = StreamDefinition::new("WrongTypeDLQ".to_string())
            .attribute("originalEvent".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("timestamp".to_string(), AttributeType::INT) // Should be LONG
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("streamName".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("WrongTypeDLQ".to_string(), Arc::new(stream_def));

        let result = validate_dlq_schema("Orders", "WrongTypeDLQ", &parsed_streams);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("timestamp"));
        assert!(err_msg.contains("has type INT, expected LONG"));
    }

    #[test]
    fn test_validate_dlq_schema_duplicate_field() {
        // All six required fields plus a second originalEvent: the duplicate
        // must not collapse into a six-field schema that passes
        let stream_def =
            create_valid_dlq_stream().attribute("originalEvent".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("DupDLQ".to_string(), Arc::new(stream_def));

        let err = validate_dlq_schema("Orders", "DupDLQ", &parsed_streams)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("declares field 'originalEvent' more than once"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validate_dlq_schema_extra_field() {
        let mut stream_def = create_valid_dlq_stream();
        stream_def = stream_def.attribute("extraField".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("ExtraDLQ".to_string(), Arc::new(stream_def));

        let result = validate_dlq_schema("Orders", "ExtraDLQ", &parsed_streams);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("has 7 fields"));
        assert!(err_msg.contains("exactly 6 required"));
    }

    #[test]
    fn test_validate_dlq_schema_wrong_field_name() {
        let stream_def = StreamDefinition::new("WrongNameDLQ".to_string())
            .attribute("original_event".to_string(), AttributeType::STRING) // Wrong name
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("timestamp".to_string(), AttributeType::LONG)
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("streamName".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("WrongNameDLQ".to_string(), Arc::new(stream_def));

        let result = validate_dlq_schema("Orders", "WrongNameDLQ", &parsed_streams);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("missing required field 'originalEvent"));
    }

    #[test]
    fn test_validate_dlq_stream_name_exists() {
        let mut parsed_streams = HashMap::new();
        parsed_streams.insert(
            "OrderErrors".to_string(),
            Arc::new(create_valid_dlq_stream()),
        );

        let result = validate_dlq_stream_name("Orders", "OrderErrors", &parsed_streams);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_dlq_stream_name_not_found() {
        let parsed_streams = HashMap::new();

        let result = validate_dlq_stream_name("Orders", "NonExistentDLQ", &parsed_streams);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("NonExistentDLQ"));
        assert!(err_msg.contains("does not exist"));
    }

    #[test]
    fn test_validate_no_recursive_dlq_normal_stream() {
        // Normal stream with DLQ is ok
        let result = validate_no_recursive_dlq("Orders", Some("error.dlq.stream"), false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_no_recursive_dlq_stream_without_dlq() {
        // DLQ stream without its own DLQ is ok
        let result = validate_no_recursive_dlq("OrderErrors", None, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_no_recursive_dlq_fails() {
        // DLQ stream with its own DLQ should fail
        let result = validate_no_recursive_dlq("OrderErrors", Some("error.dlq.stream"), true);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("OrderErrors"));
        assert!(err_msg.contains("recursive error handling not allowed"));
    }

    #[test]
    fn test_dlq_schema_field_order_independence() {
        // Fields can be in any order as long as all are present
        let stream_def = StreamDefinition::new("ReorderedDLQ".to_string())
            .attribute("streamName".to_string(), AttributeType::STRING)
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("timestamp".to_string(), AttributeType::LONG)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("originalEvent".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("ReorderedDLQ".to_string(), Arc::new(stream_def));

        let result = validate_dlq_schema("Orders", "ReorderedDLQ", &parsed_streams);
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // validate_dlq_reference / validate_source_dlq_configs
    // ------------------------------------------------------------------

    fn plain_stream(name: &str) -> Arc<StreamDefinition> {
        Arc::new(
            StreamDefinition::new(name.to_string())
                .attribute("id".to_string(), AttributeType::STRING),
        )
    }

    /// Streams: `Orders`, `Payments` (plain) and `OrderErrors` (valid DLQ)
    fn app_streams() -> HashMap<String, Arc<StreamDefinition>> {
        HashMap::from([
            ("Orders".to_string(), plain_stream("Orders")),
            ("Payments".to_string(), plain_stream("Payments")),
            (
                "OrderErrors".to_string(),
                Arc::new(create_valid_dlq_stream()),
            ),
        ])
    }

    fn config_with_dlq(element_type: ElementType, dlq: &str) -> ResolvedElementConfig {
        let mut config = ResolvedElementConfig::new(element_type);
        config
            .properties
            .insert(DLQ_STREAM_KEY.to_string(), dlq.to_string());
        config
    }

    fn source_with_dlq(stream: &str, dlq: &str) -> (String, ResolvedElementConfig) {
        (
            ElementType::Source.config_key(stream),
            config_with_dlq(ElementType::Source, dlq),
        )
    }

    fn error_messages(errors: Vec<EventFluxError>) -> Vec<String> {
        errors.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn test_validate_dlq_reference_valid() {
        let result = validate_dlq_reference("Orders", "OrderErrors", None, &app_streams());
        assert!(result.is_ok(), "got: {result:?}");
    }

    #[test]
    fn test_validate_dlq_reference_missing_stream() {
        let err = validate_dlq_reference("Orders", "OrderDeadLetters", None, &app_streams())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'OrderDeadLetters'"), "got: {err}");
        assert!(err.contains("does not exist"), "got: {err}");
    }

    #[test]
    fn test_validate_dlq_reference_bad_schema() {
        let err = validate_dlq_reference("Orders", "Payments", None, &app_streams())
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly 6 required"), "got: {err}");
    }

    #[test]
    fn test_validate_dlq_reference_recursive() {
        let err = validate_dlq_reference("Orders", "OrderErrors", Some("Other"), &app_streams())
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot have its own DLQ"), "got: {err}");
    }

    #[test]
    fn test_validate_source_dlq_configs_valid() {
        let resolved = HashMap::from([source_with_dlq("Orders", "OrderErrors")]);
        assert!(validate_source_dlq_configs(&resolved, &app_streams()).is_ok());
    }

    #[test]
    fn test_validate_source_dlq_configs_without_dlq() {
        let resolved = HashMap::from([(
            ElementType::Source.config_key("Orders"),
            ResolvedElementConfig::new(ElementType::Source),
        )]);
        assert!(validate_source_dlq_configs(&resolved, &app_streams()).is_ok());
    }

    #[test]
    fn test_validate_source_dlq_configs_ignores_non_source_configs() {
        // error.dlq.stream is a source property; other element kinds never
        // consume it, so it is not validated for them
        let resolved = HashMap::from([(
            ElementType::Sink.config_key("Orders"),
            config_with_dlq(ElementType::Sink, "Missing"),
        )]);
        assert!(validate_source_dlq_configs(&resolved, &app_streams()).is_ok());
    }

    #[test]
    fn test_validate_source_dlq_configs_rejects_dlq_with_own_dlq() {
        // OrderErrors is a valid DLQ for Orders, but is itself a source that
        // routes failures to Payments: recursive error handling
        let resolved = HashMap::from([
            source_with_dlq("Orders", "OrderErrors"),
            source_with_dlq("OrderErrors", "Payments"),
        ]);
        let errors =
            error_messages(validate_source_dlq_configs(&resolved, &app_streams()).unwrap_err());
        assert!(
            errors
                .iter()
                .any(|e| e.contains("'OrderErrors' cannot have its own DLQ")),
            "got: {errors:?}"
        );
    }

    #[test]
    fn test_validate_source_dlq_configs_rejects_self_reference() {
        let resolved = HashMap::from([source_with_dlq("OrderErrors", "OrderErrors")]);
        let errors =
            error_messages(validate_source_dlq_configs(&resolved, &app_streams()).unwrap_err());
        assert_eq!(errors.len(), 1, "got: {errors:?}");
        assert!(
            errors[0].contains("cannot have its own DLQ"),
            "got: {errors:?}"
        );
    }

    #[test]
    fn test_validate_source_dlq_configs_reports_every_error_in_stream_order() {
        let resolved = HashMap::from([
            source_with_dlq("Payments", "Missing"),
            source_with_dlq("Orders", "Payments"),
        ]);
        let errors =
            error_messages(validate_source_dlq_configs(&resolved, &app_streams()).unwrap_err());
        assert_eq!(errors.len(), 2, "got: {errors:?}");
        assert!(errors[0].contains("exactly 6 required"), "got: {errors:?}");
        assert!(errors[1].contains("'Missing'"), "got: {errors:?}");
    }
}
