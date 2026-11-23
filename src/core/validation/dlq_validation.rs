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
//! Phase 1 and Phase 2 checks for DLQ configuration.
//! - Phase 1: the DLQ stream exists
//! - Phase 2: the DLQ stream has the right schema and has no DLQ of its own

use crate::core::config::{ElementType, ResolvedElementConfig};
use crate::core::error::{DLQ_FIELDS, DLQ_STREAM_KEY};
use crate::core::exception::{EventFluxError, EventFluxResult};
use crate::query_api::definition::StreamDefinition;
use std::collections::HashMap;
use std::sync::Arc;

/// The required DLQ schema as SQL column declarations, such as
/// `originalEvent STRING, errorMessage STRING, ...`. Used in error messages.
fn required_dlq_schema() -> String {
    DLQ_FIELDS
        .iter()
        .map(|(name, attribute_type)| format!("{name} {attribute_type:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Check the DLQ stream schema against [`DLQ_FIELDS`]
///
/// **Phase 2 Validation** (Application Start)
///
/// The DLQ stream must declare exactly the DLQ fields, with the exact types, in
/// the exact order. DLQ events carry their values by position, always in
/// `DLQ_FIELDS` order, so any other order would put values under the wrong
/// attributes. Duplicate and extra fields fail the count or position checks.
///
/// # Arguments
/// * `stream_name` - Name of the stream using the DLQ
/// * `dlq_stream_name` - Name of the DLQ stream
/// * `parsed_streams` - Map of all parsed stream definitions
///
/// # Returns
/// * `Ok(())` if the schema is valid
/// * `Err(EventFluxError)` keyed by `error.dlq.stream` if the DLQ stream is missing
///   or its schema is wrong. The message lists the required schema.
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
    // 1. Find the DLQ stream's CREATE STREAM definition
    let dlq_stream = parsed_streams.get(dlq_stream_name).ok_or_else(|| {
        EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{}' not found for stream '{}'",
                dlq_stream_name, stream_name
            ),
            DLQ_STREAM_KEY,
        )
    })?;

    let schema_error = |detail: String| {
        EventFluxError::configuration_with_key(
            format!(
                "DLQ stream '{dlq_stream_name}' {detail}. DLQ streams must declare exactly, \
                 in this order: {}",
                required_dlq_schema()
            ),
            DLQ_STREAM_KEY,
        )
    };

    // 2. Exact field count (no more, no less)
    let attributes = &dlq_stream.abstract_definition.attribute_list;
    if attributes.len() != DLQ_FIELDS.len() {
        return Err(schema_error(format!(
            "has {} fields, but exactly {} required",
            attributes.len(),
            DLQ_FIELDS.len()
        )));
    }

    // 3. Each position holds the required field with the required type
    for (position, (attr, (required_name, required_type))) in
        attributes.iter().zip(DLQ_FIELDS).enumerate()
    {
        if attr.name != *required_name {
            return Err(schema_error(format!(
                "field {} is '{}', expected '{}'",
                position + 1,
                attr.name,
                required_name
            )));
        }
        if attr.attribute_type != *required_type {
            return Err(schema_error(format!(
                "field '{}' has type {:?}, expected {:?}",
                required_name, attr.attribute_type, required_type
            )));
        }
    }

    Ok(())
}

/// Check that the DLQ stream is defined
///
/// **Phase 1 Validation** (Parse Time)
///
/// Only checks that the stream exists. The schema is checked in Phase 2.
///
/// # Arguments
/// * `stream_name` - Name of the stream using the DLQ
/// * `dlq_stream_name` - Name of the DLQ stream to validate
/// * `parsed_streams` - Map of all parsed stream definitions
///
/// # Returns
/// * `Ok(())` if the DLQ stream exists
/// * `Err(EventFluxError)` keyed by `error.dlq.stream` if it does not
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

/// Check that a DLQ stream has no DLQ of its own
///
/// **Phase 2 Validation** (Application Start)
///
/// A DLQ stream may not have its own DLQ, so failures cannot loop forever.
///
/// # Arguments
/// * `stream_name` - Name of the stream being validated
/// * `dlq_config_key` - The stream's own `error.dlq.stream` value, if configured
/// * `is_dlq_stream` - Whether this stream is itself a DLQ for another stream
///
/// # Returns
/// * `Ok(())` if the check passes
/// * `Err(EventFluxError)` keyed by `error.dlq.stream` if a DLQ stream has its own DLQ
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

/// Check one source stream's DLQ reference
///
/// **Phase 2 Validation** (Application Start)
///
/// Runs every DLQ rule for `stream_name -> dlq_stream_name`. The DLQ stream must
/// exist, have the exact DLQ schema, and not send its own failures to another
/// DLQ. A stream that names itself as its DLQ fails the last rule. Returns the
/// first rule that fails.
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

/// The DLQ stream that `stream`'s source sends failures to, if its resolved
/// source configuration sets `error.dlq.stream`. Only sources have this property.
pub fn source_dlq_stream<'a>(
    resolved_configs: &'a HashMap<String, ResolvedElementConfig>,
    stream: &str,
) -> Option<&'a str> {
    resolved_configs
        .get(&ElementType::Source.config_key(stream))
        .and_then(|config| config.get(DLQ_STREAM_KEY))
        .map(String::as_str)
}

/// Check the DLQ reference of every source stream in an app
///
/// **Phase 2 Validation** (Application Start)
///
/// Checks each source whose resolved configuration, YAML merged with SQL WITH,
/// sets `error.dlq.stream`, using [`validate_dlq_reference`]. Runs whatever the
/// error strategy is, because a wrong stream reference is a configuration
/// mistake either way. Reports one error for each bad stream, not just the
/// first, sorted by stream name.
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
    use crate::query_api::definition::attribute::Type as AttributeType;
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
        // All six required fields plus a second originalEvent. The duplicate must
        // not collapse into a six field schema that passes.
        let stream_def =
            create_valid_dlq_stream().attribute("originalEvent".to_string(), AttributeType::STRING);
        // Six fields, but originalEvent twice in place of streamName
        let six_with_duplicate = StreamDefinition::new("DupSixDLQ".to_string())
            .attribute("originalEvent".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("timestamp".to_string(), AttributeType::LONG)
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("originalEvent".to_string(), AttributeType::STRING);

        let parsed_streams = HashMap::from([
            ("DupDLQ".to_string(), Arc::new(stream_def)),
            ("DupSixDLQ".to_string(), Arc::new(six_with_duplicate)),
        ]);

        let err = validate_dlq_schema("Orders", "DupDLQ", &parsed_streams)
            .unwrap_err()
            .to_string();
        assert!(err.contains("has 7 fields"), "got: {err}");

        let err = validate_dlq_schema("Orders", "DupSixDLQ", &parsed_streams)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("field 6 is 'originalEvent', expected 'streamName'"),
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
        assert!(
            err_msg.contains("field 1 is 'original_event', expected 'originalEvent'"),
            "got: {err_msg}"
        );
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
    fn test_dlq_schema_rejects_noncanonical_field_order() {
        // DLQ events carry values by position, always in DLQ_FIELDS order, so the
        // right fields in another order would get swapped values
        let stream_def = StreamDefinition::new("ReorderedDLQ".to_string())
            .attribute("streamName".to_string(), AttributeType::STRING)
            .attribute("attemptCount".to_string(), AttributeType::INT)
            .attribute("timestamp".to_string(), AttributeType::LONG)
            .attribute("errorType".to_string(), AttributeType::STRING)
            .attribute("errorMessage".to_string(), AttributeType::STRING)
            .attribute("originalEvent".to_string(), AttributeType::STRING);

        let mut parsed_streams = HashMap::new();
        parsed_streams.insert("ReorderedDLQ".to_string(), Arc::new(stream_def));

        let err = validate_dlq_schema("Orders", "ReorderedDLQ", &parsed_streams)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("field 1 is 'streamName', expected 'originalEvent'"),
            "got: {err}"
        );
        assert!(
            err.contains("in this order: originalEvent STRING, errorMessage STRING"),
            "error spells out the required schema, got: {err}"
        );
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
        // error.dlq.stream is a source property. Other element kinds never read
        // it, so it is not checked for them.
        let resolved = HashMap::from([(
            ElementType::Sink.config_key("Orders"),
            config_with_dlq(ElementType::Sink, "Missing"),
        )]);
        assert!(validate_source_dlq_configs(&resolved, &app_streams()).is_ok());
    }

    #[test]
    fn test_validate_source_dlq_configs_rejects_dlq_with_own_dlq() {
        // OrderErrors is a valid DLQ for Orders, but it is also a source that sends
        // its own failures to Payments, which is not allowed
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
