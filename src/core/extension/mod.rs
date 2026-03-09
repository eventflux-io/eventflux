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

//! Extension System for EventFlux
//!
//! This module provides the factory traits and registration mechanisms for all
//! extension points in EventFlux. Extensions are registered in `EventFluxContext`
//! and looked up by name during query compilation.
//!
//! # Extension Types
//!
//! | Extension Type | Trait | Use Case |
//! |---------------|-------|----------|
//! | Window Processors | `WindowProcessorFactory` | `WINDOW length(10)` |
//! | Window Aggregators | `AttributeAggregatorFactory` | `sum(price)`, `avg(price)` in windows |
//! | Collection Aggregators | `CollectionAggregationFunction` | `sum(e1.price)` in patterns |
//! | Scalar Functions | `ScalarFunctionExecutor` | `convert()`, `coalesce()`, `abs()` |
//! | Sources | `SourceFactory` | Kafka, HTTP, Timer sources |
//! | Sinks | `SinkFactory` | Kafka, HTTP, Log sinks |
//! | Mappers | `SourceMapperFactory`, `SinkMapperFactory` | JSON, CSV, Avro mappers |
//! | Tables | `TableFactory` | InMemory, JDBC, Cache tables |
//!
//! # Registration
//!
//! All built-in extensions are registered in `EventFluxContext::register_default_extensions()`.
//! Custom extensions can be registered via `EventFluxContext::add_*_factory()` methods.
//!
//! # Example: Adding a Custom Aggregator
//!
//! ```ignore
//! #[derive(Debug, Clone)]
//! pub struct MedianAggregatorFactory;
//!
//! impl AttributeAggregatorFactory for MedianAggregatorFactory {
//!     fn name(&self) -> &'static str { "median" }
//!     fn create(&self) -> Box<dyn AttributeAggregatorExecutor> {
//!         Box::new(MedianAggregatorExecutor::default())
//!     }
//!     fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory> {
//!         Box::new(self.clone())
//!     }
//! }
//!
//! // Register:
//! context.add_attribute_aggregator_factory("median".to_string(), Box::new(MedianAggregatorFactory));
//! ```

pub mod example_factories;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::core::function::script::Script;
use crate::core::store::Store;

// Re-export mapper factory traits from their canonical location
pub use crate::core::stream::mapper::factory::{
    // Also export the concrete factories for convenience
    BytesSinkMapperFactory,
    BytesSourceMapperFactory,
    CsvSinkMapperFactory,
    CsvSourceMapperFactory,
    JsonSinkMapperFactory,
    JsonSourceMapperFactory,
    SinkMapperFactory,
    SourceMapperFactory,
};

use crate::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext,
};
use crate::core::query::processor::Processor;
use crate::core::query::selector::attribute::aggregator::AttributeAggregatorExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use crate::query_api::execution::query::input::handler::WindowHandler;

pub trait WindowProcessorFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String>;
    fn clone_box(&self) -> Box<dyn WindowProcessorFactory>;
}
impl Clone for Box<dyn WindowProcessorFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait AttributeAggregatorFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn create(&self) -> Box<dyn AttributeAggregatorExecutor>;
    fn clone_box(&self) -> Box<dyn AttributeAggregatorFactory>;

    /// Number of arguments expected (0 for count(), 1 for sum(x), etc.)
    fn arity(&self) -> usize {
        1
    }

    /// Compute return type based on argument types
    fn return_type(&self, arg_types: &[ApiAttributeType]) -> Result<ApiAttributeType, String> {
        // Default: return DOUBLE for numeric aggregations
        if arg_types.is_empty() {
            Ok(ApiAttributeType::LONG) // count()
        } else {
            Ok(ApiAttributeType::DOUBLE)
        }
    }

    /// Description for documentation/help
    fn description(&self) -> &str {
        ""
    }
}
impl Clone for Box<dyn AttributeAggregatorFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ============================================================================
// Collection Aggregation Function Trait
// ============================================================================

/// Trait for collection aggregation functions used in pattern queries.
///
/// Collection aggregations operate over bounded event collections created by
/// count quantifiers (e.g., `e1{3,5}`). Unlike window aggregators which use
/// incremental add/remove semantics, collection aggregations perform batch
/// computation over complete collections.
///
/// # Example
///
/// ```sql
/// FROM PATTERN (e1=FailedLogin{3,5} -> e2=AccountLocked)
/// SELECT count(e1), sum(e1.responseTime), avg(e1.responseTime)
/// ```
///
/// # Implementation
///
/// ```ignore
/// #[derive(Debug, Clone)]
/// pub struct MedianFunction;
///
/// impl CollectionAggregationFunction for MedianFunction {
///     fn name(&self) -> &'static str { "median" }
///     fn aggregate(&self, values: &[f64]) -> Option<f64> {
///         if values.is_empty() { return None; }
///         let mut sorted = values.to_vec();
///         sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
///         let mid = sorted.len() / 2;
///         Some(if sorted.len() % 2 == 0 {
///             (sorted[mid - 1] + sorted[mid]) / 2.0
///         } else {
///             sorted[mid]
///         })
///     }
///     fn return_type(&self, _: ApiAttributeType) -> ApiAttributeType {
///         ApiAttributeType::DOUBLE
///     }
///     fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
///         Box::new(self.clone())
///     }
/// }
/// ```
pub trait CollectionAggregationFunction: Debug + Send + Sync {
    /// Unique name for this aggregation function (e.g., "sum", "avg", "median")
    fn name(&self) -> &'static str;

    /// Aggregate over a slice of numeric values.
    ///
    /// Returns `None` if the slice is empty or if no valid values exist.
    /// This matches SQL NULL semantics for aggregations over empty sets.
    fn aggregate(&self, values: &[f64]) -> Option<f64>;

    /// Whether this function supports count-only mode (no attribute needed).
    ///
    /// When `true`, `count(e1)` is valid and counts events in the collection.
    /// When `false`, an attribute must be provided (e.g., `sum(e1.price)`).
    fn supports_count_only(&self) -> bool {
        false
    }

    /// Determine the return type based on the input attribute type.
    ///
    /// - `avg` always returns DOUBLE
    /// - `sum` preserves the input type (INT → LONG, FLOAT → DOUBLE)
    /// - `min`/`max` preserve the input type
    fn return_type(&self, input_type: ApiAttributeType) -> ApiAttributeType;

    /// Clone this function for registry storage.
    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction>;

    /// Description for documentation/help
    fn description(&self) -> &str {
        ""
    }
}

impl Clone for Box<dyn CollectionAggregationFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ============================================================================
// Built-in Collection Aggregation Functions
// ============================================================================

/// Collection count function: `count(e1)` - counts events in collection
#[derive(Debug, Clone)]
pub struct CollectionCountFunction;

impl CollectionAggregationFunction for CollectionCountFunction {
    fn name(&self) -> &'static str {
        "count"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        Some(values.len() as f64)
    }

    fn supports_count_only(&self) -> bool {
        true
    }

    fn return_type(&self, _input_type: ApiAttributeType) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Counts events in a pattern collection"
    }
}

/// Collection sum function: `sum(e1.price)` - sums attribute values
#[derive(Debug, Clone)]
pub struct CollectionSumFunction;

impl CollectionAggregationFunction for CollectionSumFunction {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum())
        }
    }

    fn return_type(&self, input_type: ApiAttributeType) -> ApiAttributeType {
        match input_type {
            ApiAttributeType::INT | ApiAttributeType::LONG => ApiAttributeType::LONG,
            _ => ApiAttributeType::DOUBLE,
        }
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Sums attribute values across a pattern collection"
    }
}

/// Collection average function: `avg(e1.price)` - averages attribute values
#[derive(Debug, Clone)]
pub struct CollectionAvgFunction;

impl CollectionAggregationFunction for CollectionAvgFunction {
    fn name(&self) -> &'static str {
        "avg"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }

    fn return_type(&self, _input_type: ApiAttributeType) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Averages attribute values across a pattern collection"
    }
}

/// Collection min function: `min(e1.price)` - finds minimum value
#[derive(Debug, Clone)]
pub struct CollectionMinFunction;

impl CollectionAggregationFunction for CollectionMinFunction {
    fn name(&self) -> &'static str {
        "min"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        values.iter().copied().reduce(f64::min)
    }

    fn return_type(&self, input_type: ApiAttributeType) -> ApiAttributeType {
        input_type
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Finds minimum attribute value in a pattern collection"
    }
}

/// Collection max function: `max(e1.price)` - finds maximum value
#[derive(Debug, Clone)]
pub struct CollectionMaxFunction;

impl CollectionAggregationFunction for CollectionMaxFunction {
    fn name(&self) -> &'static str {
        "max"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        values.iter().copied().reduce(f64::max)
    }

    fn return_type(&self, input_type: ApiAttributeType) -> ApiAttributeType {
        input_type
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Finds maximum attribute value in a pattern collection"
    }
}

/// Collection standard deviation function: `stdDev(e1.price)`
#[derive(Debug, Clone)]
pub struct CollectionStdDevFunction;

impl CollectionAggregationFunction for CollectionStdDevFunction {
    fn name(&self) -> &'static str {
        "stdDev"
    }

    fn aggregate(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let n = values.len() as f64;
        let mean: f64 = values.iter().sum::<f64>() / n;
        let sum_sq_diff: f64 = values.iter().map(|x| (x - mean).powi(2)).sum();
        Some((sum_sq_diff / n).sqrt())
    }

    fn return_type(&self, _input_type: ApiAttributeType) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_box(&self) -> Box<dyn CollectionAggregationFunction> {
        Box::new(self.clone())
    }

    fn description(&self) -> &str {
        "Computes population standard deviation across a pattern collection"
    }
}

pub trait SourceFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;

    /// List supported formats for this source extension
    /// Example: &["json", "avro", "bytes"]
    /// NO DEFAULT - Must be explicitly implemented
    fn supported_formats(&self) -> &[&str];

    /// List required configuration properties
    /// Example: &["kafka.brokers", "kafka.topic"]
    /// NO DEFAULT - Must be explicitly implemented
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["kafka.consumer.group", "kafka.security.protocol"]
    /// NO DEFAULT - Must be explicitly implemented
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use Source instance
    /// Validates configuration and returns error if invalid
    /// Source is guaranteed to be in valid state upon successful return
    /// NO DEFAULT - Must be explicitly implemented
    fn create_initialized(
        &self,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<
        Box<dyn crate::core::stream::input::source::Source>,
        crate::core::exception::EventFluxError,
    >;

    /// Legacy create method for backward compatibility
    /// Deprecated: Use create_initialized instead
    /// Returns None if factory requires configuration parameters
    fn create(&self) -> Option<Box<dyn crate::core::stream::input::source::Source>> {
        self.create_initialized(&std::collections::HashMap::new())
            .ok()
    }

    fn clone_box(&self) -> Box<dyn SourceFactory>;
}
impl Clone for Box<dyn SourceFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait SinkFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;

    /// List supported formats for this sink extension
    /// Example: &["json", "csv", "bytes"]
    /// NO DEFAULT - Must be explicitly implemented
    fn supported_formats(&self) -> &[&str];

    /// List required configuration properties
    /// Example: &["http.url", "http.method"]
    /// NO DEFAULT - Must be explicitly implemented
    fn required_parameters(&self) -> &[&str];

    /// List optional configuration properties
    /// Example: &["http.headers", "http.timeout"]
    /// NO DEFAULT - Must be explicitly implemented
    fn optional_parameters(&self) -> &[&str];

    /// Create a fully initialized, ready-to-use Sink instance
    /// Validates configuration and returns error if invalid
    /// Sink is guaranteed to be in valid state upon successful return
    /// NO DEFAULT - Must be explicitly implemented
    fn create_initialized(
        &self,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<
        Box<dyn crate::core::stream::output::sink::Sink>,
        crate::core::exception::EventFluxError,
    >;

    /// Legacy create method for backward compatibility
    /// Deprecated: Use create_initialized instead
    /// Returns None if factory requires configuration parameters
    fn create(&self) -> Option<Box<dyn crate::core::stream::output::sink::Sink>> {
        self.create_initialized(&std::collections::HashMap::new())
            .ok()
    }

    fn clone_box(&self) -> Box<dyn SinkFactory>;
}
impl Clone for Box<dyn SinkFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait StoreFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn create(&self) -> Box<dyn Store>;
    fn clone_box(&self) -> Box<dyn StoreFactory>;
}
impl Clone for Box<dyn StoreFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// SourceMapperFactory and SinkMapperFactory are re-exported from
// crate::core::stream::mapper::factory at the top of this module

pub trait TableFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn create(
        &self,
        table_name: String,
        properties: std::collections::HashMap<String, String>,
        ctx: Arc<crate::core::config::eventflux_context::EventFluxContext>,
    ) -> Result<Arc<dyn crate::core::table::Table>, String>;
    fn clone_box(&self) -> Box<dyn TableFactory>;
}
impl Clone for Box<dyn TableFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

pub trait ScriptFactory: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn create(&self) -> Box<dyn Script>;
    fn clone_box(&self) -> Box<dyn ScriptFactory>;
}
impl Clone for Box<dyn ScriptFactory> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug, Clone)]
pub struct TimerSourceFactory;

impl SourceFactory for TimerSourceFactory {
    fn name(&self) -> &'static str {
        "timer"
    }

    fn supported_formats(&self) -> &[&str] {
        // Timer uses internal binary passthrough format (no external format needed)
        // When no format is specified, PassthroughMapper is automatically used
        &[]
    }

    fn required_parameters(&self) -> &[&str] {
        &[] // Timer has no required parameters
    }

    fn optional_parameters(&self) -> &[&str] {
        &["timer.interval"] // Interval in milliseconds
    }

    fn create_initialized(
        &self,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<
        Box<dyn crate::core::stream::input::source::Source>,
        crate::core::exception::EventFluxError,
    > {
        // Parse optional interval parameter
        let interval_ms = if let Some(interval_str) = config.get("timer.interval") {
            interval_str.parse::<u64>().map_err(|_| {
                crate::core::exception::EventFluxError::invalid_parameter_with_details(
                    "timer.interval must be a valid integer",
                    "timer.interval",
                    "positive integer (milliseconds)",
                )
            })?
        } else {
            1000 // Default 1 second
        };

        Ok(Box::new(
            crate::core::stream::input::source::timer_source::TimerSource::new(interval_ms),
        ))
    }

    fn clone_box(&self) -> Box<dyn SourceFactory> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LogSinkFactory;

impl SinkFactory for LogSinkFactory {
    fn name(&self) -> &'static str {
        "log"
    }

    fn supported_formats(&self) -> &[&str] {
        &["json", "text", "csv", "bytes"] // Log sink accepts all formats
    }

    fn required_parameters(&self) -> &[&str] {
        &[] // Log sink has no required parameters
    }

    fn optional_parameters(&self) -> &[&str] {
        &["log.prefix", "log.level"] // Optional logging parameters
    }

    fn create_initialized(
        &self,
        _config: &std::collections::HashMap<String, String>,
    ) -> Result<
        Box<dyn crate::core::stream::output::sink::Sink>,
        crate::core::exception::EventFluxError,
    > {
        // LogSink doesn't need configuration validation for now
        // Future: could add log level validation, prefix customization, etc.
        Ok(Box::new(crate::core::stream::output::sink::LogSink::new()))
    }

    fn clone_box(&self) -> Box<dyn SinkFactory> {
        Box::new(self.clone())
    }
}

/// FFI callback type used when dynamically loading extensions.
pub type RegisterFn = unsafe extern "C" fn(&crate::core::eventflux_manager::EventFluxManager);

/// Symbol names looked up by [`EventFluxManager::set_extension`].
pub const REGISTER_EXTENSION_FN: &[u8] = b"register_extension";
pub const REGISTER_WINDOWS_FN: &[u8] = b"register_windows";
pub const REGISTER_FUNCTIONS_FN: &[u8] = b"register_functions";
pub const REGISTER_SOURCES_FN: &[u8] = b"register_sources";
pub const REGISTER_SINKS_FN: &[u8] = b"register_sinks";
pub const REGISTER_STORES_FN: &[u8] = b"register_stores";
pub const REGISTER_SOURCE_MAPPERS_FN: &[u8] = b"register_source_mappers";
pub const REGISTER_SINK_MAPPERS_FN: &[u8] = b"register_sink_mappers";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_context::EventFluxContext;
    use crate::core::extension::example_factories::{HttpSinkFactory, KafkaSourceFactory};

    #[test]
    fn test_factory_registration() {
        let context = EventFluxContext::new();

        // Test built-in factory registration
        let factory = context.get_source_factory("timer");
        assert!(factory.is_some());
        assert_eq!(factory.unwrap().name(), "timer");

        let sink_factory = context.get_sink_factory("log");
        assert!(sink_factory.is_some());
        assert_eq!(sink_factory.unwrap().name(), "log");
    }

    #[test]
    fn test_kafka_source_factory_registration() {
        let context = EventFluxContext::new();
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));

        let factory = context.get_source_factory("kafka");
        assert!(factory.is_some());
        assert_eq!(factory.unwrap().name(), "kafka");
    }

    #[test]
    fn test_format_support_validation() {
        // Timer uses internal binary format only (no external formats)
        let factory = TimerSourceFactory;
        assert!(
            factory.supported_formats().is_empty(),
            "Timer should not support external formats (uses binary passthrough)"
        );

        // Kafka supports external formats
        let kafka_factory = KafkaSourceFactory;
        assert!(kafka_factory.supported_formats().contains(&"json"));
        assert!(kafka_factory.supported_formats().contains(&"avro"));
        assert!(!kafka_factory.supported_formats().contains(&"xml"));
    }

    #[test]
    fn test_create_initialized_missing_params() {
        let factory = KafkaSourceFactory;
        let config = std::collections::HashMap::new();

        let result = factory.create_initialized(&config);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Missing required parameter"));
    }

    #[test]
    fn test_create_initialized_invalid_config() {
        let factory = KafkaSourceFactory;
        let mut config = std::collections::HashMap::new();
        config.insert("kafka.bootstrap.servers".to_string(), "".to_string());
        config.insert("kafka.topic".to_string(), "test".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("cannot be empty"));
    }

    #[test]
    fn test_create_initialized_valid_config() {
        let factory = KafkaSourceFactory;
        let mut config = std::collections::HashMap::new();
        config.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        config.insert("kafka.topic".to_string(), "test-topic".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_timer_source_factory_interval_config() {
        let factory = TimerSourceFactory;
        let mut config = std::collections::HashMap::new();
        config.insert("timer.interval".to_string(), "5000".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_timer_source_factory_invalid_interval() {
        let factory = TimerSourceFactory;
        let mut config = std::collections::HashMap::new();
        config.insert("timer.interval".to_string(), "invalid".to_string());

        let result = factory.create_initialized(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_http_sink_factory_format_support() {
        let factory = HttpSinkFactory;
        assert!(factory.supported_formats().contains(&"json"));
        assert!(factory.supported_formats().contains(&"xml"));
        assert!(!factory.supported_formats().contains(&"avro"));
    }

    #[test]
    fn test_http_sink_factory_required_params() {
        let factory = HttpSinkFactory;
        assert_eq!(factory.required_parameters(), &["http.url"]);
    }

    #[test]
    fn test_http_sink_factory_create() {
        let factory = HttpSinkFactory;
        let mut config = std::collections::HashMap::new();
        config.insert(
            "http.url".to_string(),
            "http://localhost:8080/events".to_string(),
        );

        let result = factory.create_initialized(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_factory_lookup_and_validation() {
        let context = EventFluxContext::new();
        context.add_source_factory("kafka".to_string(), Box::new(KafkaSourceFactory));

        // 1. Look up source factory by extension
        let source_factory = context.get_source_factory("kafka");
        assert!(source_factory.is_some());
        let source_factory = source_factory.unwrap();

        // 2. Validate format support
        let format = "json";
        assert!(source_factory.supported_formats().contains(&format));

        // 3. Create fully initialized instances
        let mut source_config = std::collections::HashMap::new();
        source_config.insert(
            "kafka.bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        source_config.insert("kafka.topic".to_string(), "test".to_string());

        let source = source_factory.create_initialized(&source_config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_unsupported_format_detection() {
        let factory = KafkaSourceFactory;
        let format = "xml";

        // Kafka doesn't support XML format
        assert!(!factory.supported_formats().contains(&format));
    }

    #[test]
    fn test_sink_factory_parameters() {
        let factory = LogSinkFactory;
        assert_eq!(factory.required_parameters(), &[] as &[&str]);
        assert!(factory.optional_parameters().contains(&"log.prefix"));
        assert!(factory.optional_parameters().contains(&"log.level"));
    }

    #[test]
    fn test_source_factory_parameters() {
        let factory = KafkaSourceFactory;
        assert!(factory
            .required_parameters()
            .contains(&"kafka.bootstrap.servers"));
        assert!(factory.required_parameters().contains(&"kafka.topic"));
        assert!(factory
            .optional_parameters()
            .contains(&"kafka.consumer.group"));
        assert!(factory.optional_parameters().contains(&"kafka.timeout"));
    }

    #[test]
    fn test_factory_clone() {
        let factory = TimerSourceFactory;
        let cloned = factory.clone_box();
        assert_eq!(cloned.name(), "timer");
        assert_eq!(cloned.supported_formats(), factory.supported_formats());
    }

    #[test]
    fn test_collection_aggregation_functions_registered() {
        let context = EventFluxContext::new();

        // Verify all built-in collection aggregation functions are registered
        let names = context.list_collection_aggregation_function_names();
        assert!(names.contains(&"count".to_string()));
        assert!(names.contains(&"sum".to_string()));
        assert!(names.contains(&"avg".to_string()));
        assert!(names.contains(&"min".to_string()));
        assert!(names.contains(&"max".to_string()));
        assert!(names.contains(&"stdDev".to_string()));
    }

    #[test]
    fn test_collection_aggregation_function_lookup() {
        let context = EventFluxContext::new();

        // Test lookup and execution
        let sum_fn = context.get_collection_aggregation_function("sum");
        assert!(sum_fn.is_some());
        let sum_fn = sum_fn.unwrap();
        assert_eq!(sum_fn.name(), "sum");
        assert_eq!(sum_fn.aggregate(&[1.0, 2.0, 3.0, 4.0]), Some(10.0));

        let avg_fn = context.get_collection_aggregation_function("avg");
        assert!(avg_fn.is_some());
        let avg_fn = avg_fn.unwrap();
        assert_eq!(avg_fn.name(), "avg");
        assert_eq!(avg_fn.aggregate(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));

        let count_fn = context.get_collection_aggregation_function("count");
        assert!(count_fn.is_some());
        let count_fn = count_fn.unwrap();
        assert!(count_fn.supports_count_only());
        assert_eq!(count_fn.aggregate(&[1.0, 2.0, 3.0]), Some(3.0));

        // Test non-existent function returns None
        let unknown = context.get_collection_aggregation_function("unknown");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_collection_aggregation_function_edge_cases() {
        let context = EventFluxContext::new();

        // Test empty input
        let sum_fn = context.get_collection_aggregation_function("sum").unwrap();
        assert_eq!(sum_fn.aggregate(&[]), None);

        let avg_fn = context.get_collection_aggregation_function("avg").unwrap();
        assert_eq!(avg_fn.aggregate(&[]), None);

        // Count on empty returns 0
        let count_fn = context
            .get_collection_aggregation_function("count")
            .unwrap();
        assert_eq!(count_fn.aggregate(&[]), Some(0.0));

        // Test min/max
        let min_fn = context.get_collection_aggregation_function("min").unwrap();
        assert_eq!(min_fn.aggregate(&[5.0, 2.0, 8.0, 1.0]), Some(1.0));
        assert_eq!(min_fn.aggregate(&[]), None);

        let max_fn = context.get_collection_aggregation_function("max").unwrap();
        assert_eq!(max_fn.aggregate(&[5.0, 2.0, 8.0, 1.0]), Some(8.0));
        assert_eq!(max_fn.aggregate(&[]), None);
    }

    #[test]
    fn test_window_factories_registered() {
        let context = EventFluxContext::new();

        // Verify all built-in window factories are registered
        let names = context.list_window_factory_names();
        assert!(names.contains(&"length".to_string()));
        assert!(names.contains(&"time".to_string()));
        assert!(names.contains(&"lengthBatch".to_string()));
        assert!(names.contains(&"timeBatch".to_string()));
        assert!(names.contains(&"externalTime".to_string()));
        assert!(names.contains(&"externalTimeBatch".to_string()));
        assert!(names.contains(&"session".to_string()));
        assert!(names.contains(&"sort".to_string()));
        assert!(names.contains(&"lossyCounting".to_string()));
        assert!(names.contains(&"cron".to_string()));
    }

    #[test]
    fn test_attribute_aggregators_registered() {
        let context = EventFluxContext::new();

        // Verify all built-in aggregators are registered
        let names = context.list_attribute_aggregator_names();
        assert!(names.contains(&"sum".to_string()));
        assert!(names.contains(&"avg".to_string()));
        assert!(names.contains(&"count".to_string()));
        assert!(names.contains(&"min".to_string()));
        assert!(names.contains(&"max".to_string()));
        assert!(names.contains(&"distinctCount".to_string()));
        assert!(names.contains(&"minforever".to_string()));
        assert!(names.contains(&"maxforever".to_string()));
    }
}
