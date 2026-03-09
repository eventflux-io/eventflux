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

use thiserror::Error;

/// Main error type for EventFlux operations
#[derive(Error, Debug)]
pub enum EventFluxError {
    /// Errors that occur during EventFlux app creation and parsing
    #[error("EventFlux app creation error: {message}")]
    EventFluxAppCreation {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Runtime errors during query execution
    #[error("EventFlux app runtime error: {message}")]
    EventFluxAppRuntime {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Errors in query parsing and compilation
    #[error("Query creation error: {message}")]
    QueryCreation {
        message: String,
        query: Option<String>,
    },

    /// Runtime errors in query execution
    #[error("Query runtime error: {message}")]
    QueryRuntime {
        message: String,
        query_name: Option<String>,
    },

    /// Errors in on-demand query creation
    #[error("On-demand query creation error: {message}")]
    OnDemandQueryCreation { message: String },

    /// Runtime errors in on-demand query execution
    #[error("On-demand query runtime error: {message}")]
    OnDemandQueryRuntime { message: String },

    /// Store query errors
    #[error("Store query error: {message}")]
    StoreQuery {
        message: String,
        store_name: Option<String>,
    },

    /// Definition not found errors
    #[error("{definition_type} definition '{name}' does not exist")]
    DefinitionNotExist {
        definition_type: String,
        name: String,
    },

    /// Query not found errors
    #[error("Query '{name}' does not exist in EventFlux app '{app_name}'")]
    QueryNotExist { name: String, app_name: String },

    /// Attribute not found errors
    #[error("Attribute '{attribute}' does not exist in {context}")]
    NoSuchAttribute { attribute: String, context: String },

    /// Extension not found errors
    #[error("{extension_type} extension '{name}' not found")]
    ExtensionNotFound {
        extension_type: String,
        name: String,
    },

    /// Connection unavailable errors
    #[error("Connection unavailable: {message}")]
    ConnectionUnavailable {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Database runtime errors
    #[error("Database runtime error: {message}")]
    DatabaseRuntime {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Table/Store errors
    #[error("Queryable record table error: {message}")]
    QueryableRecordTable {
        message: String,
        table_name: Option<String>,
    },

    /// Persistence errors
    #[error("Persistence store error: {message}")]
    PersistenceStore {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// State persistence errors
    #[error("Cannot persist EventFlux app state: {message}")]
    CannotPersistState {
        message: String,
        app_name: Option<String>,
    },

    /// State restoration errors
    #[error("Cannot restore EventFlux app state: {message}")]
    CannotRestoreState {
        message: String,
        app_name: Option<String>,
    },

    /// State clearing errors
    #[error("Cannot clear EventFlux app state: {message}")]
    CannotClearState {
        message: String,
        app_name: Option<String>,
    },

    /// Operation not supported errors
    #[error("Operation not supported: {message}")]
    OperationNotSupported {
        message: String,
        operation: Option<String>,
    },

    /// Data type errors
    #[error("Type error: {message}")]
    TypeError {
        message: String,
        expected: Option<String>,
        actual: Option<String>,
    },

    /// Invalid parameter errors
    #[error("Invalid parameter: {message}")]
    InvalidParameter {
        message: String,
        parameter: Option<String>,
        expected: Option<String>,
    },

    /// Mapping/Marshalling errors
    #[error("Mapping failed: {message}")]
    MappingFailed {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        config_key: Option<String>,
    },

    /// Class loading errors (for extensions)
    #[error("Cannot load class/extension: {message}")]
    CannotLoadClass {
        message: String,
        class_name: Option<String>,
    },

    /// Parser errors from SQL parser
    #[error("Parse error: {message}")]
    ParseError {
        message: String,
        line: Option<usize>,
        column: Option<usize>,
    },

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// Database errors from rusqlite
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Send errors for channels
    #[error("Send error: {message}")]
    SendError { message: String },

    /// Processor errors
    #[error("Processor error: {message}")]
    ProcessorError {
        message: String,
        processor: Option<String>,
    },

    /// Generic other errors (to be phased out)
    #[error("{0}")]
    Other(String),
}

/// Result type alias for EventFlux operations
pub type EventFluxResult<T> = Result<T, EventFluxError>;

impl EventFluxError {
    /// Create a new EventFluxAppCreation error
    pub fn app_creation(message: impl Into<String>) -> Self {
        EventFluxError::EventFluxAppCreation {
            message: message.into(),
            source: None,
        }
    }

    /// Create a new EventFluxAppRuntime error
    pub fn app_runtime(message: impl Into<String>) -> Self {
        EventFluxError::EventFluxAppRuntime {
            message: message.into(),
            source: None,
        }
    }

    /// Create a new TypeError
    pub fn type_error(
        message: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        EventFluxError::TypeError {
            message: message.into(),
            expected: Some(expected.into()),
            actual: Some(actual.into()),
        }
    }

    /// Create a new InvalidParameter error
    pub fn invalid_parameter(message: impl Into<String>, parameter: impl Into<String>) -> Self {
        EventFluxError::InvalidParameter {
            message: message.into(),
            parameter: Some(parameter.into()),
            expected: None,
        }
    }

    /// Create a new InvalidParameter error with expected value
    pub fn invalid_parameter_with_details(
        message: impl Into<String>,
        parameter: impl Into<String>,
        expected: impl Into<String>,
    ) -> Self {
        EventFluxError::InvalidParameter {
            message: message.into(),
            parameter: Some(parameter.into()),
            expected: Some(expected.into()),
        }
    }

    /// Create a new Configuration error
    pub fn configuration(message: impl Into<String>) -> Self {
        EventFluxError::Configuration {
            message: message.into(),
            config_key: None,
        }
    }

    /// Create a new Configuration error with config key
    pub fn configuration_with_key(
        message: impl Into<String>,
        config_key: impl Into<String>,
    ) -> Self {
        EventFluxError::Configuration {
            message: message.into(),
            config_key: Some(config_key.into()),
        }
    }

    /// Create a missing parameter error (uses InvalidParameter variant)
    pub fn missing_parameter(parameter: impl Into<String>) -> Self {
        let param = parameter.into();
        EventFluxError::InvalidParameter {
            message: format!("Missing required parameter: {}", param),
            parameter: Some(param),
            expected: None,
        }
    }

    /// Create an unsupported format error (uses Configuration variant)
    pub fn unsupported_format(format: impl Into<String>, extension: impl Into<String>) -> Self {
        let fmt = format.into();
        let ext = extension.into();
        EventFluxError::Configuration {
            message: format!("Format '{}' is not supported by extension '{}'", fmt, ext),
            config_key: Some("format".to_string()),
        }
    }

    /// Create a validation failed error (uses Configuration variant)
    pub fn validation_failed(message: impl Into<String>) -> Self {
        EventFluxError::Configuration {
            message: format!("Validation failed: {}", message.into()),
            config_key: None,
        }
    }

    /// Create a new ExtensionNotFound error
    pub fn extension_not_found(extension_type: impl Into<String>, name: impl Into<String>) -> Self {
        EventFluxError::ExtensionNotFound {
            extension_type: extension_type.into(),
            name: name.into(),
        }
    }

    /// Create a new DefinitionNotExist error
    pub fn definition_not_exist(
        definition_type: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        EventFluxError::DefinitionNotExist {
            definition_type: definition_type.into(),
            name: name.into(),
        }
    }

    /// Add source error context
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        match &mut self {
            EventFluxError::EventFluxAppCreation { source: src, .. }
            | EventFluxError::EventFluxAppRuntime { source: src, .. }
            | EventFluxError::ConnectionUnavailable { source: src, .. }
            | EventFluxError::DatabaseRuntime { source: src, .. }
            | EventFluxError::PersistenceStore { source: src, .. }
            | EventFluxError::MappingFailed { source: src, .. } => {
                *src = Some(Box::new(source));
            }
            _ => {}
        }
        self
    }

    /// Determine if this error is retriable (transient)
    ///
    /// Retriable errors are those that may succeed on retry, typically due to
    /// temporary resource unavailability, network issues, or transient system states.
    ///
    /// # Returns
    /// - `true` - Error is retriable (temporary condition)
    /// - `false` - Error is permanent (retry will not help)
    ///
    /// # Categories
    ///
    /// ## Retriable (Transient)
    /// - Connection issues (network, database)
    /// - Temporary resource unavailability
    /// - Transient runtime errors
    /// - IO errors (many are transient)
    /// - Send errors (channels may become available)
    ///
    /// ## Non-Retriable (Permanent)
    /// - Configuration errors
    /// - Validation errors
    /// - Missing definitions/extensions
    /// - Type mismatches
    /// - Malformed input
    pub fn is_retriable(&self) -> bool {
        match self {
            // Transient network/connection errors
            EventFluxError::ConnectionUnavailable { .. } => true,

            // Transient database errors
            EventFluxError::DatabaseRuntime { .. } => true,
            EventFluxError::Sqlite(_) => true, // Many SQLite errors are transient (locks, etc.)

            // Transient query runtime errors
            EventFluxError::QueryRuntime { .. } => true,
            EventFluxError::OnDemandQueryRuntime { .. } => true,

            // Transient storage/persistence errors
            EventFluxError::PersistenceStore { .. } => true,

            // IO errors (many are transient - network, file locks, etc.)
            EventFluxError::Io(_) => true,

            // Channel send errors (receiver may become available)
            EventFluxError::SendError { .. } => true,

            // Processor errors (may be transient depending on state)
            EventFluxError::ProcessorError { .. } => true,

            // Store query errors (may be transient locks)
            EventFluxError::StoreQuery { .. } => true,
            EventFluxError::QueryableRecordTable { .. } => true,

            // All other errors are permanent (configuration, validation, etc.)
            EventFluxError::EventFluxAppCreation { .. } => false,
            EventFluxError::EventFluxAppRuntime { .. } => false,
            EventFluxError::QueryCreation { .. } => false,
            EventFluxError::OnDemandQueryCreation { .. } => false,
            EventFluxError::DefinitionNotExist { .. } => false,
            EventFluxError::QueryNotExist { .. } => false,
            EventFluxError::NoSuchAttribute { .. } => false,
            EventFluxError::ExtensionNotFound { .. } => false,
            EventFluxError::OperationNotSupported { .. } => false,
            EventFluxError::TypeError { .. } => false,
            EventFluxError::InvalidParameter { .. } => false,
            EventFluxError::MappingFailed { .. } => false, // Data format issues are permanent
            EventFluxError::Configuration { .. } => false,
            EventFluxError::CannotLoadClass { .. } => false,
            EventFluxError::ParseError { .. } => false,
            EventFluxError::Serialization(_) => false,
            EventFluxError::CannotPersistState { .. } => false,
            EventFluxError::CannotRestoreState { .. } => false,
            EventFluxError::CannotClearState { .. } => false,
            EventFluxError::Other(_) => false, // Conservative: unknown errors are not retriable
        }
    }

    /// Get error category for logging and metrics
    pub fn category(&self) -> ErrorCategory {
        if self.is_retriable() {
            ErrorCategory::Transient
        } else {
            match self {
                EventFluxError::Configuration { .. }
                | EventFluxError::InvalidParameter { .. }
                | EventFluxError::TypeError { .. } => ErrorCategory::Configuration,
                _ => ErrorCategory::Permanent,
            }
        }
    }
}

/// Error category for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Transient error that may succeed on retry
    Transient,
    /// Permanent error that will not succeed on retry
    Permanent,
    /// Configuration/validation error
    Configuration,
}

/// Convert String errors to EventFluxError::Other (for backward compatibility)
impl From<String> for EventFluxError {
    fn from(s: String) -> Self {
        EventFluxError::Other(s)
    }
}

/// Convert &str errors to EventFluxError::Other (for backward compatibility)
impl From<&str> for EventFluxError {
    fn from(s: &str) -> Self {
        EventFluxError::Other(s.to_string())
    }
}

/// Extension trait for converting Results with String errors to EventFluxError
pub trait IntoEventFluxResult<T> {
    fn into_eventflux_result(self) -> EventFluxResult<T>;
}

impl<T> IntoEventFluxResult<T> for Result<T, String> {
    fn into_eventflux_result(self) -> EventFluxResult<T> {
        self.map_err(EventFluxError::from)
    }
}
