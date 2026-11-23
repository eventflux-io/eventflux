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

/// Main error type for EventFlux.
#[derive(Error, Debug)]
pub enum EventFluxError {
    /// Building or parsing an EventFlux app failed.
    #[error("EventFlux app creation error: {message}")]
    EventFluxAppCreation {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A running EventFlux app hit an error.
    #[error("EventFlux app runtime error: {message}")]
    EventFluxAppRuntime {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Parsing or compiling a query failed.
    #[error("Query creation error: {message}")]
    QueryCreation {
        message: String,
        query: Option<String>,
    },

    /// A running query hit an error.
    #[error("Query runtime error: {message}")]
    QueryRuntime {
        message: String,
        query_name: Option<String>,
    },

    /// Building an on demand query failed.
    #[error("On-demand query creation error: {message}")]
    OnDemandQueryCreation { message: String },

    /// Running an on demand query failed.
    #[error("On-demand query runtime error: {message}")]
    OnDemandQueryRuntime { message: String },

    #[error("Store query error: {message}")]
    StoreQuery {
        message: String,
        store_name: Option<String>,
    },

    /// A named definition was not found.
    #[error("{definition_type} definition '{name}' does not exist")]
    DefinitionNotExist {
        definition_type: String,
        name: String,
    },

    /// A named query was not found in the app.
    #[error("Query '{name}' does not exist in EventFlux app '{app_name}'")]
    QueryNotExist { name: String, app_name: String },

    /// A named attribute was not found.
    #[error("Attribute '{attribute}' does not exist in {context}")]
    NoSuchAttribute { attribute: String, context: String },

    /// A named extension was not found.
    #[error("{extension_type} extension '{name}' not found")]
    ExtensionNotFound {
        extension_type: String,
        name: String,
    },

    /// A connection to an outside system is down.
    #[error("Connection unavailable: {message}")]
    ConnectionUnavailable {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Database runtime error: {message}")]
    DatabaseRuntime {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A table or store failed.
    #[error("Queryable record table error: {message}")]
    QueryableRecordTable {
        message: String,
        table_name: Option<String>,
    },

    /// The persistence store failed.
    #[error("Persistence store error: {message}")]
    PersistenceStore {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Saving app state failed.
    #[error("Cannot persist EventFlux app state: {message}")]
    CannotPersistState {
        message: String,
        app_name: Option<String>,
    },

    /// Restoring app state failed.
    #[error("Cannot restore EventFlux app state: {message}")]
    CannotRestoreState {
        message: String,
        app_name: Option<String>,
    },

    /// Clearing app state failed.
    #[error("Cannot clear EventFlux app state: {message}")]
    CannotClearState {
        message: String,
        app_name: Option<String>,
    },

    #[error("Operation not supported: {message}")]
    OperationNotSupported {
        message: String,
        operation: Option<String>,
    },

    /// A value had the wrong data type.
    #[error("Type error: {message}")]
    TypeError {
        message: String,
        expected: Option<String>,
        actual: Option<String>,
    },

    #[error("Invalid parameter: {message}")]
    InvalidParameter {
        message: String,
        parameter: Option<String>,
        expected: Option<String>,
    },

    /// Converting data to or from an outside format failed.
    #[error("Mapping failed: {message}")]
    MappingFailed {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Configuration error: {message}")]
    Configuration {
        message: String,
        config_key: Option<String>,
    },

    /// Loading an extension failed.
    #[error("Cannot load class/extension: {message}")]
    CannotLoadClass {
        message: String,
        class_name: Option<String>,
    },

    /// The SQL parser rejected the input.
    #[error("Parse error: {message}")]
    ParseError {
        message: String,
        line: Option<usize>,
        column: Option<usize>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] crate::core::util::serialization::SerdeError),

    /// An error from rusqlite.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Sending on a channel or to a stream failed.
    #[error("Send error: {message}")]
    SendError { message: String },

    /// Events were rejected because the target stream junction is shutting down.
    ///
    /// Carries the count of rejected events so senders that do not wait for a
    /// result (query output edges) can record the loss instead of hiding it.
    #[error("Stream junction is shutting down ({count} event(s) rejected)")]
    ShutdownRejected { count: usize },

    #[error("Processor error: {message}")]
    ProcessorError {
        message: String,
        processor: Option<String>,
    },

    /// Catch all for plain message errors. Meant to be replaced by specific
    /// variants over time.
    #[error("{0}")]
    Other(String),
}

/// Result type for EventFlux operations.
pub type EventFluxResult<T> = Result<T, EventFluxError>;

impl EventFluxError {
    /// Create an `EventFluxAppCreation` error with no source.
    pub fn app_creation(message: impl Into<String>) -> Self {
        EventFluxError::EventFluxAppCreation {
            message: message.into(),
            source: None,
        }
    }

    /// Create an `EventFluxAppRuntime` error with no source.
    pub fn app_runtime(message: impl Into<String>) -> Self {
        EventFluxError::EventFluxAppRuntime {
            message: message.into(),
            source: None,
        }
    }

    /// Create a `TypeError`.
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

    /// Create an `InvalidParameter` error with no expected value.
    pub fn invalid_parameter(message: impl Into<String>, parameter: impl Into<String>) -> Self {
        EventFluxError::InvalidParameter {
            message: message.into(),
            parameter: Some(parameter.into()),
            expected: None,
        }
    }

    /// Create an `InvalidParameter` error with the expected value.
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

    /// Create a `Configuration` error with no config key.
    pub fn configuration(message: impl Into<String>) -> Self {
        EventFluxError::Configuration {
            message: message.into(),
            config_key: None,
        }
    }

    /// Create a `Configuration` error naming the config key.
    pub fn configuration_with_key(
        message: impl Into<String>,
        config_key: impl Into<String>,
    ) -> Self {
        EventFluxError::Configuration {
            message: message.into(),
            config_key: Some(config_key.into()),
        }
    }

    /// Create an `InvalidParameter` error saying a required parameter is missing.
    pub fn missing_parameter(parameter: impl Into<String>) -> Self {
        let param = parameter.into();
        EventFluxError::InvalidParameter {
            message: format!("Missing required parameter: {}", param),
            parameter: Some(param),
            expected: None,
        }
    }

    /// Create a `Configuration` error, with config key `format`, saying the
    /// extension does not support the format.
    pub fn unsupported_format(format: impl Into<String>, extension: impl Into<String>) -> Self {
        let fmt = format.into();
        let ext = extension.into();
        EventFluxError::Configuration {
            message: format!("Format '{}' is not supported by extension '{}'", fmt, ext),
            config_key: Some("format".to_string()),
        }
    }

    /// Create a `Configuration` error whose message starts with
    /// `Validation failed: `.
    pub fn validation_failed(message: impl Into<String>) -> Self {
        EventFluxError::Configuration {
            message: format!("Validation failed: {}", message.into()),
            config_key: None,
        }
    }

    /// Create an `ExtensionNotFound` error.
    pub fn extension_not_found(extension_type: impl Into<String>, name: impl Into<String>) -> Self {
        EventFluxError::ExtensionNotFound {
            extension_type: extension_type.into(),
            name: name.into(),
        }
    }

    /// Create a `DefinitionNotExist` error.
    pub fn definition_not_exist(
        definition_type: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        EventFluxError::DefinitionNotExist {
            definition_type: definition_type.into(),
            name: name.into(),
        }
    }

    /// Attach the underlying cause.
    ///
    /// Only variants with a `source` field keep it: `EventFluxAppCreation`,
    /// `EventFluxAppRuntime`, `ConnectionUnavailable`, `DatabaseRuntime`,
    /// `PersistenceStore` and `MappingFailed`. For any other variant the cause is
    /// silently dropped.
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

    /// Returns true if the error may go away on retry.
    ///
    /// These are usually caused by something temporary, such as a resource that
    /// is busy, a network problem, or a passing system state.
    ///
    /// # Returns
    /// - `true` - The condition may be temporary, so a retry may work.
    /// - `false` - The error is permanent and a retry will not help.
    ///
    /// # Categories
    ///
    /// ## Can be retried
    /// - Connection problems, with the network or a database.
    /// - Query and on demand query runtime errors.
    /// - Persistence store, store query and table errors.
    /// - IO errors, since many are temporary.
    /// - Send errors, since the receiver may come back.
    /// - Processor errors.
    ///
    /// ## Cannot be retried
    /// - Configuration and validation errors.
    /// - Missing definitions, queries, attributes or extensions.
    /// - Type mismatches and bad input.
    /// - App creation and app runtime errors.
    /// - State save, restore and clear errors.
    /// - Shutdown rejections and `Other`.
    pub fn is_retriable(&self) -> bool {
        match self {
            EventFluxError::ConnectionUnavailable { .. } => true,

            EventFluxError::DatabaseRuntime { .. } => true,
            EventFluxError::Sqlite(_) => true, // many SQLite errors are temporary, such as locks

            EventFluxError::QueryRuntime { .. } => true,
            EventFluxError::OnDemandQueryRuntime { .. } => true,

            EventFluxError::PersistenceStore { .. } => true,

            // Many IO errors are temporary, such as network problems or file locks.
            EventFluxError::Io(_) => true,

            // The receiver may become available again.
            EventFluxError::SendError { .. } => true,

            // Shutdown is final for this run of the runtime, so a retry cannot work.
            EventFluxError::ShutdownRejected { .. } => false,

            // May be temporary, depending on processor state.
            EventFluxError::ProcessorError { .. } => true,

            // May be temporary locks.
            EventFluxError::StoreQuery { .. } => true,
            EventFluxError::QueryableRecordTable { .. } => true,

            // Everything else is permanent, such as configuration or validation.
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
            EventFluxError::MappingFailed { .. } => false, // bad data format stays bad
            EventFluxError::Configuration { .. } => false,
            EventFluxError::CannotLoadClass { .. } => false,
            EventFluxError::ParseError { .. } => false,
            EventFluxError::Serialization(_) => false,
            EventFluxError::CannotPersistState { .. } => false,
            EventFluxError::CannotRestoreState { .. } => false,
            EventFluxError::CannotClearState { .. } => false,
            EventFluxError::Other(_) => false, // unknown errors are not retried, to be safe
        }
    }

    /// Returns the category used for logging and metrics.
    ///
    /// Errors that can be retried are `Transient`. Of the rest, `Configuration`,
    /// `InvalidParameter` and `TypeError` are `Configuration`, and all others are
    /// `Permanent`.
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

/// Broad kind of an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// May work on retry.
    Transient,
    /// Will not work on retry.
    Permanent,
    /// A configuration or validation problem.
    Configuration,
}

/// Wraps a `String` error as `EventFluxError::Other`, for code that still returns
/// plain string errors.
impl From<String> for EventFluxError {
    fn from(s: String) -> Self {
        EventFluxError::Other(s)
    }
}

/// Wraps a `&str` error as `EventFluxError::Other`, for code that still returns
/// plain string errors.
impl From<&str> for EventFluxError {
    fn from(s: &str) -> Self {
        EventFluxError::Other(s.to_string())
    }
}

/// Turns a `Result` with a `String` error into an [`EventFluxResult`], wrapping
/// the error as `EventFluxError::Other`.
pub trait IntoEventFluxResult<T> {
    fn into_eventflux_result(self) -> EventFluxResult<T>;
}

impl<T> IntoEventFluxResult<T> for Result<T, String> {
    fn into_eventflux_result(self) -> EventFluxResult<T> {
        self.map_err(EventFluxError::from)
    }
}
