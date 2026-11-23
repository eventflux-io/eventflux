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

// Top level files, ported from the Java core package
pub mod eventflux_app_runtime; // Java EventFluxAppRuntime and its Impl
pub mod eventflux_app_runtime_builder;
pub mod eventflux_manager; // Java EventFluxManager

// Sub packages, matching the Java packages
pub mod aggregation;
pub mod config;
pub mod data_source;
pub mod debugger;
pub mod distributed; // distributed processing
pub mod error; // error handling and DLQ
pub mod event;
pub mod exception; // core error types
pub mod executor;
pub mod extension;
pub mod function; // UDFs, like the Java Script
pub mod partition;
pub mod persistence;
pub mod query;
pub mod store;
pub mod stream;
pub mod table;
pub mod trigger;
pub mod util;
pub mod validation; // app validation, including DLQ and circular dependency checks
pub mod window;

// Re-export the main public types from core.
pub use self::data_source::{DataSource, DataSourceConfig, SqliteDataSource};
pub use self::error::{
    create_dlq_event, BackoffStrategy, DlqConfig, DlqFallbackStrategy, ErrorAction, ErrorConfig,
    ErrorConfigBuilder, ErrorHandler, ErrorStrategy, FailConfig, LogLevel, RetryConfig,
    SourceErrorContext, DLQ_STREAM_KEY,
};
pub use self::eventflux_app_runtime::EventFluxAppRuntime;
pub use self::eventflux_app_runtime_builder::EventFluxAppRuntimeBuilder;
pub use self::eventflux_manager::EventFluxManager;
pub use self::exception::{EventFluxError, EventFluxResult};
pub use self::validation::{
    detect_circular_dependencies, validate_dlq_reference, validate_dlq_schema,
    validate_dlq_stream_name, validate_no_recursive_dlq, validate_source_dlq_configs,
    QuerySourceExtractor,
};
// More re-exports can be added here as modules grow.
