// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/mod.rs

// Top-level files from Java io.eventflux.core
pub mod eventflux_app_runtime; // For EventFluxAppRuntime.java (and Impl)
pub mod eventflux_app_runtime_builder;
pub mod eventflux_manager; // For EventFluxManager.java // Declare the module

// Sub-packages, corresponding to Java packages
pub mod aggregation;
pub mod config;
pub mod data_source;
pub mod debugger;
pub mod distributed; // Added for distributed processing
pub mod error; // M5: Error Handling & DLQ System
pub mod event;
pub mod exception; // For custom core-specific error types
pub mod executor;
pub mod extension;
pub mod function; // For UDFs like Script.java
pub mod partition;
pub mod persistence; // Added
pub mod query;
pub mod store;
pub mod stream;
pub mod table;
pub mod trigger;
pub mod util;
pub mod validation; // M4: 3-Phase Validation System
pub mod window;

// Re-export key public-facing structs from core
pub use self::data_source::{DataSource, DataSourceConfig, SqliteDataSource};
pub use self::error::{
    create_dlq_event, error_properties_to_flat_config, extract_error_properties, BackoffStrategy,
    DlqConfig, DlqFallbackStrategy, ErrorAction, ErrorConfig, ErrorConfigBuilder, ErrorHandler,
    ErrorIntegrationHelper, ErrorStrategy, FailConfig, LogLevel, RetryConfig, SourceErrorContext,
};
pub use self::eventflux_app_runtime::EventFluxAppRuntime;
pub use self::eventflux_app_runtime_builder::EventFluxAppRuntimeBuilder;
pub use self::eventflux_manager::EventFluxManager; // Added
pub use self::exception::{EventFluxError, EventFluxResult};
pub use self::validation::{
    detect_circular_dependencies, validate_dlq_schema, validate_dlq_stream_name,
    validate_no_recursive_dlq, QuerySourceExtractor,
};
// Other important re-exports will be added as these modules are built out.
