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

//! # Configuration Management
//!
//! This module provides comprehensive configuration management for EventFlux applications.
//!
//! ## Configuration Reload Policy
//!
//! **Note**: Hot reload functionality is not supported in this implementation.
//!
//! To apply configuration changes:
//! 1. Enable state persistence in your EventFlux application
//! 2. Gracefully shutdown the current runtime
//! 3. Update configuration files or environment variables
//! 4. Restart the application with new configuration
//! 5. State will be automatically restored, ensuring continuity
//!
//! This approach maintains data consistency and reliable state recovery.

// Context modules
pub mod eventflux_app_context;
pub mod eventflux_context;
pub mod eventflux_on_demand_query_context;
pub mod eventflux_query_context;
pub mod statistics_configuration;

// Configuration management modules
pub mod element_config_resolver;
pub mod error;
pub mod loader;
pub mod manager;
pub mod monitoring;
pub mod override_utils;
pub mod processor_config_reader;
pub mod resolver;
pub mod security;
pub mod service_discovery;
pub mod stream_config;
pub mod toml_config;
pub mod types;
pub mod validation_api;
pub mod validator;

// Re-export context types
pub use self::eventflux_app_context::EventFluxAppContext;
pub use self::eventflux_context::EventFluxContext;
pub use self::eventflux_on_demand_query_context::EventFluxOnDemandQueryContext;
pub use self::eventflux_query_context::EventFluxQueryContext;
pub use self::statistics_configuration::StatisticsConfiguration;

// Re-export main configuration types for easy access
pub use element_config_resolver::{
    ConfigSource, ElementConfigResolver, ElementType, ResolvedElementConfig,
    ResolvedElementConfigBuilder,
};
pub use error::{ConfigError, ConfigResult, ValidationError, ValidationResult};
pub use manager::ConfigManager;
pub use override_utils::apply_config_overrides;
pub use processor_config_reader::{ConfigValue, ProcessorConfigReader};
pub use stream_config::{FlatConfig, PropertySource, StreamType, StreamTypeConfig};
pub use toml_config::{
    load_toml_config, substitute_env_vars, substitute_toml_env_vars, LoadedConfig, TomlConfig,
    TomlStreamConfig, TomlTableConfig,
};
pub use types::*;

// Main configuration loading functions for simple usage
use std::path::Path;

/// Load configuration from the default locations with automatic environment detection
pub async fn load_config() -> ConfigResult<EventFluxConfig> {
    let manager = ConfigManager::new();
    manager.load_unified_config().await
}

/// Load configuration from a specific YAML file
pub async fn load_config_from_file<P: AsRef<Path>>(path: P) -> ConfigResult<EventFluxConfig> {
    let manager = ConfigManager::new();
    manager.load_from_file(path).await
}

/// Load configuration with custom manager settings
pub async fn load_config_with_manager(manager: ConfigManager) -> ConfigResult<EventFluxConfig> {
    manager.load_unified_config().await
}

// Re-exporting placeholders from eventflux_context.rs for now if they are widely used,
// though ideally they'd be replaced by actual types from their own modules.
// Example:
// pub use self::eventflux_context::{
//     PersistenceStorePlaceholder,
//     DataSourcePlaceholder,
//     // etc.
// };
// For EventFluxAppContext placeholders:
// pub use self::eventflux_app_context::{
//     StatisticsManagerPlaceholder,
//     TimestampGeneratorPlaceholder,
//     // etc.
// };
