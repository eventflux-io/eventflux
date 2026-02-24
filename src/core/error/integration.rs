// SPDX-License-Identifier: MIT OR Apache-2.0

//! # M7 Integration Utilities
//!
//! This module provides utilities for integrating M5 error handling with M7 stream initialization.
//!
//! ## Overview
//!
//! When M7 initializes streams with error handling configuration, it needs to:
//! 1. Extract error.* properties from stream WITH clause
//! 2. Lookup DLQ stream InputHandler (if error.dlq.stream specified)
//! 3. Create SourceErrorContext with proper configuration
//! 4. Pass to Source/Sink factory
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! // In M7 stream initialization:
//! let error_helper = ErrorIntegrationHelper::new(&app_runtime);
//!
//! // Extract error config from stream properties
//! if let Some(error_ctx) = error_helper.create_source_error_context(
//!     &stream_properties,
//!     "InputStream"
//! )? {
//!     // Pass error_ctx to source factory
//!     source.set_error_context(error_ctx);
//! }
//! ```

use super::source_support::{ErrorConfigBuilder, SourceErrorContext};
use crate::core::config::{FlatConfig, PropertySource};
use crate::core::stream::input::input_handler::InputHandler;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Helper for integrating error handling during stream initialization (M7)
///
/// This provides utilities for M7 to extract error configuration from stream
/// properties and lookup DLQ stream junctions.
pub struct ErrorIntegrationHelper {
    /// Lookup function for DLQ stream junctions
    dlq_lookup: Box<dyn Fn(&str) -> Option<Arc<Mutex<InputHandler>>> + Send + Sync>,
}

impl ErrorIntegrationHelper {
    /// Create a new integration helper with a DLQ lookup function
    ///
    /// # Arguments
    /// * `dlq_lookup` - Function that takes stream name and returns InputHandler if found
    ///
    /// # Example
    /// ```rust,ignore
    /// let helper = ErrorIntegrationHelper::new(|stream_name| {
    ///     app_runtime.get_stream_junction(stream_name)
    /// });
    /// ```
    pub fn new<F>(dlq_lookup: F) -> Self
    where
        F: Fn(&str) -> Option<Arc<Mutex<InputHandler>>> + Send + Sync + 'static,
    {
        Self {
            dlq_lookup: Box::new(dlq_lookup),
        }
    }

    /// Create SourceErrorContext from stream properties
    ///
    /// This is the main integration point for M7 stream initialization.
    ///
    /// # Arguments
    /// * `properties` - Stream properties from WITH clause
    /// * `stream_name` - Name of the source stream
    ///
    /// # Returns
    /// * `Ok(Some(SourceErrorContext))` - Error handling configured
    /// * `Ok(None)` - No error handling configured
    /// * `Err(String)` - Configuration error
    ///
    /// # Example
    /// ```rust,ignore
    /// // Stream WITH clause:
    /// // WITH (
    /// //   source = 'timer',
    /// //   interval = '1000',
    /// //   error.strategy = 'dlq',
    /// //   error.dlq.stream = 'ErrorStream'
    /// // )
    ///
    /// if let Some(ctx) = helper.create_source_error_context(&properties, "InputStream")? {
    ///     source.set_error_context(ctx);
    /// }
    /// ```
    pub fn create_source_error_context(
        &self,
        properties: &HashMap<String, String>,
        stream_name: &str,
    ) -> Result<Option<SourceErrorContext>, String> {
        // Check if error handling is configured
        let error_config_builder = ErrorConfigBuilder::from_properties(properties);
        if !error_config_builder.is_configured() {
            return Ok(None);
        }

        // Extract error configuration
        let error_config = error_config_builder.build()?.ok_or_else(|| {
            "Error handling configured but failed to build ErrorConfig".to_string()
        })?;

        // Lookup DLQ junction if needed
        let dlq_junction = if let Some(dlq_stream) = properties.get("error.dlq.stream") {
            match (self.dlq_lookup)(dlq_stream) {
                Some(junction) => Some(junction),
                None => {
                    return Err(format!(
                        "DLQ stream '{}' not found or not yet initialized. \
                         Ensure DLQ stream is defined before source stream '{}'",
                        dlq_stream, stream_name
                    ));
                }
            }
        } else {
            None
        };

        // Build FlatConfig for SourceErrorContext
        let mut config = FlatConfig::new();
        for (key, value) in properties {
            if key.starts_with("error.") {
                config.set(key.clone(), value.clone(), PropertySource::SqlWith);
            }
        }

        // Create SourceErrorContext
        let ctx = SourceErrorContext::from_config(&config, dlq_junction, stream_name.to_string())?;

        Ok(Some(ctx))
    }

    /// Validate error configuration in stream properties
    ///
    /// This can be called during validation phase to catch configuration errors early.
    ///
    /// # Arguments
    /// * `properties` - Stream properties from WITH clause
    /// * `stream_name` - Name of the source stream
    ///
    /// # Returns
    /// * `Ok(())` - Configuration is valid
    /// * `Err(String)` - Configuration error with details
    pub fn validate_error_config(
        &self,
        properties: &HashMap<String, String>,
        stream_name: &str,
    ) -> Result<(), String> {
        let error_config_builder = ErrorConfigBuilder::from_properties(properties);
        if !error_config_builder.is_configured() {
            return Ok(());
        }

        // Validate error configuration can be built
        let error_config = error_config_builder.build()?;
        if error_config.is_none() {
            return Err("Error handling configured but failed to build".to_string());
        }

        // Validate DLQ stream exists if specified
        if let Some(dlq_stream) = properties.get("error.dlq.stream") {
            if dlq_stream.is_empty() {
                return Err("error.dlq.stream cannot be empty".to_string());
            }

            // Check if DLQ stream exists (warning only, not fatal during validation)
            if (self.dlq_lookup)(dlq_stream).is_none() {
                // Note: During validation phase, DLQ stream might not be initialized yet
                // This is just a warning, not a fatal error
                eprintln!(
                    "Warning: DLQ stream '{}' not found for stream '{}'. \
                     Ensure it is defined before this stream.",
                    dlq_stream, stream_name
                );
            }
        }

        Ok(())
    }

    /// Check if error handling is configured in properties
    ///
    /// Quick check without full validation.
    pub fn has_error_config(properties: &HashMap<String, String>) -> bool {
        ErrorConfigBuilder::from_properties(properties).is_configured()
    }
}

/// Utility function to extract error.* properties from a property map
///
/// This is useful for factory implementations that need to pass only
/// error-related properties to error handling subsystem.
///
/// # Example
/// ```
/// use eventflux::core::error::integration::extract_error_properties;
/// use std::collections::HashMap;
///
/// let mut all_props = HashMap::new();
/// all_props.insert("source".to_string(), "timer".to_string());
/// all_props.insert("interval".to_string(), "1000".to_string());
/// all_props.insert("error.strategy".to_string(), "retry".to_string());
/// all_props.insert("error.retry.max-attempts".to_string(), "3".to_string());
///
/// let error_props = extract_error_properties(&all_props);
/// assert_eq!(error_props.len(), 2);
/// assert!(error_props.contains_key("error.strategy"));
/// ```
pub fn extract_error_properties(properties: &HashMap<String, String>) -> HashMap<String, String> {
    properties
        .iter()
        .filter(|(key, _)| key.starts_with("error."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Utility function to create FlatConfig from error properties
///
/// Convenience method for converting error properties to FlatConfig.
pub fn error_properties_to_flat_config(properties: &HashMap<String, String>) -> FlatConfig {
    let mut config = FlatConfig::new();
    for (key, value) in properties {
        if key.starts_with("error.") {
            config.set(key.clone(), value.clone(), PropertySource::SqlWith);
        }
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full DLQ junction lookup tests require runtime context
    // These tests focus on configuration extraction logic

    fn create_simple_helper() -> ErrorIntegrationHelper {
        // Simple helper that doesn't find any DLQ streams
        ErrorIntegrationHelper::new(|_stream_name| None)
    }

    #[test]
    fn test_no_error_config() {
        let helper = create_simple_helper();
        let properties = HashMap::new();

        let result = helper.create_source_error_context(&properties, "TestStream");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_error_config_without_dlq() {
        let helper = create_simple_helper();
        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "drop".to_string());

        let result = helper.create_source_error_context(&properties, "TestStream");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_error_config_with_dlq_not_found() {
        let helper = create_simple_helper();
        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "dlq".to_string());
        properties.insert(
            "error.dlq.stream".to_string(),
            "NonExistentStream".to_string(),
        );

        let result = helper.create_source_error_context(&properties, "TestStream");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("DLQ stream 'NonExistentStream' not found"));
    }

    #[test]
    fn test_validate_error_config_valid() {
        let helper = create_simple_helper();
        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "retry".to_string());

        let result = helper.validate_error_config(&properties, "TestStream");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_error_config_empty_dlq_stream() {
        let helper = create_simple_helper();
        let mut properties = HashMap::new();
        properties.insert("error.strategy".to_string(), "dlq".to_string());
        properties.insert("error.dlq.stream".to_string(), "".to_string());

        let result = helper.validate_error_config(&properties, "TestStream");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn test_has_error_config() {
        let mut properties = HashMap::new();
        assert!(!ErrorIntegrationHelper::has_error_config(&properties));

        properties.insert("error.strategy".to_string(), "drop".to_string());
        assert!(ErrorIntegrationHelper::has_error_config(&properties));
    }

    #[test]
    fn test_extract_error_properties() {
        let mut properties = HashMap::new();
        properties.insert("source".to_string(), "timer".to_string());
        properties.insert("interval".to_string(), "1000".to_string());
        properties.insert("error.strategy".to_string(), "retry".to_string());
        properties.insert("error.retry.max-attempts".to_string(), "3".to_string());

        let error_props = extract_error_properties(&properties);
        assert_eq!(error_props.len(), 2);
        assert_eq!(error_props.get("error.strategy").unwrap(), "retry");
        assert_eq!(error_props.get("error.retry.max-attempts").unwrap(), "3");
        assert!(!error_props.contains_key("source"));
    }

    #[test]
    fn test_error_properties_to_flat_config() {
        let mut properties = HashMap::new();
        properties.insert("source".to_string(), "timer".to_string());
        properties.insert("error.strategy".to_string(), "retry".to_string());
        properties.insert("error.retry.max-attempts".to_string(), "3".to_string());

        let config = error_properties_to_flat_config(&properties);
        assert!(config.contains("error.strategy"));
        assert!(config.contains("error.retry.max-attempts"));
        assert!(!config.contains("source"));
    }
}
