// SPDX-License-Identifier: MIT OR Apache-2.0

//! # Error Handling & Dead-Letter Queue (DLQ) System
//!
//! This module provides comprehensive error handling for EventFlux with multiple strategies,
//! retry mechanisms with configurable backoff, and dead-letter queue support.
//!
//! ## Error Strategies
//!
//! Four primary error handling strategies are supported:
//!
//! 1. **Drop** (default): Log error and discard the failed event
//! 2. **Retry**: Retry with exponential/linear/fixed backoff
//! 3. **DLQ**: Send failed events to a dead-letter queue stream
//! 4. **Fail**: Terminate application immediately on error
//!
//! ## Configuration Example
//!
//! ### Retry Strategy
//! ```toml
//! [application]
//! error.strategy = "retry"
//! error.retry.max-attempts = 3
//! error.retry.backoff = "exponential"
//! error.retry.initial-delay = "100ms"
//! error.retry.max-delay = "30s"
//! ```
//!
//! ### DLQ Strategy
//! ```toml
//! [application]
//! error.strategy = "dlq"
//! error.dlq.stream = "ErrorStream"
//! error.dlq.fallback-strategy = "log"
//! ```
//!
//! ### DLQ with Fallback Retry
//! ```toml
//! [application]
//! error.strategy = "dlq"
//! error.dlq.stream = "ErrorStream"
//! error.dlq.fallback-strategy = "retry"
//! error.dlq.fallback-retry.max-attempts = 5
//! error.dlq.fallback-retry.initial-delay = "200ms"
//! ```
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use eventflux::core::error::*;
//! use eventflux::core::config::FlatConfig;
//!
//! // Parse error configuration
//! let mut config = FlatConfig::new();
//! config.set("error.strategy", "retry", PropertySource::SqlWith);
//! config.set("error.retry.max-attempts", "5", PropertySource::SqlWith);
//!
//! let error_config = ErrorConfig::from_flat_config(&config)?;
//!
//! // Apply retry logic
//! if let Some(retry_config) = error_config.retry_config() {
//!     for attempt in 1..=retry_config.max_attempts {
//!         match process_event() {
//!             Ok(result) => break,
//!             Err(e) if attempt < retry_config.max_attempts => {
//!                 let delay = retry_config.calculate_delay(attempt);
//!                 thread::sleep(delay);
//!             }
//!             Err(e) => {
//!                 // Max retries exceeded, apply fallback
//!                 handle_final_failure(e);
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## DLQ Event Schema
//!
//! Failed events sent to DLQ streams follow this exact schema:
//! - `originalEvent` STRING - Serialized original event as JSON
//! - `errorMessage` STRING - Human-readable error description
//! - `errorType` STRING - EventFluxError variant name
//! - `timestamp` BIGINT - Milliseconds since epoch
//! - `attemptCount` INT - Number of retry attempts made
//! - `streamName` STRING - Source stream name

pub mod config;
pub mod dlq;
pub mod handler;
pub mod integration;
pub mod retry;
pub mod source_support;
pub mod strategy;

// Re-export main types for convenience
pub use config::{ErrorConfig, FailConfig, LogLevel};
pub use dlq::{create_dlq_event, DlqConfig, DlqFallbackStrategy};
pub use handler::{ErrorAction, ErrorHandler};
pub use integration::{
    error_properties_to_flat_config, extract_error_properties, ErrorIntegrationHelper,
};
pub use retry::{
    calculate_backoff, exponential_backoff, linear_backoff, parse_duration, BackoffStrategy,
    RetryConfig,
};
pub use source_support::{ErrorConfigBuilder, SourceErrorContext};
pub use strategy::ErrorStrategy;
