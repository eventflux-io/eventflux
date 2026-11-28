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

//! # Error handling and dead letter queue (DLQ)
//!
//! Decides what happens to an event that fails. Supports four strategies, retries
//! with configurable backoff, and a dead letter queue, which is a stream that
//! collects failed events for later inspection.
//!
//! ## Error strategies
//!
//! 1. **Drop**, the default. Log the error and discard the event.
//! 2. **Retry**. Retry with exponential, linear or fixed backoff. When retries run
//!    out, or the error cannot be retried, the event goes to the DLQ if
//!    `error.dlq.stream` is set and is dropped otherwise.
//! 3. **DLQ**. Send the failed event to the DLQ stream.
//! 4. **Fail**. Stop the source on the first error.
//!
//! ## Configuration example
//!
//! ### Retry strategy
//! ```toml
//! [application]
//! error.strategy = "retry"
//! error.retry.max-attempts = 3
//! error.retry.backoff = "exponential"
//! error.retry.initial-delay = "100ms"
//! error.retry.max-delay = "30s"
//! ```
//!
//! ### DLQ strategy
//! ```toml
//! [application]
//! error.strategy = "dlq"
//! error.dlq.stream = "ErrorStream"
//! error.dlq.fallback-strategy = "log"
//! ```
//!
//! ### DLQ with fallback retry
//!
//! The fallback applies when the DLQ send itself fails.
//! ```toml
//! [application]
//! error.strategy = "dlq"
//! error.dlq.stream = "ErrorStream"
//! error.dlq.fallback-strategy = "retry"
//! error.dlq.fallback-retry.max-attempts = 5
//! error.dlq.fallback-retry.initial-delay = "200ms"
//! ```
//!
//! ## Usage example
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
//! ## DLQ event schema
//!
//! Events are positional, so a DLQ stream must declare exactly these attributes in
//! this order (see [`DLQ_FIELDS`]):
//! - `originalEvent` STRING. The original event serialized as JSON.
//! - `errorMessage` STRING. Readable error text.
//! - `errorType` STRING. The `EventFluxError` variant name.
//! - `timestamp` BIGINT. When the error was recorded, in milliseconds since the
//!   Unix epoch.
//! - `attemptCount` INT. Attempts made, counting the first one.
//! - `streamName` STRING. Source stream name.

pub mod config;
pub mod dlq;
pub mod handler;
pub mod retry;
pub mod source_support;
pub mod strategy;

// The main types, exported from here.
pub use config::{ErrorConfig, FailConfig, LogLevel};
pub use dlq::{
    create_dlq_event, DlqConfig, DlqFallbackStrategy, DLQ_FALLBACK_RETRY_KEY_PREFIX, DLQ_FIELDS,
    DLQ_STREAM_KEY,
};
pub use handler::{ErrorAction, ErrorHandler};
pub use retry::{
    calculate_backoff, exponential_backoff, linear_backoff, parse_duration, BackoffStrategy,
    RetryConfig, RETRY_KEY_PREFIX,
};
pub use source_support::{ErrorConfigBuilder, SourceErrorContext};
pub use strategy::ErrorStrategy;

/// Implements serde `Serialize` and `Deserialize` for an enum through its
/// `as_str()` and `FromStr`. YAML then takes the same values as the matching
/// `error.*` property, with the same error message for a bad value.
macro_rules! serde_via_str {
    ($($ty:ty),+ $(,)?) => {$(
        impl serde::Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }
    )+};
}

serde_via_str!(
    ErrorStrategy,
    LogLevel,
    BackoffStrategy,
    DlqFallbackStrategy
);
