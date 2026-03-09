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

//! Retry configuration and backoff strategies for error handling.
//!
//! This module provides configurable retry logic with multiple backoff strategies
//! including exponential, linear, and fixed delay patterns.

use crate::core::config::FlatConfig;
use std::time::Duration;

/// Backoff strategy for retry delays
///
/// Determines how the delay between retry attempts increases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackoffStrategy {
    /// Exponential backoff: delay = initial_delay * 2^(attempt-1)
    ///
    /// Provides aggressive backoff to avoid overwhelming failing systems.
    /// Example: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, ...
    Exponential,

    /// Linear backoff: delay = initial_delay * attempt
    ///
    /// Provides gradual, predictable increase in delays.
    /// Example: 100ms, 200ms, 300ms, 400ms, 500ms, ...
    Linear,

    /// Fixed delay: delay = initial_delay (constant)
    ///
    /// Uses the same delay for all retry attempts.
    /// Example: 100ms, 100ms, 100ms, 100ms, ...
    Fixed,
}

impl BackoffStrategy {
    /// Parse backoff strategy from string (case-insensitive)
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "exponential" => Ok(BackoffStrategy::Exponential),
            "linear" => Ok(BackoffStrategy::Linear),
            "fixed" => Ok(BackoffStrategy::Fixed),
            _ => Err(format!(
                "Invalid backoff strategy '{}'. Valid values: 'exponential', 'linear', 'fixed'",
                s
            )),
        }
    }

    /// Convert backoff strategy to string representation
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            BackoffStrategy::Exponential => "exponential",
            BackoffStrategy::Linear => "linear",
            BackoffStrategy::Fixed => "fixed",
        }
    }
}

impl Default for BackoffStrategy {
    /// Default backoff strategy is Exponential
    #[inline]
    fn default() -> Self {
        BackoffStrategy::Exponential
    }
}

/// Retry configuration with backoff settings
///
/// Configures how failed operations should be retried, including
/// maximum attempts, backoff strategy, and delay bounds.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: usize,

    /// Backoff strategy for calculating delays
    pub backoff: BackoffStrategy,

    /// Initial delay before first retry
    pub initial_delay: Duration,

    /// Maximum delay cap (prevents unbounded exponential growth)
    pub max_delay: Duration,
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new(
        max_attempts: usize,
        backoff: BackoffStrategy,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, String> {
        if max_attempts == 0 {
            return Err("max_attempts must be greater than 0".to_string());
        }

        if initial_delay > max_delay {
            return Err("initial_delay cannot be greater than max_delay".to_string());
        }

        Ok(Self {
            max_attempts,
            backoff,
            initial_delay,
            max_delay,
        })
    }

    /// Parse retry configuration from FlatConfig
    ///
    /// Extracts retry settings from configuration with sensible defaults:
    /// - max_attempts: 3
    /// - backoff: exponential
    /// - initial_delay: 100ms
    /// - max_delay: 30s
    pub fn from_flat_config(config: &FlatConfig) -> Result<Self, String> {
        let max_attempts = config
            .get("error.retry.max-attempts")
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|_| "error.retry.max-attempts must be a positive integer".to_string())
            })
            .transpose()?
            .unwrap_or(3); // Default: 3 attempts

        if max_attempts == 0 {
            return Err("error.retry.max-attempts must be greater than 0".to_string());
        }

        let backoff = config
            .get("error.retry.backoff")
            .map(|s| BackoffStrategy::from_str(s))
            .transpose()?
            .unwrap_or(BackoffStrategy::Exponential); // Default: exponential

        let initial_delay = config
            .get("error.retry.initial-delay")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_millis(100)); // Default: 100ms

        let max_delay = config
            .get("error.retry.max-delay")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_secs(30)); // Default: 30s

        if initial_delay > max_delay {
            return Err(
                "error.retry.initial-delay cannot be greater than error.retry.max-delay"
                    .to_string(),
            );
        }

        Ok(RetryConfig {
            max_attempts,
            backoff,
            initial_delay,
            max_delay,
        })
    }

    /// Calculate the delay for a specific retry attempt
    ///
    /// # Arguments
    /// * `attempt` - The retry attempt number (1-indexed)
    ///
    /// # Returns
    /// Duration to wait before this retry attempt
    pub fn calculate_delay(&self, attempt: usize) -> Duration {
        calculate_backoff(attempt, self)
    }
}

impl Default for RetryConfig {
    /// Default retry configuration:
    /// - 3 max attempts
    /// - Exponential backoff
    /// - 100ms initial delay
    /// - 30s max delay
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }
}

/// Calculate exponential backoff delay for a retry attempt
///
/// Formula: delay = min(initial_delay * 2^(attempt-1), max_delay)
///
/// # Example with initial=100ms, max=30s:
/// - Attempt 1: 100ms
/// - Attempt 2: 200ms
/// - Attempt 3: 400ms
/// - Attempt 4: 800ms
/// - Attempt 5: 1.6s
/// - Attempt 6: 3.2s
/// - Attempt 7: 6.4s
/// - Attempt 8: 12.8s
/// - Attempt 9: 25.6s
/// - Attempt 10+: 30s (capped)
pub fn exponential_backoff(
    attempt: usize,
    initial_delay: Duration,
    max_delay: Duration,
) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }

    // Calculate 2^(attempt-1) using checked operations to prevent overflow
    let multiplier = 2u64.saturating_pow((attempt - 1) as u32);
    let delay_ms = (initial_delay.as_millis() as u64).saturating_mul(multiplier);

    let delay = Duration::from_millis(delay_ms);

    if delay > max_delay {
        max_delay
    } else {
        delay
    }
}

/// Calculate linear backoff delay for a retry attempt
///
/// Formula: delay = min(initial_delay * attempt, max_delay)
pub fn linear_backoff(attempt: usize, initial_delay: Duration, max_delay: Duration) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }

    let delay = initial_delay.saturating_mul(attempt as u32);

    if delay > max_delay {
        max_delay
    } else {
        delay
    }
}

/// Calculate backoff delay based on retry configuration
///
/// Applies the configured backoff strategy to determine the delay.
pub fn calculate_backoff(attempt: usize, config: &RetryConfig) -> Duration {
    match config.backoff {
        BackoffStrategy::Exponential => {
            exponential_backoff(attempt, config.initial_delay, config.max_delay)
        }
        BackoffStrategy::Linear => linear_backoff(attempt, config.initial_delay, config.max_delay),
        BackoffStrategy::Fixed => config.initial_delay,
    }
}

/// Parse duration from string
///
/// Supports formats like:
/// - "100ms", "1s", "30s"
/// - "1000" (milliseconds by default)
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();

    // Try parsing with suffix first
    if let Some(stripped) = s.strip_suffix("ms") {
        let millis = stripped
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid duration '{}': expected number before 'ms'", s))?;
        return Ok(Duration::from_millis(millis));
    }

    if let Some(stripped) = s.strip_suffix('s') {
        let secs = stripped
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid duration '{}': expected number before 's'", s))?;
        return Ok(Duration::from_secs(secs));
    }

    if let Some(stripped) = s.strip_suffix('m') {
        let mins = stripped
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid duration '{}': expected number before 'm'", s))?;
        return Ok(Duration::from_secs(mins * 60));
    }

    // Default to milliseconds if no suffix
    let millis = s.parse::<u64>().map_err(|_| {
        format!(
            "Invalid duration '{}': expected number or duration with suffix (ms, s, m)",
            s
        )
    })?;
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // BackoffStrategy Tests
    // ========================================================================

    #[test]
    fn test_backoff_strategy_from_str() {
        assert_eq!(
            BackoffStrategy::from_str("exponential").unwrap(),
            BackoffStrategy::Exponential
        );
        assert_eq!(
            BackoffStrategy::from_str("Exponential").unwrap(),
            BackoffStrategy::Exponential
        );
        assert_eq!(
            BackoffStrategy::from_str("linear").unwrap(),
            BackoffStrategy::Linear
        );
        assert_eq!(
            BackoffStrategy::from_str("fixed").unwrap(),
            BackoffStrategy::Fixed
        );
        assert!(BackoffStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_backoff_strategy_as_str() {
        assert_eq!(BackoffStrategy::Exponential.as_str(), "exponential");
        assert_eq!(BackoffStrategy::Linear.as_str(), "linear");
        assert_eq!(BackoffStrategy::Fixed.as_str(), "fixed");
    }

    #[test]
    fn test_backoff_strategy_default() {
        assert_eq!(BackoffStrategy::default(), BackoffStrategy::Exponential);
    }

    // ========================================================================
    // Exponential Backoff Tests
    // ========================================================================

    #[test]
    fn test_exponential_backoff() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        assert_eq!(
            exponential_backoff(1, initial, max),
            Duration::from_millis(100)
        );
        assert_eq!(
            exponential_backoff(2, initial, max),
            Duration::from_millis(200)
        );
        assert_eq!(
            exponential_backoff(3, initial, max),
            Duration::from_millis(400)
        );
        assert_eq!(
            exponential_backoff(4, initial, max),
            Duration::from_millis(800)
        );
        assert_eq!(
            exponential_backoff(5, initial, max),
            Duration::from_millis(1600)
        );
        assert_eq!(
            exponential_backoff(10, initial, max),
            Duration::from_secs(30)
        ); // Capped
    }

    #[test]
    fn test_exponential_backoff_zero_attempt() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        assert_eq!(exponential_backoff(0, initial, max), Duration::ZERO);
    }

    // ========================================================================
    // Linear Backoff Tests
    // ========================================================================

    #[test]
    fn test_linear_backoff() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);

        assert_eq!(linear_backoff(1, initial, max), Duration::from_millis(100));
        assert_eq!(linear_backoff(2, initial, max), Duration::from_millis(200));
        assert_eq!(linear_backoff(3, initial, max), Duration::from_millis(300));
        assert_eq!(linear_backoff(200, initial, max), Duration::from_secs(10)); // Capped
    }

    #[test]
    fn test_linear_backoff_zero_attempt() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);
        assert_eq!(linear_backoff(0, initial, max), Duration::ZERO);
    }

    // ========================================================================
    // RetryConfig Tests
    // ========================================================================

    #[test]
    fn test_retry_config_new() {
        let config = RetryConfig::new(
            5,
            BackoffStrategy::Exponential,
            Duration::from_millis(200),
            Duration::from_secs(60),
        );
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.backoff, BackoffStrategy::Exponential);
        assert_eq!(config.initial_delay, Duration::from_millis(200));
        assert_eq!(config.max_delay, Duration::from_secs(60));
    }

    #[test]
    fn test_retry_config_new_invalid_max_attempts() {
        let config = RetryConfig::new(
            0,
            BackoffStrategy::Exponential,
            Duration::from_millis(100),
            Duration::from_secs(30),
        );
        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("max_attempts must be greater than 0"));
    }

    #[test]
    fn test_retry_config_new_invalid_delays() {
        let config = RetryConfig::new(
            3,
            BackoffStrategy::Exponential,
            Duration::from_secs(60), // initial > max
            Duration::from_secs(30),
        );
        assert!(config.is_err());
        assert!(config
            .unwrap_err()
            .contains("initial_delay cannot be greater than max_delay"));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.backoff, BackoffStrategy::Exponential);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn test_retry_config_from_flat_config_defaults() {
        let config = FlatConfig::new();
        let retry_config = RetryConfig::from_flat_config(&config).unwrap();

        assert_eq!(retry_config.max_attempts, 3);
        assert_eq!(retry_config.backoff, BackoffStrategy::Exponential);
        assert_eq!(retry_config.initial_delay, Duration::from_millis(100));
        assert_eq!(retry_config.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn test_retry_config_from_flat_config_custom() {
        use crate::core::config::PropertySource;

        let mut config = FlatConfig::new();
        config.set("error.retry.max-attempts", "5", PropertySource::SqlWith);
        config.set("error.retry.backoff", "linear", PropertySource::SqlWith);
        config.set(
            "error.retry.initial-delay",
            "200ms",
            PropertySource::SqlWith,
        );
        config.set("error.retry.max-delay", "60s", PropertySource::SqlWith);

        let retry_config = RetryConfig::from_flat_config(&config).unwrap();

        assert_eq!(retry_config.max_attempts, 5);
        assert_eq!(retry_config.backoff, BackoffStrategy::Linear);
        assert_eq!(retry_config.initial_delay, Duration::from_millis(200));
        assert_eq!(retry_config.max_delay, Duration::from_secs(60));
    }

    #[test]
    fn test_retry_config_calculate_delay() {
        let config = RetryConfig {
            max_attempts: 5,
            backoff: BackoffStrategy::Exponential,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        };

        assert_eq!(config.calculate_delay(1), Duration::from_millis(100));
        assert_eq!(config.calculate_delay(2), Duration::from_millis(200));
        assert_eq!(config.calculate_delay(3), Duration::from_millis(400));
    }

    // ========================================================================
    // Duration Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
        assert_eq!(
            parse_duration("1500ms").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(parse_duration("50 ms").unwrap(), Duration::from_millis(50));
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5 s").unwrap(), Duration::from_secs(5));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2 m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn test_parse_duration_default_ms() {
        assert_eq!(parse_duration("100").unwrap(), Duration::from_millis(100));
        assert_eq!(parse_duration("5000").unwrap(), Duration::from_millis(5000));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("100x").is_err());
        assert!(parse_duration("").is_err());
    }

    // ========================================================================
    // Calculate Backoff Tests
    // ========================================================================

    #[test]
    fn test_calculate_backoff_exponential() {
        let config = RetryConfig {
            max_attempts: 5,
            backoff: BackoffStrategy::Exponential,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        assert_eq!(calculate_backoff(1, &config), Duration::from_millis(100));
        assert_eq!(calculate_backoff(2, &config), Duration::from_millis(200));
        assert_eq!(calculate_backoff(3, &config), Duration::from_millis(400));
    }

    #[test]
    fn test_calculate_backoff_linear() {
        let config = RetryConfig {
            max_attempts: 5,
            backoff: BackoffStrategy::Linear,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        assert_eq!(calculate_backoff(1, &config), Duration::from_millis(100));
        assert_eq!(calculate_backoff(2, &config), Duration::from_millis(200));
        assert_eq!(calculate_backoff(3, &config), Duration::from_millis(300));
    }

    #[test]
    fn test_calculate_backoff_fixed() {
        let config = RetryConfig {
            max_attempts: 5,
            backoff: BackoffStrategy::Fixed,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        };

        assert_eq!(calculate_backoff(1, &config), Duration::from_millis(100));
        assert_eq!(calculate_backoff(2, &config), Duration::from_millis(100));
        assert_eq!(calculate_backoff(3, &config), Duration::from_millis(100));
        assert_eq!(calculate_backoff(10, &config), Duration::from_millis(100));
    }
}
