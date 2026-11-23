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

//! Retry settings and backoff.
//!
//! Backoff is how the wait between retries grows. Exponential, linear and fixed
//! patterns are supported.

use std::str::FromStr;
use std::time::Duration;

use crate::core::config::FlatConfig;

/// How the wait between retries grows.
///
/// Every pattern is capped at `max_delay`, except `Fixed`, which never changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackoffStrategy {
    /// delay = initial_delay * 2^(attempt-1)
    ///
    /// Backs off fast so a failing system is not flooded.
    /// Example: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, ...
    Exponential,

    /// delay = initial_delay * attempt
    ///
    /// Grows slowly and evenly.
    /// Example: 100ms, 200ms, 300ms, 400ms, 500ms, ...
    Linear,

    /// delay = initial_delay
    ///
    /// The same wait every time.
    /// Example: 100ms, 100ms, 100ms, 100ms, ...
    Fixed,
}

impl std::str::FromStr for BackoffStrategy {
    type Err = String;

    /// Parse `exponential`, `linear` or `fixed`, ignoring case.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
}

impl BackoffStrategy {
    /// Returns the lowercase name, such as `"exponential"`.
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
    /// Returns `Exponential`.
    #[inline]
    fn default() -> Self {
        BackoffStrategy::Exponential
    }
}

/// How failed operations are retried.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Most attempts, counting the first one. 3 means up to 2 retries. Must be
    /// greater than 0.
    pub max_attempts: usize,

    pub backoff: BackoffStrategy,

    /// Wait before the first retry.
    pub initial_delay: Duration,

    /// Longest wait. Stops exponential and linear growth.
    pub max_delay: Duration,
}

impl RetryConfig {
    /// Build retry settings.
    ///
    /// # Errors
    ///
    /// Returns an error message if `max_attempts` is 0 or `initial_delay` is
    /// longer than `max_delay`.
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

    /// Parse retry settings from `error.retry.*` properties.
    ///
    /// Defaults when a key is missing:
    /// - `error.retry.max-attempts`: 3
    /// - `error.retry.backoff`: exponential
    /// - `error.retry.initial-delay`: 100ms
    /// - `error.retry.max-delay`: 30s
    ///
    /// Delays use the [`parse_duration`] format.
    ///
    /// # Errors
    ///
    /// Returns an error message if a value fails to parse, `max-attempts` is 0, or
    /// `initial-delay` is longer than `max-delay`.
    pub fn from_flat_config(config: &FlatConfig) -> Result<Self, String> {
        let max_attempts = config
            .get("error.retry.max-attempts")
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|_| "error.retry.max-attempts must be a positive integer".to_string())
            })
            .transpose()?
            .unwrap_or(3);

        if max_attempts == 0 {
            return Err("error.retry.max-attempts must be greater than 0".to_string());
        }

        let backoff = config
            .get("error.retry.backoff")
            .map(|s| BackoffStrategy::from_str(s))
            .transpose()?
            .unwrap_or(BackoffStrategy::Exponential);

        let initial_delay = config
            .get("error.retry.initial-delay")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_millis(100));

        let max_delay = config
            .get("error.retry.max-delay")
            .map(|s| parse_duration(s))
            .transpose()?
            .unwrap_or(Duration::from_secs(30));

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

    /// Returns the wait before a retry.
    ///
    /// # Arguments
    /// * `attempt` - Retry number, starting at 1. 0 gives zero for exponential and
    ///   linear backoff.
    pub fn calculate_delay(&self, attempt: usize) -> Duration {
        calculate_backoff(attempt, self)
    }
}

impl Default for RetryConfig {
    /// Returns 3 attempts, exponential backoff, 100ms initial delay and 30s max
    /// delay.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }
}

/// Returns the exponential backoff wait for a retry.
///
/// Formula is `min(initial_delay * 2^(attempt-1), max_delay)`. `attempt` starts at
/// 1, and 0 gives zero. Works in whole milliseconds.
///
/// # Example with initial 100ms and max 30s
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
    exponential_backoff_with_multiplier(attempt, initial_delay, max_delay, 2.0)
}

/// Returns the exponential backoff wait with a chosen growth factor.
///
/// `multiplier` should be at least 1. The result is capped at `max_delay`, and a
/// value too large to hold stays at the cap. `attempt` starts at 1, and 0 gives
/// zero. Works in whole milliseconds.
pub fn exponential_backoff_with_multiplier(
    attempt: usize,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }

    let max_ms = max_delay.as_millis() as u64 as f64;
    let factor = multiplier.powi((attempt - 1) as i32);
    // Cap while still an f64, before the cast, so a huge value lands on the cap
    // instead of wrapping.
    let delay_ms = (initial_delay.as_millis() as u64 as f64 * factor).min(max_ms);

    Duration::from_millis(delay_ms as u64)
}

/// Returns the linear backoff wait for a retry.
///
/// Formula is `min(initial_delay * attempt, max_delay)`. `attempt` starts at 1,
/// and 0 gives zero.
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

/// Returns the wait for a retry using the backoff pattern in `config`.
pub fn calculate_backoff(attempt: usize, config: &RetryConfig) -> Duration {
    match config.backoff {
        BackoffStrategy::Exponential => {
            exponential_backoff(attempt, config.initial_delay, config.max_delay)
        }
        BackoffStrategy::Linear => linear_backoff(attempt, config.initial_delay, config.max_delay),
        BackoffStrategy::Fixed => config.initial_delay,
    }
}

/// Parse a duration string.
///
/// Takes a whole number with an optional unit. `ms` is milliseconds, `s` seconds
/// and `m` minutes, such as `"100ms"`, `"30s"` or `"5m"`. A bare number such as
/// `"1000"` is milliseconds. Spaces around the number are allowed.
///
/// # Errors
///
/// Returns an error message if the number is missing or not a whole number, or
/// the unit is unknown.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();

    // Check "ms" before "s", since "ms" also ends in "s".
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

    // No unit means milliseconds.
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
