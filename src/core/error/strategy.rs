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

//! Error strategies.
//!
//! The choices for what to do when an event fails.

/// What to do with an event that fails.
///
/// Set through the `error.strategy` property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorStrategy {
    /// Log the error and discard the event. The default.
    ///
    /// Processing continues with the next event.
    Drop,

    /// Retry with backoff.
    ///
    /// The delay pattern comes from `error.retry.backoff`. When retries run out, or
    /// the error cannot be retried, the event goes to the DLQ if `error.dlq.stream`
    /// is set and is dropped otherwise.
    Retry,

    /// Send the event to a dead letter queue (DLQ) stream for later inspection.
    ///
    /// Needs `error.dlq.stream` to name a stream that exists in the application.
    Dlq,

    /// Stop the source on the first error.
    ///
    /// Use when carrying on after an error would be unsafe.
    Fail,
}

impl ErrorStrategy {
    /// Returns the lowercase name, such as `"drop"`.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            ErrorStrategy::Drop => "drop",
            ErrorStrategy::Retry => "retry",
            ErrorStrategy::Dlq => "dlq",
            ErrorStrategy::Fail => "fail",
        }
    }

    /// Returns true for `Retry`, the only strategy that needs retry settings.
    #[inline]
    pub const fn requires_retry_config(&self) -> bool {
        matches!(self, ErrorStrategy::Retry)
    }

    /// Returns true for `Dlq`. `Retry` can also use DLQ settings but does not
    /// need them.
    #[inline]
    pub const fn requires_dlq_config(&self) -> bool {
        matches!(self, ErrorStrategy::Dlq)
    }
}

impl Default for ErrorStrategy {
    /// Returns `Drop`.
    #[inline]
    fn default() -> Self {
        ErrorStrategy::Drop
    }
}

impl std::str::FromStr for ErrorStrategy {
    type Err = String;

    /// Parse a strategy name, ignoring case.
    ///
    /// # Arguments
    /// * `s` - One of `drop`, `retry`, `dlq` or `fail`.
    ///
    /// # Returns
    /// * `Ok(ErrorStrategy)` - The parsed strategy.
    /// * `Err(String)` - A message listing the valid names, for any other input.
    ///
    /// # Examples
    /// ```
    /// use eventflux::core::error::ErrorStrategy;
    /// use std::str::FromStr;
    ///
    /// assert_eq!(ErrorStrategy::from_str("drop").unwrap(), ErrorStrategy::Drop);
    /// assert_eq!(ErrorStrategy::from_str("RETRY").unwrap(), ErrorStrategy::Retry);
    /// assert!(ErrorStrategy::from_str("invalid").is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop" => Ok(ErrorStrategy::Drop),
            "retry" => Ok(ErrorStrategy::Retry),
            "dlq" => Ok(ErrorStrategy::Dlq),
            "fail" => Ok(ErrorStrategy::Fail),
            _ => Err(format!(
                "Invalid error strategy '{}'. Valid values: 'drop', 'retry', 'dlq', 'fail'",
                s
            )),
        }
    }
}

impl std::fmt::Display for ErrorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_error_strategy_from_str() {
        assert_eq!(
            ErrorStrategy::from_str("drop").unwrap(),
            ErrorStrategy::Drop
        );
        assert_eq!(
            ErrorStrategy::from_str("Drop").unwrap(),
            ErrorStrategy::Drop
        );
        assert_eq!(
            ErrorStrategy::from_str("DROP").unwrap(),
            ErrorStrategy::Drop
        );

        assert_eq!(
            ErrorStrategy::from_str("retry").unwrap(),
            ErrorStrategy::Retry
        );
        assert_eq!(
            ErrorStrategy::from_str("Retry").unwrap(),
            ErrorStrategy::Retry
        );

        assert_eq!(ErrorStrategy::from_str("dlq").unwrap(), ErrorStrategy::Dlq);
        assert_eq!(ErrorStrategy::from_str("DLQ").unwrap(), ErrorStrategy::Dlq);

        assert_eq!(
            ErrorStrategy::from_str("fail").unwrap(),
            ErrorStrategy::Fail
        );
        assert_eq!(
            ErrorStrategy::from_str("FAIL").unwrap(),
            ErrorStrategy::Fail
        );
    }

    #[test]
    fn test_error_strategy_from_str_invalid() {
        assert!(ErrorStrategy::from_str("invalid").is_err());
        assert!(ErrorStrategy::from_str("").is_err());
        assert!(ErrorStrategy::from_str("discard").is_err());
        assert!(ErrorStrategy::from_str("queue").is_err());
    }

    #[test]
    fn test_error_strategy_as_str() {
        assert_eq!(ErrorStrategy::Drop.as_str(), "drop");
        assert_eq!(ErrorStrategy::Retry.as_str(), "retry");
        assert_eq!(ErrorStrategy::Dlq.as_str(), "dlq");
        assert_eq!(ErrorStrategy::Fail.as_str(), "fail");
    }

    #[test]
    fn test_error_strategy_default() {
        assert_eq!(ErrorStrategy::default(), ErrorStrategy::Drop);
    }

    #[test]
    fn test_error_strategy_display() {
        assert_eq!(format!("{}", ErrorStrategy::Drop), "drop");
        assert_eq!(format!("{}", ErrorStrategy::Retry), "retry");
        assert_eq!(format!("{}", ErrorStrategy::Dlq), "dlq");
        assert_eq!(format!("{}", ErrorStrategy::Fail), "fail");
    }

    #[test]
    fn test_requires_retry_config() {
        assert!(!ErrorStrategy::Drop.requires_retry_config());
        assert!(ErrorStrategy::Retry.requires_retry_config());
        assert!(!ErrorStrategy::Dlq.requires_retry_config());
        assert!(!ErrorStrategy::Fail.requires_retry_config());
    }

    #[test]
    fn test_requires_dlq_config() {
        assert!(!ErrorStrategy::Drop.requires_dlq_config());
        assert!(!ErrorStrategy::Retry.requires_dlq_config());
        assert!(ErrorStrategy::Dlq.requires_dlq_config());
        assert!(!ErrorStrategy::Fail.requires_dlq_config());
    }
}
