// SPDX-License-Identifier: MIT OR Apache-2.0

//! Error strategy definitions for EventFlux error handling system.
//!
//! This module defines the available error handling strategies that can be applied
//! when processing failures occur in sources, sinks, or during event processing.

/// Error handling strategy for failed events
///
/// Determines what action to take when an event processing failure occurs.
/// Strategies can be configured per stream/source/sink via configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorStrategy {
    /// Log error and discard event (default)
    ///
    /// The event is dropped and processing continues normally.
    /// A log entry is created with the error details.
    Drop,

    /// Retry with exponential backoff
    ///
    /// The operation is retried with increasing delays between attempts.
    /// After max attempts are exhausted, falls back to the configured strategy.
    Retry,

    /// Send to dead-letter queue stream
    ///
    /// Failed events are forwarded to a designated DLQ stream for later analysis.
    /// The DLQ stream must be properly configured in the application.
    Dlq,

    /// Fail application immediately
    ///
    /// Terminates the EventFlux application on first error.
    /// Use this for critical failures where continuing would be unsafe.
    Fail,
}

impl ErrorStrategy {
    /// Parse error strategy from string (case-insensitive)
    ///
    /// # Arguments
    /// * `s` - String representation of error strategy
    ///
    /// # Returns
    /// * `Ok(ErrorStrategy)` - Successfully parsed strategy
    /// * `Err(String)` - Parse error with message
    ///
    /// # Examples
    /// ```
    /// use eventflux::core::error::ErrorStrategy;
    ///
    /// assert_eq!(ErrorStrategy::from_str("drop").unwrap(), ErrorStrategy::Drop);
    /// assert_eq!(ErrorStrategy::from_str("RETRY").unwrap(), ErrorStrategy::Retry);
    /// assert!(ErrorStrategy::from_str("invalid").is_err());
    /// ```
    pub fn from_str(s: &str) -> Result<Self, String> {
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

    /// Convert error strategy to string representation
    ///
    /// Returns the canonical lowercase string representation of the strategy.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            ErrorStrategy::Drop => "drop",
            ErrorStrategy::Retry => "retry",
            ErrorStrategy::Dlq => "dlq",
            ErrorStrategy::Fail => "fail",
        }
    }

    /// Check if this strategy requires retry configuration
    #[inline]
    pub const fn requires_retry_config(&self) -> bool {
        matches!(self, ErrorStrategy::Retry)
    }

    /// Check if this strategy requires DLQ configuration
    #[inline]
    pub const fn requires_dlq_config(&self) -> bool {
        matches!(self, ErrorStrategy::Dlq)
    }
}

impl Default for ErrorStrategy {
    /// Default error strategy is Drop
    #[inline]
    fn default() -> Self {
        ErrorStrategy::Drop
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
