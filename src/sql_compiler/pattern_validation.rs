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

//! Pattern Validation Module
//!
//! Validates pattern expressions according to EventFlux grammar rules.
//! This validation occurs after parsing but before conversion to Query API.

use sqlparser::ast::{PatternExpression, PatternMode};
use std::fmt;

/// Pattern validation error types
#[derive(Debug, Clone, PartialEq)]
pub enum PatternValidationError {
    /// EVERY keyword used in SEQUENCE mode
    EveryInSequenceMode,
    /// EVERY keyword not at top level (nested in sequence or logical)
    EveryNotAtTopLevel { context: String },
    /// Multiple EVERY keywords in pattern
    MultipleEvery,
    /// Count quantifier with min_count < 1
    ZeroCountPattern { min_count: u32 },
    /// Count quantifier with unbounded max (not supported)
    UnboundedCountPattern,
    /// Count quantifier with max < min
    InvalidCountRange { min_count: u32, max_count: u32 },
    /// Absent pattern used in logical combination
    AbsentInLogical,
}

impl fmt::Display for PatternValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternValidationError::EveryInSequenceMode => {
                write!(
                    f,
                    "EVERY not allowed in SEQUENCE mode. \
                     SEQUENCE automatically resets after each match. \
                     Use PATTERN mode if you need EVERY."
                )
            }
            PatternValidationError::EveryNotAtTopLevel { context } => {
                write!(
                    f,
                    "EVERY must be at top level only, not nested in {}. \
                     Correct usage: EVERY (pattern1 -> pattern2)",
                    context
                )
            }
            PatternValidationError::MultipleEvery => {
                write!(
                    f,
                    "Multiple EVERY keywords not allowed. \
                     Use: EVERY (e1 -> e2 -> e3) instead of EVERY e1 -> EVERY e2"
                )
            }
            PatternValidationError::ZeroCountPattern { min_count } => {
                write!(
                    f,
                    "Count quantifier min_count must be >= 1, got {}. \
                     Zero-count patterns (A*, A?, A{{0,n}}) are not supported.",
                    min_count
                )
            }
            PatternValidationError::UnboundedCountPattern => {
                write!(
                    f,
                    "Count quantifier max_count must be explicitly specified. \
                     Unbounded patterns (A+, A{{1,}}, A{{n,}}) are not supported."
                )
            }
            PatternValidationError::InvalidCountRange {
                min_count,
                max_count,
            } => {
                write!(
                    f,
                    "Count quantifier max_count ({}) must be >= min_count ({})",
                    max_count, min_count
                )
            }
            PatternValidationError::AbsentInLogical => {
                write!(
                    f,
                    "Absent patterns (NOT ... FOR) cannot be used in logical combinations (AND/OR). \
                     Use sequence instead: (A AND B) -> NOT C FOR 10 seconds"
                )
            }
        }
    }
}

impl std::error::Error for PatternValidationError {}

/// Pattern validator
pub struct PatternValidator;

impl PatternValidator {
    /// Validate a pattern expression according to grammar rules
    ///
    /// This validates:
    /// 1. EVERY only in PATTERN mode
    /// 2. EVERY only at top level
    /// 3. No multiple EVERY keywords
    /// 4. Count quantifiers have valid bounds
    /// 5. Absent patterns not in logical combinations
    pub fn validate(
        mode: &PatternMode,
        pattern: &PatternExpression,
    ) -> Result<(), Vec<PatternValidationError>> {
        let mut errors = Vec::new();

        // Rule 1: EVERY only in PATTERN mode
        if let PatternMode::Sequence = mode {
            if Self::contains_every(pattern) {
                errors.push(PatternValidationError::EveryInSequenceMode);
            }
        }

        // Rule 2 & 3: EVERY position and count
        Self::validate_every_position(pattern, &mut errors);

        // Rule 4: Count quantifier validation
        Self::validate_count_quantifiers(pattern, &mut errors);

        // Rule 5: Absent patterns not in logical
        Self::validate_absent_not_in_logical(pattern, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Check if pattern contains EVERY at any level
    fn contains_every(pattern: &PatternExpression) -> bool {
        match pattern {
            PatternExpression::Every { .. } => true,
            PatternExpression::Stream { .. } => false,
            PatternExpression::Absent { .. } => false,
            PatternExpression::Count { pattern, .. } => Self::contains_every(pattern),
            PatternExpression::Sequence { first, second } => {
                Self::contains_every(first) || Self::contains_every(second)
            }
            PatternExpression::Logical { left, right, .. } => {
                Self::contains_every(left) || Self::contains_every(right)
            }
            PatternExpression::Grouped { pattern } => Self::contains_every(pattern),
        }
    }

    /// Validate EVERY is only at top level and appears at most once
    fn validate_every_position(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Every { pattern: inner } => {
                // EVERY at top level is OK, but check inner pattern has no EVERY
                if Self::contains_every(inner) {
                    errors.push(PatternValidationError::MultipleEvery);
                }
            }
            _ => {
                // Not EVERY at top - ensure NO EVERY anywhere in this pattern
                Self::validate_no_nested_every(pattern, errors);
            }
        }
    }

    /// Recursively check that EVERY doesn't appear nested
    fn validate_no_nested_every(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Every { .. } => {
                // EVERY found nested - this is an error
                errors.push(PatternValidationError::EveryNotAtTopLevel {
                    context: "pattern".to_string(),
                });
            }
            PatternExpression::Stream { .. } => {}
            PatternExpression::Absent { .. } => {}
            PatternExpression::Count { pattern, .. } => {
                Self::validate_no_nested_every(pattern, errors);
            }
            PatternExpression::Sequence { first, second } => {
                // Check if EVERY is nested in sequence
                if Self::contains_every(first) {
                    errors.push(PatternValidationError::EveryNotAtTopLevel {
                        context: "sequence".to_string(),
                    });
                }
                if Self::contains_every(second) {
                    errors.push(PatternValidationError::EveryNotAtTopLevel {
                        context: "sequence".to_string(),
                    });
                }
            }
            PatternExpression::Logical { left, right, .. } => {
                // Check if EVERY is nested in logical
                if Self::contains_every(left) {
                    errors.push(PatternValidationError::EveryNotAtTopLevel {
                        context: "logical combination".to_string(),
                    });
                }
                if Self::contains_every(right) {
                    errors.push(PatternValidationError::EveryNotAtTopLevel {
                        context: "logical combination".to_string(),
                    });
                }
            }
            PatternExpression::Grouped { pattern } => {
                Self::validate_no_nested_every(pattern, errors);
            }
        }
    }

    /// Validate count quantifiers have valid bounds
    fn validate_count_quantifiers(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Count {
                pattern: inner,
                min_count,
                max_count,
            } => {
                // Rule: min_count >= 1
                if *min_count == 0 {
                    errors.push(PatternValidationError::ZeroCountPattern {
                        min_count: *min_count,
                    });
                }

                // Rule: max_count must be explicit (we use u32::MAX as sentinel for unbounded)
                if *max_count == u32::MAX {
                    errors.push(PatternValidationError::UnboundedCountPattern);
                }

                // Rule: max >= min
                if *max_count < *min_count && *max_count != u32::MAX {
                    errors.push(PatternValidationError::InvalidCountRange {
                        min_count: *min_count,
                        max_count: *max_count,
                    });
                }

                // Recurse into inner pattern
                Self::validate_count_quantifiers(inner, errors);
            }
            PatternExpression::Stream { .. } => {}
            PatternExpression::Absent { .. } => {}
            PatternExpression::Every { pattern } => {
                Self::validate_count_quantifiers(pattern, errors);
            }
            PatternExpression::Sequence { first, second } => {
                Self::validate_count_quantifiers(first, errors);
                Self::validate_count_quantifiers(second, errors);
            }
            PatternExpression::Logical { left, right, .. } => {
                Self::validate_count_quantifiers(left, errors);
                Self::validate_count_quantifiers(right, errors);
            }
            PatternExpression::Grouped { pattern } => {
                Self::validate_count_quantifiers(pattern, errors);
            }
        }
    }

    /// Validate absent patterns are not used in logical combinations
    fn validate_absent_not_in_logical(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Logical { left, right, .. } => {
                // Check if either side is an absent pattern
                if Self::is_absent(left) || Self::is_absent(right) {
                    errors.push(PatternValidationError::AbsentInLogical);
                }
                // Recurse into children
                Self::validate_absent_not_in_logical(left, errors);
                Self::validate_absent_not_in_logical(right, errors);
            }
            PatternExpression::Every { pattern } => {
                Self::validate_absent_not_in_logical(pattern, errors);
            }
            PatternExpression::Sequence { first, second } => {
                Self::validate_absent_not_in_logical(first, errors);
                Self::validate_absent_not_in_logical(second, errors);
            }
            PatternExpression::Grouped { pattern } => {
                Self::validate_absent_not_in_logical(pattern, errors);
            }
            PatternExpression::Count { pattern, .. } => {
                Self::validate_absent_not_in_logical(pattern, errors);
            }
            PatternExpression::Stream { .. } => {}
            PatternExpression::Absent { .. } => {}
        }
    }

    /// Check if a pattern is an absent pattern (possibly wrapped in Grouped)
    fn is_absent(pattern: &PatternExpression) -> bool {
        match pattern {
            PatternExpression::Absent { .. } => true,
            PatternExpression::Grouped { pattern } => Self::is_absent(pattern),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, PatternLogicalOp};

    fn stream(name: &str) -> PatternExpression {
        PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            filter: None,
        }
    }

    fn sequence(first: PatternExpression, second: PatternExpression) -> PatternExpression {
        PatternExpression::Sequence {
            first: Box::new(first),
            second: Box::new(second),
        }
    }

    fn every(pattern: PatternExpression) -> PatternExpression {
        PatternExpression::Every {
            pattern: Box::new(pattern),
        }
    }

    fn logical_and(left: PatternExpression, right: PatternExpression) -> PatternExpression {
        PatternExpression::Logical {
            left: Box::new(left),
            op: PatternLogicalOp::And,
            right: Box::new(right),
        }
    }

    fn count(pattern: PatternExpression, min: u32, max: u32) -> PatternExpression {
        PatternExpression::Count {
            pattern: Box::new(pattern),
            min_count: min,
            max_count: max,
        }
    }

    fn absent(name: &str) -> PatternExpression {
        use sqlparser::ast::{Expr, Value};
        PatternExpression::Absent {
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            duration: Box::new(Expr::Value(
                Value::Number("10000".to_string(), false).into(),
            )),
        }
    }

    // ============================================================================
    // EVERY in SEQUENCE mode tests
    // ============================================================================

    #[test]
    fn test_every_in_sequence_mode_rejected() {
        let pattern = every(sequence(stream("A"), stream("B")));

        let result = PatternValidator::validate(&PatternMode::Sequence, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, PatternValidationError::EveryInSequenceMode)));
    }

    #[test]
    fn test_every_in_pattern_mode_allowed() {
        let pattern = every(sequence(stream("A"), stream("B")));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_no_every_in_sequence_mode_allowed() {
        let pattern = sequence(stream("A"), stream("B"));

        let result = PatternValidator::validate(&PatternMode::Sequence, &pattern);

        assert!(result.is_ok());
    }

    // ============================================================================
    // EVERY position tests
    // ============================================================================

    #[test]
    fn test_every_at_top_level_allowed() {
        let pattern = every(sequence(stream("A"), stream("B")));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_every_nested_in_sequence_rejected() {
        // (EVERY A) -> B - EVERY nested in sequence
        let pattern = sequence(every(stream("A")), stream("B"));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(
            e,
            PatternValidationError::EveryNotAtTopLevel { context } if context == "sequence"
        )));
    }

    #[test]
    fn test_every_nested_in_logical_rejected() {
        // EVERY(A) AND B - EVERY nested in logical
        let pattern = logical_and(every(stream("A")), stream("B"));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(
            e,
            PatternValidationError::EveryNotAtTopLevel { context } if context == "logical combination"
        )));
    }

    #[test]
    fn test_multiple_every_rejected() {
        // EVERY(EVERY(A) -> B) - nested EVERY
        let pattern = every(sequence(every(stream("A")), stream("B")));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, PatternValidationError::MultipleEvery)));
    }

    // ============================================================================
    // Count quantifier tests
    // ============================================================================

    #[test]
    fn test_valid_count_quantifier() {
        let pattern = count(stream("A"), 3, 5);

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_exact_count_quantifier() {
        let pattern = count(stream("A"), 3, 3);

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_zero_min_count_rejected() {
        let pattern = count(stream("A"), 0, 5);

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, PatternValidationError::ZeroCountPattern { min_count: 0 })));
    }

    #[test]
    fn test_unbounded_max_count_rejected() {
        let pattern = count(stream("A"), 1, u32::MAX);

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, PatternValidationError::UnboundedCountPattern)));
    }

    #[test]
    fn test_max_less_than_min_rejected() {
        let pattern = count(stream("A"), 5, 3);

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(
            e,
            PatternValidationError::InvalidCountRange {
                min_count: 5,
                max_count: 3
            }
        )));
    }

    // ============================================================================
    // Absent pattern in logical tests
    // ============================================================================

    #[test]
    fn test_absent_in_sequence_allowed() {
        // A -> NOT B FOR 10s - allowed
        let pattern = sequence(stream("A"), absent("B"));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_absent_in_logical_rejected() {
        // A AND (NOT B FOR 10s) - rejected
        let pattern = logical_and(stream("A"), absent("B"));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, PatternValidationError::AbsentInLogical)));
    }

    // ============================================================================
    // Complex pattern tests
    // ============================================================================

    #[test]
    fn test_valid_complex_pattern() {
        // EVERY((A{3,5} -> B) AND C)
        let pattern = every(logical_and(
            sequence(count(stream("A"), 3, 5), stream("B")),
            stream("C"),
        ));

        let result = PatternValidator::validate(&PatternMode::Pattern, &pattern);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_errors_collected() {
        // Pattern with multiple issues:
        // EVERY(A{0,MAX} -> B) in SEQUENCE mode with EVERY nested
        // This should collect multiple errors
        let inner = sequence(count(stream("A"), 0, u32::MAX), stream("B"));
        let pattern = every(inner);

        let result = PatternValidator::validate(&PatternMode::Sequence, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        // Should have at least 3 errors: EVERY in SEQUENCE, zero count, unbounded count
        assert!(errors.len() >= 3);
    }
}
