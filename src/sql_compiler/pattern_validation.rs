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

//! Pattern validation.
//!
//! Checks pattern expressions against the EventFlux grammar rules. Runs after parsing and
//! before conversion to the Query API.

use sqlparser::ast::{PatternExpression, PatternMode};
use std::fmt;

/// A broken pattern rule.
#[derive(Debug, Clone, PartialEq)]
pub enum PatternValidationError {
    /// EVERY used in SEQUENCE mode.
    EveryInSequenceMode,
    /// EVERY not at the top level. `context` says where it was found, such as
    /// "sequence" or "logical combination".
    EveryNotAtTopLevel { context: String },
    /// A top level EVERY with another EVERY inside it.
    MultipleEvery,
    /// Count quantifier with a min of 0.
    ZeroCountPattern { min_count: u32 },
    /// Count quantifier with no upper bound. Not supported.
    UnboundedCountPattern,
    /// Count quantifier whose max is below its min.
    InvalidCountRange { min_count: u32, max_count: u32 },
    /// Absent pattern (NOT ... FOR) used directly inside AND or OR.
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

/// Checks pattern expressions. Holds no data.
pub struct PatternValidator;

impl PatternValidator {
    /// Check a pattern expression against the grammar rules.
    ///
    /// The rules:
    /// 1. EVERY only in PATTERN mode
    /// 2. EVERY only at the top level
    /// 3. No more than one EVERY
    /// 4. Count quantifiers have a min of at least 1, an explicit max, and max >= min
    /// 5. Absent patterns not directly inside AND or OR
    ///
    /// # Errors
    ///
    /// Returns every broken rule found, not just the first. One pattern can give several
    /// errors.
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

        // Rule 4: Count quantifier bounds
        Self::validate_count_quantifiers(pattern, &mut errors);

        // Rule 5: Absent patterns not inside AND or OR
        Self::validate_absent_not_in_logical(pattern, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Whether EVERY appears anywhere in the pattern.
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

    /// Check that EVERY appears only at the top level and at most once.
    ///
    /// Only the outermost node counts as the top level. An EVERY wrapped in parentheses
    /// (a Grouped node) is treated as nested.
    fn validate_every_position(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Every { pattern: inner } => {
                // A top level EVERY is fine, but nothing inside it may have another one.
                if Self::contains_every(inner) {
                    errors.push(PatternValidationError::MultipleEvery);
                }
            }
            _ => {
                Self::validate_no_nested_every(pattern, errors);
            }
        }
    }

    /// Report any EVERY found below the top level.
    ///
    /// Inside a sequence or logical node it reports once per side that has an EVERY,
    /// without looking deeper on that side.
    fn validate_no_nested_every(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Every { .. } => {
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

    /// Check the bounds of every count quantifier in the pattern.
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
                if *min_count == 0 {
                    errors.push(PatternValidationError::ZeroCountPattern {
                        min_count: *min_count,
                    });
                }

                // The parser stores an open upper bound as u32::MAX.
                if *max_count == u32::MAX {
                    errors.push(PatternValidationError::UnboundedCountPattern);
                }

                if *max_count < *min_count && *max_count != u32::MAX {
                    errors.push(PatternValidationError::InvalidCountRange {
                        min_count: *min_count,
                        max_count: *max_count,
                    });
                }

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

    /// Report each AND or OR node that has an absent pattern as a direct side. Reports
    /// once per node even when both sides are absent.
    fn validate_absent_not_in_logical(
        pattern: &PatternExpression,
        errors: &mut Vec<PatternValidationError>,
    ) {
        match pattern {
            PatternExpression::Logical { left, right, .. } => {
                if Self::is_absent(left) || Self::is_absent(right) {
                    errors.push(PatternValidationError::AbsentInLogical);
                }
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

    /// Whether the pattern is an absent pattern, looking through any parentheses.
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
        // EVERY(A{0,MAX} -> B) in SEQUENCE mode breaks several rules at once, and all of
        // them should be reported.
        let inner = sequence(count(stream("A"), 0, u32::MAX), stream("B"));
        let pattern = every(inner);

        let result = PatternValidator::validate(&PatternMode::Sequence, &pattern);

        assert!(result.is_err());
        let errors = result.unwrap_err();
        // Should have at least 3 errors: EVERY in SEQUENCE, zero count, unbounded count
        assert!(errors.len() >= 3);
    }
}
