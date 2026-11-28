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

pub mod and_expression_executor;
pub mod bool_expression_executor;
pub mod case_expression_executor;
pub mod compare_expression_executor;
pub mod in_expression_executor;
pub mod is_null_expression_executor;
pub mod not_expression_executor;
pub mod or_expression_executor;

pub use self::and_expression_executor::AndExpressionExecutor;
pub use self::bool_expression_executor::BoolExpressionExecutor;
pub use self::case_expression_executor::CaseExpressionExecutor;
pub use self::compare_expression_executor::CompareExpressionExecutor;
pub use self::in_expression_executor::InExpressionExecutor;
pub use self::is_null_expression_executor::IsNullExpressionExecutor;
pub use self::not_expression_executor::NotExpressionExecutor;
pub use self::or_expression_executor::OrExpressionExecutor;
// The compare operator comes from query_api::expression::condition::CompareOperator, so it
// is not re-exported here.

// In Java these classes extend the abstract ConditionExpressionExecutor. Here they just
// implement the ExpressionExecutor trait, so no separate base type is needed. What they
// share is that they all return Attribute::Type::BOOL.

use crate::core::event::value::AttributeValue;

/// The SQL truth value of a condition result. `Some(b)` for a BOOL, and `None` for
/// UNKNOWN, which is NULL, no value, or a value of any other type.
///
/// Conditions use SQL three valued logic, so an UNKNOWN input makes a comparison,
/// AND, OR or NOT UNKNOWN unless the other side settles it. Filters, HAVING, ON and
/// CASE only take a branch on true, so they drop UNKNOWN rows.
pub(crate) fn truth_value(value: Option<AttributeValue>) -> Option<bool> {
    match value {
        Some(AttributeValue::Bool(b)) => Some(b),
        _ => None,
    }
}

/// A truth value as a condition result. UNKNOWN becomes NULL.
pub(crate) fn truth_result(truth: Option<bool>) -> Option<AttributeValue> {
    Some(truth.map_or(AttributeValue::Null, AttributeValue::Bool))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::event::complex_event::ComplexEvent;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;
    use crate::core::executor::expression_executor::ExpressionExecutor;
    use crate::query_api::definition::attribute::Type as ApiAttributeType;
    use std::sync::Arc;

    /// A BOOL condition that gives no value, as a failed CAST does.
    #[derive(Debug)]
    struct NoValue;

    impl ExpressionExecutor for NoValue {
        fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
            None
        }
        fn get_return_type(&self) -> ApiAttributeType {
            ApiAttributeType::BOOL
        }
        fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
            Box::new(NoValue)
        }
    }

    /// A BOOL condition for a label. "T" and "F" are true and false, "N" is NULL, and
    /// anything else gives no value.
    fn operand(v: &str) -> Box<dyn ExpressionExecutor> {
        let constant = |value| {
            Box::new(ConstantExpressionExecutor::new(
                value,
                ApiAttributeType::BOOL,
            ))
        };
        match v {
            "T" => constant(AttributeValue::Bool(true)),
            "F" => constant(AttributeValue::Bool(false)),
            "N" => constant(AttributeValue::Null),
            _ => Box::new(NoValue),
        }
    }

    /// The expected result for a label. "T" and "F" are true and false, anything else
    /// is NULL.
    fn result(v: &str) -> Option<AttributeValue> {
        Some(match v {
            "T" => AttributeValue::Bool(true),
            "F" => AttributeValue::Bool(false),
            _ => AttributeValue::Null,
        })
    }

    /// SQL three valued truth tables. "N" is NULL and "-" gives no value. Both count as
    /// UNKNOWN, and every UNKNOWN result is NULL.
    #[test]
    fn test_three_valued_logic() {
        let and_table = [
            ("T", "T", "T"),
            ("T", "F", "F"),
            ("T", "N", "N"),
            ("T", "-", "N"),
            ("F", "T", "F"),
            ("F", "F", "F"),
            ("F", "N", "F"),
            ("F", "-", "F"),
            ("N", "T", "N"),
            ("N", "F", "F"),
            ("N", "N", "N"),
            ("N", "-", "N"),
            ("-", "T", "N"),
            ("-", "F", "F"),
            ("-", "N", "N"),
            ("-", "-", "N"),
        ];
        for (l, r, want) in and_table {
            let exec = AndExpressionExecutor::new(operand(l), operand(r)).unwrap();
            assert_eq!(exec.execute(None), result(want), "{l} AND {r}");
        }

        let or_table = [
            ("T", "T", "T"),
            ("T", "F", "T"),
            ("T", "N", "T"),
            ("T", "-", "T"),
            ("F", "T", "T"),
            ("F", "F", "F"),
            ("F", "N", "N"),
            ("F", "-", "N"),
            ("N", "T", "T"),
            ("N", "F", "N"),
            ("N", "N", "N"),
            ("N", "-", "N"),
            // A left side with no value still runs the right side (#241)
            ("-", "T", "T"),
            ("-", "F", "N"),
            ("-", "N", "N"),
            ("-", "-", "N"),
        ];
        for (l, r, want) in or_table {
            let exec = OrExpressionExecutor::new(operand(l), operand(r)).unwrap();
            assert_eq!(exec.execute(None), result(want), "{l} OR {r}");
        }

        for (v, want) in [("T", "F"), ("F", "T"), ("N", "N"), ("-", "N")] {
            let exec = NotExpressionExecutor::new(operand(v)).unwrap();
            assert_eq!(exec.execute(None), result(want), "NOT {v}");
        }
    }
}
