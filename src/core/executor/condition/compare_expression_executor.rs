// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/compare_expression_executor.rs
// Corresponds to io.eventflux.core.executor.condition.compare.CompareConditionExpressionExecutor (abstract class)
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use crate::query_api::expression::condition::CompareOperator as ConditionCompareOperator;

#[derive(Debug)]
pub struct CompareExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    operator: ConditionCompareOperator,
    cmp_type: ComparisonType,
}

#[derive(Debug, Clone, Copy)]
enum ComparisonType {
    Int,
    Long,
    Float,
    Double,
    Bool,
    String,
}

fn as_i32(val: &AttributeValue) -> Option<i32> {
    match val {
        AttributeValue::Int(v) => Some(*v),
        AttributeValue::Long(v) => Some(*v as i32),
        AttributeValue::Float(v) => Some(*v as i32),
        AttributeValue::Double(v) => Some(*v as i32),
        _ => None,
    }
}

fn as_i64(val: &AttributeValue) -> Option<i64> {
    match val {
        AttributeValue::Int(v) => Some(*v as i64),
        AttributeValue::Long(v) => Some(*v),
        AttributeValue::Float(v) => Some(*v as i64),
        AttributeValue::Double(v) => Some(*v as i64),
        _ => None,
    }
}

fn as_f32(val: &AttributeValue) -> Option<f32> {
    match val {
        AttributeValue::Int(v) => Some(*v as f32),
        AttributeValue::Long(v) => Some(*v as f32),
        AttributeValue::Float(v) => Some(*v),
        AttributeValue::Double(v) => Some(*v as f32),
        _ => None,
    }
}

fn as_f64(val: &AttributeValue) -> Option<f64> {
    match val {
        AttributeValue::Int(v) => Some(*v as f64),
        AttributeValue::Long(v) => Some(*v as f64),
        AttributeValue::Float(v) => Some(*v as f64),
        AttributeValue::Double(v) => Some(*v),
        _ => None,
    }
}

fn compare_ord<T: Ord>(l: &T, r: &T, op: ConditionCompareOperator) -> bool {
    match op {
        ConditionCompareOperator::Equal => l == r,
        ConditionCompareOperator::NotEqual => l != r,
        ConditionCompareOperator::GreaterThan => l > r,
        ConditionCompareOperator::GreaterThanEqual => l >= r,
        ConditionCompareOperator::LessThan => l < r,
        ConditionCompareOperator::LessThanEqual => l <= r,
    }
}

fn compare_int(l: i32, r: i32, op: ConditionCompareOperator) -> bool {
    compare_ord(&l, &r, op)
}

fn compare_i64(l: i64, r: i64, op: ConditionCompareOperator) -> bool {
    compare_ord(&l, &r, op)
}

fn compare_f32(l: f32, r: f32, op: ConditionCompareOperator) -> bool {
    match op {
        ConditionCompareOperator::Equal => (l - r).abs() < f32::EPSILON,
        ConditionCompareOperator::NotEqual => (l - r).abs() >= f32::EPSILON,
        ConditionCompareOperator::GreaterThan => l > r,
        ConditionCompareOperator::GreaterThanEqual => l >= r,
        ConditionCompareOperator::LessThan => l < r,
        ConditionCompareOperator::LessThanEqual => l <= r,
    }
}

fn compare_f64(l: f64, r: f64, op: ConditionCompareOperator) -> bool {
    match op {
        ConditionCompareOperator::Equal => (l - r).abs() < f64::EPSILON,
        ConditionCompareOperator::NotEqual => (l - r).abs() >= f64::EPSILON,
        ConditionCompareOperator::GreaterThan => l > r,
        ConditionCompareOperator::GreaterThanEqual => l >= r,
        ConditionCompareOperator::LessThan => l < r,
        ConditionCompareOperator::LessThanEqual => l <= r,
    }
}

fn compare_bool(l: bool, r: bool, op: ConditionCompareOperator) -> bool {
    let lv = if l { 1 } else { 0 };
    let rv = if r { 1 } else { 0 };
    compare_ord(&lv, &rv, op)
}

impl CompareExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
        op: ConditionCompareOperator,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        use ApiAttributeType::*;

        let cmp_type = match (left_type, right_type) {
            (STRING, STRING) => ComparisonType::String,
            (BOOL, BOOL) => {
                if matches!(
                    op,
                    ConditionCompareOperator::Equal | ConditionCompareOperator::NotEqual
                ) {
                    ComparisonType::Bool
                } else {
                    return Err("Only == and != supported for BOOL".to_string());
                }
            }
            (OBJECT, _) | (_, OBJECT) | (STRING, _) | (_, STRING) | (BOOL, _) | (_, BOOL) => {
                return Err(format!(
                    "Cannot compare values of types {left_type:?} and {right_type:?}"
                ));
            }
            _ => {
                if left_type == DOUBLE || right_type == DOUBLE {
                    ComparisonType::Double
                } else if left_type == FLOAT || right_type == FLOAT {
                    ComparisonType::Float
                } else if left_type == LONG || right_type == LONG {
                    ComparisonType::Long
                } else {
                    ComparisonType::Int
                }
            }
        };

        Ok(Self {
            left_executor: left,
            right_executor: right,
            operator: op,
            cmp_type,
        })
    }
}

impl ExpressionExecutor for CompareExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val = self.left_executor.execute(event)?;
        let right_val = self.right_executor.execute(event)?;

        if matches!(left_val, AttributeValue::Null) || matches!(right_val, AttributeValue::Null) {
            return match self.operator {
                ConditionCompareOperator::NotEqual => Some(AttributeValue::Bool(true)),
                _ => Some(AttributeValue::Bool(false)),
            };
        }

        let res = match self.cmp_type {
            ComparisonType::Int => {
                let l = as_i32(&left_val)?;
                let r = as_i32(&right_val)?;
                compare_int(l, r, self.operator)
            }
            ComparisonType::Long => {
                let l = as_i64(&left_val)?;
                let r = as_i64(&right_val)?;
                compare_i64(l, r, self.operator)
            }
            ComparisonType::Float => {
                let l = as_f32(&left_val)?;
                let r = as_f32(&right_val)?;
                compare_f32(l, r, self.operator)
            }
            ComparisonType::Double => {
                let l = as_f64(&left_val)?;
                let r = as_f64(&right_val)?;
                compare_f64(l, r, self.operator)
            }
            ComparisonType::String => {
                let l = match &left_val {
                    AttributeValue::String(s) => s,
                    _ => return None,
                };
                let r = match &right_val {
                    AttributeValue::String(s) => s,
                    _ => return None,
                };
                compare_ord(l, r, self.operator)
            }
            ComparisonType::Bool => {
                let l = match &left_val {
                    AttributeValue::Bool(b) => *b,
                    _ => return None,
                };
                let r = match &right_val {
                    AttributeValue::Bool(b) => *b,
                    _ => return None,
                };
                compare_bool(l, r, self.operator)
            }
        };

        Some(AttributeValue::Bool(res))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(CompareExpressionExecutor {
            left_executor: self.left_executor.clone_executor(eventflux_app_context),
            right_executor: self.right_executor.clone_executor(eventflux_app_context),
            operator: self.operator,
            cmp_type: self.cmp_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;
    use crate::query_api::expression::condition::compare::Operator as ApiCompareOperator;
    // ApiAttributeType is imported in the outer scope
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::executor::expression_executor::ExpressionExecutor;
    use std::sync::Arc;

    #[test]
    fn test_compare_greater_than_int_true() {
        // Renamed as per subtask suggestion (though original would be fine too)
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(20),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(10),
            ApiAttributeType::INT,
        ));
        let cmp_exec =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::GreaterThan)
                .unwrap();

        let result = cmp_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(true)));
        assert_eq!(cmp_exec.get_return_type(), ApiAttributeType::BOOL);
    }

    #[test]
    fn test_compare_less_than_int_false() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(20),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(10),
            ApiAttributeType::INT,
        ));
        let cmp_exec =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::LessThan)
                .unwrap();

        let result = cmp_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(false)));
    }

    #[test]
    fn test_compare_equal_float_true() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Float(10.5),
            ApiAttributeType::FLOAT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Float(10.5),
            ApiAttributeType::FLOAT,
        ));
        let cmp_exec =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::Equal)
                .unwrap();

        let result = cmp_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_compare_not_equal_string_true() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("hello".to_string()),
            ApiAttributeType::STRING,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("world".to_string()),
            ApiAttributeType::STRING,
        ));
        let cmp_exec =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::NotEqual)
                .unwrap();

        let result = cmp_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_compare_with_null_operand() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(20),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Null,
            ApiAttributeType::INT,
        )); // Null operand
        let cmp_exec =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::GreaterThan)
                .unwrap();

        // Current logic: if either operand is Null, returns Bool(false)
        let result = cmp_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(false)));
    }

    #[test]
    fn test_compare_incompatible_types() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(20),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("text".to_string()),
            ApiAttributeType::STRING,
        ));
        let res =
            CompareExpressionExecutor::new(left_exec, right_exec, ApiCompareOperator::GreaterThan);
        assert!(res.is_err());
    }

    #[test]
    fn test_compare_clone() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(100),
            ApiAttributeType::LONG,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(50),
            ApiAttributeType::LONG,
        ));
        let cmp_exec = CompareExpressionExecutor::new(
            left_exec,
            right_exec,
            ApiCompareOperator::GreaterThanEqual,
        )
        .unwrap();

        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = cmp_exec.clone_executor(&app_ctx_placeholder);

        let result = cloned_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Bool(true)));
        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::BOOL);
    }

    #[test]
    fn test_operator_equal_int() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(5),
            ApiAttributeType::INT,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(5),
            ApiAttributeType::INT,
        ));
        let cmp = CompareExpressionExecutor::new(left, right, ApiCompareOperator::Equal).unwrap();
        assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_operator_not_equal_long() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(5),
            ApiAttributeType::LONG,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(10),
            ApiAttributeType::LONG,
        ));
        let cmp =
            CompareExpressionExecutor::new(left, right, ApiCompareOperator::NotEqual).unwrap();
        assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_operator_greater_than_cross() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Float(7.5),
            ApiAttributeType::FLOAT,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(7),
            ApiAttributeType::INT,
        ));
        let cmp =
            CompareExpressionExecutor::new(left, right, ApiCompareOperator::GreaterThan).unwrap();
        assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_operator_greater_than_equal_cross() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Double(7.0),
            ApiAttributeType::DOUBLE,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Long(7),
            ApiAttributeType::LONG,
        ));
        let cmp = CompareExpressionExecutor::new(left, right, ApiCompareOperator::GreaterThanEqual)
            .unwrap();
        assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_operator_less_than_string() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("apple".to_string()),
            ApiAttributeType::STRING,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::String("banana".to_string()),
            ApiAttributeType::STRING,
        ));
        let cmp =
            CompareExpressionExecutor::new(left, right, ApiCompareOperator::LessThan).unwrap();
        assert_eq!(cmp.execute(None), Some(AttributeValue::Bool(true)));
    }

    #[test]
    fn test_operator_less_than_equal_bool() {
        let left = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Bool(false),
            ApiAttributeType::BOOL,
        ));
        let right = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Bool(true),
            ApiAttributeType::BOOL,
        ));
        assert!(
            CompareExpressionExecutor::new(left, right, ApiCompareOperator::LessThanEqual).is_err()
        );
    }
}
