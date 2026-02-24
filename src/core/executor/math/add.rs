// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/math/add.rs
use super::common::CoerceNumeric;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum // Use CoerceNumeric from common.rs

#[derive(Debug)] // Clone not straightforward due to Box<dyn ExpressionExecutor>
pub struct AddExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType, // Determined at construction based on operand types
}

impl AddExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        let return_type = match (left_type, right_type) {
            // Handle error cases first
            (ApiAttributeType::STRING, _) | (_, ApiAttributeType::STRING) => {
                return Err(format!("String concatenation with '+' is not supported. Use concat(). Found input types {left_type:?} and {right_type:?}."));
            }
            (ApiAttributeType::BOOL, _) | (_, ApiAttributeType::BOOL) => {
                return Err(format!("Arithmetic addition not supported for BOOL types. Found input types {left_type:?} and {right_type:?}."));
            }
            (ApiAttributeType::OBJECT, _) | (_, ApiAttributeType::OBJECT) => {
                return Err(format!("Arithmetic addition not supported for OBJECT types. Found input types {left_type:?} and {right_type:?}."));
            }
            // Handle numeric types in order of precedence
            (ApiAttributeType::DOUBLE, ApiAttributeType::DOUBLE) => ApiAttributeType::DOUBLE,
            (ApiAttributeType::DOUBLE, _) | (_, ApiAttributeType::DOUBLE) => {
                ApiAttributeType::DOUBLE
            }
            (ApiAttributeType::FLOAT, ApiAttributeType::FLOAT) => ApiAttributeType::FLOAT,
            (ApiAttributeType::FLOAT, _) | (_, ApiAttributeType::FLOAT) => ApiAttributeType::FLOAT,
            (ApiAttributeType::LONG, ApiAttributeType::LONG) => ApiAttributeType::LONG,
            (ApiAttributeType::LONG, ApiAttributeType::INT)
            | (ApiAttributeType::INT, ApiAttributeType::LONG) => ApiAttributeType::LONG,
            (ApiAttributeType::INT, ApiAttributeType::INT) => ApiAttributeType::INT,
        };
        Ok(Self {
            left_executor: left,
            right_executor: right,
            return_type,
        })
    }
}

impl ExpressionExecutor for AddExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val_opt = self.left_executor.execute(event);
        let right_val_opt = self.right_executor.execute(event);

        match (left_val_opt, right_val_opt) {
            (Some(left_val), Some(right_val)) => {
                if matches!(left_val, AttributeValue::Null)
                    || matches!(right_val, AttributeValue::Null)
                {
                    return Some(AttributeValue::Null);
                }
                match self.return_type {
                    ApiAttributeType::INT => {
                        let l = left_val.to_i32_or_err_str("Add")?;
                        let r = right_val.to_i32_or_err_str("Add")?;
                        Some(AttributeValue::Int(l.wrapping_add(r)))
                    }
                    ApiAttributeType::LONG => {
                        let l = left_val.to_i64_or_err_str("Add")?;
                        let r = right_val.to_i64_or_err_str("Add")?;
                        Some(AttributeValue::Long(l.wrapping_add(r)))
                    }
                    ApiAttributeType::FLOAT => {
                        let l = left_val.to_f32_or_err_str("Add")?;
                        let r = right_val.to_f32_or_err_str("Add")?;
                        Some(AttributeValue::Float(l + r))
                    }
                    ApiAttributeType::DOUBLE => {
                        let l = left_val.to_f64_or_err_str("Add")?;
                        let r = right_val.to_f64_or_err_str("Add")?;
                        Some(AttributeValue::Double(l + r))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(AddExpressionExecutor {
            left_executor: self.left_executor.clone_executor(eventflux_app_context),
            right_executor: self.right_executor.clone_executor(eventflux_app_context),
            return_type: self.return_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;
    // ApiAttributeType is imported in the outer scope
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::executor::expression_executor::ExpressionExecutor;
    use std::sync::Arc;

    #[test]
    fn test_add_int_int() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(5),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(10),
            ApiAttributeType::INT,
        ));
        let add_exec = AddExpressionExecutor::new(left_exec, right_exec).unwrap();

        assert_eq!(add_exec.get_return_type(), ApiAttributeType::INT);
        let result = add_exec.execute(None); // Event is None for constant operands
        assert_eq!(result, Some(AttributeValue::Int(15)));
    }

    #[test]
    fn test_add_float_int_to_float() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Float(5.5),
            ApiAttributeType::FLOAT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(10),
            ApiAttributeType::INT,
        ));
        // Constructor should promote return type to FLOAT
        let add_exec = AddExpressionExecutor::new(left_exec, right_exec).unwrap();

        assert_eq!(add_exec.get_return_type(), ApiAttributeType::FLOAT);
        let result = add_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Float(15.5)));
    }

    #[test]
    fn test_add_null_propagation() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(5),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Null,
            ApiAttributeType::INT,
        )); // One operand is Null
        let add_exec = AddExpressionExecutor::new(left_exec, right_exec).unwrap();

        let result = add_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Null)); // Expect Null result
    }

    #[test]
    fn test_add_clone() {
        let left_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(7),
            ApiAttributeType::INT,
        ));
        let right_exec = Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(8),
            ApiAttributeType::INT,
        ));
        let add_exec = AddExpressionExecutor::new(left_exec, right_exec).unwrap();

        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_add_exec = add_exec.clone_executor(&app_ctx_placeholder);

        assert_eq!(cloned_add_exec.get_return_type(), ApiAttributeType::INT);
        let result = cloned_add_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(15)));
    }
}
