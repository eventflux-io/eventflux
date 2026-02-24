// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/math/multiply.rs
use super::common::CoerceNumeric; // Use CoerceNumeric from common.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Corrected import
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiplyExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType, // Corrected type
}

impl MultiplyExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        let return_type = match (left_type, right_type) {
            // Handle error cases first
            (ApiAttributeType::STRING, _)
            | (_, ApiAttributeType::STRING)
            | (ApiAttributeType::BOOL, _)
            | (_, ApiAttributeType::BOOL)
            | (ApiAttributeType::OBJECT, _)
            | (_, ApiAttributeType::OBJECT) => {
                return Err(format!(
                    "Multiplication not supported for input types {left_type:?} and {right_type:?}"
                ));
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

impl ExpressionExecutor for MultiplyExpressionExecutor {
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
                        let l = left_val.to_i32_or_err_str("Multiply")?;
                        let r = right_val.to_i32_or_err_str("Multiply")?;
                        Some(AttributeValue::Int(l.wrapping_mul(r)))
                    }
                    ApiAttributeType::LONG => {
                        let l = left_val.to_i64_or_err_str("Multiply")?;
                        let r = right_val.to_i64_or_err_str("Multiply")?;
                        Some(AttributeValue::Long(l.wrapping_mul(r)))
                    }
                    ApiAttributeType::FLOAT => {
                        let l = left_val.to_f32_or_err_str("Multiply")?;
                        let r = right_val.to_f32_or_err_str("Multiply")?;
                        Some(AttributeValue::Float(l * r))
                    }
                    ApiAttributeType::DOUBLE => {
                        let l = left_val.to_f64_or_err_str("Multiply")?;
                        let r = right_val.to_f64_or_err_str("Multiply")?;
                        Some(AttributeValue::Double(l * r))
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    } // Corrected

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(
            MultiplyExpressionExecutor::new(
                self.left_executor.clone_executor(eventflux_app_context),
                self.right_executor.clone_executor(eventflux_app_context),
            )
            .expect("Cloning MultiplyExpressionExecutor failed"),
        )
    }
}
