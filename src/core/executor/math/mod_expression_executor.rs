// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/math/mod_expression_executor.rs
use super::common::CoerceNumeric;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum // Use CoerceNumeric from common.rs

#[derive(Debug)]
pub struct ModExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType, // Modulo result type usually matches operand types after promotion
}

impl ModExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        // Type promotion for modulo: result type is usually the type of the dividend after promoting both to a common numeric type.
        // EventFlux's typed ModExpressionExecutors (Int, Long, Float, Double) imply the result type matches this promoted type.
        let return_type = match (left_type, right_type) {
            // Handle error cases first
            (ApiAttributeType::STRING, _)
            | (_, ApiAttributeType::STRING)
            | (ApiAttributeType::BOOL, _)
            | (_, ApiAttributeType::BOOL)
            | (ApiAttributeType::OBJECT, _)
            | (_, ApiAttributeType::OBJECT) => {
                return Err(format!(
                    "Modulo not supported for input types {left_type:?} and {right_type:?}"
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

impl ExpressionExecutor for ModExpressionExecutor {
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

                // Division by zero check for modulo
                let temp_r_for_zero_check = right_val.to_f64_or_err_str("Mod")?; // Check as f64 to catch all numeric zeros
                if temp_r_for_zero_check == 0.0 {
                    // log_error!("Modulo by zero error");
                    return Some(AttributeValue::Null);
                }

                match self.return_type {
                    ApiAttributeType::INT => {
                        let l = left_val.to_i32_or_err_str("Mod")?;
                        let r = right_val.to_i32_or_err_str("Mod")?;
                        if r == 0 {
                            return Some(AttributeValue::Null);
                        } // Explicit check for integer 0
                        Some(AttributeValue::Int(l % r))
                    }
                    ApiAttributeType::LONG => {
                        let l = left_val.to_i64_or_err_str("Mod")?;
                        let r = right_val.to_i64_or_err_str("Mod")?;
                        if r == 0 {
                            return Some(AttributeValue::Null);
                        }
                        Some(AttributeValue::Long(l % r))
                    }
                    ApiAttributeType::FLOAT => {
                        let l = left_val.to_f32_or_err_str("Mod")?;
                        let r = right_val.to_f32_or_err_str("Mod")?;
                        if r == 0.0 {
                            return Some(AttributeValue::Null);
                        }
                        Some(AttributeValue::Float(l % r))
                    }
                    ApiAttributeType::DOUBLE => {
                        let l = left_val.to_f64_or_err_str("Mod")?;
                        let r = right_val.to_f64_or_err_str("Mod")?;
                        if r == 0.0 {
                            return Some(AttributeValue::Null);
                        }
                        Some(AttributeValue::Double(l % r))
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
        Box::new(ModExpressionExecutor {
            left_executor: self.left_executor.clone_executor(eventflux_app_context),
            right_executor: self.right_executor.clone_executor(eventflux_app_context),
            return_type: self.return_type,
        })
    }
}
