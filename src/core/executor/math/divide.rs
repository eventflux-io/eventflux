// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/math/divide.rs
use super::common::CoerceNumeric;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum // Use CoerceNumeric from common.rs

#[derive(Debug)]
pub struct DivideExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType, // Division often results in float/double
}

impl DivideExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        let left_type = left.get_return_type();
        let right_type = right.get_return_type();

        // Determine return type: typically Double for division, unless both are Int/Long and result should be Int/Long (integer division).
        // EventFlux's Java typed executors (e.g., DivideExpressionExecutorInt) suggest it maintains integer division if possible.
        // However, to avoid loss of precision and match general SQL behavior for `/`, promoting to DOUBLE is safest.
        // If strict integer division is needed, a separate DivInt executor or different operator could be used.
        // For now, promoting to DOUBLE for `/` operator.
        let return_type = match (left_type, right_type) {
            (ApiAttributeType::STRING, _)
            | (_, ApiAttributeType::STRING)
            | (ApiAttributeType::BOOL, _)
            | (_, ApiAttributeType::BOOL)
            | (ApiAttributeType::OBJECT, _)
            | (_, ApiAttributeType::OBJECT) => {
                return Err(format!(
                    "Division not supported for input types {left_type:?} and {right_type:?}"
                ));
            }
            // Any division involving numbers results in DOUBLE for safety and precision by default
            _ => ApiAttributeType::DOUBLE,
        };
        // One could refine this: if both are INT, maybe return INT (integer division), or if both are LONG, return LONG.
        // Or, if strict type matching like Java's per-type executors is desired:
        // let return_type = match (left_type, right_type) {
        //     (ApiAttributeType::DOUBLE, _) | (_, ApiAttributeType::DOUBLE) => ApiAttributeType::DOUBLE,
        //     (ApiAttributeType::FLOAT, _) | (_, ApiAttributeType::FLOAT) => ApiAttributeType::FLOAT,
        //     (ApiAttributeType::LONG, _) | (_, ApiAttributeType::LONG) => ApiAttributeType::LONG, // if long div long -> long
        //     (ApiAttributeType::INT, _) | (_, ApiAttributeType::INT) => ApiAttributeType::INT, // if int div int -> int
        //     _ => return Err(...)
        // };
        // The prompt example for Add determined return type by promoting. Let's follow that.
        // The Add example promotes to the "largest" type present. Division is trickier.
        // For now, will default to Double as per the prompt's general approach of a single executor per op.

        Ok(Self {
            left_executor: left,
            right_executor: right,
            return_type,
        })
    }
}

impl ExpressionExecutor for DivideExpressionExecutor {
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

                // Coerce both to f64 for division, then cast to return_type if needed (though return_type is usually DOUBLE)
                let l = left_val.to_f64_or_err_str("Divide")?;
                let r = right_val.to_f64_or_err_str("Divide")?;

                if r == 0.0 {
                    // Division by zero. EventFlux might return null or throw an error.
                    // Returning Null is a common SQL behavior.
                    // log_error!("Division by zero error");
                    return Some(AttributeValue::Null);
                }
                let result = l / r;

                match self.return_type {
                    // This logic might be simplified if return_type is always DOUBLE
                    ApiAttributeType::DOUBLE => Some(AttributeValue::Double(result)),
                    ApiAttributeType::FLOAT => Some(AttributeValue::Float(result as f32)),
                    ApiAttributeType::LONG => Some(AttributeValue::Long(result as i64)), // Truncating
                    ApiAttributeType::INT => Some(AttributeValue::Int(result as i32)), // Truncating
                    _ => {
                        // log_error!("Division resulted in unexpected return type: {:?}", self.return_type);
                        None
                    }
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
        Box::new(DivideExpressionExecutor {
            left_executor: self.left_executor.clone_executor(eventflux_app_context),
            right_executor: self.right_executor.clone_executor(eventflux_app_context),
            return_type: self.return_type,
        })
    }
}
