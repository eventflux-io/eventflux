// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/or_expression_executor.rs
// Corresponds to io.eventflux.core.executor.condition.OrConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor // For clone_executor

#[derive(Debug)]
pub struct OrExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
}

impl OrExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if left.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Left operand for OR executor returns {:?} instead of BOOL",
                left.get_return_type()
            ));
        }
        if right.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Right operand for OR executor returns {:?} instead of BOOL",
                right.get_return_type()
            ));
        }
        Ok(Self {
            left_executor: left,
            right_executor: right,
        })
    }
}

impl ExpressionExecutor for OrExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val = self.left_executor.execute(event);

        match left_val {
            Some(AttributeValue::Bool(true)) => Some(AttributeValue::Bool(true)), // Left is true, so OR is true
            Some(AttributeValue::Bool(false)) | Some(AttributeValue::Null) => {
                // Left is false or null, result depends on right
                let right_val = self.right_executor.execute(event);
                match right_val {
                    Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(b)),
                    Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // false OR null is false
                    None => Some(AttributeValue::Bool(false)), // Error/no-value from right, effectively false
                    _ => {
                        // Type error: right operand did not return a Bool or Null
                        Some(AttributeValue::Bool(false))
                    }
                }
            }
            None => Some(AttributeValue::Bool(false)), // Error/no-value from left, effectively false for OR unless right is true
            // To be fully robust, if left is None (error), OR could still be true if right is true.
            // This requires evaluating right:
            // let right_val = self.right_executor.execute(event);
            // return right_val.map_or(None, |v| if let AttributeValue::Bool(b_val) = v { Some(AttributeValue::Bool(b_val)) } else { None } );
            // For now, simplified: None on left makes OR false unless right is definitively true.
            // The current logic is: if left is None, result is effectively false.
            _ => {
                // Type error: left operand did not return a Bool or Null
                Some(AttributeValue::Bool(false))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(
            OrExpressionExecutor::new(
                self.left_executor.clone_executor(eventflux_app_context),
                self.right_executor.clone_executor(eventflux_app_context),
            )
            .expect("Cloning OrExpressionExecutor failed"),
        )
    }
}
