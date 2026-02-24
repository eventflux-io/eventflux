// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/and_expression_executor.rs
// Corresponds to io.eventflux.core.executor.condition.AndConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor // For clone_executor

#[derive(Debug)] // Cannot Clone/Default easily due to Box<dyn ExpressionExecutor>
pub struct AndExpressionExecutor {
    // In Java, these extend ConditionExpressionExecutor, which itself is an ExpressionExecutor.
    // So, the composed executors here are the left and right conditions.
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
}

impl AndExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        // Java constructor checks if both return BOOL.
        if left.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Left operand for AND executor returns {:?} instead of BOOL",
                left.get_return_type()
            ));
        }
        if right.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Right operand for AND executor returns {:?} instead of BOOL",
                right.get_return_type()
            ));
        }
        Ok(Self {
            left_executor: left,
            right_executor: right,
        })
    }
}

impl ExpressionExecutor for AndExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val = self.left_executor.execute(event);

        match left_val {
            Some(AttributeValue::Bool(true)) => {
                // Left is true, result depends on right
                let right_val = self.right_executor.execute(event);
                match right_val {
                    Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(b)),
                    Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // null in boolean context is false
                    None => Some(AttributeValue::Bool(false)), // Or propagate None if an error/no-value occurred
                    _ => {
                        // Type error: right operand did not return a Bool or Null
                        // This should ideally be caught by constructor validation or return Err.
                        // For now, treating as false as per some interpretations of boolean logic with non-booleans.
                        // More robust: return None or Err(ExecutionError::TypeError)
                        Some(AttributeValue::Bool(false))
                    }
                }
            }
            Some(AttributeValue::Bool(false)) => Some(AttributeValue::Bool(false)), // Left is false, so AND is false
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // null AND anything is false
            None => Some(AttributeValue::Bool(false)), // Error/no-value from left, effectively false for AND
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
            AndExpressionExecutor::new(
                self.left_executor.clone_executor(eventflux_app_context),
                self.right_executor.clone_executor(eventflux_app_context),
            )
            .expect("Cloning AndExpressionExecutor failed"),
        ) // Assumes new won't fail if original was valid
    }
}
