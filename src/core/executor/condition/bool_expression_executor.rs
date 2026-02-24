// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/bool_expression_executor.rs
// Corresponds to io.eventflux.core.executor.condition.BoolConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor // For clone_executor

#[derive(Debug)]
pub struct BoolExpressionExecutor {
    condition_executor: Box<dyn ExpressionExecutor>,
}

impl BoolExpressionExecutor {
    pub fn new(condition_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if condition_executor.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "BoolConditionExpressionExecutor expects an inner executor returning BOOL, but got {:?}",
                condition_executor.get_return_type()
            ));
        }
        Ok(Self { condition_executor })
    }
}

impl ExpressionExecutor for BoolExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.condition_executor.execute(event) {
            Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(b)),
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // null is false in boolean context
            None => Some(AttributeValue::Bool(false)), // Error/no-value treated as false
            _ => {
                // This case should ideally be prevented by the constructor check,
                // but as a fallback, non-bool result is treated as false.
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
            BoolExpressionExecutor::new(
                self.condition_executor
                    .clone_executor(eventflux_app_context),
            )
            .expect("Cloning BoolExpressionExecutor failed"),
        ) // new returns Result
    }
}
