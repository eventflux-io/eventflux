// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/not_expression_executor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor // For clone_executor

#[derive(Debug)]
pub struct NotExpressionExecutor {
    executor: Box<dyn ExpressionExecutor>,
}

impl NotExpressionExecutor {
    pub fn new(executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if executor.get_return_type() != ApiAttributeType::BOOL {
            // Corrected
            return Err(format!(
                "Operand for NOT executor returns {:?} instead of BOOL",
                executor.get_return_type()
            ));
        }
        Ok(Self { executor })
    }
}

impl ExpressionExecutor for NotExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.executor.execute(event) {
            Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(!b)),
            Some(AttributeValue::Null) => Some(AttributeValue::Null), // NOT NULL is NULL
            None => None, // Error or no value from child
            _ => {
                // Type error
                Some(AttributeValue::Bool(false)) // Or None or Err
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        // Corrected
        ApiAttributeType::BOOL // Corrected
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(
            NotExpressionExecutor::new(self.executor.clone_executor(eventflux_app_context))
                .expect("Cloning NotExpressionExecutor failed"),
        )
    }
}
