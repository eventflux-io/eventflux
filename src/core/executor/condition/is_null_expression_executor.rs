// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/condition/is_null_expression_executor.rs
// Corresponds to io.eventflux.core.executor.condition.IsNullConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor // For clone_executor

#[derive(Debug)]
pub struct IsNullExpressionExecutor {
    // In Java, IsNullConditionExpressionExecutor takes one ExpressionExecutor.
    // IsNullStreamConditionExpressionExecutor is different (takes streamId, etc.)
    // This struct is for the attribute version.
    executor: Box<dyn ExpressionExecutor>,
}

impl IsNullExpressionExecutor {
    pub fn new(executor: Box<dyn ExpressionExecutor>) -> Self {
        Self { executor }
    }
}

impl ExpressionExecutor for IsNullExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.executor.execute(event) {
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(true)), // If it's explicitly Null
            Some(_) => Some(AttributeValue::Bool(false)), // If it's any other value
            None => Some(AttributeValue::Bool(true)), // If the expression failed to execute / returned no value, treat as null.
                                                      // This behavior might need to align with EventFlux's specific null propagation for IS NULL.
                                                      // Java: Object result = expressionExecutor.execute(event); if (result == null) return TRUE; else return FALSE;
                                                      // This implies if execute() returns Java null (Rust None), it's true.
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(IsNullExpressionExecutor::new(
            self.executor.clone_executor(eventflux_app_context),
        )) // new doesn't return Result, so no unwrap needed
    }
}
