// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/constant_expression_executor.rs
// Corresponds to io.eventflux.core.executor.ConstantExpressionExecutor
use super::expression_executor::ExpressionExecutor;
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue; // Enum for actual data
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum

/// Executor that returns a constant value.
#[derive(Debug, Clone)] // Constant values are cloneable
pub struct ConstantExpressionExecutor {
    value: AttributeValue, // Stores the constant value directly using our AttributeValue enum
    return_type: ApiAttributeType, // Stores the type of the constant
}

impl ConstantExpressionExecutor {
    pub fn new(value: AttributeValue, return_type: ApiAttributeType) -> Self {
        // In a more robust implementation, we might validate that the AttributeValue variant
        // matches the provided Attribute::Type.
        // e.g., if return_type is Attribute::Type::INT, value should be AttributeValue::Int.
        // For now, assuming the caller provides consistent types, as in ExpressionParser.
        Self { value, return_type }
    }

    // Getter for the value if needed (not in Java ExpressionExecutor interface)
    pub fn get_value(&self) -> &AttributeValue {
        &self.value
    }
}

impl ExpressionExecutor for ConstantExpressionExecutor {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        // Constants don't depend on the event, so _event is ignored.
        // Return a clone of the stored constant value.
        Some(self.value.clone())
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    // ApiAttributeType is already imported in the outer scope
    use crate::core::config::eventflux_app_context::EventFluxAppContext; // For clone_executor signature
    use crate::core::executor::expression_executor::ExpressionExecutor; // Trait
    use std::sync::Arc;

    #[test]
    fn test_constant_string() {
        let exec = ConstantExpressionExecutor::new(
            AttributeValue::String("hello".to_string()),
            ApiAttributeType::STRING,
        );
        let result = exec.execute(None); // Event is None for constant
        assert_eq!(result, Some(AttributeValue::String("hello".to_string())));
        assert_eq!(exec.get_return_type(), ApiAttributeType::STRING);
    }

    #[test]
    fn test_constant_int() {
        let exec = ConstantExpressionExecutor::new(AttributeValue::Int(123), ApiAttributeType::INT);
        let result = exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(123)));
        assert_eq!(exec.get_return_type(), ApiAttributeType::INT);
    }

    #[test]
    fn test_constant_clone() {
        let exec = ConstantExpressionExecutor::new(
            AttributeValue::String("clone_me".to_string()),
            ApiAttributeType::STRING,
        );
        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = exec.clone_executor(&app_ctx_placeholder);

        let result = cloned_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::String("clone_me".to_string())));
        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::STRING);
    }
}
