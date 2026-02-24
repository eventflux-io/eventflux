// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/coalesce_function_executor.rs
// Corresponds to io.eventflux.core.executor.function.CoalesceFunctionExecutor
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
                                                                       // Note: Java CoalesceFunctionExecutor extends FunctionExecutor, which handles state.
                                                                       // This simplified Rust version makes it stateless for now.

#[derive(Debug)] // Clone not straightforward due to Vec<Box<dyn ExpressionExecutor>>
pub struct CoalesceFunctionExecutor {
    attribute_executors: Vec<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType, // Determined by the first non-nullable executor, or common type.
}

impl CoalesceFunctionExecutor {
    pub fn new(attribute_executors: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if attribute_executors.is_empty() {
            return Err(
                "CoalesceFunctionExecutor requires at least one attribute executor".to_string(),
            );
        }
        // Determine return type: In Java, it checks if all parameters are of the same type.
        // If so, that type is the return type. If not, it throws a validation exception.
        // This logic is in the `init` method of the Java version.
        let first_type = attribute_executors[0].get_return_type();
        for exec in &attribute_executors[1..] {
            if exec.get_return_type() != first_type {
                // This is stricter than just picking the first. Java ensures all are same.
                return Err(format!(
                    "Coalesce parameters must all be of the same type. Found {:?} and {:?}",
                    first_type,
                    exec.get_return_type()
                ));
            }
        }
        let return_type = first_type;
        Ok(Self {
            attribute_executors,
            return_type,
        })
    }
}

impl ExpressionExecutor for CoalesceFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        // Java's FunctionExecutor.execute(ComplexEvent) calls an abstract execute(Object[] data, S state)
        // or execute(Object data, S state).
        // CoalesceFunctionExecutor implements execute(Object[] data, S state).
        // It iterates through data (results of attribute_executors) and returns the first non-null.

        for executor in &self.attribute_executors {
            match executor.execute(event) {
                Some(AttributeValue::Null) => continue, // Explicitly check for our Null variant
                Some(value) => return Some(value),      // Found a non-null value
                None => continue, // If an executor itself returns None (e.g. error or no value), treat as null for coalesce
            }
        }
        Some(AttributeValue::Null) // If all executors returned Null or None
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
        let cloned_execs = self
            .attribute_executors
            .iter()
            .map(|e| e.clone_executor(eventflux_app_context))
            .collect();
        // new already determines the return type from the cloned_execs.
        // If new fails (e.g. if cloning results in incompatible types somehow, though unlikely here),
        // this clone_executor call will panic. This might be acceptable if cloning is expected to succeed.
        Box::new(
            CoalesceFunctionExecutor::new(cloned_execs)
                .expect("Cloning CoalesceFunctionExecutor failed"),
        )
    }
}
