// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/expression_executor.rs
// Corresponds to io.eventflux.core.executor.ExpressionExecutor (interface)

use crate::core::event::complex_event::ComplexEvent; // The ComplexEvent trait
use crate::core::event::value::AttributeValue; // The enum for actual data values
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::any::Any;
use std::fmt::Debug;
// use std::sync::Arc; // Not needed for trait definition itself

// ExpressionExecutor is an interface in Java. In Rust, it's a trait.
// It's responsible for executing a parsed EventFlux expression and returning a value.
/// Trait for all expression executors which can be executed on an event.
pub trait ExpressionExecutor: Debug + Send + Sync + 'static {
    // `event` is Option<&dyn ComplexEvent> because some expressions (like constants)
    // don't need an event to be evaluated. Others (like variables) do.
    // The Option<&dyn ComplexEvent> allows passing None for constant expressions.
    // The return type is Option<AttributeValue> because an expression might evaluate to null,
    // or an error could occur during execution (though errors might be better handled by Result).
    // Java returns Object, which can be null.
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue>;

    // Returns the data type of the value that this executor will return.
    fn get_return_type(&self) -> ApiAttributeType;

    // Method to clone the executor.
    // This is useful if the execution plan needs to be cloned (e.g., for partitioning).
    // Requires EventFluxAppContext because some executors might hold references or need context for cloning.
    // For stateless executors like ConstantExpressionExecutor, context might not be strictly needed for cloning itself,
    // but the interface should be consistent.
    // Child executors (if any) would be cloned recursively using their own clone_executor methods.
    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor>;

    fn as_any(&self) -> &dyn Any
    where
        Self: Sized + 'static,
    {
        self
    }

    fn is_attribute_aggregator(&self) -> bool {
        false
    }

    /// Returns true if this executor is a VariableExpressionExecutor (accesses event attributes)
    /// This is used for validation in contexts where only variable expressions are allowed
    fn is_variable_executor(&self) -> bool {
        false
    }
}

// To allow `Box<dyn ExpressionExecutor>` to be `Clone`.
// This requires that all implementors of `ExpressionExecutor` are also `Clone`
// and that their `clone_executor` method correctly reconstructs them.
// However, making the trait itself require Clone (pub trait ExpressionExecutor: Debug + Send + Sync + Clone)
// is often problematic with trait objects. The clone_box pattern (or clone_executor here) is preferred.
// We don't need `impl Clone for Box<dyn ExpressionExecutor>` if we just call `executor.clone_executor()`.
//         self.clone_executor(???) // Problem: clone() doesn't take app_context
//     }
// }
// So, direct calls to `some_box_dyn_exec.clone_executor(ctx)` are better.

// Added Arc import, which might be needed by clone_executor implementations if they store Arc<EventFluxAppContext>
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use std::sync::Arc;
