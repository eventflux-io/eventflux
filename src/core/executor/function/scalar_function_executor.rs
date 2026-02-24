// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/scalar_function_executor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::executor::expression_executor::ExpressionExecutor;
use std::sync::Arc;

/// Trait for implementing user defined scalar functions (UDFs).
///
/// A `ScalarFunctionExecutor` is both a factory and the runtime instance of a
/// scalar function.  The same instance that is registered with
/// [`EventFluxManager`](crate::core::eventflux_manager::EventFluxManager) will be cloned
/// using [`clone_scalar_function`] whenever the parser requires a new copy for a
/// query plan.  Implementations are therefore free to hold arbitrary internal
/// state (for example inside a `Box<dyn Any>`) which can be initialised during
/// [`init`] and cleaned up in [`destroy`].
pub trait ScalarFunctionExecutor: ExpressionExecutor {
    /// Initialise the function instance.
    ///
    /// This hook is invoked by the expression parser once the argument
    /// executors for a function call are available.  Implementations may use it
    /// to validate argument types, capture references to the arguments or set up
    /// any internal state required for [`execute`](ExpressionExecutor::execute).
    fn init(
        &mut self,
        argument_executors: &Vec<Box<dyn ExpressionExecutor>>,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String>;

    /// Release any resources held by this instance.
    ///
    /// Called when the corresponding [`AttributeFunctionExpressionExecutor`]
    /// is dropped.  The default implementation does nothing.
    fn destroy(&mut self) {}

    // Returns the name of the function (e.g., "myUDF" or "custom:myUDF")
    // Used for identification and potentially by the Debug impl.
    fn get_name(&self) -> String;

    // For cloning itself when the parent AttributeFunctionExpressionExecutor is cloned.
    // This should create a "fresh" instance of the UDF logic, ready for its own `init`.
    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor>;
}

// Helper for cloning Box<dyn ScalarFunctionExecutor>
impl Clone for Box<dyn ScalarFunctionExecutor> {
    fn clone(&self) -> Self {
        self.clone_scalar_function()
    }
}

// Manual Debug for Box<dyn ScalarFunctionExecutor> is removed.
// The default Debug impl for Box<T> where T: Debug should be sufficient,
// and ScalarFunctionExecutor requires Debug.
// impl Debug for Box<dyn ScalarFunctionExecutor> {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        f.debug_struct("Box<dyn ScalarFunctionExecutor>")
//         .field("name", &self.get_name())
//         .finish_non_exhaustive()
//    }
// }
