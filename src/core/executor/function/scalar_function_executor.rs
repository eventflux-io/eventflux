/*
 * Copyright 2025-2026 EventFlux.io
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::executor::expression_executor::ExpressionExecutor;
use std::sync::Arc;

/// Trait for user defined scalar functions (UDFs).
///
/// A `ScalarFunctionExecutor` is both the factory and the runtime instance of a
/// scalar function. The instance registered with
/// [`EventFluxManager`](crate::core::eventflux_manager::EventFluxManager) is cloned
/// with [`clone_scalar_function`] each time the parser needs a new copy for a
/// query plan. So implementations can hold any internal state (for example inside
/// a `Box<dyn Any>`), set it up in [`init`] and clean it up in [`destroy`].
pub trait ScalarFunctionExecutor: ExpressionExecutor {
    /// Sets up the function instance.
    ///
    /// The expression parser calls this once the argument executors for a function
    /// call are ready. Implementations can use it to check argument types, keep
    /// references to the arguments, or set up any state that
    /// [`execute`](ExpressionExecutor::execute) needs. An `Err` fails the build of
    /// the calling executor.
    fn init(
        &mut self,
        argument_executors: &[Box<dyn ExpressionExecutor>],
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<(), String>;

    /// Releases any resources this instance holds.
    ///
    /// Called when the owning [`AttributeFunctionExpressionExecutor`]
    /// is dropped. The default does nothing.
    fn destroy(&mut self) {}

    // Name of the function, for example "myUDF" or "custom:myUDF". Used to identify it.
    fn get_name(&self) -> String;

    // Makes a fresh instance of the function, ready for its own `init`. Called when the
    // owning AttributeFunctionExpressionExecutor is cloned and when the parser takes a copy
    // from the registered factory.
    fn clone_scalar_function(&self) -> Box<dyn ScalarFunctionExecutor>;
}

// Lets Box<dyn ScalarFunctionExecutor> be cloned through clone_scalar_function
impl Clone for Box<dyn ScalarFunctionExecutor> {
    fn clone(&self) -> Self {
        self.clone_scalar_function()
    }
}

// No hand written Debug for Box<dyn ScalarFunctionExecutor> is needed. The trait requires
// Debug, so the standard Debug for Box<T> covers it.
// impl Debug for Box<dyn ScalarFunctionExecutor> {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        f.debug_struct("Box<dyn ScalarFunctionExecutor>")
//         .field("name", &self.get_name())
//         .finish_non_exhaustive()
//    }
// }
