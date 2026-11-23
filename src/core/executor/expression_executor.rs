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

// Corresponds to io.eventflux.core.executor.ExpressionExecutor (interface)

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::any::Any;
use std::fmt::Debug;
// use std::sync::Arc; // Not needed for trait definition itself

/// Trait for all expression executors. An executor runs one parsed EventFlux expression
/// against an event and returns its value.
pub trait ExpressionExecutor: Debug + Send + Sync + 'static {
    // `event` is optional because some expressions, like constants, do not need an event.
    // Others, like variables, do.
    // Returns None when there is no value or the expression could not be evaluated, much
    // like Java returning null. Errors are not reported separately.
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue>;

    // Type of the value this executor returns
    fn get_return_type(&self) -> ApiAttributeType;

    // Makes an independent copy of the executor, for example when the execution plan is
    // copied for partitioning. Implementations copy child executors by calling their own
    // clone_executor. The context is passed to every implementation so executors that hold
    // context can use the new one, even though stateless ones like
    // ConstantExpressionExecutor ignore it.
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

    /// Returns true if this executor is a VariableExpressionExecutor, which reads event
    /// attributes. Used to validate places where only variable expressions are allowed.
    fn is_variable_executor(&self) -> bool {
        false
    }
}

// There is no `impl Clone for Box<dyn ExpressionExecutor>`. A Clone bound on the trait does
// not work well with trait objects, and Clone::clone cannot take the app context. Call
// `executor.clone_executor(ctx)` instead.
//         self.clone_executor(???) // Problem: clone() doesn't take app_context
//     }
// }

// Used by the clone_executor signature above
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use std::sync::Arc;
