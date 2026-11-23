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

// Corresponds to io.eventflux.core.executor.function.CoalesceFunctionExecutor
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

/// COALESCE function. Returns the first argument value that is not NULL.
///
/// Stateless, unlike the Java version, which extends the stateful FunctionExecutor.
#[derive(Debug)] // No Clone because of the Vec<Box<dyn ExpressionExecutor>> field
pub struct CoalesceFunctionExecutor {
    attribute_executors: Vec<Box<dyn ExpressionExecutor>>,
    return_type: ApiAttributeType, // The shared type of all arguments
}

impl CoalesceFunctionExecutor {
    /// # Errors
    ///
    /// Returns an error when there are no arguments or when the arguments do not all have
    /// the same return type.
    pub fn new(attribute_executors: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if attribute_executors.is_empty() {
            return Err(
                "CoalesceFunctionExecutor requires at least one attribute executor".to_string(),
            );
        }
        // All arguments must share one type, which becomes the return type. Java does the
        // same check in its `init` method.
        let first_type = attribute_executors[0].get_return_type();
        for exec in &attribute_executors[1..] {
            if exec.get_return_type() != first_type {
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
        // Arguments run in order and stop at the first value that is not NULL
        for executor in &self.attribute_executors {
            match executor.execute(event) {
                Some(AttributeValue::Null) => continue,
                Some(value) => return Some(value),
                None => continue, // No value counts as NULL
            }
        }
        Some(AttributeValue::Null) // Every argument was NULL or gave no value
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
        // Panics if `new` rejects the copies. That should not happen, since the copies keep
        // the same return types the original passed with.
        Box::new(
            CoalesceFunctionExecutor::new(cloned_execs)
                .expect("Cloning CoalesceFunctionExecutor failed"),
        )
    }
}
