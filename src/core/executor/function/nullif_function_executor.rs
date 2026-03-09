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

// src/core/executor/function/nullif_function_executor.rs
// NULLIF(a, b) returns NULL if a = b, otherwise returns a

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

#[derive(Debug)]
pub struct NullIfFunctionExecutor {
    first_executor: Box<dyn ExpressionExecutor>,
    second_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType,
}

impl NullIfFunctionExecutor {
    pub fn new(
        first_executor: Box<dyn ExpressionExecutor>,
        second_executor: Box<dyn ExpressionExecutor>,
    ) -> Self {
        let return_type = first_executor.get_return_type();
        Self {
            first_executor,
            second_executor,
            return_type,
        }
    }
}

impl ExpressionExecutor for NullIfFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let first_value = self.first_executor.execute(event)?;
        let second_value = self.second_executor.execute(event)?;

        // NULLIF returns NULL if the two values are equal, otherwise returns the first value
        if first_value == second_value {
            Some(AttributeValue::Null)
        } else {
            Some(first_value)
        }
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
        Box::new(NullIfFunctionExecutor::new(
            self.first_executor.clone_executor(eventflux_app_context),
            self.second_executor.clone_executor(eventflux_app_context),
        ))
    }
}
