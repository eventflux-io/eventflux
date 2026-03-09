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

// src/core/executor/function/uuid_function_executor.rs
// Corresponds to io.eventflux.core.executor.function.UUIDFunctionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType; // Import Type enum
use std::sync::Arc; // For EventFluxAppContext in clone_executor
use uuid::Uuid; // Requires `uuid` crate with "v4" feature // For clone_executor

// Java UUIDFunctionExecutor extends FunctionExecutor but is stateless and takes no arguments.
#[derive(Debug, Default, Clone)] // Can be Clone and Default as it has no fields
pub struct UuidFunctionExecutor;

impl UuidFunctionExecutor {
    pub fn new() -> Self {
        // Java init checks attributeExpressionExecutors.length == 0
        // This is implicit if new() takes no arguments.
        Default::default()
    }
}

impl ExpressionExecutor for UuidFunctionExecutor {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        // Java execute(Object data, S state) returns UUID.randomUUID().toString();
        // `data` would be null as there are no args.
        Some(AttributeValue::String(
            Uuid::new_v4().hyphenated().to_string(),
        ))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}
