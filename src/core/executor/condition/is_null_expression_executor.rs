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

// Corresponds to io.eventflux.core.executor.condition.IsNullConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// `IS NULL` check on an expression value. True for NULL and for no value.
///
/// This is the attribute version. The Java stream version
/// (IsNullStreamConditionExpressionExecutor, which takes a stream id) is separate.
#[derive(Debug)]
pub struct IsNullExpressionExecutor {
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
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(true)),
            Some(_) => Some(AttributeValue::Bool(false)),
            None => Some(AttributeValue::Bool(true)), // No value counts as NULL. Java returns TRUE
                                                      // when execute() gives null, which maps to None.
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
        ))
    }
}
