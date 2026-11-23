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

// Corresponds to io.eventflux.core.executor.condition.BoolConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// Wraps a BOOL expression so it can be used as a condition.
///
/// NULL, no value, or a non BOOL result counts as false.
#[derive(Debug)]
pub struct BoolExpressionExecutor {
    condition_executor: Box<dyn ExpressionExecutor>,
}

impl BoolExpressionExecutor {
    pub fn new(condition_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if condition_executor.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "BoolConditionExpressionExecutor expects an inner executor returning BOOL, but got {:?}",
                condition_executor.get_return_type()
            ));
        }
        Ok(Self { condition_executor })
    }
}

impl ExpressionExecutor for BoolExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.condition_executor.execute(event) {
            Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(b)),
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // NULL counts as false
            None => Some(AttributeValue::Bool(false)), // No value counts as false
            _ => {
                // The constructor checks the declared type. A non BOOL value at runtime
                // still counts as false.
                Some(AttributeValue::Bool(false))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(
            BoolExpressionExecutor::new(
                self.condition_executor
                    .clone_executor(eventflux_app_context),
            )
            .expect("Cloning BoolExpressionExecutor failed"),
        ) // Cannot fail, since the original passed the same type check
    }
}
