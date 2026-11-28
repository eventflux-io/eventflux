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

// Corresponds to io.eventflux.core.executor.condition.OrConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::condition::{truth_result, truth_value};
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// Logical OR of two BOOL conditions, with SQL three valued logic.
///
/// True if either side is true, false if both are false, and NULL otherwise. NULL, no
/// value and a non BOOL result all count as UNKNOWN. Short circuits when the left side
/// is true.
#[derive(Debug)]
pub struct OrExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
}

impl OrExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if left.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Left operand for OR executor returns {:?} instead of BOOL",
                left.get_return_type()
            ));
        }
        if right.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Right operand for OR executor returns {:?} instead of BOOL",
                right.get_return_type()
            ));
        }
        Ok(Self {
            left_executor: left,
            right_executor: right,
        })
    }
}

impl ExpressionExecutor for OrExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left = truth_value(self.left_executor.execute(event));
        if left == Some(true) {
            return truth_result(Some(true));
        }
        let right = truth_value(self.right_executor.execute(event));
        truth_result(match (left, right) {
            (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        })
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(
            OrExpressionExecutor::new(
                self.left_executor.clone_executor(eventflux_app_context),
                self.right_executor.clone_executor(eventflux_app_context),
            )
            .expect("Cloning OrExpressionExecutor failed"),
        )
    }
}
