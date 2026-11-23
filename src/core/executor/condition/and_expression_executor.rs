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

// Corresponds to io.eventflux.core.executor.condition.AndConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// Logical AND of two BOOL conditions. Short circuits when the left side is not true.
///
/// NULL, no value, or a non BOOL result on either side counts as false, so the
/// result is always a BOOL.
#[derive(Debug)] // No Clone or Default because of the Box<dyn ExpressionExecutor> fields
pub struct AndExpressionExecutor {
    left_executor: Box<dyn ExpressionExecutor>,
    right_executor: Box<dyn ExpressionExecutor>,
}

impl AndExpressionExecutor {
    pub fn new(
        left: Box<dyn ExpressionExecutor>,
        right: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        // Both sides must return BOOL, as in the Java constructor
        if left.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Left operand for AND executor returns {:?} instead of BOOL",
                left.get_return_type()
            ));
        }
        if right.get_return_type() != ApiAttributeType::BOOL {
            return Err(format!(
                "Right operand for AND executor returns {:?} instead of BOOL",
                right.get_return_type()
            ));
        }
        Ok(Self {
            left_executor: left,
            right_executor: right,
        })
    }
}

impl ExpressionExecutor for AndExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let left_val = self.left_executor.execute(event);

        match left_val {
            Some(AttributeValue::Bool(true)) => {
                // Left is true, so the right side decides
                let right_val = self.right_executor.execute(event);
                match right_val {
                    Some(AttributeValue::Bool(b)) => Some(AttributeValue::Bool(b)),
                    Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // NULL counts as false
                    None => Some(AttributeValue::Bool(false)), // No value counts as false
                    _ => {
                        // Right side returned something other than BOOL or NULL. The
                        // constructor checks the declared type, but a wrong runtime value
                        // still counts as false here rather than raising an error.
                        Some(AttributeValue::Bool(false))
                    }
                }
            }
            Some(AttributeValue::Bool(false)) => Some(AttributeValue::Bool(false)),
            Some(AttributeValue::Null) => Some(AttributeValue::Bool(false)), // NULL AND anything is false
            None => Some(AttributeValue::Bool(false)), // No value from the left counts as false
            _ => {
                // Left side returned something other than BOOL or NULL
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
            AndExpressionExecutor::new(
                self.left_executor.clone_executor(eventflux_app_context),
                self.right_executor.clone_executor(eventflux_app_context),
            )
            .expect("Cloning AndExpressionExecutor failed"),
        ) // Cannot fail, since the original passed the same type checks
    }
}
