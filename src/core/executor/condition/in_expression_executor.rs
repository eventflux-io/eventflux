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

// Corresponds to io.eventflux.core.executor.condition.InConditionExpressionExecutor
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::table::InMemoryCompiledCondition;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// `value IN table` check. True when the table contains a row keyed by the value.
///
/// The table is looked up by id on every call. False when the value is NULL or
/// missing, when the table is not found, or when the lookup fails (the error is logged).
#[derive(Debug)]
pub struct InExpressionExecutor {
    // Simpler than the Java version, which also kept streamEventSize,
    // isMatchingEventAStateEvent, matchingStreamIndex, a CompiledCondition and the Table.
    value_executor: Box<dyn ExpressionExecutor>, // Gives the value to look for in the table
    table_id: String,
    eventflux_app_context: Arc<EventFluxAppContext>,
}

impl InExpressionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        table_id: String,
        eventflux_app_context: Arc<EventFluxAppContext>,
    ) -> Self {
        Self {
            value_executor,
            table_id,
            eventflux_app_context,
        }
    }
}

impl ExpressionExecutor for InExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value_to_check = self.value_executor.execute(event);
        let value = match value_to_check {
            Some(v) => {
                if matches!(v, AttributeValue::Null) {
                    return Some(AttributeValue::Bool(false));
                }
                v
            }
            None => return Some(AttributeValue::Bool(false)),
        };

        let table_opt = self
            .eventflux_app_context
            .get_eventflux_context()
            .get_table(&self.table_id);

        if let Some(table) = table_opt {
            let key = InMemoryCompiledCondition {
                values: vec![value.clone()],
            };
            match table.contains(&key) {
                Ok(contains) => Some(AttributeValue::Bool(contains)),
                Err(e) => {
                    log::error!("Table contains check failed: {}", e);
                    Some(AttributeValue::Bool(false))
                }
            }
        } else {
            // Table not found
            Some(AttributeValue::Bool(false))
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(InExpressionExecutor::new(
            self.value_executor.clone_executor(eventflux_app_context),
            self.table_id.clone(),
            Arc::clone(eventflux_app_context),
        ))
    }
}
