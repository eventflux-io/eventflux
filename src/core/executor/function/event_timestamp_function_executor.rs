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
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct EventTimestampFunctionExecutor {
    event_executor: Option<Box<dyn ExpressionExecutor>>,
}

impl EventTimestampFunctionExecutor {
    pub fn new(event_executor: Option<Box<dyn ExpressionExecutor>>) -> Self {
        Self { event_executor }
    }
}

impl ExpressionExecutor for EventTimestampFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        if self.event_executor.is_some() {
            // simplified: not supporting explicit event argument yet
            None
        } else {
            Some(AttributeValue::Long(event?.get_timestamp()))
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(EventTimestampFunctionExecutor {
            event_executor: self.event_executor.as_ref().map(|e| e.clone_executor(ctx)),
        })
    }
}
