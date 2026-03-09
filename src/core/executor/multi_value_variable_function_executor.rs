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
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MultiValueVariableFunctionExecutor {
    chain_index: i32,
    // Position array information for attribute retrieval
    attribute_position: [i32; 2],
}

impl MultiValueVariableFunctionExecutor {
    pub fn new(chain_index: i32, attribute_position: [i32; 2]) -> Self {
        Self {
            chain_index,
            attribute_position,
        }
    }
}

impl ExpressionExecutor for MultiValueVariableFunctionExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;
        let mut results: Vec<AttributeValue> = Vec::new();
        let mut stream_event_opt = state_event
            .stream_events
            .get(self.chain_index as usize)?
            .as_ref();
        while let Some(stream_event) = stream_event_opt {
            if let Some(val) = stream_event.get_attribute_by_position(&self.attribute_position) {
                results.push(val.clone());
            } else {
                results.push(AttributeValue::Null);
            }
            stream_event_opt = match stream_event.next.as_deref() {
                Some(n) => n.as_any().downcast_ref::<StreamEvent>(),
                None => None,
            };
        }
        Some(AttributeValue::Object(Some(Box::new(results))))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::OBJECT
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}
