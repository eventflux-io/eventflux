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
use crate::core::util::eventflux_constants::{
    STREAM_EVENT_CHAIN_INDEX, STREAM_EVENT_INDEX_IN_CHAIN,
};
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventVariableFunctionExecutor {
    position: [i32; 2],
}

impl EventVariableFunctionExecutor {
    pub fn new(stream_event_chain_index: i32, stream_event_index_in_chain: i32) -> Self {
        Self {
            position: [stream_event_chain_index, stream_event_index_in_chain],
        }
    }
}

impl ExpressionExecutor for EventVariableFunctionExecutor {
    fn execute(&self, event_opt: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let complex_event = event_opt?;
        let state_event = complex_event.as_any().downcast_ref::<StateEvent>()?;
        let chain_idx = self.position[STREAM_EVENT_CHAIN_INDEX] as usize;
        let mut current = state_event.stream_events.get(chain_idx)?.as_ref()?;
        let mut idx = 0usize;
        while idx < self.position[STREAM_EVENT_INDEX_IN_CHAIN] as usize {
            let next_ce = match current.next.as_deref() {
                Some(n) => n,
                None => return Some(AttributeValue::Null),
            };
            current = match next_ce.as_any().downcast_ref::<StreamEvent>() {
                Some(se) => se,
                None => return Some(AttributeValue::Null),
            };
            idx += 1;
        }
        let cloned = current.clone_without_next();
        Some(AttributeValue::Object(Some(Box::new(cloned))))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::OBJECT
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}
