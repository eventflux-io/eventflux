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

use crate::core::aggregation::AggregationRuntime;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct InsertIntoAggregationProcessor {
    meta: CommonProcessorMeta,
    runtime: Arc<Mutex<AggregationRuntime>>,
}

impl InsertIntoAggregationProcessor {
    pub fn new(
        runtime: Arc<Mutex<AggregationRuntime>>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            runtime,
        }
    }
}

impl Processor for InsertIntoAggregationProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut event) = chunk {
            let next = event.set_next(None);
            if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                self.runtime.lock().unwrap().process(se);
            }
            chunk = next;
        }
    }
    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }
    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {}
    fn clone_processor(&self, qctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            Arc::clone(&self.runtime),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(qctx),
        ))
    }
    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }
    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }
    fn is_stateful(&self) -> bool {
        true
    }
}
