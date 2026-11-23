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

// src/core/query/output/callback_processor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::event::Event;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::stream::output::stream_callback::StreamCallback;
use std::sync::{Arc, Mutex};

/// Last processor in a query chain. Turns each event into a plain Event and hands the
/// batch to a StreamCallback.
#[derive(Debug)]
pub struct CallbackProcessor {
    meta: CommonProcessorMeta,
    callback: Arc<Mutex<Box<dyn StreamCallback>>>,
}

impl CallbackProcessor {
    pub fn new(
        callback: Arc<Mutex<Box<dyn StreamCallback>>>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        // query_name: String, // query_name is in query_ctx
    ) -> Self {
        // CommonProcessorMeta reads the query name from query_ctx
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            callback,
        }
    }
}

// Turn a ComplexEvent into a plain Event. Keeps only output_data, the timestamp and
// the expired flag, so anything else on the ComplexEvent is lost. Missing output_data
// becomes an empty Vec.
fn complex_event_to_simple_event(ce_box: Box<dyn ComplexEvent>) -> Event {
    let data = ce_box
        .get_output_data()
        .map_or_else(Vec::new, |d| d.to_vec());
    let timestamp = ce_box.get_timestamp();
    let is_expired = ce_box.is_expired();

    // Event's constructor takes a fresh id from the global counter
    let mut event = Event::new_with_data(timestamp, data);
    event.set_is_expired(is_expired);
    event
}

impl Processor for CallbackProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        let mut events_vec: Vec<Event> = Vec::new();
        let mut current_opt = complex_event_chunk;

        while let Some(mut current_box) = current_opt {
            // Unlink the event from the chain so it can be converted on its own
            let next_event_in_chunk = current_box.set_next(None);
            events_vec.push(complex_event_to_simple_event(current_box));
            current_opt = next_event_in_chunk;
        }

        if !events_vec.is_empty() {
            // Panics if the callback mutex is poisoned
            self.callback
                .lock()
                .expect("Callback Mutex poisoned")
                .receive_events(&events_vec);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    } // last in the chain
    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {
        /* ignored, nothing comes after this processor */
    }

    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(Self::new(
            Arc::clone(&self.callback),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(eventflux_query_context), // the clone takes the new query context
                                                 // self.meta.query_name.clone() // query_name is in eventflux_query_context
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
        false // this processor keeps no state, though the callback it holds may
    }
}
