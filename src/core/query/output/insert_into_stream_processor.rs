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

// src/core/query/output/insert_into_stream_processor.rs
// Handles INSERT INTO a stream. The Java code has no class with this name. There the
// work is split across OutputCallback and the sink classes.
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::stream::stream_junction::StreamJunction;
use std::sync::{Arc, Mutex};

/// Last processor of a query with INSERT INTO a stream. Sends each chunk to the output
/// stream's junction.
#[derive(Debug)]
pub struct InsertIntoStreamProcessor {
    meta: CommonProcessorMeta, // next_processor stays None, nothing follows this processor
    output_stream_junction: Arc<Mutex<StreamJunction>>,
    // is_fault_stream: bool, // If this processor specifically handles fault stream insertion
}

impl InsertIntoStreamProcessor {
    pub fn new(
        output_sj: Arc<Mutex<StreamJunction>>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        // query_name parameter removed as it's in query_ctx
    ) -> Self {
        // next_processor in CommonProcessorMeta stays None, since this is the last processor
        let common_meta = CommonProcessorMeta::new(app_ctx, query_ctx);
        Self {
            meta: common_meta,
            output_stream_junction: output_sj,
            // is_fault_stream: false, // Default
        }
    }
}

impl Processor for InsertIntoStreamProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        // Hold one guard for both the send and the error handling. Locking the
        // junction again in the error arm would deadlock, because the temporary in an
        // `if let` condition lives until the end of the whole statement, so the first
        // guard would still be held. Panics if the junction mutex is poisoned.
        let junction = self
            .output_stream_junction
            .lock()
            .expect("Output StreamJunction Mutex poisoned");
        match junction.send_complex_event_chunk(complex_event_chunk) {
            Ok(()) => {}
            Err(crate::core::exception::error::EventFluxError::ShutdownRejected { count }) => {
                // Nobody upstream retries this send, so events rejected by a junction
                // that is shutting down would be missing from the delivered and dropped
                // counts unless counted here.
                junction.record_cascade_drop(count as u64);
                log::debug!(
                    "[{}] dropped {count} query-output event(s): junction is shutting down",
                    junction.stream_id
                );
            }
            Err(e) => {
                log::error!(
                    "Error sending event chunk to output stream junction '{}': {e}",
                    junction.stream_id
                );
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None // last processor in the query chain
    }

    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {
        // Ignored. This is the last processor, and the StreamJunction it writes to
        // takes the place of a next processor.
        if _next.is_some() {
            // log_warn!("InsertIntoStreamProcessor is a terminal processor; set_next_processor has no effect.");
        }
    }

    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(Self::new(
            Arc::clone(&self.output_stream_junction),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(eventflux_query_context), // the clone takes the new query context
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        // The real mode comes from processors earlier in the chain, such as windows.
        // The placeholder always returns DEFAULT.
        self.meta
            .eventflux_query_context
            .processing_mode_placeholder()
    }

    fn is_stateful(&self) -> bool {
        false
    }
}

// Stand in for a processing mode on EventFluxQueryContext
impl EventFluxQueryContext {
    // TODO: make this a real field set while the query is parsed. Always DEFAULT for now.
    pub fn processing_mode_placeholder(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }
}
