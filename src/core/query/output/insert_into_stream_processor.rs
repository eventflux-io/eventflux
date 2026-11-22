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
// This is a conceptual equivalent to part of EventFlux's output handling logic,
// specifically for INSERT INTO clauses. Java doesn't have a direct one-to-one class
// with this name; output handling is often part of OutputCallback or specific Sink logic.
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent; // Trait
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::stream::stream_junction::StreamJunction;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct InsertIntoStreamProcessor {
    meta: CommonProcessorMeta, // Contains eventflux_app_context, query_name. Next_processor is None for this typically.
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
        // next_processor in CommonProcessorMeta will be None for a terminal processor like this.
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
        // One guard for the send AND the error handling. Re-locking the
        // junction in the error arm would self-deadlock: the scrutinee
        // temporary of an `if let` lives until the end of the whole
        // statement, so the first guard would still be held.
        let junction = self
            .output_stream_junction
            .lock()
            .expect("Output StreamJunction Mutex poisoned");
        match junction.send_complex_event_chunk(complex_event_chunk) {
            Ok(()) => {}
            Err(crate::core::exception::error::EventFluxError::ShutdownRejected { count }) => {
                // Fire-and-forget edge: nobody upstream retries, so events
                // rejected by a shutting-down junction would vanish from the
                // delivered/dropped ledger unless counted here.
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
        None // This is typically a terminal processor in a query's chain
    }

    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {
        // No operation, as this is a terminal processor for its query path.
        // Its "next" is effectively the StreamJunction it writes to.
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
            Arc::clone(&self.meta.eventflux_app_context), // Clone AppContext from meta
            Arc::clone(eventflux_query_context), // Use the new QueryContext for the cloned instance
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        // Usually determined by upstream processors (e.g., window, aggregation)
        // For a simple insert, it might be considered DEFAULT or pass-through.
        self.meta
            .eventflux_query_context
            .processing_mode_placeholder()
    }

    fn is_stateful(&self) -> bool {
        false // InsertIntoStreamProcessor itself is typically stateless
    }
}

// Adding placeholder to EventFluxQueryContext for processing_mode for now
impl EventFluxQueryContext {
    // TODO: This should be a proper field determined during query parsing.
    pub fn processing_mode_placeholder(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }
}
