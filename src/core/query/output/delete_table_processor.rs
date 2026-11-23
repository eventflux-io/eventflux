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
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::table::{InMemoryCompiledCondition, Table};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Runs a table DELETE for each incoming stream event.
///
/// Two modes:
/// 1. Legacy mode: deletes rows that equal the event's `output_data`
/// 2. Expression mode: deletes rows for which the WHERE condition is true
///
/// Errors from the table are ignored. This is the last processor in the chain.
pub struct DeleteTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// WHERE condition for expression mode. None means legacy mode.
    condition_executor: Option<Box<dyn ExpressionExecutor>>,
    /// Number of events skipped, such as non StreamEvent input in expression mode.
    /// In an Arc so all clones of this processor share one count.
    dropped_events: Arc<AtomicU64>,
}

impl std::fmt::Debug for DeleteTableProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteTableProcessor")
            .field("meta", &self.meta)
            .field("table", &self.table)
            .field("condition_executor", &self.condition_executor)
            .finish()
    }
}

impl DeleteTableProcessor {
    /// Create a processor in legacy mode, which matches rows on the event's output_data
    pub fn new(
        table: Arc<dyn Table>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            table,
            condition_executor: None,
            dropped_events: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a processor in expression mode, for deletes driven by stream events.
    ///
    /// # Arguments
    /// * `table` - Table to delete from
    /// * `condition_executor` - Runs the WHERE condition
    pub fn new_with_expression(
        table: Arc<dyn Table>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        condition_executor: Box<dyn ExpressionExecutor>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            table,
            condition_executor: Some(condition_executor),
            dropped_events: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of events skipped because they could not be processed, summed across all
    /// clones. Useful for alerts on lost data.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Legacy mode. Deletes rows equal to the event's output_data. Does nothing when
    /// the event has no output_data.
    fn process_legacy(&self, event: &dyn ComplexEvent) {
        if let Some(data) = event.get_output_data() {
            let cond = InMemoryCompiledCondition {
                values: data.to_vec(),
            };
            let _ = self.table.delete(&cond);
        }
    }

    /// Expression mode. Deletes the rows for which the condition, run on the stream
    /// event and each row, is true.
    fn process_with_expression(
        &self,
        se: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) {
        let _ = self.table.delete_with_expression(se, condition_executor);
    }
}

impl Processor for DeleteTableProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut event) = chunk {
            let next = event.set_next(None);

            if let Some(ref cond_exec) = self.condition_executor {
                // Expression mode needs a StreamEvent. Join and window sources send a
                // StateEvent, which is not supported. The WHERE expression refers to stream
                // positions, and those would be lost if the streams were merged into one
                // made up event.
                if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                    self.process_with_expression(se, cond_exec.as_ref());
                } else {
                    let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                    // Warn on the first drop, then again every 1000 drops
                    if count == 1 {
                        log::warn!(
                            "DELETE processor received non-StreamEvent input (likely from JOIN/WINDOW). \
                             DELETE currently only supports simple stream sources. Events will be skipped. \
                             Monitor dropped_events metric for ongoing counts."
                        );
                    } else if count % 1000 == 0 {
                        log::warn!(
                            "DELETE processor: {} events dropped (non-StreamEvent inputs from JOIN/WINDOW)",
                            count
                        );
                    }
                }
            } else {
                // Legacy mode works with any ComplexEvent, through its output_data
                self.process_legacy(event.as_ref());
            }

            chunk = next;
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        None
    }
    fn set_next_processor(&mut self, _next: Option<Arc<Mutex<dyn Processor>>>) {}
    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        let condition_executor = self
            .condition_executor
            .as_ref()
            .map(|exec| exec.clone_executor(&query_ctx.eventflux_app_context));

        Box::new(Self {
            meta: CommonProcessorMeta::new(
                Arc::clone(&self.meta.eventflux_app_context),
                Arc::clone(query_ctx),
            ),
            table: Arc::clone(&self.table),
            condition_executor,
            // Share one drop count across all clones
            dropped_events: Arc::clone(&self.dropped_events),
        })
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
        true // DELETE changes table state
    }
}
