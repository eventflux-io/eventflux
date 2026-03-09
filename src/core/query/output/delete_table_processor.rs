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

/// Processor that handles DELETE table operations triggered by stream events.
///
/// Supports two modes:
/// 1. Legacy mode: Uses `output_data` as the condition for deletion
/// 2. Expression mode: Uses a condition executor to find and delete matching rows
pub struct DeleteTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// Expression executor for evaluating WHERE condition (stream-triggered mode)
    condition_executor: Option<Box<dyn ExpressionExecutor>>,
    /// Counter for dropped events (e.g., non-StreamEvent inputs that can't be processed).
    /// Wrapped in Arc so metric is shared across processor clones for accurate observability.
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
    /// Creates a legacy DeleteTableProcessor (uses event data directly)
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

    /// Creates an expression-based DeleteTableProcessor for stream-triggered deletes
    ///
    /// # Arguments
    /// * `table` - The target table to delete from
    /// * `condition_executor` - Executor for the WHERE condition expression
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

    /// Returns the count of dropped events (events that couldn't be processed).
    /// Useful for monitoring and alerting on data loss.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Process using legacy mode (output_data as condition)
    fn process_legacy(&self, event: &dyn ComplexEvent) {
        if let Some(data) = event.get_output_data() {
            let cond = InMemoryCompiledCondition {
                values: data.to_vec(),
            };
            let _ = self.table.delete(&cond);
        }
    }

    /// Process using expression mode (evaluate condition against stream event + table rows)
    fn process_with_expression(
        &self,
        se: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) {
        // Use the table's expression-based delete method
        let _ = self.table.delete_with_expression(se, condition_executor);
    }
}

impl Processor for DeleteTableProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut event) = chunk {
            let next = event.set_next(None);

            if let Some(ref cond_exec) = self.condition_executor {
                // Expression-based mode
                // DELETE requires direct StreamEvent input. Join/window sources (StateEvent)
                // are not supported because WHERE expressions are built with stream positions
                // that would be lost when collapsing multi-stream data into a synthetic event.
                if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                    self.process_with_expression(se, cond_exec.as_ref());
                } else {
                    // Track dropped events for monitoring
                    let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                    // Log initial warning, then periodic reminders every 1000 drops
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
                // Legacy mode - works with any ComplexEvent via output_data
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
        // Clone executor if present
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
            // Share metrics across clones for accurate aggregate observability
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
        true // DELETE operations affect table state
    }
}
