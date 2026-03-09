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
use crate::core::table::Table;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Processor that handles UPSERT (UPDATE or INSERT) table operations triggered by stream events.
///
/// Semantics:
/// 1. Evaluate the ON condition against stream event + each table row
/// 2. If any row matches: UPDATE that row with new values from stream event
/// 3. If no row matches: INSERT the stream event as a new row
pub struct UpsertTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// Expression executor for evaluating ON condition
    condition_executor: Box<dyn ExpressionExecutor>,
    /// Expression executors for evaluating SET clause values (optional, for partial updates)
    /// If empty, uses output_data from stream event as new row values
    set_value_executors: Vec<Box<dyn ExpressionExecutor>>,
    /// Column indices in the table for each SET clause attribute (for partial updates)
    /// If empty, full row replacement is used
    set_column_indices: Vec<usize>,
    /// Counter for dropped events (e.g., non-StreamEvent inputs that can't be processed).
    /// Wrapped in Arc so metric is shared across processor clones for accurate observability.
    dropped_events: Arc<AtomicU64>,
}

impl std::fmt::Debug for UpsertTableProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpsertTableProcessor")
            .field("meta", &self.meta)
            .field("table", &self.table)
            .field("condition_executor", &self.condition_executor)
            .field("set_value_executors", &self.set_value_executors)
            .field("set_column_indices", &self.set_column_indices)
            .finish()
    }
}

impl UpsertTableProcessor {
    /// Creates a new UpsertTableProcessor
    ///
    /// # Arguments
    /// * `table` - The target table for upsert operations
    /// * `condition_executor` - Executor for the ON condition expression
    /// * `set_value_executors` - Executors for SET clause values (empty for INSERT-style upsert)
    /// * `set_column_indices` - Column indices for each SET clause (empty for full row replacement)
    pub fn new(
        table: Arc<dyn Table>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        condition_executor: Box<dyn ExpressionExecutor>,
        set_value_executors: Vec<Box<dyn ExpressionExecutor>>,
        set_column_indices: Vec<usize>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            table,
            condition_executor,
            set_value_executors,
            set_column_indices,
            dropped_events: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the count of dropped events (events that couldn't be processed).
    /// Useful for monitoring and alerting on data loss.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Perform the upsert operation for a single stream event.
    ///
    /// Delegates to the Table trait's `upsert_with_set_executors` method, allowing
    /// JDBC/external tables to override with database-native UPSERT/MERGE operations.
    fn process_upsert(&self, se: &StreamEvent) {
        let _ = self.table.upsert_with_set_executors(
            se,
            self.condition_executor.as_ref(),
            &self.set_column_indices,
            &self.set_value_executors,
        );
    }
}

impl Processor for UpsertTableProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut event) = chunk {
            let next = event.set_next(None);

            // UPSERT requires direct StreamEvent input. Join/window sources (StateEvent)
            // are not supported because ON/SET expressions are built with stream positions
            // that would be lost when collapsing multi-stream data into a synthetic event.
            if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                self.process_upsert(se);
            } else {
                // Track dropped events for monitoring
                let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                // Log initial warning, then periodic reminders every 1000 drops
                if count == 1 {
                    log::warn!(
                        "UPSERT processor received non-StreamEvent input (likely from JOIN/WINDOW). \
                         UPSERT currently only supports simple stream sources. Events will be skipped. \
                         Monitor dropped_events metric for ongoing counts."
                    );
                } else if count % 1000 == 0 {
                    log::warn!(
                        "UPSERT processor: {} events dropped (non-StreamEvent inputs from JOIN/WINDOW)",
                        count
                    );
                }
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
            .clone_executor(&query_ctx.eventflux_app_context);
        let set_value_executors = self
            .set_value_executors
            .iter()
            .map(|exec| exec.clone_executor(&query_ctx.eventflux_app_context))
            .collect();

        Box::new(Self {
            meta: CommonProcessorMeta::new(
                Arc::clone(&self.meta.eventflux_app_context),
                Arc::clone(query_ctx),
            ),
            table: Arc::clone(&self.table),
            condition_executor,
            set_value_executors,
            set_column_indices: self.set_column_indices.clone(),
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
        true // UPSERT operations affect table state
    }
}
