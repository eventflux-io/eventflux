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
use crate::core::table::{InMemoryCompiledCondition, InMemoryCompiledUpdateSet, Table};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Processor that handles UPDATE table operations triggered by stream events.
///
/// Supports two modes:
/// 1. Legacy mode: Uses `before_window_data` as condition and `output_data` as new values
/// 2. Expression mode: Uses a condition executor to match rows and value executors for SET clause
pub struct UpdateTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// Expression executor for evaluating WHERE condition (stream-triggered mode)
    condition_executor: Option<Box<dyn ExpressionExecutor>>,
    /// Expression executors for evaluating SET clause values (stream-triggered mode)
    set_value_executors: Vec<Box<dyn ExpressionExecutor>>,
    /// Column indices in the table that correspond to each SET clause (stream-triggered mode)
    set_column_indices: Vec<usize>,
    /// Counter for dropped events (e.g., non-StreamEvent inputs that can't be processed).
    /// Wrapped in Arc so metric is shared across processor clones for accurate observability.
    dropped_events: Arc<AtomicU64>,
}

impl std::fmt::Debug for UpdateTableProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateTableProcessor")
            .field("meta", &self.meta)
            .field("table", &self.table)
            .field("condition_executor", &self.condition_executor)
            .field("set_value_executors", &self.set_value_executors)
            .field("set_column_indices", &self.set_column_indices)
            .finish()
    }
}

impl UpdateTableProcessor {
    /// Creates a legacy UpdateTableProcessor (uses event data directly)
    pub fn new(
        table: Arc<dyn Table>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            table,
            condition_executor: None,
            set_value_executors: Vec::new(),
            set_column_indices: Vec::new(),
            dropped_events: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates an expression-based UpdateTableProcessor for stream-triggered updates
    ///
    /// # Arguments
    /// * `table` - The target table to update
    /// * `condition_executor` - Executor for the WHERE condition expression
    /// * `set_value_executors` - Executors for each SET clause value expression
    /// * `set_column_indices` - Column indices in the table for each SET clause attribute
    pub fn new_with_expression(
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
            condition_executor: Some(condition_executor),
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

    /// Process using legacy mode (before_window_data -> condition, output_data -> new values)
    fn process_legacy(&self, se: &StreamEvent) {
        let old = se.before_window_data.clone();
        if let Some(new) = se.get_output_data() {
            let cond = InMemoryCompiledCondition { values: old };
            let us = InMemoryCompiledUpdateSet {
                values: new.to_vec(),
            };
            let _ = self.table.update(&cond, &us);
        }
    }

    /// Process using expression mode - delegates to Table trait methods for extensibility.
    ///
    /// SET expressions are evaluated with a StateEvent containing both the stream event
    /// and the matched table row, allowing expressions like `SET price = stockTable.price + stream.delta`
    ///
    /// This method delegates to the Table trait's update methods, allowing JDBC/external
    /// tables to override the default in-memory implementation with database-native operations.
    fn process_with_expression(
        &self,
        se: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) {
        if !self.set_value_executors.is_empty() {
            // SET executors present - use per-row evaluation via table method
            // This allows SET expressions to reference table columns
            let _ = self.table.update_with_set_executors(
                se,
                condition_executor,
                &self.set_column_indices,
                &self.set_value_executors,
            );
        } else {
            // No SET executors - use stream values directly via table method
            // Extract values from stream output_data or before_window_data
            let set_values = if let Some(output) = se.get_output_data() {
                output.to_vec()
            } else {
                se.before_window_data.clone()
            };

            if !set_values.is_empty() {
                let _ = self.table.update_with_expression(
                    se,
                    condition_executor,
                    &self.set_column_indices,
                    &set_values,
                );
            }
        }
    }
}

impl Processor for UpdateTableProcessor {
    fn process(&self, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut event) = chunk {
            let next = event.set_next(None);

            // UPDATE requires direct StreamEvent input. Join/window sources (StateEvent)
            // are not supported because WHERE/SET expressions are built with stream positions
            // that would be lost when collapsing multi-stream data into a synthetic event.
            if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                if let Some(ref cond_exec) = self.condition_executor {
                    // Expression-based mode
                    self.process_with_expression(se, cond_exec.as_ref());
                } else {
                    // Legacy mode
                    self.process_legacy(se);
                }
            } else {
                // Track dropped events for monitoring
                let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                // Log initial warning, then periodic reminders every 1000 drops
                if count == 1 {
                    log::warn!(
                        "UPDATE processor received non-StreamEvent input (likely from JOIN/WINDOW). \
                         UPDATE currently only supports simple stream sources. Events will be skipped. \
                         Monitor dropped_events metric for ongoing counts."
                    );
                } else if count % 1000 == 0 {
                    log::warn!(
                        "UPDATE processor: {} events dropped (non-StreamEvent inputs from JOIN/WINDOW)",
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
        // Clone executors if present
        let condition_executor = self
            .condition_executor
            .as_ref()
            .map(|exec| exec.clone_executor(&query_ctx.eventflux_app_context));
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
        true
    }
}
