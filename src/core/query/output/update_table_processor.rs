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

/// Runs a table UPDATE for each incoming stream event.
///
/// Two modes:
/// 1. Legacy mode: rows equal to `before_window_data` get `output_data` as new values
/// 2. Expression mode: the WHERE condition picks the rows and the SET executors give
///    the new values
///
/// Only StreamEvent input is handled. Anything else is counted and skipped. Errors
/// from the table are ignored. This is the last processor in the chain.
pub struct UpdateTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// WHERE condition for expression mode. None means legacy mode.
    condition_executor: Option<Box<dyn ExpressionExecutor>>,
    /// One executor per SET value, in expression mode
    set_value_executors: Vec<Box<dyn ExpressionExecutor>>,
    /// Table column index for each SET entry, in expression mode
    set_column_indices: Vec<usize>,
    /// Number of events skipped, such as non StreamEvent input.
    /// In an Arc so all clones of this processor share one count.
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
    /// Create a processor in legacy mode, which uses the event data directly
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

    /// Create a processor in expression mode, for updates driven by stream events.
    ///
    /// # Arguments
    /// * `table` - Table to update
    /// * `condition_executor` - Runs the WHERE condition
    /// * `set_value_executors` - One executor per SET value
    /// * `set_column_indices` - Table column index for each SET entry
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

    /// Number of events skipped because they could not be processed, summed across all
    /// clones. Useful for alerts on lost data.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Legacy mode. Rows equal to before_window_data get output_data as new values.
    /// Does nothing when the event has no output_data.
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

    /// Expression mode. Hands the work to the Table trait's update methods.
    ///
    /// SET expressions run on a StateEvent that holds both the stream event and the
    /// matched table row, so expressions like
    /// `SET price = stockTable.price + stream.delta` work.
    ///
    /// Going through the Table trait lets JDBC and other outside tables replace the
    /// default in memory code with their own database operations.
    fn process_with_expression(
        &self,
        se: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) {
        if !self.set_value_executors.is_empty() {
            // Run the SET executors once per matched row, so they can read table columns
            let _ = self.table.update_with_set_executors(
                se,
                condition_executor,
                &self.set_column_indices,
                &self.set_value_executors,
            );
        } else {
            // No SET executors, so the new values come straight from the stream event:
            // output_data, or before_window_data when there is none. Nothing happens
            // when the values are empty.
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

            // UPDATE needs a StreamEvent. Join and window sources send a StateEvent, which
            // is not supported. The WHERE and SET expressions refer to stream positions,
            // and those would be lost if the streams were merged into one made up event.
            if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                if let Some(ref cond_exec) = self.condition_executor {
                    self.process_with_expression(se, cond_exec.as_ref());
                } else {
                    self.process_legacy(se);
                }
            } else {
                let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                // Warn on the first drop, then again every 1000 drops
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
        true
    }
}
