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

/// Runs a table UPSERT (update, or insert when nothing matches) for each incoming
/// stream event.
///
/// Steps:
/// 1. Run the ON condition on the stream event and each table row
/// 2. If rows match, update them with new values from the stream event. Which matched
///    rows change depends on the table.
/// 3. If no row matches, insert the stream event as a new row
///
/// Only StreamEvent input is handled. Anything else is counted and skipped. Errors
/// from the table are ignored. This is the last processor in the chain.
pub struct UpsertTableProcessor {
    meta: CommonProcessorMeta,
    table: Arc<dyn Table>,
    /// Runs the ON condition
    condition_executor: Box<dyn ExpressionExecutor>,
    /// One executor per SET value, for partial updates.
    /// When empty, the stream event's output_data becomes the new row.
    set_value_executors: Vec<Box<dyn ExpressionExecutor>>,
    /// Table column index for each SET entry, for partial updates.
    /// When empty, the whole row is replaced.
    set_column_indices: Vec<usize>,
    /// Number of events skipped, such as non StreamEvent input.
    /// In an Arc so all clones of this processor share one count.
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
    /// Create an upsert processor.
    ///
    /// # Arguments
    /// * `table` - Table to upsert into
    /// * `condition_executor` - Runs the ON condition
    /// * `set_value_executors` - One executor per SET value. Empty means the event's
    ///   values become the whole row.
    /// * `set_column_indices` - Table column index for each SET entry. Empty means the
    ///   whole row is replaced.
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

    /// Number of events skipped because they could not be processed, summed across all
    /// clones. Useful for alerts on lost data.
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Upsert one stream event.
    ///
    /// Calls the Table trait's `upsert_with_set_executors`, so JDBC and other outside
    /// tables can use their own UPSERT or MERGE. The result is ignored.
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

            // UPSERT needs a StreamEvent. Join and window sources send a StateEvent, which
            // is not supported. The ON and SET expressions refer to stream positions, and
            // those would be lost if the streams were merged into one made up event.
            if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
                self.process_upsert(se);
            } else {
                let count = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;

                // Warn on the first drop, then again every 1000 drops
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
        true // UPSERT changes table state
    }
}
