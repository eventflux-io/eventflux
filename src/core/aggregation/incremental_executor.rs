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

use std::sync::{Arc, Mutex};

use crate::core::event::stream::{
    stream_event::StreamEvent, stream_event_factory::StreamEventFactory,
};
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::aggregation::time_period::Duration as TimeDuration;

use super::base_incremental_value_store::BaseIncrementalValueStore;
use crate::core::table::{InMemoryTable, Table};

pub struct IncrementalExecutor {
    pub duration: TimeDuration,
    pub base_store: Arc<BaseIncrementalValueStore>,
    pub next: Option<Arc<IncrementalExecutor>>, // simplified chain
    pub group_by_fn: Box<dyn Fn(&StreamEvent) -> String + Send + Sync>,
    pub bucket_start: Mutex<i64>,
    pub table: Arc<InMemoryTable>,
    event_factory: StreamEventFactory,
}

impl std::fmt::Debug for IncrementalExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalExecutor")
            .field("duration", &self.duration)
            .finish()
    }
}

impl IncrementalExecutor {
    pub fn new(
        duration: TimeDuration,
        executors: Vec<Box<dyn ExpressionExecutor>>,
        group_by_fn: Box<dyn Fn(&StreamEvent) -> String + Send + Sync>,
        table: Arc<InMemoryTable>,
    ) -> Self {
        let factory = StreamEventFactory::new(0, 0, executors.len());
        Self {
            duration,
            base_store: Arc::new(BaseIncrementalValueStore::new(executors)),
            next: None,
            group_by_fn,
            bucket_start: Mutex::new(-1),
            table,
            event_factory: factory,
        }
    }

    fn bucket_ms(&self) -> i64 {
        self.duration.to_millis() as i64
    }

    fn flush(&self, bucket_start: i64) {
        let (_timestamp, groups) = self.base_store.drain_grouped_values();
        for (_k, values) in groups.into_iter() {
            println!("flush {:?} -> {:?}", self.duration, values);
            let mut ev = self.event_factory.new_instance();
            ev.timestamp = bucket_start;
            if let Some(ref mut out) = ev.output_data {
                for (i, v) in values.iter().enumerate() {
                    out[i] = v.clone();
                }
            }
            if let Err(e) = self.table.insert(&values) {
                log::error!("Failed to insert aggregation into table: {}", e);
                // Best-effort persistence: downstream processing continues so the
                // event pipeline is not blocked by a table write failure.
                // TODO: Route to error handler / DLQ when error infrastructure is wired.
            }
            if let Some(ref next) = self.next {
                next.execute(&ev);
            }
        }
        // reset aggregators for next bucket
        use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
        for exec in &self.base_store.expression_executors {
            let mut r = self.event_factory.new_instance();
            r.timestamp = bucket_start;
            r.set_event_type(ComplexEventType::Reset);
            exec.execute(Some(&r));
        }
    }

    pub fn execute(&self, event: &StreamEvent) {
        let mut start = self.bucket_start.lock().unwrap();
        if *start == -1 {
            *start = event.timestamp - (event.timestamp % self.bucket_ms());
        }
        while event.timestamp >= *start + self.bucket_ms() {
            let b_start = *start;
            *start += self.bucket_ms();
            self.flush(b_start);
        }

        let key = (self.group_by_fn)(event);
        self.base_store.process(&key, event);
    }
}
