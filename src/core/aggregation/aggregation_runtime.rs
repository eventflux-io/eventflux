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

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::event::stream::stream_event::StreamEvent;
use crate::query_api::aggregation::time_period::Duration as TimeDuration;
use crate::query_api::aggregation::within::Within;

use super::incremental_executor::IncrementalExecutor;
use crate::core::table::InMemoryTable;

#[derive(Debug)]
pub struct AggregationRuntime {
    pub name: String,
    pub executors: HashMap<TimeDuration, Arc<IncrementalExecutor>>, // root executor per duration
    pub tables: HashMap<TimeDuration, Arc<InMemoryTable>>,          // aggregated rows per duration
}

impl AggregationRuntime {
    pub fn new(name: String, executors: HashMap<TimeDuration, Arc<IncrementalExecutor>>) -> Self {
        Self {
            name,
            executors,
            tables: HashMap::new(),
        }
    }

    pub fn process(&self, event: &StreamEvent) {
        // Sends the event to one executor only. HashMap order is arbitrary, so this is not
        // always the smallest duration. Other durations get data only through that
        // executor's `next` chain.
        if let Some(exec) = self.executors.values().next() {
            exec.execute(event);
        }
    }

    pub fn add_table(&mut self, dur: TimeDuration, table: Arc<InMemoryTable>) {
        self.tables.insert(dur, table);
    }

    pub fn get_table(&self, dur: TimeDuration) -> Option<Arc<InMemoryTable>> {
        self.tables.get(&dur).cloned()
    }

    pub fn query_all(
        &self,
        dur: TimeDuration,
    ) -> Vec<Vec<crate::core::event::value::AttributeValue>> {
        if let Some(table) = self.tables.get(&dur) {
            table.all_rows()
        } else {
            Vec::new()
        }
    }

    /// Returns aggregated rows for a `within` and `per` query.
    ///
    /// Returns all table rows for the `per` duration. Without `per`, uses the
    /// first table found, in arbitrary HashMap order. The `within` clause is
    /// ignored. Returns an empty list when no matching table exists.
    pub fn query(
        &self,
        _within: Option<Within>,
        per: Option<TimeDuration>,
    ) -> Vec<Vec<crate::core::event::value::AttributeValue>> {
        if let Some(d) = per {
            return self.query_all(d);
        }
        if let Some((&d, _)) = self.tables.iter().next() {
            return self.query_all(d);
        }
        Vec::new()
    }
}
