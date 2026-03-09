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
    pub executors: HashMap<TimeDuration, Arc<IncrementalExecutor>>, // root executor by duration
    pub tables: HashMap<TimeDuration, Arc<InMemoryTable>>,          // aggregated data
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
        // For now, always use the smallest duration executor
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

    /// Query aggregated data optionally using `within` and `per` clauses.
    /// Currently, these clauses are not fully supported and the implementation
    /// simply returns the table rows for the requested `per` duration when
    /// provided. If no `per` duration is specified, the first available
    /// duration is used. The `within` clause is ignored.
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
