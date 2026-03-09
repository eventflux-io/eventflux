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

use crate::core::event::stream::{
    stream_event::StreamEvent, stream_event_factory::StreamEventFactory,
};
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct BaseIncrementalValueStore {
    pub expression_executors: Vec<Box<dyn ExpressionExecutor>>,
    grouped_values: Mutex<HashMap<String, Vec<AttributeValue>>>,
    timestamp: Mutex<i64>,
    event_factory: StreamEventFactory,
}

impl BaseIncrementalValueStore {
    pub fn new(expression_executors: Vec<Box<dyn ExpressionExecutor>>) -> Self {
        let factory = StreamEventFactory::new(0, 0, expression_executors.len());
        Self {
            expression_executors,
            grouped_values: Mutex::new(HashMap::new()),
            timestamp: Mutex::new(-1),
            event_factory: factory,
        }
    }

    pub fn process(&self, group: &str, event: &StreamEvent) {
        let mut map = self.grouped_values.lock().unwrap();
        let entry = map
            .entry(group.to_string())
            .or_insert_with(|| vec![AttributeValue::default(); self.expression_executors.len()]);
        for (i, exec) in self.expression_executors.iter().enumerate() {
            if let Some(val) = exec.execute(Some(event)) {
                if i < entry.len() {
                    entry[i] = val;
                }
            }
        }
        *self.timestamp.lock().unwrap() = event.timestamp;
    }

    pub fn clear(&self) {
        self.grouped_values.lock().unwrap().clear();
    }

    pub fn get_grouped_events(&self) -> HashMap<String, StreamEvent> {
        let map = self.grouped_values.lock().unwrap();
        let timestamp = *self.timestamp.lock().unwrap();
        let mut out = HashMap::new();
        for (k, v) in map.iter() {
            let mut se = self.event_factory.new_instance();
            se.timestamp = timestamp;
            if let Some(ref mut out_vec) = se.output_data {
                for (i, val) in v.iter().enumerate() {
                    out_vec[i] = val.clone();
                }
            }
            out.insert(k.clone(), se);
        }
        out
    }

    /// Drain all grouped values returning the timestamp and the values.
    /// This clears the internal storage.
    pub fn drain_grouped_values(&self) -> (i64, HashMap<String, Vec<AttributeValue>>) {
        let mut map = self.grouped_values.lock().unwrap();
        let ts = *self.timestamp.lock().unwrap();
        let out = std::mem::take(&mut *map);
        (ts, out)
    }
}
