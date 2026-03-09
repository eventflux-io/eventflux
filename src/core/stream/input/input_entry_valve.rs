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

use super::input_handler::InputProcessor;
use crate::core::event::event::Event;
use crate::core::util::thread_barrier::ThreadBarrier;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct InputEntryValve {
    pub barrier: Arc<ThreadBarrier>,
    pub input_processor: Arc<Mutex<dyn InputProcessor>>,
}

impl InputEntryValve {
    pub fn new(
        barrier: Arc<ThreadBarrier>,
        input_processor: Arc<Mutex<dyn InputProcessor>>,
    ) -> Self {
        Self {
            barrier,
            input_processor,
        }
    }
}

impl InputProcessor for InputEntryValve {
    fn send_event_with_data(
        &mut self,
        timestamp: i64,
        data: Vec<crate::core::event::value::AttributeValue>,
        stream_index: usize,
    ) -> Result<(), String> {
        self.barrier.enter();
        let res = self
            .input_processor
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_event_with_data(timestamp, data, stream_index);
        self.barrier.exit();
        res
    }

    fn send_single_event(&mut self, event: Event, stream_index: usize) -> Result<(), String> {
        self.barrier.enter();
        let res = self
            .input_processor
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_single_event(event, stream_index);
        self.barrier.exit();
        res
    }

    fn send_multiple_events(
        &mut self,
        events: Vec<Event>,
        stream_index: usize,
    ) -> Result<(), String> {
        self.barrier.enter();
        let res = self
            .input_processor
            .lock()
            .map_err(|_| "processor mutex poisoned".to_string())?
            .send_multiple_events(events, stream_index);
        self.barrier.exit();
        res
    }
}
