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

use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;
use crate::core::stream::stream_junction::StreamJunction;
use crate::core::util::scheduler::{Schedulable, Scheduler};
use crate::query_api::definition::TriggerDefinition;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct TriggerRuntime {
    pub definition: Arc<TriggerDefinition>,
    pub stream_junction: Arc<Mutex<StreamJunction>>,
    scheduler: Arc<Scheduler>,
    started: AtomicBool,
    /// Flag shared with the TriggerTask of the current start.
    /// Set to false on shutdown so tasks from earlier starts stop emitting.
    active_flag: Mutex<Arc<AtomicBool>>,
}

impl TriggerRuntime {
    pub fn new(
        definition: Arc<TriggerDefinition>,
        stream_junction: Arc<Mutex<StreamJunction>>,
        scheduler: Arc<Scheduler>,
    ) -> Self {
        Self {
            definition,
            stream_junction,
            scheduler,
            started: AtomicBool::new(false),
            active_flag: Mutex::new(Arc::new(AtomicBool::new(false))),
        }
    }

    // Schedules the trigger. `at_every` emits on a fixed period, `at = 'start'` emits
    // once right away, and any other `at` value is scheduled as a cron expression.
    // A cron scheduling error is ignored. Does nothing if already started.
    // Each event carries the fire time in ms as a single Long attribute.
    pub fn start(&self) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }

        // A new flag per start. Tasks from an earlier start keep their old flag,
        // which shutdown set to false.
        let active = Arc::new(AtomicBool::new(true));
        *self.active_flag.lock().unwrap() = Arc::clone(&active);

        let task = Arc::new(TriggerTask {
            junction: Arc::clone(&self.stream_junction),
            active: Arc::clone(&active),
        });
        if let Some(period) = self.definition.at_every {
            self.scheduler
                .schedule_periodic(period, task, None, Some(Arc::clone(&active)));
        } else if let Some(at) = &self.definition.at {
            if at.trim().eq_ignore_ascii_case("start") {
                // Emit once, now, on the calling thread.
                let now = chrono::Utc::now().timestamp_millis();
                TriggerTask {
                    junction: Arc::clone(&self.stream_junction),
                    active: Arc::new(AtomicBool::new(true)),
                }
                .on_time(now);
            } else {
                let _ = self
                    .scheduler
                    .schedule_cron(at, task, None, Some(Arc::clone(&active)));
            }
        }
    }

    pub fn shutdown(&self) {
        self.started.store(false, Ordering::Release);
        // Turn off this start's tasks so their scheduled runs stop emitting.
        self.active_flag
            .lock()
            .unwrap()
            .store(false, Ordering::Release);
    }
}

#[derive(Debug)]
struct TriggerTask {
    junction: Arc<Mutex<StreamJunction>>,
    active: Arc<AtomicBool>,
}

impl Schedulable for TriggerTask {
    fn on_time(&self, timestamp: i64) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let ev = Event::new_with_data(timestamp, vec![AttributeValue::Long(timestamp)]);
        if let Err(e) = self.junction.lock().unwrap().send_event(ev) {
            log::error!("Trigger failed to send event: {}", e);
        }
    }
}
