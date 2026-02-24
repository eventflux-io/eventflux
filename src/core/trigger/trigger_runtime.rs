// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/trigger/trigger_runtime.rs

use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;
use crate::core::stream::stream_junction::StreamJunction;
use crate::core::util::scheduler::{Schedulable, Scheduler};
use crate::query_api::definition::TriggerDefinition;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct TriggerRuntime {
    pub definition: Arc<TriggerDefinition>,
    pub stream_junction: Arc<Mutex<StreamJunction>>,
    scheduler: Arc<Scheduler>,
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
        }
    }

    pub fn start(&self) {
        let task = Arc::new(TriggerTask {
            junction: Arc::clone(&self.stream_junction),
        });
        if let Some(period) = self.definition.at_every {
            self.scheduler.schedule_periodic(period, task, None);
        } else if let Some(at) = &self.definition.at {
            if at.trim().eq_ignore_ascii_case("start") {
                // Emit immediately
                let now = chrono::Utc::now().timestamp_millis();
                TriggerTask {
                    junction: Arc::clone(&self.stream_junction),
                }
                .on_time(now);
            } else {
                let _ = self.scheduler.schedule_cron(at, task, None);
            }
        }
    }

    pub fn shutdown(&self) {}
}

#[derive(Debug)]
struct TriggerTask {
    junction: Arc<Mutex<StreamJunction>>,
}

impl Schedulable for TriggerTask {
    fn on_time(&self, timestamp: i64) {
        let ev = Event::new_with_data(timestamp, vec![AttributeValue::Long(timestamp)]);
        self.junction.lock().unwrap().send_event(ev);
    }
}
