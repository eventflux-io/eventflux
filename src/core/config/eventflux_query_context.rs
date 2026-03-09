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

// Corresponds to io.eventflux.core.config.EventFluxQueryContext
use super::eventflux_app_context::EventFluxAppContext;
use crate::core::persistence::StateHolder;
use crate::core::util::id_generator::IdGenerator;
use crate::query_api::execution::query::output::OutputEventType;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex}; // From query_api, as Java uses it.
                             // use crate::core::util::statistics::LatencyTracker; // TODO: Define LatencyTracker
                             // use crate::core::util::IdGenerator; // TODO: Define IdGenerator

// Placeholders
#[derive(Debug, Clone, Default)]
pub struct LatencyTrackerPlaceholder {}

#[derive(Debug)] // Custom Clone below
pub struct EventFluxQueryContext {
    // transient EventFluxAppContext eventfluxAppContext in Java
    // Store as Arc<EventFluxAppContext> because EventFluxAppContext is likely shared.
    // Or, if EventFluxQueryContext has a lifetime tied to EventFluxAppContext, it could be a reference.
    // Arc is safer for now.
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub name: String,                                       // Query name
    pub partition_id: String,                               // Defaulted in Java if null
    pub partitioned: bool,                                  // Java default false
    pub output_event_type: Option<OutputEventType>,         // Java type, optional
    pub latency_tracker: Option<LatencyTrackerPlaceholder>, // transient in Java, Option in Rust
    pub id_generator: IdGenerator,                          // new-ed in Java constructor
    pub stateful: AtomicBool,                               // whether any state holders registered
    pub aggregator_counter: AtomicUsize,                    // counter for unique aggregator IDs
}

impl Clone for EventFluxQueryContext {
    fn clone(&self) -> Self {
        Self {
            eventflux_app_context: Arc::clone(&self.eventflux_app_context),
            name: self.name.clone(),
            partition_id: self.partition_id.clone(),
            partitioned: self.partitioned,
            output_event_type: self.output_event_type,
            latency_tracker: self.latency_tracker.clone(),
            id_generator: self.id_generator.clone(),
            stateful: AtomicBool::new(self.stateful.load(Ordering::SeqCst)),
            aggregator_counter: AtomicUsize::new(self.aggregator_counter.load(Ordering::SeqCst)),
        }
    }
}

impl EventFluxQueryContext {
    pub fn new(
        eventflux_app_context: Arc<EventFluxAppContext>,
        query_name: String,
        partition_id: Option<String>,
    ) -> Self {
        let default_partition_id = "DEFAULT_PARTITION_ID_PLACEHOLDER".to_string(); // TODO: Use EventFluxConstants
        Self {
            eventflux_app_context,
            name: query_name,
            partition_id: partition_id.unwrap_or(default_partition_id),
            partitioned: false,
            output_event_type: None,
            latency_tracker: None,
            id_generator: IdGenerator::default(),
            stateful: AtomicBool::new(false),
            aggregator_counter: AtomicUsize::new(0),
        }
    }

    // --- Getters and Setters ---
    pub fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.eventflux_app_context)
    }

    // In Java, getEventFluxContext() delegates through eventfluxAppContext.
    // pub fn get_eventflux_context(&self) -> Arc<EventFluxContext> { // Assuming EventFluxContext is the type
    //     self.eventflux_app_context.get_eventflux_context()
    // }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn is_partitioned(&self) -> bool {
        self.partitioned
    }

    pub fn set_partitioned(&mut self, partitioned: bool) {
        self.partitioned = partitioned;
    }

    pub fn get_output_event_type(&self) -> Option<OutputEventType> {
        self.output_event_type
    }

    pub fn set_output_event_type(&mut self, output_event_type: OutputEventType) {
        self.output_event_type = Some(output_event_type);
    }

    pub fn get_latency_tracker(&self) -> Option<&LatencyTrackerPlaceholder> {
        self.latency_tracker.as_ref()
    }

    pub fn set_latency_tracker(&mut self, tracker: LatencyTrackerPlaceholder) {
        self.latency_tracker = Some(tracker);
    }

    pub fn generate_new_id(&mut self) -> String {
        self.id_generator.create_new_id()
    }

    pub fn is_stateful(&self) -> bool {
        self.stateful.load(Ordering::SeqCst)
    }

    /// Get the next unique aggregator ID for this query.
    /// Each call returns an incremented counter, ensuring unique IDs per aggregator instance.
    pub fn next_aggregator_id(&self) -> usize {
        self.aggregator_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Register a state holder with the application's `SnapshotService`.
    /// The provided `name` is namespaced by the query name to ensure uniqueness.
    pub fn register_state_holder(&self, name: String, holder: Arc<Mutex<dyn StateHolder>>) {
        if let Some(service) = self.eventflux_app_context.get_snapshot_service() {
            let key = format!("{}::{}", self.name, name);
            service.register_state_holder(key, holder);
            self.stateful.store(true, Ordering::SeqCst);
        }
    }
}
