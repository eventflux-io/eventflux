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

use crate::core::aggregation::AggregationRuntime;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::ApplicationConfig;
use crate::core::eventflux_app_runtime::EventFluxAppRuntime;
use crate::core::partition::PartitionRuntime;
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::stream::stream_junction::StreamJunction;
// WindowRuntime is used through its full path below, so this import stays off.
// use crate::core::window::WindowRuntime;
use crate::query_api::definition::StreamDefinition as ApiStreamDefinition;
use crate::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Empty stand in for a table runtime. It holds nothing yet.
#[derive(Debug, Clone, Default)]
pub struct TableRuntimePlaceholder {}
use crate::core::trigger::TriggerRuntime;

#[derive(Debug)]
pub struct EventFluxAppRuntimeBuilder {
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub application_config: Option<ApplicationConfig>, // YAML or TOML config, if any

    pub stream_definition_map: HashMap<String, Arc<ApiStreamDefinition>>,
    pub table_definition_map: HashMap<String, Arc<crate::query_api::definition::TableDefinition>>,
    pub window_definition_map: HashMap<String, Arc<crate::query_api::definition::WindowDefinition>>,
    pub aggregation_definition_map:
        HashMap<String, Arc<crate::query_api::definition::AggregationDefinition>>,

    pub stream_junction_map: HashMap<String, Arc<Mutex<StreamJunction>>>,
    pub table_map: HashMap<String, Arc<Mutex<TableRuntimePlaceholder>>>,
    pub window_map: HashMap<String, Arc<Mutex<crate::core::window::WindowRuntime>>>,
    pub aggregation_map: HashMap<String, Arc<Mutex<AggregationRuntime>>>,

    pub query_runtimes: Vec<Arc<QueryRuntime>>,
    pub partition_runtimes: Vec<Arc<PartitionRuntime>>,
    pub trigger_runtimes: Vec<Arc<TriggerRuntime>>,
}

impl EventFluxAppRuntimeBuilder {
    pub fn new(
        eventflux_app_context: Arc<EventFluxAppContext>,
        application_config: Option<ApplicationConfig>,
    ) -> Self {
        Self {
            eventflux_app_context,
            application_config,
            stream_definition_map: HashMap::new(),
            table_definition_map: HashMap::new(),
            window_definition_map: HashMap::new(),
            aggregation_definition_map: HashMap::new(),
            stream_junction_map: HashMap::new(),
            table_map: HashMap::new(),
            window_map: HashMap::new(),
            aggregation_map: HashMap::new(),
            query_runtimes: Vec::new(),
            partition_runtimes: Vec::new(),
            trigger_runtimes: Vec::new(),
        }
    }

    // --- Add API definitions, keyed by definition id ---
    pub fn add_stream_definition(&mut self, stream_def: Arc<ApiStreamDefinition>) {
        self.stream_definition_map
            .insert(stream_def.abstract_definition.id.clone(), stream_def);
    }
    pub fn add_table_definition(
        &mut self,
        table_def: Arc<crate::query_api::definition::TableDefinition>,
    ) {
        self.table_definition_map
            .insert(table_def.abstract_definition.id.clone(), table_def);
    }
    pub fn add_window_definition(
        &mut self,
        window_def: Arc<crate::query_api::definition::WindowDefinition>,
    ) {
        self.window_definition_map.insert(
            window_def.stream_definition.abstract_definition.id.clone(),
            window_def,
        );
    }
    pub fn add_aggregation_definition(
        &mut self,
        agg_def: Arc<crate::query_api::definition::AggregationDefinition>,
    ) {
        self.aggregation_definition_map
            .insert(agg_def.abstract_definition.id.clone(), agg_def);
    }
    // FunctionDefinition and TriggerDefinition are not stored here. Functions are
    // registered elsewhere and triggers are added as runtimes with add_trigger_runtime.

    // --- Add runtime parts ---
    pub fn add_stream_junction(&mut self, stream_id: String, junction: Arc<Mutex<StreamJunction>>) {
        self.stream_junction_map.insert(stream_id, junction);
    }
    pub fn add_table(
        &mut self,
        table_id: String,
        table_runtime: Arc<Mutex<TableRuntimePlaceholder>>,
    ) {
        self.table_map.insert(table_id, table_runtime);
    }
    pub fn add_window(
        &mut self,
        window_id: String,
        window_runtime: Arc<Mutex<crate::core::window::WindowRuntime>>,
    ) {
        self.window_map.insert(window_id, window_runtime);
    }
    pub fn add_aggregation_runtime(
        &mut self,
        agg_id: String,
        agg_runtime: Arc<Mutex<AggregationRuntime>>,
    ) {
        self.aggregation_map.insert(agg_id, agg_runtime);
    }
    pub fn add_query_runtime(&mut self, query_runtime: Arc<QueryRuntime>) {
        self.query_runtimes.push(query_runtime);
    }
    pub fn add_partition_runtime(&mut self, partition_runtime: Arc<PartitionRuntime>) {
        self.partition_runtimes.push(partition_runtime);
    }
    pub fn add_trigger_runtime(&mut self, trigger_runtime: Arc<TriggerRuntime>) {
        self.trigger_runtimes.push(trigger_runtime);
    }

    // Consumes the builder and returns the runtime in the Created state.
    // Uses the context's scheduler, or a new default one if the context has none. The
    // new one is not set back on the context. Errors if a source DLQ reference is
    // invalid, with all errors joined by "; ".
    pub fn build(
        self,
        api_eventflux_app: Arc<ApiEventFluxApp>,
    ) -> Result<EventFluxAppRuntime, String> {
        let input_manager = crate::core::stream::input::input_manager::InputManager::new(
            Arc::clone(&self.eventflux_app_context),
            self.stream_junction_map.clone(),
        );

        let scheduler = self
            .eventflux_app_context
            .get_scheduler()
            .unwrap_or_else(|| {
                Arc::new(crate::core::util::Scheduler::new(Arc::new(
                    crate::core::util::ExecutorService::default(),
                )))
            });

        // Resolve every element config (YAML base plus SQL WITH overrides). This runs
        // ONCE, here at construction, before anything is attached.
        let resolved_configs = {
            use crate::core::config::ElementConfigResolver;

            let yaml_config = self.application_config.as_ref();
            let resolver = ElementConfigResolver::new(yaml_config);

            resolver.resolve_all(
                &api_eventflux_app.stream_definition_map,
                &api_eventflux_app.table_definition_map,
            )
        };

        // Fail early on a broken DLQ reference. Otherwise a missing or misnamed DLQ
        // stream would only show up at the first failed event, which would then be
        // dropped instead of sent to the DLQ (#207).
        crate::core::validation::validate_source_dlq_configs(
            &resolved_configs,
            &api_eventflux_app.stream_definition_map,
        )
        .map_err(|errors| {
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        })?;

        Ok(EventFluxAppRuntime {
            name: self.eventflux_app_context.name.clone(),
            eventflux_app: api_eventflux_app,
            eventflux_app_context: self.eventflux_app_context,
            state: Arc::new(std::sync::RwLock::new(
                crate::core::eventflux_app_runtime::RuntimeState::Created,
            )),
            stream_junction_map: self.stream_junction_map,
            input_manager: Arc::new(input_manager),
            query_runtimes: self.query_runtimes,
            partition_runtimes: self.partition_runtimes,
            scheduler: Some(scheduler),
            table_map: self.table_map,
            window_map: self.window_map,
            aggregation_map: self.aggregation_map,
            trigger_runtimes: self.trigger_runtimes,
            source_handlers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            sink_handlers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            table_handlers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            resolved_configs,
            callback_map: Arc::new(std::sync::RwLock::new(HashMap::new())),
            lifecycle: std::sync::Mutex::new(()),
        })
    }
}
