// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/eventflux_app_runtime_builder.rs
use crate::core::aggregation::AggregationRuntime;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::ApplicationConfig;
use crate::core::eventflux_app_runtime::EventFluxAppRuntime; // Actual EventFluxAppRuntime
use crate::core::partition::PartitionRuntime;
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::stream::stream_junction::StreamJunction;
// use crate::core::window::WindowRuntime; // TODO: Will be used when window runtime is implemented
use crate::query_api::definition::StreamDefinition as ApiStreamDefinition;
use crate::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp; // For build() method arg // Added this import

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Placeholders for runtime components until they are defined
#[derive(Debug, Clone, Default)]
pub struct TableRuntimePlaceholder {}
use crate::core::trigger::TriggerRuntime;

#[derive(Debug)]
pub struct EventFluxAppRuntimeBuilder {
    pub eventflux_app_context: Arc<EventFluxAppContext>, // Set upon creation, not Option
    pub application_config: Option<ApplicationConfig>, // Optional configuration for the application

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
            eventflux_app_context, // No longer Option
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

    // --- Methods to add API definitions ---
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
    // FunctionDefinition and TriggerDefinition are handled differently (often registered in context or directly creating runtimes)

    // --- Methods to add runtime components ---
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

    // build() method that consumes the builder and returns a EventFluxAppRuntime
    pub fn build(
        self,
        api_eventflux_app: Arc<ApiEventFluxApp>,
    ) -> Result<EventFluxAppRuntime, String> {
        // Create InputManager and populate it
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

        // Pre-resolve all element configurations (YAML base + SQL WITH overrides)
        // This happens ONCE at construction time, before any attachment
        let resolved_configs = {
            use crate::core::config::ElementConfigResolver;

            let yaml_config = self.application_config.as_ref();
            let resolver = ElementConfigResolver::new(yaml_config);

            resolver.resolve_all(
                &api_eventflux_app.stream_definition_map,
                &api_eventflux_app.table_definition_map,
            )
        };

        Ok(EventFluxAppRuntime {
            name: self.eventflux_app_context.name.clone(),
            eventflux_app: api_eventflux_app, // The original parsed API definition
            eventflux_app_context: self.eventflux_app_context, // The context created for this app instance
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
        })
    }
}
