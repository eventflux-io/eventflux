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

// Corresponds to io.eventflux.core.config.EventFluxAppContext
use super::eventflux_context::EventFluxContext;
use crate::core::config::{
    ApplicationConfig, ConfigManager, EventFluxConfig, ProcessorConfigReader,
};
use crate::core::util::executor_service::ExecutorService;
use crate::core::util::id_generator::IdGenerator;
use crate::core::util::Scheduler;
use crate::query_api::EventFluxApp; // From query_api
use std::cell::RefCell;
use std::collections::HashMap; // For scriptFunctionMap
use std::sync::Arc;
use std::thread_local;
// use super::statistics_manager::StatisticsManager; // TODO: Define later
// use super::timestamp_generator::TimestampGenerator; // TODO: Define later
// use super::snapshot_service::SnapshotService; // TODO: Define later
// use super::id_generator::IdGenerator; // TODO: Define later
// use crate::core::function::Script; // TODO: Define later
// use crate::core::trigger::Trigger; // TODO: Define later
// use crate::core::util::extension::ExternalReferencedHolder; // TODO: Define later
// use crate::core::util::Scheduler; // TODO: Define later
// use com::lmax::disruptor::ExceptionHandler; // This is a Java type
// use java::beans::ExceptionListener; // This is a Java type

// Placeholders for complex Java/EventFlux types not yet ported/defined
#[derive(Debug, Clone, Default)]
pub struct StatisticsManagerPlaceholder {}
#[derive(Debug, Clone, Default)]
pub struct TimestampGeneratorPlaceholder {}
use crate::core::persistence::SnapshotService;
use crate::core::util::thread_barrier::ThreadBarrier;
#[derive(Debug, Clone, Default)]
pub struct ScriptPlaceholder {}
#[derive(Debug, Clone)]
pub struct ScriptFunctionPlaceholder {
    pub return_type: crate::query_api::definition::attribute::Type,
}

impl Default for ScriptFunctionPlaceholder {
    fn default() -> Self {
        Self {
            return_type: crate::query_api::definition::attribute::Type::OBJECT,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct TriggerPlaceholder {}
#[derive(Debug, Clone, Default)]
pub struct ExternalReferencedHolderPlaceholder {}
#[derive(Debug, Clone, Default)]
pub struct SchedulerPlaceholder {}
#[derive(Debug, Clone, Default)]
pub struct DisruptorExceptionHandlerPlaceholder {}
#[derive(Debug, Clone, Default)]
pub struct ExceptionListenerPlaceholder {}

thread_local! {
    static GROUP_BY_KEY: RefCell<Option<String>> = const { RefCell::new(None) };
    static PARTITION_KEY: RefCell<Option<String>> = const { RefCell::new(None) };
}

// Placeholder for stats Level enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MetricsLevelPlaceholder {
    #[default]
    OFF,
    BASIC,
    DETAIL,
}

/// Context specific to a single EventFlux Application instance.
#[derive(Debug, Clone)] // Default is tricky due to Arc<EventFluxApp> and Arc<EventFluxContext>
pub struct EventFluxAppContext {
    pub eventflux_context: Arc<EventFluxContext>, // Shared global context
    pub name: String,                             // Name of the EventFluxApp
    pub eventflux_app_string: String,             // The raw EventFluxQL string
    pub eventflux_app: Arc<EventFluxApp>,         // Parsed EventFluxApp definition from query_api

    pub is_playback: bool,
    pub is_enforce_order: bool,
    pub root_metrics_level: MetricsLevelPlaceholder,
    pub statistics_manager: Option<StatisticsManagerPlaceholder>, // Manages collection and reporting of runtime statistics. Option because it's set post-construction in Java.

    // Threading and scheduling (using placeholders)
    pub executor_service: Option<Arc<ExecutorService>>, // General purpose thread pool for the EventFlux App.
    pub scheduled_executor_service: Option<Arc<crate::core::util::ScheduledExecutorService>>, // Thread pool for scheduled tasks.
    pub scheduler: Option<Arc<Scheduler>>, // Exposed scheduler for timers

    // External references and lifecycle management (using placeholders)
    // pub external_referenced_holders: Vec<ExternalReferencedHolderPlaceholder>, // Manages external resources that need lifecycle management.
    // pub trigger_holders: Vec<TriggerPlaceholder>, // Holds trigger runtime instances.
    pub snapshot_service: Option<Arc<SnapshotService>>, // Manages state snapshotting and persistence.
    pub thread_barrier: Option<Arc<ThreadBarrier>>, // Coordinates threads when ordering is enforced.
    pub config_reader: Option<Arc<ProcessorConfigReader>>, // Configuration reader for processors
    pub timestamp_generator: TimestampGeneratorPlaceholder, // Generates timestamps, especially for playback mode. Has a default in Java if not set by user.
    pub id_generator: Option<IdGenerator>, // Generates unique IDs for runtime elements. Option because it's set.

    // pub script_function_map: HashMap<String, ScriptPlaceholder>, // Holds script function implementations.

    // Exception handling (using placeholders)
    // pub disruptor_exception_handler: Option<DisruptorExceptionHandlerPlaceholder>, // Handles exceptions from async event processing.
    // pub runtime_exception_listener: Option<ExceptionListenerPlaceholder>, // Listener for runtime exceptions.
    pub buffer_size: i32, // Default buffer size for event channels.
    // pub included_metrics: Option<Vec<String>>, // Specific metrics to include if statistics are enabled.
    pub transport_channel_creation_enabled: bool, // Whether transport channels (for sources/sinks) should be created.
    // pub scheduler_list: Vec<SchedulerPlaceholder>, // List of schedulers.

    // Simplified placeholder fields for now
    _external_refs_placeholder: Vec<String>,
    _triggers_placeholder: Vec<String>,
    /// Placeholder storage for script functions keyed by name.
    pub script_function_map: HashMap<String, ScriptFunctionPlaceholder>,
    _schedulers_placeholder: Vec<String>,

    // Configuration system integration
    /// Global EventFlux configuration
    pub global_config: Arc<EventFluxConfig>,
    /// Application-specific configuration
    pub app_config: Option<ApplicationConfig>,
    /// Configuration manager for dynamic updates
    pub config_manager: Option<Arc<ConfigManager>>,
}

impl EventFluxAppContext {
    // ---- Static flow utilities ----
    pub fn start_group_by_flow(key: String) {
        GROUP_BY_KEY.with(|k| *k.borrow_mut() = Some(key));
    }

    pub fn stop_group_by_flow() {
        GROUP_BY_KEY.with(|k| *k.borrow_mut() = None);
    }

    pub fn start_partition_flow(key: String) {
        PARTITION_KEY.with(|k| *k.borrow_mut() = Some(key));
    }

    pub fn stop_partition_flow() {
        PARTITION_KEY.with(|k| *k.borrow_mut() = None);
    }

    pub fn get_current_flow_id() -> String {
        let partition = PARTITION_KEY.with(|k| k.borrow().clone().unwrap_or_default());
        let group_by = GROUP_BY_KEY.with(|k| k.borrow().clone().unwrap_or_default());
        format!("{partition}--{group_by}")
    }

    pub fn get_partition_flow_id() -> Option<String> {
        PARTITION_KEY.with(|k| k.borrow().clone())
    }

    pub fn get_group_by_flow_id() -> Option<String> {
        GROUP_BY_KEY.with(|k| k.borrow().clone())
    }

    // Constructor needs essential parts. Others can be set via builder methods or setters.
    pub fn new(
        eventflux_context: Arc<EventFluxContext>,
        name: String,
        eventflux_app_definition: Arc<EventFluxApp>, // Renamed from eventflux_app to avoid confusion with field
        eventflux_app_string: String,
    ) -> Self {
        Self::new_with_config(
            eventflux_context,
            name,
            eventflux_app_definition,
            eventflux_app_string,
            Arc::new(EventFluxConfig::default()),
            None,
            None,
        )
    }

    /// Constructor with configuration support
    pub fn new_with_config(
        eventflux_context: Arc<EventFluxContext>,
        name: String,
        eventflux_app_definition: Arc<EventFluxApp>,
        eventflux_app_string: String,
        global_config: Arc<EventFluxConfig>,
        app_config: Option<ApplicationConfig>,
        config_manager: Option<Arc<ConfigManager>>,
    ) -> Self {
        // Default values from Java EventFluxAppContext() constructor and field initializers
        let default_buffer = eventflux_context.get_default_junction_buffer_size() as i32;

        // Initialize shared executor service for all stream junctions
        // This prevents unbounded thread growth (one pool instead of one per junction)
        let thread_count = num_cpus::get();
        let shared_executor = Arc::new(crate::core::util::ExecutorService::new(
            "shared-junction-pool",
            thread_count,
        ));

        Self {
            eventflux_context,
            name,
            eventflux_app_string,
            eventflux_app: eventflux_app_definition,
            is_playback: false,
            is_enforce_order: false, // Default not specified in Java, assuming false
            root_metrics_level: MetricsLevelPlaceholder::default(), // Java defaults to Level.OFF
            statistics_manager: None, // Initialized later in Java
            executor_service: Some(shared_executor),
            scheduled_executor_service: None,
            scheduler: None,
            // external_referenced_holders: Vec::new(),
            // trigger_holders: Vec::new(),
            snapshot_service: None,
            thread_barrier: None,
            config_reader: Some(Arc::new(ProcessorConfigReader::new(
                app_config.clone(),
                Some(global_config.as_ref().clone()),
            ))),
            timestamp_generator: TimestampGeneratorPlaceholder::default(), // Java new-s one if null
            id_generator: None,                                            // Set later
            // script_function_map: HashMap::new(),
            // disruptor_exception_handler: None, // Uses eventfluxContext's default if not set
            // runtime_exception_listener: None,
            buffer_size: default_buffer,
            // included_metrics: None,
            transport_channel_creation_enabled: false, // Default not specified, assuming false
            // scheduler_list: Vec::new(),
            _external_refs_placeholder: Vec::new(),
            _triggers_placeholder: Vec::new(),
            script_function_map: HashMap::new(),
            _schedulers_placeholder: Vec::new(),

            // Configuration system integration
            global_config,
            app_config,
            config_manager,
        }
    }

    /// Register a script function with the given name and return type.
    pub fn add_script_function(
        &mut self,
        name: String,
        return_type: crate::query_api::definition::attribute::Type,
    ) {
        self.script_function_map
            .insert(name, ScriptFunctionPlaceholder { return_type });
    }

    /// Lookup a registered script function placeholder by name.
    pub fn get_script_function(&self, name: &str) -> Option<&ScriptFunctionPlaceholder> {
        self.script_function_map.get(name)
    }

    // --- Getter and Setter examples ---
    pub fn get_eventflux_context(&self) -> Arc<EventFluxContext> {
        Arc::clone(&self.eventflux_context)
    }

    /// Convenience wrapper that lists available scalar function names.
    pub fn list_scalar_function_names(&self) -> Vec<String> {
        self.eventflux_context.list_scalar_function_names()
    }

    /// Convenience wrapper that lists available attribute aggregators.
    pub fn list_attribute_aggregator_names(&self) -> Vec<String> {
        self.eventflux_context.list_attribute_aggregator_names()
    }

    // In Java, getAttributes() delegates to eventfluxContext.getAttributes().
    pub fn get_attributes(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, String>> {
        // Delegate to EventFluxContext's thread-safe attributes map
        self.eventflux_context.get_attributes()
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn is_playback(&self) -> bool {
        self.is_playback
    }

    pub fn set_playback(&mut self, playback: bool) {
        self.is_playback = playback;
    }

    pub fn is_enforce_order(&self) -> bool {
        self.is_enforce_order
    }

    pub fn set_enforce_order(&mut self, enforce: bool) {
        self.is_enforce_order = enforce;
    }

    pub fn get_root_metrics_level(&self) -> MetricsLevelPlaceholder {
        self.root_metrics_level
    }

    pub fn set_root_metrics_level(&mut self, level: MetricsLevelPlaceholder) {
        self.root_metrics_level = level;
    }

    pub fn get_buffer_size(&self) -> i32 {
        self.buffer_size
    }

    pub fn set_buffer_size(&mut self, size: i32) {
        self.buffer_size = size;
    }

    pub fn is_transport_channel_creation_enabled(&self) -> bool {
        self.transport_channel_creation_enabled
    }

    pub fn set_transport_channel_creation_enabled(&mut self, enabled: bool) {
        self.transport_channel_creation_enabled = enabled;
    }

    pub fn get_snapshot_service(&self) -> Option<Arc<SnapshotService>> {
        self.snapshot_service.as_ref().map(Arc::clone)
    }

    pub fn set_snapshot_service(&mut self, service: Arc<SnapshotService>) {
        self.snapshot_service = Some(service);
    }

    pub fn get_thread_barrier(&self) -> Option<Arc<ThreadBarrier>> {
        self.thread_barrier.as_ref().cloned()
    }

    pub fn set_thread_barrier(&mut self, barrier: Arc<ThreadBarrier>) {
        self.thread_barrier = Some(barrier);
    }

    /// Get the configuration reader for processors
    pub fn get_config_reader(&self) -> Option<Arc<ProcessorConfigReader>> {
        self.config_reader.clone()
    }

    /// Set the configuration reader
    pub fn set_config_reader(&mut self, reader: Arc<ProcessorConfigReader>) {
        self.config_reader = Some(reader);
    }

    pub fn get_executor_service(&self) -> Option<Arc<ExecutorService>> {
        self.executor_service.as_ref().cloned()
    }

    pub fn set_executor_service(&mut self, service: Arc<ExecutorService>) {
        self.executor_service = Some(service);
    }

    pub fn get_scheduled_executor_service(
        &self,
    ) -> Option<Arc<crate::core::util::ScheduledExecutorService>> {
        self.scheduled_executor_service.as_ref().cloned()
    }

    pub fn set_scheduled_executor_service(
        &mut self,
        service: Arc<crate::core::util::ScheduledExecutorService>,
    ) {
        self.scheduled_executor_service = Some(service);
    }

    pub fn get_scheduler(&self) -> Option<Arc<Scheduler>> {
        self.scheduler.as_ref().cloned()
    }

    pub fn set_scheduler(&mut self, scheduler: Arc<Scheduler>) {
        self.scheduler = Some(scheduler);
    }

    pub fn get_timestamp_generator(&self) -> &TimestampGeneratorPlaceholder {
        &self.timestamp_generator
    }

    pub fn set_timestamp_generator(&mut self, generator: TimestampGeneratorPlaceholder) {
        self.timestamp_generator = generator;
    }

    pub fn set_id_generator(&mut self, id_gen: IdGenerator) {
        self.id_generator = Some(id_gen);
    }

    // ---- Configuration system methods ----

    /// Get the global EventFlux configuration
    pub fn get_global_config(&self) -> &EventFluxConfig {
        &self.global_config
    }

    /// Get application-specific configuration if available
    pub fn get_app_config(&self) -> Option<&ApplicationConfig> {
        self.app_config.as_ref()
    }

    /// Get configuration manager for dynamic updates
    pub fn get_config_manager(&self) -> Option<&ConfigManager> {
        self.config_manager.as_ref().map(|m| m.as_ref())
    }

    /// Update the global configuration
    pub fn update_global_config(&mut self, config: Arc<EventFluxConfig>) {
        self.global_config = config;
    }

    /// Update the application-specific configuration
    pub fn update_app_config(&mut self, config: Option<ApplicationConfig>) {
        self.app_config = config;
    }

    /// Set the configuration manager
    pub fn set_config_manager(&mut self, manager: Option<Arc<ConfigManager>>) {
        self.config_manager = manager;
    }

    /// Get a configuration value from the global config using a dot-notation path
    /// Example: "eventflux.runtime.performance.thread_pool_size"
    pub fn get_config_value<T>(&self, path: &str) -> Option<T>
    where
        T: serde::de::DeserializeOwned + std::fmt::Debug + Clone,
    {
        // This is a simplified implementation - in a full implementation you would
        // walk the configuration structure using the path to find the specific value
        // For now, we'll implement basic commonly used configuration values

        match path {
            "eventflux.runtime.performance.thread_pool_size" => Some(
                serde_json::from_value(serde_json::json!(
                    self.global_config
                        .eventflux
                        .runtime
                        .performance
                        .thread_pool_size
                ))
                .ok()?,
            ),
            "eventflux.runtime.performance.event_buffer_size" => Some(
                serde_json::from_value(serde_json::json!(
                    self.global_config
                        .eventflux
                        .runtime
                        .performance
                        .event_buffer_size
                ))
                .ok()?,
            ),
            "eventflux.runtime.performance.batch_processing" => Some(
                serde_json::from_value(serde_json::json!(
                    self.global_config
                        .eventflux
                        .runtime
                        .performance
                        .batch_processing
                ))
                .ok()?,
            ),
            "eventflux.runtime.performance.async_processing" => Some(
                serde_json::from_value(serde_json::json!(
                    self.global_config
                        .eventflux
                        .runtime
                        .performance
                        .async_processing
                ))
                .ok()?,
            ),
            _ => {
                // For unsupported paths, return None
                None
            }
        }
    }

    /// Get the thread pool size from configuration
    pub fn get_configured_thread_pool_size(&self) -> usize {
        self.global_config
            .eventflux
            .runtime
            .performance
            .thread_pool_size
    }

    /// Get the event buffer size from configuration
    pub fn get_configured_event_buffer_size(&self) -> usize {
        self.global_config
            .eventflux
            .runtime
            .performance
            .event_buffer_size
    }

    /// Check if batch processing is enabled
    pub fn is_batch_processing_enabled(&self) -> bool {
        self.global_config
            .eventflux
            .runtime
            .performance
            .batch_processing
    }

    /// Check if async processing is enabled
    pub fn is_async_processing_enabled(&self) -> bool {
        self.global_config
            .eventflux
            .runtime
            .performance
            .async_processing
    }

    /// Get the runtime mode
    pub fn get_runtime_mode(&self) -> &crate::core::config::RuntimeMode {
        &self.global_config.eventflux.runtime.mode
    }

    /// Check if the application is running in distributed mode
    pub fn is_distributed_mode(&self) -> bool {
        self.global_config.is_distributed_mode()
    }
}

#[cfg(test)]
impl EventFluxAppContext {
    /// Convenience constructor used across unit tests to build a minimal
    /// `EventFluxAppContext` instance.  Having this single implementation avoids
    /// multiple `impl` blocks in test modules which previously caused duplicate
    /// method definition errors during compilation.
    pub fn default_for_testing() -> Self {
        use crate::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp;

        Self::new_with_config(
            Arc::new(EventFluxContext::default()),
            "test_app_ctx".to_string(),
            Arc::new(ApiEventFluxApp::new("test_api_app".to_string())),
            String::new(),
            Arc::new(EventFluxConfig::default()),
            None,
            None,
        )
    }
}
