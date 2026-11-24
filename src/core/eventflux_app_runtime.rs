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

// Port of the Java EventFluxAppRuntime interface and its EventFluxAppRuntimeImpl.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::config::{ApplicationConfig, EventFluxConfig};
use crate::core::eventflux_app_runtime_builder::TableRuntimePlaceholder;
use crate::core::partition::PartitionRuntime;
use crate::core::persistence::SnapshotService;
use crate::core::query::output::callback_processor::CallbackProcessor;
use crate::core::query::query_runtime::QueryRuntime;
use crate::core::stream::input::input_handler::InputHandler;
use crate::core::stream::input::input_manager::InputManager;
use crate::core::stream::output::sink::SinkCallbackAdapter;
use crate::core::stream::output::stream_callback::StreamCallback;
use crate::core::stream::stream_junction::{
    poll_quiescent, poll_settled, StreamJunction, HALT_SETTLE_TIMEOUT, JUNCTION_DRAIN_TIMEOUT,
    MAX_DRAIN_TIMEOUT,
};
use crate::core::trigger::TriggerRuntime;
use crate::core::util::parser::eventflux_app_parser::EventFluxAppParser;
use crate::core::window::WindowRuntime;
use crate::query_api::EventFluxApp as ApiEventFluxApp;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::RwLock;

use crate::core::query::processor::Processor;

/// Tracks registered callbacks, mapping callback_id to (stream_id, processor).
#[rustfmt::skip]
type CallbackMap = Arc<RwLock<HashMap<String, (String, Arc<Mutex<dyn Processor>>)>>>;

/// Lifecycle states of an EventFluxAppRuntime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    /// Built but not started yet.
    Created,
    /// Attaching sources, sinks and triggers. Nothing in this file sets it today,
    /// start() goes straight from Created to Starting.
    Attaching,
    /// start() is running.
    Starting,
    /// Started and processing events.
    Running,
    /// shutdown() is running.
    Stopping,
    /// shutdown() finished.
    Stopped,
    /// start() failed. Some parts may still be set up. start() can be retried.
    Failed,
}

/// Runs a single EventFlux app and manages its lifecycle.
///
/// Built through `new` or `new_with_config`, which return a Result. There is no Default.
#[derive(Debug)]
pub struct EventFluxAppRuntime {
    pub name: String,
    pub eventflux_app: Arc<ApiEventFluxApp>, // the parsed app definition
    pub eventflux_app_context: Arc<EventFluxAppContext>,

    pub state: Arc<RwLock<RuntimeState>>,

    // Runtime parts built by EventFluxAppRuntimeBuilder.
    pub stream_junction_map: HashMap<String, Arc<Mutex<StreamJunction>>>,
    pub input_manager: Arc<InputManager>,
    pub query_runtimes: Vec<Arc<QueryRuntime>>,
    pub partition_runtimes: Vec<Arc<PartitionRuntime>>,
    pub trigger_runtimes: Vec<Arc<TriggerRuntime>>,
    pub scheduler: Option<Arc<crate::core::util::Scheduler>>,
    pub table_map: HashMap<String, Arc<Mutex<TableRuntimePlaceholder>>>,
    pub window_map: HashMap<String, Arc<Mutex<WindowRuntime>>>,
    pub aggregation_map: HashMap<String, Arc<Mutex<crate::core::aggregation::AggregationRuntime>>>,

    // Source and sink handlers, started and stopped with the runtime.
    pub source_handlers: Arc<
        std::sync::RwLock<HashMap<String, Arc<crate::core::stream::handler::SourceStreamHandler>>>,
    >,
    pub sink_handlers: Arc<
        std::sync::RwLock<HashMap<String, Arc<crate::core::stream::handler::SinkStreamHandler>>>,
    >,

    // Table handlers. Tables are only queried, so they have no start or stop.
    pub table_handlers: Arc<std::sync::RwLock<HashMap<String, Arc<dyn crate::core::table::Table>>>>,

    /// Fully merged configs for every element (sources, sinks, tables and so on).
    ///
    /// Resolved once at construction. Keys look like "type:name", for example
    /// "source:EventInput".
    ///
    /// Why resolve up front:
    /// - All config precedence rules live in one merge step.
    /// - Config errors show up before any element starts.
    /// - New element types can plug into the same resolver.
    /// - Each config records where each property came from, which helps debugging.
    pub resolved_configs: HashMap<String, crate::core::config::ResolvedElementConfig>,

    /// Registered callbacks, kept so they can be removed later.
    /// See `add_callback()` and `remove_callback()`.
    pub callback_map: CallbackMap,

    /// Makes start() and shutdown() run one at a time. The RuntimeState check
    /// alone is not enough. shutdown() accepts the Starting state, and if the two
    /// interleave their junction restart and stop calls, the runtime can end up
    /// "Running" with every junction rejecting events.
    pub(crate) lifecycle: Mutex<()>,
}

impl EventFluxAppRuntime {
    // Builds the runtime from a parsed app. Plays the role of the Java
    // EventFluxManager.createEventFluxAppRuntime(api_eventflux_app).
    pub fn new(
        api_eventflux_app: Arc<ApiEventFluxApp>,
        // Used to build the EventFluxAppContext.
        eventflux_context: Arc<crate::core::config::eventflux_context::EventFluxContext>,
        eventflux_app_string: Option<String>,
    ) -> Result<Self, String> {
        // 1. Build the EventFluxAppContext, using @app annotation settings when present.
        let mut name = api_eventflux_app.name.clone();
        let mut is_playback = false;
        let mut enforce_order = false;
        let mut root_metrics =
            crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::OFF;
        let mut buffer_size = 0i32;
        let mut transport_creation = false;

        if let Some(app_ann) = api_eventflux_app
            .annotations
            .iter()
            .find(|a| a.name.eq_ignore_ascii_case("app"))
        {
            for el in &app_ann.elements {
                match el.key.to_lowercase().as_str() {
                    "name" => name = el.value.clone(),
                    "playback" => is_playback = el.value.eq_ignore_ascii_case("true"),
                    "enforce.order" | "enforceorder" => {
                        enforce_order = el.value.eq_ignore_ascii_case("true")
                    }
                    "statistics" | "stats" => root_metrics = match el.value.to_lowercase().as_str()
                    {
                        "true" | "basic" => {
                            crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::BASIC
                        }
                        "detail" | "detailed" => {
                            crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::DETAIL
                        }
                        _ => crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::OFF,
                    },
                    "buffer_size" | "buffersize" => {
                        if let Ok(sz) = el.value.parse::<i32>() {
                            buffer_size = sz;
                        }
                    }
                    "transport.channel.creation" => {
                        transport_creation = el.value.eq_ignore_ascii_case("true");
                    }
                    _ => {}
                }
            }
        }

        let mut ctx = EventFluxAppContext::new(
            eventflux_context,
            name.clone(),
            Arc::clone(&api_eventflux_app),
            eventflux_app_string.unwrap_or_default(),
        );
        ctx.set_playback(is_playback);
        ctx.set_enforce_order(enforce_order);
        ctx.set_root_metrics_level(root_metrics);
        if buffer_size > 0 {
            ctx.set_buffer_size(buffer_size);
        }
        ctx.set_transport_channel_creation_enabled(transport_creation);

        // Always set a ThreadBarrier. Input handlers pass through it, so incoming
        // events can be held back while state is restored.
        let thread_barrier = Arc::new(crate::core::util::thread_barrier::ThreadBarrier::new());
        ctx.set_thread_barrier(thread_barrier);

        // The scheduler gets its own thread pool, so its sleeps never block event
        // processing threads. It reuses the context's scheduled executor if one exists.
        let scheduler = if let Some(exec) = ctx.get_scheduled_executor_service() {
            Arc::new(crate::core::util::Scheduler::new(Arc::clone(
                &exec.executor,
            )))
        } else {
            // Default pool size is EVENTFLUX_EXECUTOR_THREADS, or the CPU count.
            Arc::new(crate::core::util::Scheduler::new(Arc::new(
                crate::core::util::ExecutorService::default(),
            )))
        };
        ctx.set_scheduler(Arc::clone(&scheduler));
        let mut ss = SnapshotService::new(name.clone());
        if let Some(store) = ctx.eventflux_context.get_persistence_store() {
            ss.persistence_store = Some(store);
        }
        let snapshot_service = Arc::new(ss);
        ctx.set_snapshot_service(Arc::clone(&snapshot_service));
        let eventflux_app_context = Arc::new(ctx);

        // 2. Parse the app into a builder.
        let builder = EventFluxAppParser::parse_eventflux_app_runtime_builder(
            &api_eventflux_app,
            eventflux_app_context,
            None,
        )?;

        // 3. Build the runtime from the builder.
        builder.build(api_eventflux_app)
    }

    /// Builds a runtime with an application config, or falls back to `new` when
    /// `app_config` is None.
    ///
    /// With a config, @app annotations are not read and `eventflux_app_string` is
    /// ignored. Returns an error if parsing or building fails.
    pub fn new_with_config(
        api_eventflux_app: Arc<ApiEventFluxApp>,
        eventflux_context: Arc<crate::core::config::eventflux_context::EventFluxContext>,
        eventflux_app_string: Option<String>,
        app_config: Option<ApplicationConfig>,
    ) -> Result<Self, String> {
        if let Some(config) = app_config {
            Self::new_with_applied_config(
                api_eventflux_app,
                eventflux_context,
                eventflux_app_string,
                &config,
            )
        } else {
            Self::new(api_eventflux_app, eventflux_context, eventflux_app_string)
        }
    }

    /// Builds a runtime whose context comes from an ApplicationConfig.
    ///
    /// Unlike `new`, this sets no scheduler on the context, so the builder creates a
    /// default one.
    fn new_with_applied_config(
        api_eventflux_app: Arc<ApiEventFluxApp>,
        eventflux_context: Arc<crate::core::config::eventflux_context::EventFluxContext>,
        _eventflux_app_string: Option<String>,
        app_config: &ApplicationConfig,
    ) -> Result<Self, String> {
        // 1. Build the EventFluxAppContext from the YAML or TOML ApplicationConfig.
        let name = api_eventflux_app.name.clone();
        let mut root_metrics =
            crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::OFF;
        let buffer_size = 0i32;

        // monitoring.metrics_enabled turns on BASIC metrics.
        if let Some(ref monitoring) = app_config.monitoring {
            if monitoring.metrics_enabled {
                root_metrics =
                    crate::core::config::eventflux_app_context::MetricsLevelPlaceholder::BASIC;
            }
        }

        let mut ctx = EventFluxAppContext::new_with_config(
            eventflux_context,
            name.clone(),
            Arc::clone(&api_eventflux_app),
            String::new(),                        // eventflux_app_string
            Arc::new(EventFluxConfig::default()), // global_config
            Some(app_config.clone()),             // app_config
            None,                                 // config_manager
        );

        ctx.set_root_metrics_level(root_metrics);
        if buffer_size > 0 {
            ctx.set_buffer_size(buffer_size);
        }

        if let Some(ref _error_handling) = app_config.error_handling {
            // Not applied yet. Error handling settings from the config are ignored here.
        }

        // Always set a ThreadBarrier. Input handlers pass through it, so incoming
        // events can be held back while state is restored.
        let thread_barrier = Arc::new(crate::core::util::ThreadBarrier::new());
        ctx.set_thread_barrier(Arc::clone(&thread_barrier));

        // 2. Create the SnapshotService, using the context's persistence store if set.
        let mut ss = SnapshotService::new(name.clone());
        if let Some(store) = ctx.eventflux_context.get_persistence_store() {
            ss.persistence_store = Some(store);
        }
        let snapshot_service = Arc::new(ss);
        ctx.set_snapshot_service(Arc::clone(&snapshot_service));
        let eventflux_app_context = Arc::new(ctx);

        // 3. Parse the app into a builder, passing the config along.
        let builder = EventFluxAppParser::parse_eventflux_app_runtime_builder(
            &api_eventflux_app,
            eventflux_app_context,
            Some(app_config.clone()),
        )?;

        // 4. Build the runtime from the builder.
        let runtime = builder.build(api_eventflux_app)?;

        // Sources and sinks are not attached here. start() attaches them, so the
        // lifecycle stays clean:
        //  - Construction: build the runtime and register parts
        //  - Start: attach and start I/O sources and sinks
        //  - Shutdown: stop I/O and clean up
        //
        // start() reads the config from eventflux_app_context.app_config.

        Ok(runtime)
    }

    pub fn get_input_handler(&self, stream_id: &str) -> Option<Arc<Mutex<InputHandler>>> {
        self.input_manager.get_input_handler(stream_id)
    }

    pub fn get_table_input_handler(
        &self,
        table_id: &str,
    ) -> Option<crate::core::stream::input::table_input_handler::TableInputHandler> {
        self.input_manager.get_table_input_handler(table_id)
    }

    /// Subscribes a callback to the events of a stream.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream to subscribe to
    /// * `callback` - Receives the stream's events
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - A new UUID callback id to pass to `remove_callback()`
    /// * `Err(String)` - If the stream does not exist
    ///
    /// # Panics
    ///
    /// Panics if the junction mutex or the callback map lock is poisoned.
    pub fn add_callback(
        &self,
        stream_id: &str,
        callback: Box<dyn StreamCallback>,
    ) -> Result<String, String> {
        let output_junction = self
            .stream_junction_map
            .get(stream_id)
            .ok_or_else(|| format!("StreamJunction '{stream_id}' not found to add callback"))?
            .clone();

        let callback_id = Uuid::new_v4().hyphenated().to_string();
        let query_name_for_callback = format!("callback_processor_{}_{}", stream_id, &callback_id);
        let query_context_for_callback = Arc::new(EventFluxQueryContext::new(
            Arc::clone(&self.eventflux_app_context),
            query_name_for_callback.clone(),
            None, // a stream callback belongs to no partition
        ));

        let callback_processor: Arc<Mutex<dyn Processor>> =
            Arc::new(Mutex::new(CallbackProcessor::new(
                Arc::new(Mutex::new(callback)),
                Arc::clone(&self.eventflux_app_context),
                query_context_for_callback,
            )));

        output_junction
            .lock()
            .expect("Output StreamJunction Mutex poisoned")
            .subscribe(Arc::clone(&callback_processor));

        // Keep the processor so remove_callback can unsubscribe it.
        self.callback_map
            .write()
            .expect("callback_map RwLock poisoned")
            .insert(
                callback_id.clone(),
                (stream_id.to_string(), callback_processor),
            );

        Ok(callback_id)
    }

    /// Unsubscribes a callback added with `add_callback()`.
    ///
    /// # Arguments
    ///
    /// * `callback_id` - The id returned by `add_callback()`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The callback was removed
    /// * `Err(String)` - If the callback id is unknown or the stream junction no longer
    ///   exists. In the second case the id is already gone from the map.
    ///
    /// # Panics
    ///
    /// Panics if the callback map lock or the junction mutex is poisoned.
    pub fn remove_callback(&self, callback_id: &str) -> Result<(), String> {
        let (stream_id, processor) = self
            .callback_map
            .write()
            .expect("callback_map RwLock poisoned")
            .remove(callback_id)
            .ok_or_else(|| format!("Callback '{}' not found", callback_id))?;

        let junction = self
            .stream_junction_map
            .get(&stream_id)
            .ok_or_else(|| format!("StreamJunction '{}' not found", stream_id))?
            .clone();

        junction
            .lock()
            .expect("StreamJunction Mutex poisoned")
            .unsubscribe(&processor);

        Ok(())
    }

    // ========================================================================
    // Stream handler management
    // ========================================================================

    /// Registers a source handler for a stream, replacing any existing one.
    pub fn register_source_handler(
        &self,
        stream_name: String,
        handler: Arc<crate::core::stream::handler::SourceStreamHandler>,
    ) {
        self.source_handlers
            .write()
            .unwrap()
            .insert(stream_name, handler);
    }

    /// Returns the source handler for a stream, if one is registered.
    pub fn get_source_handler(
        &self,
        stream_name: &str,
    ) -> Option<Arc<crate::core::stream::handler::SourceStreamHandler>> {
        self.source_handlers
            .read()
            .unwrap()
            .get(stream_name)
            .cloned()
    }

    /// Registers a sink handler for a stream, replacing any existing one.
    pub fn register_sink_handler(
        &self,
        stream_name: String,
        handler: Arc<crate::core::stream::handler::SinkStreamHandler>,
    ) {
        self.sink_handlers
            .write()
            .unwrap()
            .insert(stream_name, handler);
    }

    /// Returns the sink handler for a stream, if one is registered.
    pub fn get_sink_handler(
        &self,
        stream_name: &str,
    ) -> Option<Arc<crate::core::stream::handler::SinkStreamHandler>> {
        self.sink_handlers.read().unwrap().get(stream_name).cloned()
    }

    /// Registers a table by name, replacing any existing one.
    pub fn register_table_handler(
        &self,
        table_name: String,
        table: Arc<dyn crate::core::table::Table>,
    ) {
        self.table_handlers
            .write()
            .unwrap()
            .insert(table_name, table);
    }

    /// Returns the table registered under `table_name`, if any.
    pub fn get_table_handler(
        &self,
        table_name: &str,
    ) -> Option<Arc<dyn crate::core::table::Table>> {
        self.table_handlers.read().unwrap().get(table_name).cloned()
    }

    /// Returns the merged config for one element.
    ///
    /// Configs are resolved at construction by merging the YAML base with SQL WITH
    /// properties.
    ///
    /// # Arguments
    ///
    /// * `element_type` - Type of element (source, sink, table and so on)
    /// * `name` - Name of the element
    ///
    /// # Returns
    ///
    /// The resolved config, or None if there is none for this element.
    pub fn get_resolved_config(
        &self,
        element_type: crate::core::config::ElementType,
        name: &str,
    ) -> Option<&crate::core::config::ResolvedElementConfig> {
        self.resolved_configs.get(&element_type.config_key(name))
    }

    /// Subscribes a sink handler to a stream junction so the sink receives its events.
    ///
    /// Wires the sink the same way `add_callback` wires a callback. Unlike
    /// `add_callback`, the subscription is not tracked and cannot be removed later.
    /// Returns an error if the stream does not exist.
    pub fn attach_sink_to_junction(
        &self,
        stream_name: &str,
        handler: Arc<crate::core::stream::handler::SinkStreamHandler>,
    ) -> Result<(), String> {
        let output_junction = self
            .stream_junction_map
            .get(stream_name)
            .ok_or_else(|| format!("StreamJunction '{}' not found to attach sink", stream_name))?
            .clone();

        let query_name_for_sink = format!(
            "sink_handler_{}_{}",
            stream_name,
            Uuid::new_v4().hyphenated()
        );
        let query_context_for_sink = Arc::new(EventFluxQueryContext::new(
            Arc::clone(&self.eventflux_app_context),
            query_name_for_sink.clone(),
            None,
        ));

        // Share the handler's own sink Arc instead of cloning the sink. Start, stop and
        // event delivery must all reach the same sink instance. Cloning it once left
        // SQL defined sinks never started.
        let sink_arc = handler.sink(); // Arc<Mutex<Box<dyn Sink>>>

        // Use the handler's mapper for a custom format. With no format, fall back to
        // the binary PassthroughMapper.
        let mapper = handler.mapper().unwrap_or_else(|| {
            Arc::new(Mutex::new(Box::new(
                crate::core::stream::output::mapper::PassthroughMapper::new(),
            )
                as Box<dyn crate::core::stream::output::mapper::SinkMapper>))
        });

        // Flow: events -> mapper.map() -> bytes -> sink.publish()
        let adapter = SinkCallbackAdapter {
            sink: Arc::clone(&sink_arc),
            mapper,
        };

        let callback_box: Box<dyn StreamCallback> = Box::new(adapter);

        let sink_callback_arc = Arc::new(Mutex::new(callback_box));

        let callback_processor = Arc::new(Mutex::new(CallbackProcessor::new(
            sink_callback_arc,
            Arc::clone(&self.eventflux_app_context),
            query_context_for_sink,
        )));

        output_junction
            .lock()
            .expect("Output StreamJunction Mutex poisoned")
            .subscribe(callback_processor);

        Ok(())
    }

    /// Starts every registered source handler.
    ///
    /// Tries every source even after a failure, logs each failure, and returns one
    /// error listing them all. Sources that did start are left running. The caller
    /// must roll them back.
    pub fn start_all_sources(&self) -> Result<(), String> {
        let mut errors = Vec::new();

        for handler in self.source_handlers.read().unwrap().values() {
            if let Err(e) = handler.start() {
                let error_msg = format!("Failed to start source '{}': {}", handler.stream_id(), e);
                log::error!("[EventFluxAppRuntime] {}", error_msg);
                errors.push(error_msg);
            }
        }

        if !errors.is_empty() {
            Err(format!(
                "Failed to start {} source(s): {}",
                errors.len(),
                errors.join("; ")
            ))
        } else {
            Ok(())
        }
    }

    /// Stops every registered source handler and waits for all of them.
    ///
    /// Each stop runs on its own scoped thread, so the total wait is about one
    /// grace period rather than one per source. `Source` implementations only need
    /// a correct `stop()` for this.
    pub fn stop_all_sources(&self) {
        let handlers = self.source_handlers.read().unwrap();
        std::thread::scope(|s| {
            for handler in handlers.values() {
                s.spawn(|| handler.stop());
            }
        });
    }

    /// Starts every registered sink handler.
    ///
    /// Tries every sink, logs each one that is not running afterwards, and returns
    /// one error listing them all. Sinks that did start are left running.
    pub fn start_all_sinks(&self) -> Result<(), String> {
        let mut errors = Vec::new();

        for handler in self.sink_handlers.read().unwrap().values() {
            handler.start();
            // SinkStreamHandler::start() returns nothing, so check is_running()
            // to see whether it worked.
            if !handler.is_running() {
                let error_msg = format!("Failed to start sink '{}'", handler.stream_id());
                log::error!("[EventFluxAppRuntime] {}", error_msg);
                errors.push(error_msg);
            }
        }

        if !errors.is_empty() {
            Err(format!(
                "Failed to start {} sink(s): {}",
                errors.len(),
                errors.join("; ")
            ))
        } else {
            Ok(())
        }
    }

    /// Stops every registered sink handler, one after another.
    pub fn stop_all_sinks(&self) {
        for handler in self.sink_handlers.read().unwrap().values() {
            handler.stop();
        }
    }

    /// Undoes a failed start by stopping everything start() may have started.
    ///
    /// Best effort. Nothing here returns an error.
    fn rollback_startup(&self) {
        log::warn!("Rolling back runtime startup for '{}'", self.name);

        // Stop partitions and triggers in reverse order.
        // Must not clear restored state. A retried start() comes in through the
        // Failed arm with restarting=false and skips auto restore, so it relies on
        // the state from the failed attempt surviving this rollback.
        for pr in self.partition_runtimes.iter().rev() {
            pr.shutdown();
        }

        for tr in self.trigger_runtimes.iter().rev() {
            tr.shutdown();
        }

        self.stop_all_sinks();

        self.stop_all_sources();

        // Halt the junctions. start() restarts them before sinks and sources, so a
        // failure after that point would leave consumers live and junctions taking
        // events into a rolled back setup. It would also leak the consumer threads
        // if the runtime is then thrown away. Halt instead of a graceful stop,
        // because the sinks are already stopped. Draining the backlog now would
        // deliver into dead sinks (the #136 window) and still count the events as
        // processed. So wait for consumers to settle, then count the frozen backlog
        // as dropped. A retried start() purges it and clears the flags through
        // restart_processing.
        let rollback_handles: Vec<_> = self
            .stream_junction_map
            .values()
            .map(|j| j.lock().unwrap().drain_handle())
            .filter(|h| h.is_async())
            .collect();
        for junction in self.stream_junction_map.values() {
            junction.lock().unwrap().halt_processing();
        }
        poll_settled(&rollback_handles, Instant::now() + HALT_SETTLE_TIMEOUT);
        for junction in self.stream_junction_map.values() {
            junction.lock().unwrap().abandon_pending();
        }

        log::info!("Rollback completed for '{}'", self.name);
    }

    /// Starts the runtime.
    ///
    /// Attaches the configured sources, sinks and tables, then starts junctions,
    /// sinks, sources, triggers and partitions in that order. Calling it while
    /// Running or Starting does nothing and returns `Ok`. From Stopped it first
    /// restores the last persisted revision, if a persistence store has one.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The runtime is running
    /// * `Err(EventFluxError)` - Attachment or startup failed, or the runtime was in
    ///   a state it cannot start from. The state is then set to Failed, except for
    ///   the bad state case, and start() can be retried.
    ///
    /// # Error handling
    ///
    /// Every attachment error is logged as it happens, then all of them are returned
    /// as one error before anything starts. Elements that did attach stay attached.
    /// A failure while starting rolls back everything started so far.
    ///
    /// # Panics
    ///
    /// Panics if the lifecycle mutex is poisoned.
    pub fn start(&self) -> Result<(), crate::core::exception::EventFluxError> {
        use crate::core::exception::EventFluxError;

        // Run one at a time with shutdown(). Otherwise a shutdown, which is allowed
        // from Starting, could interleave its junction stops with our restarts and
        // leave a "Running" runtime whose junctions all reject events.
        let _lifecycle = self.lifecycle.lock().expect("lifecycle mutex poisoned");

        // 1. Check the state. Starting twice is harmless.
        let restarting;
        {
            let mut state = self.state.write().unwrap();
            match *state {
                RuntimeState::Created | RuntimeState::Failed => {
                    restarting = false;
                    *state = RuntimeState::Starting;
                }
                RuntimeState::Running => {
                    log::debug!("Runtime already running, start() is no-op");
                    return Ok(());
                }
                RuntimeState::Starting => {
                    // Another start is in progress, so this call does nothing.
                    log::debug!("Runtime already starting, start() is no-op");
                    return Ok(());
                }
                RuntimeState::Stopped => {
                    restarting = true;
                    *state = RuntimeState::Starting;
                }
                _ => {
                    return Err(EventFluxError::app_runtime(format!(
                        "Cannot start runtime in state: {:?}",
                        *state
                    )));
                }
            }
        }

        // Auto restore only on a restart from Stopped. On the first start (Created) the
        // user may already have called restore_revision() by hand.
        if restarting {
            if let Some(service) = self.eventflux_app_context.get_snapshot_service() {
                if let Some(store) = &service.persistence_store {
                    if let Some(revision) = store.get_last_revision(&self.name) {
                        match self.restore_revision(&revision) {
                            Ok(()) => {
                                log::info!("Auto-restored state (revision: {})", revision)
                            }
                            Err(e) => {
                                log::warn!(
                                    "Auto-restore failed, clearing partial state and starting fresh: {}",
                                    e
                                );
                                // Clear only the in memory state. Keep the persisted
                                // revisions for debugging or manual recovery.
                                service.clear_state_holders();
                                self.clear_select_processor_group_states();
                            }
                        }
                    }
                }
            }
        }

        let mut all_errors = Vec::new();

        // 2. Attach all elements. Every source and sink comes from its resolved
        // config, where SQL WITH properties already win over YAML.
        let app_config = self.eventflux_app_context.app_config.as_ref();
        match self.auto_attach_streams(app_config) {
            Ok((sources, sinks)) => {
                if !sources.is_empty() || !sinks.is_empty() {
                    log::info!(
                        "Auto-attached {} source(s), {} sink(s)",
                        sources.len(),
                        sinks.len()
                    );
                }
            }
            Err(errors) => {
                log::error!("Failed to auto-attach streams ({} error(s)):", errors.len());
                for (i, e) in errors.iter().enumerate() {
                    log::error!("  {}. {}", i + 1, e);
                }
                all_errors.extend(errors);
            }
        }

        // Each step below is safe to repeat and collects its errors.
        if let Some(app_config) = app_config {
            match self.auto_attach_tables_from_config(app_config) {
                Ok(tables) => {
                    if !tables.is_empty() {
                        log::info!(
                            "Successfully attached {} table(s): {}",
                            tables.len(),
                            tables.join(", ")
                        );
                    }
                }
                Err(errors) => {
                    log::error!("Failed to auto-attach tables ({} error(s)):", errors.len());
                    for (idx, err) in errors.iter().enumerate() {
                        log::error!("  {}. {}", idx + 1, err);
                    }
                    all_errors.extend(errors);
                }
            }
        }

        // 3. Fail before starting anything if any attachment failed.
        if !all_errors.is_empty() {
            *self.state.write().unwrap() = RuntimeState::Failed;
            log::error!(
                "EventFluxAppRuntime '{}' failed during attachment with {} error(s)",
                self.name,
                all_errors.len()
            );
            return Err(EventFluxError::app_runtime(format!(
                "Runtime startup failed during attachment: {}",
                all_errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }

        // 4. Start everything, rolling back on error.
        // Junctions first. shutdown() leaves shutdown flags set on every junction,
        // sync and async, so a restarted runtime must clear them before anything
        // can flow. Does nothing on the first start.
        for junction in self.stream_junction_map.values() {
            if let Err(e) = junction.lock().unwrap().restart_processing() {
                log::error!("Failed to restart stream junction: {}", e);
                self.rollback_startup();
                *self.state.write().unwrap() = RuntimeState::Failed;
                return Err(EventFluxError::app_runtime(format!(
                    "Failed to restart stream junction: {}",
                    e
                )));
            }
        }

        // Downstream first. Sinks start before sources, so every sink is
        // subscribed and connected by the time the first event is read. The
        // reverse order lost events published into sink streams that were not
        // ready yet (#136). Shutdown mirrors this, stopping sources first and
        // sinks last.
        if let Err(e) = self.start_all_sinks() {
            log::error!("Failed to start sinks: {}", e);
            self.rollback_startup();
            *self.state.write().unwrap() = RuntimeState::Failed;
            return Err(EventFluxError::app_runtime(format!(
                "Failed to start sinks: {}",
                e
            )));
        }

        // Sources start last. This is what starts reading events.
        if let Err(e) = self.start_all_sources() {
            log::error!("Failed to start sources: {}", e);
            self.rollback_startup();
            *self.state.write().unwrap() = RuntimeState::Failed;
            return Err(EventFluxError::app_runtime(format!(
                "Failed to start sources: {}",
                e
            )));
        }

        if self.scheduler.is_some() {
            log::info!(
                "Scheduler initialized for EventFluxAppRuntime '{}'",
                self.name
            );
        }

        for tr in &self.trigger_runtimes {
            tr.start();
        }

        for pr in &self.partition_runtimes {
            pr.start();
        }

        // 5. Done.
        *self.state.write().unwrap() = RuntimeState::Running;
        log::info!("EventFluxAppRuntime '{}' started successfully", self.name);
        Ok(())
    }

    /// Returns how long shutdown waits for async junctions to drain.
    ///
    /// Defaults to `JUNCTION_DRAIN_TIMEOUT`. Set `EVENTFLUX_JUNCTION_DRAIN_TIMEOUT_MS`
    /// to override it, the same way `EVENTFLUX_EXECUTOR_THREADS` works. A value that
    /// does not parse is ignored. Capped at `MAX_DRAIN_TIMEOUT` so `Instant + timeout`
    /// cannot overflow on huge values.
    fn junction_drain_timeout() -> Duration {
        std::env::var("EVENTFLUX_JUNCTION_DRAIN_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(JUNCTION_DRAIN_TIMEOUT)
            .min(MAX_DRAIN_TIMEOUT)
    }

    /// Stops the runtime.
    ///
    /// Stops producers, drains async junctions, flushes queries, persists state if a
    /// persistence store is set, then stops sinks and partitions. Does nothing
    /// unless the state is Running, Starting or Failed.
    ///
    /// # Panics
    ///
    /// Panics if the lifecycle mutex is poisoned.
    pub fn shutdown(&self) {
        // Run one at a time with start(). See the `lifecycle` field. A shutdown
        // called during start() waits for it to finish, then tears down cleanly.
        let _lifecycle = self.lifecycle.lock().expect("lifecycle mutex poisoned");

        {
            let mut state = self.state.write().unwrap();
            match *state {
                RuntimeState::Running | RuntimeState::Starting | RuntimeState::Failed => {
                    *state = RuntimeState::Stopping;
                }
                _ => return, // already stopped, stopping or never started
            }
        }

        // Stop every producer before the drain. First triggers, then the scheduler,
        // then sources. Scheduler ticks (timer windows, rate limiters) publish into
        // junctions without being tracked. A tick during or after the drain would be
        // missed by the quiescence check and left uncounted.
        for tr in &self.trigger_runtimes {
            tr.shutdown();
        }
        if let Some(scheduler) = &self.scheduler {
            scheduler.shutdown();
        }
        self.stop_all_sources();

        // Drain async junctions before touching sinks (#138). With sources
        // stopped, wait until everything goes quiet WITHOUT setting shutdown
        // flags. Flagging junctions first breaks chains, because an event drained
        // from junction A flows through a query into junction B, whose flag would
        // reject it.
        let drain_handles: Vec<_> = self
            .stream_junction_map
            .values()
            .map(|j| j.lock().unwrap().drain_handle())
            .filter(|h| h.is_async())
            .collect();
        let drain_timeout = Self::junction_drain_timeout();
        let drain_deadline = Instant::now() + drain_timeout;
        // The first drain's result is ignored on purpose. This pass only makes
        // sure the flush sees the drained events. The second drain checks that
        // everything is quiet again after the flush.
        poll_quiescent(&drain_handles, drain_deadline);

        // Flush what is left through the pipeline so state is stable. Runs after
        // the first drain so the flush sees drained events. Flush output can flow
        // on into async junctions, hence the second drain. Both drains share one
        // deadline.
        for qr in &self.query_runtimes {
            qr.flush();
        }
        let drained = poll_quiescent(&drain_handles, drain_deadline);

        let settled;
        if drained {
            // Queues are empty, so it is safe to set the shutdown flags, which
            // stay set until restart. Consumer loops exit quickly.
            for junction in self.stream_junction_map.values() {
                junction.lock().unwrap().stop_processing();
            }
            // Wait for consumers to settle before persisting, even on this clean
            // path. A producer that raced the flags may have slipped in one more
            // event, and the exiting consumers would deliver it while the snapshot
            // below is taken. Normally instant, since the queues are empty.
            settled = poll_settled(&drain_handles, Instant::now() + HALT_SETTLE_TIMEOUT);
        } else {
            // Timed out with a backlog. Halt consumers so they stop draining
            // BEFORE counting the leftovers. A graceful stop would let them keep
            // delivering the "abandoned" events while the persist below runs, and
            // into sinks that are about to stop.
            for junction in self.stream_junction_map.values() {
                junction.lock().unwrap().halt_processing();
            }
            // Let live consumers finish their current event and any dispatch in
            // flight, so the abandoned counts are exact and the snapshot sees no
            // change while it is taken. Bounded by HALT_SETTLE_TIMEOUT, so a stuck
            // subscriber cannot block shutdown forever.
            settled = poll_settled(&drain_handles, Instant::now() + HALT_SETTLE_TIMEOUT);
            if !settled {
                log::warn!(
                    "EventFluxAppRuntime '{}': subscriber work still in flight {:?} after halt; abandoned counts may include events that are also delivered",
                    self.name,
                    HALT_SETTLE_TIMEOUT
                );
            }
            let mut abandoned = 0u64;
            for junction in self.stream_junction_map.values() {
                abandoned += junction.lock().unwrap().abandon_pending();
            }
            log::warn!(
                "EventFluxAppRuntime '{}' drain timed out after {:?}; abandoned {} queued event(s)",
                self.name,
                drain_timeout,
                abandoned
            );
        }

        // Persist after the drain, so the snapshot covers every event the runtime
        // accepted, not just those processed before shutdown was called. Skipped
        // when subscriber work may still be changing state (not settled). A half
        // written snapshot is worse than keeping the previous revision, which auto
        // restore will fall back to.
        if settled {
            if let Some(service) = self.eventflux_app_context.get_snapshot_service() {
                if service.persistence_store.is_some() {
                    match self.persist() {
                        Ok(report) => {
                            log::info!("Auto-persisted on shutdown (revision: {})", report.revision)
                        }
                        Err(e) => log::warn!("Auto-persist on shutdown failed: {}", e),
                    }
                }
            }
        } else {
            log::warn!(
                "EventFluxAppRuntime '{}': skipping auto-persist — subscriber work was still running after the settle timeout and the snapshot could be torn; the previous revision remains the restore point",
                self.name
            );
        }

        self.stop_all_sinks();

        // The scheduler was already shut down before the drain, with the triggers.
        for pr in &self.partition_runtimes {
            pr.shutdown();
        }

        *self.state.write().unwrap() = RuntimeState::Stopped;
        log::info!("EventFluxAppRuntime '{}' shutdown", self.name);
    }

    /// Persists a snapshot of the current state through the SnapshotService.
    ///
    /// # Returns
    ///
    /// * `Ok(PersistReport)` - Every component was saved. `failure_count` is always 0.
    /// * `Err(String)` - No snapshot service is set, or `SnapshotService::persist`
    ///   failed. That includes no persistence store, and a partial save where some
    ///   components failed. A partial revision is still written to the store.
    pub fn persist(&self) -> Result<crate::core::persistence::PersistReport, String> {
        let service = self
            .eventflux_app_context
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        service.persist()
    }

    /// Returns a snapshot of the current state as bytes. Errors if no SnapshotService is set.
    pub fn snapshot(&self) -> Result<Vec<u8>, String> {
        let service = self
            .eventflux_app_context
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        Ok(service.snapshot())
    }

    /// Restores state from snapshot bytes. Errors if no SnapshotService is set.
    ///
    /// Unlike `restore_revision`, this does not hold back events through the
    /// ThreadBarrier and does not clear SelectProcessor group states.
    pub fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        let service = self
            .eventflux_app_context
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        service.set_state(snapshot.to_vec());
        Ok(())
    }

    /// Restores a persisted revision through the SnapshotService.
    ///
    /// With a ThreadBarrier set, new events are held back and in flight events
    /// finish first. On success, SelectProcessor group states are cleared so
    /// aggregators start fresh. Errors if no SnapshotService is set or the restore
    /// fails.
    pub fn restore_revision(&self, revision: &str) -> Result<(), String> {
        let service = self
            .eventflux_app_context
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;

        if let Some(barrier) = self.eventflux_app_context.get_thread_barrier() {
            // Stop new events from entering.
            barrier.lock();

            // Wait for threads already inside to finish their current event.
            while barrier.get_active_threads() > 0 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }

            let result = service.restore_revision(revision);

            if result.is_ok() {
                self.clear_select_processor_group_states();
            }

            barrier.unlock();

            result
        } else {
            // No barrier, so events may race the restore. Group states are not
            // cleared on this path.
            service.restore_revision(revision)
        }
    }

    /// Clears all persisted and in memory state.
    ///
    /// Call it between `shutdown()` and `start()` for a clean restart with nothing
    /// left from earlier runs. In any state other than `Stopped` it only logs a
    /// warning and does nothing.
    pub fn clear_state(&self) {
        {
            let state = self.state.read().unwrap();
            if !matches!(*state, RuntimeState::Stopped) {
                log::warn!(
                    "Cannot clear state while runtime '{}' is in {:?} state",
                    self.name,
                    *state
                );
                return;
            }
        }
        if let Some(service) = self.eventflux_app_context.get_snapshot_service() {
            if let Some(store) = &service.persistence_store {
                store.clear_all_revisions(&self.name);
            }
            // Reset every registered state holder, such as window buffers and
            // aggregator totals.
            service.clear_state_holders();
        }
        // Clear per group state held in SelectProcessors.
        self.clear_select_processor_group_states();
        log::info!("Cleared all state for '{}'", self.name);
    }

    /// Clears group states in every query's processor chain, so restored or cleared
    /// state starts fresh.
    fn clear_select_processor_group_states(&self) {
        for query_runtime in &self.query_runtimes {
            if let Some(ref processor) = query_runtime.processor_chain_head {
                Self::clear_processor_chain_group_states(processor);
            }
        }
    }

    /// Clears group states on this processor and every processor after it.
    ///
    /// Skips the rest of the chain if a processor mutex is poisoned.
    fn clear_processor_chain_group_states(
        processor: &Arc<Mutex<dyn crate::core::query::processor::Processor>>,
    ) {
        if let Ok(proc) = processor.lock() {
            // Does nothing for processors other than SelectProcessor.
            proc.clear_group_states();

            if let Some(ref next) = proc.next_processor() {
                Self::clear_processor_chain_group_states(next);
            }
        }
    }

    /// Queries an aggregation with optional `within` and `per` clauses.
    ///
    /// Returns an empty list if `agg_id` is unknown. See
    /// `AggregationRuntime::query` for how the clauses are handled.
    pub fn query_aggregation(
        &self,
        agg_id: &str,
        within: Option<crate::query_api::aggregation::Within>,
        per: Option<crate::query_api::aggregation::TimeDuration>,
    ) -> Vec<Vec<crate::core::event::value::AttributeValue>> {
        if let Some(rt) = self.aggregation_map.get(agg_id) {
            rt.lock().unwrap().query(within, per)
        } else {
            Vec::new()
        }
    }

    /// Attaches the sources and sinks of every stream from its resolved config.
    ///
    /// The resolved configs already hold the YAML base with SQL WITH laid on top
    /// (see [`ElementConfigResolver`](crate::core::config::ElementConfigResolver)),
    /// so this is the only place streams get a connector. A SQL WITH `type` of
    /// source or sink attaches that one kind, internal attaches nothing, and any
    /// other value is an error. A stream with no SQL type attaches whatever source
    /// and sink its YAML config gave it.
    ///
    /// Streams that already have a handler are left alone, so calling it again is
    /// safe. Keeps going when some streams fail.
    ///
    /// # Arguments
    ///
    /// * `app_config` - The YAML app config, if any. Used only to report YAML
    ///   sources and sinks for streams the app does not define.
    ///
    /// # Returns
    ///
    /// * `Ok((sources, sinks))` - Names of the streams with a source or sink,
    ///   including ones that were already attached
    /// * `Err(errors)` - Every error found. Streams that did attach stay attached.
    fn auto_attach_streams(
        &self,
        app_config: Option<&ApplicationConfig>,
    ) -> Result<(Vec<String>, Vec<String>), Vec<crate::core::exception::EventFluxError>> {
        use crate::core::config::ElementType;
        use crate::core::exception::EventFluxError;

        let mut errors = Vec::new();
        let mut attached_sources = Vec::new();
        let mut attached_sinks = Vec::new();

        for (stream_id, stream_def) in &self.eventflux_app.stream_definition_map {
            let sql_type = stream_def
                .with_config
                .as_ref()
                .and_then(|with_config| with_config.get("type"))
                .map(String::as_str);

            let kinds = match sql_type {
                Some("source") => vec![ElementType::Source],
                Some("sink") => vec![ElementType::Sink],
                Some("internal") => Vec::new(),
                Some(other) => {
                    errors.push(EventFluxError::configuration(format!(
                        "Invalid stream type '{}' for stream '{}'. Expected: 'source', 'sink', or 'internal'",
                        other, stream_id
                    )));
                    continue;
                }
                None => [ElementType::Source, ElementType::Sink]
                    .into_iter()
                    .filter(|kind| self.get_resolved_config(*kind, stream_id).is_some())
                    .collect(),
            };

            for kind in kinds {
                match self.attach_from_resolved_config(kind, stream_id) {
                    Ok(()) if kind == ElementType::Source => {
                        attached_sources.push(stream_id.clone())
                    }
                    Ok(()) => attached_sinks.push(stream_id.clone()),
                    Err(e) => {
                        log::error!(
                            "[EventFluxAppRuntime] Failed to attach {} '{}': {}",
                            kind,
                            stream_id,
                            e
                        );
                        errors.push(e);
                    }
                }
            }
        }

        // The resolver only sees streams the app defines, so a YAML source or sink
        // for any other stream would otherwise be skipped without a word.
        let undefined = app_config
            .into_iter()
            .flat_map(|config| &config.streams)
            .filter(|(name, stream)| {
                (stream.source.is_some() || stream.sink.is_some())
                    && !self.eventflux_app.stream_definition_map.contains_key(*name)
            });
        for (name, _) in undefined {
            errors.push(EventFluxError::configuration(format!(
                "YAML config has a source or sink for stream '{}', but the app does not define that stream",
                name
            )));
        }

        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok((attached_sources, attached_sinks))
        }
    }

    /// Returns the attribute names of a stream in declaration order, or an empty list
    /// if the stream is not defined. Format mappers use them to map fields by name.
    fn stream_field_names(&self, stream_name: &str) -> Vec<String> {
        self.eventflux_app
            .stream_definition_map
            .get(stream_name)
            .map(|stream_def| stream_def.abstract_definition.attribute_name_list())
            .unwrap_or_default()
    }

    /// Returns the `InputHandler` of the DLQ stream that the source of `stream_name`
    /// sends failed events to.
    ///
    /// Checks the DLQ reference again as a cheap guard. App creation already
    /// checked every resolved source config, and every source is attached from
    /// one. Errors if the reference is invalid or the input handler cannot be
    /// built.
    fn dlq_input_handler(
        &self,
        stream_name: &str,
        dlq_stream: &str,
    ) -> Result<Arc<Mutex<InputHandler>>, crate::core::exception::EventFluxError> {
        use crate::core::exception::EventFluxError;
        use crate::core::validation::{source_dlq_stream, validate_dlq_reference};

        validate_dlq_reference(
            stream_name,
            dlq_stream,
            source_dlq_stream(&self.resolved_configs, dlq_stream),
            &self.eventflux_app.stream_definition_map,
        )?;

        // Every defined stream has a junction before attachment starts, so the DLQ
        // stream does not need to be set up in any particular order.
        self.input_manager
            .construct_input_handler(dlq_stream)
            .map_err(|e| {
                EventFluxError::app_creation(format!(
                    "Failed to resolve DLQ stream '{}' for source stream '{}': {}",
                    dlq_stream, stream_name, e
                ))
            })
    }

    /// Attaches a source to a stream.
    ///
    /// Creates the source and mapper, wires the DLQ stream if one is set, builds
    /// the handler and registers it. The caller checks for an existing handler
    /// first.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - Name of the stream to attach
    /// * `stream_type_config` - The StreamTypeConfig, already built
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The source is attached. `start_all_sources` starts it later,
    ///   when start() reaches the execution phase.
    /// * `Err(EventFluxError)` - Source setup, input handler or DLQ wiring failed.
    ///   The message names the stream and extension.
    fn attach_source(
        &self,
        stream_name: &str,
        stream_type_config: &crate::core::config::stream_config::StreamTypeConfig,
    ) -> Result<(), crate::core::exception::EventFluxError> {
        use crate::core::error::DLQ_STREAM_KEY;
        use crate::core::exception::EventFluxError;
        use crate::core::stream::handler::SourceStreamHandler;
        use crate::core::stream::stream_initializer::initialize_source_stream;

        let field_names = self.stream_field_names(stream_name);

        let mut initialized = initialize_source_stream(
            &self.eventflux_app_context.eventflux_context,
            stream_type_config,
            &field_names,
        )
        .map_err(|e| {
            EventFluxError::app_creation(format!(
                "Failed to initialize source '{}' (extension={}, format={:?}): {}",
                stream_name,
                stream_type_config.extension().unwrap_or("<none>"),
                stream_type_config.format(),
                e
            ))
        })?;

        let input_handler = self
            .input_manager
            .construct_input_handler(stream_name)
            .map_err(|e| {
                EventFluxError::app_creation(format!(
                    "Failed to construct InputHandler for source stream '{}': {}",
                    stream_name, e
                ))
            })?;

        // Factories build sources with no DLQ junction. Wire it now so failed
        // events reach the DLQ stream instead of being dropped (#207).
        if let Some(dlq_stream) = stream_type_config.properties.get(DLQ_STREAM_KEY) {
            let dlq_handler = self.dlq_input_handler(stream_name, dlq_stream)?;
            initialized
                .source
                .set_error_dlq_junction(stream_name, dlq_handler)
                .map_err(|e| {
                    EventFluxError::app_creation(format!(
                        "Cannot route source '{}' (extension={}) failures to DLQ stream '{}': {}",
                        stream_name,
                        stream_type_config.extension().unwrap_or("<none>"),
                        dlq_stream,
                        e
                    ))
                })?;
        }

        let handler = Arc::new(SourceStreamHandler::new(
            initialized.source,
            initialized.mapper,
            input_handler,
            stream_name.to_string(),
        ));

        // Register only. The source is NOT started here, attachment just wires
        // parts together. Reading starts in the execution phase
        // (start_all_sources), after every sink is subscribed and started. A
        // source that read at attach time could publish into a sink stream with
        // no subscriber yet and silently drop events (#136).
        self.register_source_handler(stream_name.to_string(), handler);

        Ok(())
    }

    /// Attaches a sink to a stream.
    ///
    /// Sink version of [`Self::attach_source`]. Creates the sink and mapper,
    /// builds the handler, registers it and subscribes it to the stream's
    /// junction. The caller checks for an existing handler first.
    ///
    /// The handler is registered before the junction subscribe. If the subscribe
    /// fails, the handler stays registered.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - Name of the stream to attach
    /// * `stream_type_config` - The StreamTypeConfig, already built
    fn attach_sink(
        &self,
        stream_name: &str,
        stream_type_config: &crate::core::config::stream_config::StreamTypeConfig,
    ) -> Result<(), crate::core::exception::EventFluxError> {
        use crate::core::exception::EventFluxError;
        use crate::core::stream::handler::SinkStreamHandler;
        use crate::core::stream::stream_initializer::initialize_sink_stream;

        // Schema field names give format mappers real output keys, such as JSON
        // "symbol" instead of "field_0".
        let field_names = self.stream_field_names(stream_name);

        let initialized = initialize_sink_stream(
            &self.eventflux_app_context.eventflux_context,
            stream_type_config,
            &field_names,
        )
        .map_err(|e| {
            EventFluxError::app_creation(format!(
                "Failed to initialize sink '{}' (extension={}, format={:?}): {}",
                stream_name,
                stream_type_config.extension().unwrap_or("<none>"),
                stream_type_config.format(),
                e
            ))
        })?;

        let handler = Arc::new(SinkStreamHandler::new(
            initialized.sink,
            initialized.mapper,
            stream_name.to_string(),
        ));

        self.register_sink_handler(stream_name.to_string(), Arc::clone(&handler));

        self.attach_sink_to_junction(stream_name, handler)
            .map_err(|e| {
                EventFluxError::app_creation(format!(
                    "Failed to attach sink '{}' to its stream junction: {}",
                    stream_name, e
                ))
            })
    }

    /// Attaches the source or sink of one stream from its resolved config.
    ///
    /// The resolved config is the YAML base merged with SQL WITH at construction
    /// time, so nothing is merged here. Does nothing if the stream already has a
    /// handler of that kind.
    ///
    /// # Arguments
    ///
    /// * `kind` - `ElementType::Source` or `ElementType::Sink`
    /// * `stream_name` - Name of the stream to attach
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Attached, or already was. `start_all_sources` starts a source
    ///   later.
    /// * `Err(EventFluxError)` - `kind` is not source or sink, there is no resolved
    ///   config, no `extension`, an invalid StreamTypeConfig, or a failure in
    ///   [`Self::attach_source`] or [`Self::attach_sink`]
    fn attach_from_resolved_config(
        &self,
        kind: crate::core::config::ElementType,
        stream_name: &str,
    ) -> Result<(), crate::core::exception::EventFluxError> {
        use crate::core::config::stream_config::{StreamType, StreamTypeConfig};
        use crate::core::config::ElementType;
        use crate::core::exception::EventFluxError;

        type Attach =
            fn(&EventFluxAppRuntime, &str, &StreamTypeConfig) -> Result<(), EventFluxError>;
        let (stream_type, attached, attach): (StreamType, bool, Attach) = match kind {
            ElementType::Source => (
                StreamType::Source,
                self.get_source_handler(stream_name).is_some(),
                Self::attach_source,
            ),
            ElementType::Sink => (
                StreamType::Sink,
                self.get_sink_handler(stream_name).is_some(),
                Self::attach_sink,
            ),
            other => {
                return Err(EventFluxError::configuration(format!(
                    "Cannot attach a connector of kind '{}' to stream '{}'",
                    other, stream_name
                )))
            }
        };
        if attached {
            return Ok(());
        }

        let resolved = self.get_resolved_config(kind, stream_name).ok_or_else(|| {
            EventFluxError::configuration(format!(
                "No resolved configuration found for {} stream '{}'. \
                 Ensure the stream has WITH clause or YAML config.",
                kind, stream_name
            ))
        })?;

        let extension = resolved.extension.clone().ok_or_else(|| {
            EventFluxError::configuration(format!(
                "Missing 'extension' property for {kind} stream '{stream_name}'. \
                 A {kind} stream requires: type='{kind}', extension='<name>', format='<type>'"
            ))
        })?;

        let stream_type_config = StreamTypeConfig::new(
            stream_type,
            Some(extension.clone()),
            resolved.format.clone(),
            resolved.properties.clone(),
        )
        .map_err(|e| {
            EventFluxError::configuration(format!(
                "Invalid StreamTypeConfig for {} '{}' (extension={}, format={:?}): {}",
                kind, stream_name, extension, resolved.format, e
            ))
        })?;

        attach(self, stream_name, &stream_type_config)?;

        log::info!(
            "[EventFluxAppRuntime] Attached {} '{}' (extension={}, format={:?}) [{}]",
            kind,
            stream_name,
            extension,
            resolved.format,
            resolved.get_source_summary()
        );

        Ok(())
    }

    /// Builds and registers every table defined in the YAML or TOML config.
    ///
    /// Tables are only queried, so unlike sources and sinks they need no start or
    /// stop. Tables already registered are skipped. Keeps going after a failure.
    ///
    /// # Arguments
    ///
    /// * `app_config` - Application config holding the table definitions
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<String>)` - Names of the tables handled, including skipped ones
    /// * `Err(Vec<EventFluxError>)` - Every error found
    fn auto_attach_tables_from_config(
        &self,
        app_config: &ApplicationConfig,
    ) -> Result<Vec<String>, Vec<crate::core::exception::EventFluxError>> {
        use crate::core::config::types::application_config::DefinitionConfig;

        let mut errors = Vec::new();
        let mut successes = Vec::new();

        for (table_name, def_config) in &app_config.definitions {
            if let DefinitionConfig::Table(table_config) = def_config {
                if self.get_table_handler(table_name).is_some() {
                    successes.push(table_name.clone());
                    continue;
                }

                match self.attach_single_table(table_name, table_config) {
                    Ok(()) => {
                        successes.push(table_name.clone());
                        log::info!(
                            "[EventFluxAppRuntime] Successfully attached table '{}' (extension={})",
                            table_name,
                            table_config.store.store_type
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "[EventFluxAppRuntime] Failed to attach table '{}': {}",
                            table_name,
                            e
                        );
                        errors.push(e);
                    }
                }
            }
        }

        if !errors.is_empty() {
            Err(errors)
        } else {
            Ok(successes)
        }
    }

    /// Builds one table from its YAML TableConfig and registers it.
    ///
    /// Converts the TableConfig to the TableTypeConfig the initializer takes.
    /// Errors if the conversion or the table factory fails.
    fn attach_single_table(
        &self,
        table_name: &str,
        table_config: &crate::core::config::types::application_config::TableConfig,
    ) -> Result<(), crate::core::exception::EventFluxError> {
        use crate::core::exception::EventFluxError;
        use crate::core::stream::stream_initializer::initialize_table;

        let table_type_config = Self::convert_table_config(table_config).map_err(|e| {
            EventFluxError::configuration(format!(
                "Failed to convert TableConfig for '{}': {}",
                table_name, e
            ))
        })?;

        let table = initialize_table(
            &self.eventflux_app_context.eventflux_context,
            &table_type_config,
            table_name,
        )
        .map_err(|e| {
            EventFluxError::app_creation(format!(
                "Failed to initialize table '{}' (extension={}): {}",
                table_name, table_config.store.store_type, e
            ))
        })?
        .table;

        self.register_table_handler(table_name.to_string(), table);

        Ok(())
    }

    /// Converts a YAML TableConfig to the flat TableTypeConfig the initializer takes.
    ///
    /// The store type becomes the extension. Properties hold `extension`, the
    /// flattened store connection, and the pool settings as
    /// `<extension>.pool.max_size`, `.min_size` and `.connection_timeout_ms`.
    /// The schema, caching and indexing sections are not used. Errors if the
    /// connection cannot be flattened or TableTypeConfig rejects the result.
    fn convert_table_config(
        table_config: &crate::core::config::types::application_config::TableConfig,
    ) -> Result<crate::core::config::stream_config::TableTypeConfig, String> {
        use crate::core::config::stream_config::TableTypeConfig;
        use crate::core::config::types::application_config::flatten_yaml_into;

        let extension = table_config.store.store_type.clone();

        let mut properties = HashMap::new();

        properties.insert("extension".to_string(), extension.clone());

        flatten_yaml_into(&table_config.store.connection, "", &mut properties)?;

        if let Some(ref pool) = table_config.store.pool {
            properties.insert(
                format!("{}.pool.max_size", extension),
                pool.max_size.to_string(),
            );
            properties.insert(
                format!("{}.pool.min_size", extension),
                pool.min_size.to_string(),
            );
            properties.insert(
                format!("{}.pool.connection_timeout_ms", extension),
                pool.connection_timeout.as_millis().to_string(),
            );
        }

        // TableTypeConfig::new validates the result.
        TableTypeConfig::new(extension, properties)
    }

    // The old get_yaml_source_base_properties() and get_yaml_sink_base_properties()
    // methods are gone. ElementConfigResolver now does that work, resolving every
    // element config when the runtime is built.
    // See src/core/config/element_config_resolver.rs.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::types::application_config::{StoreConfig, TableConfig};

    #[test]
    fn test_convert_table_config_inmemory() {
        let table_config = TableConfig {
            store: StoreConfig {
                store_type: "inMemory".to_string(),
                connection: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
                pool: None,
                security: None,
            },
            schema: None,
            caching: None,
            indexing: None,
        };

        let result = EventFluxAppRuntime::convert_table_config(&table_config);
        assert!(result.is_ok());

        let table_type_config = result.unwrap();
        assert_eq!(table_type_config.extension(), "inMemory");
        assert!(table_type_config.properties.contains_key("extension"));
    }

    #[test]
    fn test_convert_table_config_with_connection() {
        let mut connection = serde_yaml::Mapping::new();
        connection.insert(
            serde_yaml::Value::String("host".to_string()),
            serde_yaml::Value::String("localhost".to_string()),
        );
        connection.insert(
            serde_yaml::Value::String("port".to_string()),
            serde_yaml::Value::Number(3306.into()),
        );
        connection.insert(
            serde_yaml::Value::String("database".to_string()),
            serde_yaml::Value::String("testdb".to_string()),
        );

        let table_config = TableConfig {
            store: StoreConfig {
                store_type: "mysql".to_string(),
                connection: serde_yaml::Value::Mapping(connection),
                pool: None,
                security: None,
            },
            schema: None,
            caching: None,
            indexing: None,
        };

        let result = EventFluxAppRuntime::convert_table_config(&table_config);
        assert!(result.is_ok());

        let table_type_config = result.unwrap();
        assert_eq!(table_type_config.extension(), "mysql");
        assert_eq!(
            table_type_config.properties.get("host"),
            Some(&"localhost".to_string())
        );
        assert_eq!(
            table_type_config.properties.get("port"),
            Some(&"3306".to_string())
        );
        assert_eq!(
            table_type_config.properties.get("database"),
            Some(&"testdb".to_string())
        );
    }

    #[test]
    fn test_convert_table_config_skips_null_connection_value() {
        let connection: serde_yaml::Value =
            serde_yaml::from_str("host: localhost\ndatabase:\n").unwrap();

        let table_config = TableConfig {
            store: StoreConfig {
                store_type: "mysql".to_string(),
                connection,
                pool: None,
                security: None,
            },
            schema: None,
            caching: None,
            indexing: None,
        };

        let table_type_config = EventFluxAppRuntime::convert_table_config(&table_config).unwrap();
        assert_eq!(
            table_type_config.properties.get("host"),
            Some(&"localhost".to_string())
        );
        assert!(!table_type_config.properties.contains_key("database"));
    }

    #[test]
    fn test_convert_table_config_with_pool() {
        use crate::core::config::types::application_config::ConnectionPoolConfig;
        use std::time::Duration;

        let pool_config = ConnectionPoolConfig {
            max_size: 20,
            min_size: 5,
            connection_timeout: Duration::from_secs(60),
            idle_timeout: None,
            max_lifetime: None,
        };

        let table_config = TableConfig {
            store: StoreConfig {
                store_type: "postgres".to_string(),
                connection: serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
                pool: Some(pool_config),
                security: None,
            },
            schema: None,
            caching: None,
            indexing: None,
        };

        let result = EventFluxAppRuntime::convert_table_config(&table_config);
        assert!(result.is_ok());

        let table_type_config = result.unwrap();
        assert_eq!(table_type_config.extension(), "postgres");
        assert_eq!(
            table_type_config.properties.get("postgres.pool.max_size"),
            Some(&"20".to_string())
        );
        assert_eq!(
            table_type_config.properties.get("postgres.pool.min_size"),
            Some(&"5".to_string())
        );
        assert_eq!(
            table_type_config
                .properties
                .get("postgres.pool.connection_timeout_ms"),
            Some(&"60000".to_string())
        );
    }
}
