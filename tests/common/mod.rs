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

pub mod pattern_chain_test_utils;

use eventflux::core::config::{ConfigManager, EventFluxConfig};
use eventflux::core::event::event::Event;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::eventflux_app_runtime::EventFluxAppRuntime;
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::persistence::PersistenceStore;
use eventflux::core::stream::input::table_input_handler::TableInputHandler;
use eventflux::core::stream::output::stream_callback::StreamCallback;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[allow(dead_code)]
#[derive(Debug)]
struct CollectCallback {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
}

#[allow(dead_code)]
impl StreamCallback for CollectCallback {
    fn receive_events(&self, events: &[Event]) {
        let mut vec = self.events.lock().unwrap();
        for e in events {
            vec.push(e.data.clone());
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct AppRunner {
    runtime: Arc<EventFluxAppRuntime>,
    pub collected: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
    _manager: EventFluxManager,
}

#[allow(dead_code)]
impl AppRunner {
    // ============================================================================
    // TIER 1: Basic constructors (no configuration)
    // ============================================================================

    /// Create a new AppRunner with a default EventFluxManager.
    ///
    /// Adds the collecting callback before starting the runtime, so events fired
    /// by start triggers are collected too.
    ///
    /// # Arguments
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let runner = AppRunner::new(
    ///     "CREATE STREAM In (x INT); SELECT x FROM In;",
    ///     "In"
    /// ).await;
    /// ```
    pub async fn new(app_string: &str, out_stream: &str) -> Self {
        let manager = EventFluxManager::new();
        let runtime = manager
            .create_eventflux_app_runtime_from_string(app_string)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        // Start only after the callback is added so start trigger events are collected
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create a new AppRunner from an EventFluxApp built in code instead of parsed
    /// from SQL.
    ///
    /// Adds the collecting callback before starting the runtime, so events fired
    /// by start triggers are collected too.
    ///
    /// # Arguments
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let app = EventFluxApp::new("MyApp");
    /// let runner = AppRunner::new_from_api(app, "Out").await;
    /// ```
    pub async fn new_from_api(
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new();
        let runtime = manager
            .create_eventflux_app_runtime_from_api(Arc::new(app), None)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        // Start only after the callback is added so start trigger events are collected
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // TIER 2: Constructors that take a config
    // ============================================================================

    /// Create AppRunner with an EventFluxConfig that is already loaded.
    ///
    /// Mirrors `EventFluxManager::new_with_config()`. Use this when the test
    /// builds the config object in code.
    ///
    /// # Arguments
    /// * `config` - Loaded EventFluxConfig
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let config = EventFluxConfig::default();
    /// let runner = AppRunner::new_with_config(
    ///     config,
    ///     "CREATE STREAM In (x INT); SELECT x FROM In;",
    ///     "In"
    /// ).await;
    /// ```
    ///
    /// # See Also
    /// * `EventFluxManager::new_with_config()` - Matching manager method
    /// * `new_with_config_manager()` - Loads config from files and the environment
    pub async fn new_with_config(
        config: EventFluxConfig,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config(config);
        Self::new_with_manager(manager, app_string, out_stream).await
    }

    /// Create AppRunner from an EventFluxApp built in code, with an EventFluxConfig
    /// that is already loaded.
    ///
    /// Same as `new_with_config()` but takes an EventFluxApp instead of SQL.
    ///
    /// # Arguments
    /// * `config` - Loaded EventFluxConfig
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let config = EventFluxConfig::default();
    /// let app = EventFluxApp::new("MyApp");
    /// let runner = AppRunner::new_from_api_with_config(config, app, "Out").await;
    /// ```
    pub async fn new_from_api_with_config(
        config: EventFluxConfig,
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config(config);
        Self::new_from_api_with_manager(manager, app, out_stream).await
    }

    // ============================================================================
    // TIER 3: Constructors that take a ConfigManager
    // ============================================================================

    /// Create AppRunner with a ConfigManager that loads the config.
    ///
    /// Mirrors `EventFluxManager::new_with_config_manager()`. The ConfigManager
    /// loads config from its registered loaders, such as YAML files and
    /// environment variables, and merges them by priority.
    ///
    /// # Arguments
    /// * `config_manager` - ConfigManager with its loaders already registered
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let config_manager = ConfigManager::new(); // Uses default loaders
    /// let runner = AppRunner::new_with_config_manager(
    ///     config_manager,
    ///     "CREATE STREAM In (x INT); SELECT x FROM In;",
    ///     "In"
    /// ).await;
    /// ```
    ///
    /// # See Also
    /// * `EventFluxManager::new_with_config_manager()` - Matching manager method
    /// * `new_from_yaml()` - Shortcut that loads config from one YAML file
    pub async fn new_with_config_manager(
        config_manager: ConfigManager,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config_manager(config_manager);
        Self::new_with_manager(manager, app_string, out_stream).await
    }

    /// Create AppRunner from an EventFluxApp built in code, with a ConfigManager
    /// that loads the config.
    ///
    /// Same as `new_with_config_manager()` but takes an EventFluxApp instead of SQL.
    ///
    /// # Arguments
    /// * `config_manager` - ConfigManager with its loaders already registered
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let config_manager = ConfigManager::new();
    /// let app = EventFluxApp::new("MyApp");
    /// let runner = AppRunner::new_from_api_with_config_manager(
    ///     config_manager,
    ///     app,
    ///     "Out"
    /// ).await;
    /// ```
    pub async fn new_from_api_with_config_manager(
        config_manager: ConfigManager,
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config_manager(config_manager);
        Self::new_from_api_with_manager(manager, app, out_stream).await
    }

    // ============================================================================
    // TIER 4: Shortcuts that load config from a file
    // ============================================================================

    /// Create AppRunner with config loaded from a YAML file.
    ///
    /// Shortcut for `ConfigManager::from_file()` plus `new_with_config_manager()`.
    /// Environment variables override values from the YAML file.
    ///
    /// Useful to test that a SQL WITH clause wins over the YAML file. When both
    /// set the same stream option, the SQL WITH value is used.
    ///
    /// # Arguments
    /// * `yaml_path` - Path to the YAML config file (accepts &str, String, Path, PathBuf)
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Returns
    /// Always `Ok(Self)` today. The file is not read until the runtime is built,
    /// so this function never returns `Err`.
    ///
    /// # Panics
    /// Panics if the runtime cannot be created, for example when the SQL does not
    /// parse or the callback cannot be added.
    ///
    /// # Example
    /// ```rust,ignore
    /// let runner = AppRunner::new_from_yaml(
    ///     "config/eventflux.yaml",
    ///     r#"
    ///         CREATE STREAM TimerInput (tick STRING) WITH (
    ///             type = 'source',
    ///             extension = 'timer',
    ///             "timer.interval" = '100'  -- SQL overrides YAML value
    ///         );
    ///         SELECT tick FROM TimerInput;
    ///     "#,
    ///     "TimerInput"
    /// ).await?;
    /// ```
    ///
    /// # See Also
    /// * `ConfigManager::from_file()` - The config loader this uses
    /// * `new_with_config_manager()` - For full control over which loaders run
    pub async fn new_from_yaml<P: AsRef<Path>>(
        yaml_path: P,
        app_string: &str,
        out_stream: &str,
    ) -> Result<Self, String> {
        let config_manager = ConfigManager::from_file(yaml_path.as_ref().to_path_buf());
        Ok(Self::new_with_config_manager(config_manager, app_string, out_stream).await)
    }

    /// Create AppRunner from an EventFluxApp built in code, with config loaded from
    /// a YAML file.
    ///
    /// Same as `new_from_yaml()` but takes an EventFluxApp instead of SQL.
    ///
    /// # Arguments
    /// * `yaml_path` - Path to the YAML config file (accepts &str, String, Path, PathBuf)
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Returns
    /// Always `Ok(Self)` today. Failures to build the runtime panic instead.
    ///
    /// # Example
    /// ```rust,ignore
    /// let app = EventFluxApp::new("MyApp");
    /// let runner = AppRunner::new_from_api_with_yaml(
    ///     "config/eventflux.yaml",
    ///     app,
    ///     "Out"
    /// ).await?;
    /// ```
    pub async fn new_from_api_with_yaml<P: AsRef<Path>>(
        yaml_path: P,
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
    ) -> Result<Self, String> {
        let config_manager = ConfigManager::from_file(yaml_path.as_ref().to_path_buf());
        Ok(Self::new_from_api_with_config_manager(config_manager, app, out_stream).await)
    }

    // ============================================================================
    // TIER 5: Constructors that take a persistence store
    // ============================================================================

    /// Create AppRunner with the given persistence store.
    ///
    /// The store holds state for `persist()` and `restore_revision()`. Examples
    /// are Redis, file and in memory stores.
    ///
    /// # Arguments
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    /// * `store` - Persistence store to use
    ///
    /// # Example
    /// ```rust,ignore
    /// let store = Arc::new(InMemoryPersistenceStore::new());
    /// let runner = AppRunner::new_with_store(
    ///     "CREATE STREAM In (x INT); SELECT x FROM In;",
    ///     "In",
    ///     store
    /// ).await;
    /// ```
    pub async fn new_with_store(
        app_string: &str,
        out_stream: &str,
        store: Arc<dyn PersistenceStore>,
    ) -> Self {
        let manager = EventFluxManager::new();
        manager.set_persistence_store(store).unwrap();
        let runtime = manager
            .create_eventflux_app_runtime_from_string(app_string)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create AppRunner from an EventFluxApp built in code, with the given
    /// persistence store.
    ///
    /// Same as `new_with_store()` but takes an EventFluxApp instead of SQL.
    ///
    /// # Arguments
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    /// * `store` - Persistence store to use
    ///
    /// # Example
    /// ```rust,ignore
    /// let app = EventFluxApp::new("MyApp");
    /// let store = Arc::new(InMemoryPersistenceStore::new());
    /// let runner = AppRunner::new_from_api_with_store(app, "Out", store).await;
    /// ```
    pub async fn new_from_api_with_store(
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
        store: Arc<dyn PersistenceStore>,
    ) -> Self {
        let manager = EventFluxManager::new();
        manager.set_persistence_store(store).unwrap();
        let runtime = manager
            .create_eventflux_app_runtime_from_api(Arc::new(app), None)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        // Start only after the callback is added so start trigger events are collected
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // TIER 6: Full control constructors (manager set up by the caller)
    // ============================================================================

    /// Create AppRunner with an EventFluxManager the caller has already set up.
    ///
    /// Gives the most control. Use it when you need to
    /// * register custom extension factories (sources, sinks, windows and so on)
    /// * set up config from several layers
    /// * do any other setup on the manager before the runtime is built
    ///
    /// The config based SQL constructors (`new_with_config()`,
    /// `new_with_config_manager()`, `new_from_yaml()`) delegate to this method.
    ///
    /// # Arguments
    /// * `manager` - EventFluxManager already set up with factories, config and so on
    /// * `app_string` - SQL application text
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let manager = EventFluxManager::new();
    /// manager.add_source_factory("timer".to_string(), Box::new(TimerSourceFactory));
    /// manager.add_sink_factory("log".to_string(), Box::new(LogSinkFactory));
    /// manager.set_persistence_store(store);
    ///
    /// let runner = AppRunner::new_with_manager(
    ///     manager,
    ///     "CREATE STREAM In (x INT); SELECT x FROM In;",
    ///     "In"
    /// ).await;
    /// ```
    ///
    /// # See Also
    /// * `new_with_config()` - For config without custom factories
    /// * `new_with_config_manager()` - Loads config from files and the environment
    pub async fn new_with_manager(
        manager: EventFluxManager,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let runtime = manager
            .create_eventflux_app_runtime_from_string(app_string)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create AppRunner WITHOUT starting the runtime.
    ///
    /// Needed for state recovery tests, which must
    /// 1. create the runtime (this builds all processors and windows)
    /// 2. add callbacks
    /// 3. restore state from a checkpoint
    /// 4. only then start the runtime
    ///
    /// If the runtime starts before the restore, the windows come up with empty
    /// buffers.
    ///
    /// **Important**: the caller MUST call `runtime().start()` when ready.
    pub async fn new_with_manager_no_start(
        manager: EventFluxManager,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let runtime = manager
            .create_eventflux_app_runtime_from_string(app_string)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        // Do NOT call runtime.start() here. The manager does not start the runtime
        // either, so it stays stopped until the caller starts it.
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create AppRunner from an EventFluxApp built in code, with an EventFluxManager
    /// the caller has already set up.
    ///
    /// Same as `new_with_manager()` but takes an EventFluxApp instead of SQL.
    ///
    /// # Arguments
    /// * `manager` - EventFluxManager already set up with factories, config and so on
    /// * `app` - EventFluxApp built in code
    /// * `out_stream` - Name of the stream to collect output events from
    ///
    /// # Example
    /// ```rust,ignore
    /// let manager = EventFluxManager::new();
    /// manager.add_source_factory("custom".to_string(), Box::new(CustomSourceFactory));
    ///
    /// let app = EventFluxApp::new("MyApp");
    /// let runner = AppRunner::new_from_api_with_manager(manager, app, "Out").await;
    /// ```
    pub async fn new_from_api_with_manager(
        manager: EventFluxManager,
        app: eventflux::query_api::eventflux_app::EventFluxApp,
        out_stream: &str,
    ) -> Self {
        let runtime = manager
            .create_eventflux_app_runtime_from_api(Arc::new(app), None)
            .await
            .expect("runtime");
        let collected = Arc::new(Mutex::new(Vec::new()));
        runtime
            .add_callback(
                out_stream,
                Box::new(CollectCallback {
                    events: Arc::clone(&collected),
                }),
            )
            .expect("add cb");
        // Start only after the callback is added so start trigger events are collected
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // Event sending methods
    // ============================================================================

    /// Send one event to a stream.
    ///
    /// The event has timestamp 0. Use `send_with_ts()` when the test depends on
    /// event time. Does nothing if the stream has no input handler.
    ///
    /// # Panics
    /// Panics if the input handler fails to send the event.
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `data` - Attribute values of the event
    pub fn send(&self, stream_id: &str, data: Vec<AttributeValue>) {
        if let Some(handler) = self.runtime.get_input_handler(stream_id) {
            handler
                .lock()
                .unwrap()
                .send_event_with_timestamp(0, data)
                .unwrap();
        }
    }

    /// Send one event with the given timestamp.
    ///
    /// Does nothing if the stream has no input handler.
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `ts` - Event timestamp in milliseconds since epoch
    /// * `data` - Attribute values of the event
    pub fn send_with_ts(&self, stream_id: &str, ts: i64, data: Vec<AttributeValue>) {
        if let Some(handler) = self.runtime.get_input_handler(stream_id) {
            handler
                .lock()
                .unwrap()
                .send_event_with_timestamp(ts, data)
                .unwrap();
        }
    }

    /// Send several events to a stream in one call.
    ///
    /// All events have timestamp 0. Does nothing if the stream has no input
    /// handler.
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `batch` - Events to send, each one a list of attribute values
    pub fn send_batch(&self, stream_id: &str, batch: Vec<Vec<AttributeValue>>) {
        if let Some(handler) = self.runtime.get_input_handler(stream_id) {
            let events: Vec<Event> = batch
                .into_iter()
                .map(|d| Event::new_with_data(0, d))
                .collect();
            handler
                .lock()
                .unwrap()
                .send_multiple_events(events)
                .unwrap();
        }
    }

    // ============================================================================
    // Lifecycle and state methods
    // ============================================================================

    /// Shut down the runtime and return all collected events.
    ///
    /// Consumes the AppRunner. Tests usually call this last to get their results.
    ///
    /// # Returns
    /// Data of every event the output stream callback has received
    pub fn shutdown(self) -> Vec<Vec<AttributeValue>> {
        self.runtime.shutdown();
        self.collected.lock().unwrap().clone()
    }

    /// Save the current runtime state and return its revision id.
    ///
    /// Needs a persistence store on the manager. Panics if the save fails.
    ///
    /// # Returns
    /// Revision id to pass to `restore_revision()`
    pub fn persist(&self) -> String {
        self.runtime.persist().expect("persist failed").revision
    }

    /// Restore runtime state from a saved revision. Panics if the restore fails.
    ///
    /// # Arguments
    /// * `rev` - Revision id from an earlier `persist()` call
    pub fn restore_revision(&self, rev: &str) {
        self.runtime.restore_revision(rev).expect("restore")
    }

    /// Take a snapshot of the current runtime state. Panics if it fails.
    ///
    /// # Returns
    /// The snapshot as bytes
    pub fn snapshot(&self) -> Vec<u8> {
        self.runtime.snapshot().expect("snapshot")
    }

    /// Restore runtime state from a snapshot. Panics if the restore fails.
    ///
    /// # Arguments
    /// * `snap` - Snapshot bytes from an earlier `snapshot()` call
    pub fn restore(&self, snap: &[u8]) {
        self.runtime.restore(snap).expect("restore")
    }

    // ============================================================================
    // Runtime access methods
    // ============================================================================

    /// Return the EventFluxAppRuntime behind this runner, for calls AppRunner
    /// does not wrap.
    pub fn runtime(&self) -> Arc<EventFluxAppRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Get the input handler for a table.
    ///
    /// # Arguments
    /// * `table_id` - Name of the table
    ///
    /// # Returns
    /// The table input handler, or None if no such table exists
    pub fn get_table_input_handler(&self, table_id: &str) -> Option<TableInputHandler> {
        self.runtime.get_table_input_handler(table_id)
    }

    /// Read rows from an aggregation, optionally limited by time.
    ///
    /// # Arguments
    /// * `agg_id` - Aggregation id
    /// * `within` - Optional time range to read from
    /// * `per` - Optional bucket duration to group rows by
    ///
    /// # Returns
    /// The aggregated rows
    pub fn get_aggregation_data(
        &self,
        agg_id: &str,
        within: Option<eventflux::query_api::aggregation::Within>,
        per: Option<eventflux::query_api::aggregation::time_period::Duration>,
    ) -> Vec<Vec<AttributeValue>> {
        self.runtime.query_aggregation(agg_id, within, per)
    }
}

// ============================================================================
// Connector test helpers
// ============================================================================

/// Source callback that records every payload it receives and lets a test wait
/// for them up to a deadline. Shared by the connector integration tests.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CollectingCallback {
    payloads: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

#[allow(dead_code)]
impl CollectingCallback {
    pub fn new() -> Self {
        Self {
            payloads: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Wait until at least `count` payloads have arrived or `timeout` has passed.
    /// Checks every 20 ms. Returns whatever was collected by then.
    pub fn wait_for(&self, count: usize, timeout: std::time::Duration) -> Vec<Vec<u8>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let payloads = self.payloads.lock().unwrap();
            if payloads.len() >= count || std::time::Instant::now() >= deadline {
                return payloads.clone();
            }
            drop(payloads);
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl eventflux::core::stream::input::source::SourceCallback for CollectingCallback {
    fn on_data(&self, data: &[u8]) -> Result<(), eventflux::core::exception::EventFluxError> {
        self.payloads.lock().unwrap().push(data.to_vec());
        Ok(())
    }
}

/// Return a localhost port that is free right now. Another process may take it
/// before the test binds it, which is fine for tests.
#[allow(dead_code)]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Build a manager whose config holds one app named `app_name`.
///
/// `app_config_yaml` is a whole application config document, such as
/// `"streams:\n  Out:\n    sink: ..."`, not just its `streams` section.
/// The app name is also set as `eventflux.application.name`, so a runtime created
/// from any SQL gets this config. Panics if the YAML is not a valid app config.
#[allow(dead_code)]
pub fn manager_with_app_config(app_name: &str, app_config_yaml: &str) -> EventFluxManager {
    use eventflux::core::config::types::global_config::ApplicationGlobalConfig;
    use eventflux::core::config::ApplicationConfig;

    let app_config: ApplicationConfig =
        serde_yaml::from_str(app_config_yaml).expect("valid application config");
    let mut config = EventFluxConfig::default();
    config.eventflux.application = Some(ApplicationGlobalConfig {
        name: Some(app_name.to_string()),
        ..Default::default()
    });
    config.applications.insert(app_name.to_string(), app_config);
    EventFluxManager::new_with_config(config)
}

/// Columns every DLQ stream must declare, in this exact order (see
/// `eventflux::core::error::DLQ_FIELDS`).
#[allow(dead_code)]
pub const DLQ_COLUMNS: &str = "originalEvent STRING, errorMessage STRING, errorType STRING, \
                               timestamp LONG, attemptCount INT, streamName STRING";

/// Block until something accepts connections on `port` at 127.0.0.1. Panics after
/// 10 seconds. Use this instead of a fixed warm up sleep.
#[allow(dead_code)]
pub fn wait_listening(port: u16) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("nothing listening on 127.0.0.1:{port} within 10s");
}
