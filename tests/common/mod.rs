// SPDX-License-Identifier: MIT OR Apache-2.0

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

#[derive(Debug)]
struct CollectCallback {
    events: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
}

impl StreamCallback for CollectCallback {
    fn receive_events(&self, events: &[Event]) {
        let mut vec = self.events.lock().unwrap();
        for e in events {
            vec.push(e.data.clone());
        }
    }
}

#[derive(Debug)]
pub struct AppRunner {
    runtime: Arc<EventFluxAppRuntime>,
    pub collected: Arc<Mutex<Vec<Vec<AttributeValue>>>>,
    _manager: EventFluxManager,
}

impl AppRunner {
    // ============================================================================
    // TIER 1: Basic Constructors (No Configuration)
    // ============================================================================

    /// Create a new AppRunner with default configuration.
    ///
    /// This is the simplest constructor for testing. It creates a default
    /// EventFluxManager with no custom configuration.
    ///
    /// Creates the runtime, adds callbacks, then starts it. This ensures
    /// start-trigger events can be observed.
    ///
    /// # Arguments
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
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
        // Start after adding callbacks to observe start-trigger events
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create a new AppRunner from an EventFluxApp API object.
    ///
    /// Use this when you've already constructed the EventFluxApp programmatically
    /// instead of parsing from SQL.
    ///
    /// Creates the runtime, adds callbacks, then starts it. This ensures
    /// start-trigger events can be observed.
    ///
    /// # Arguments
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
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
        // Start after adding callbacks to observe start-trigger events
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // TIER 2: Configuration-Aware Constructors
    // ============================================================================

    /// Create AppRunner with a pre-loaded EventFluxConfig.
    ///
    /// Mirrors EventFluxManager::new_with_config(). Use this when you have
    /// a programmatically constructed configuration object.
    ///
    /// # Arguments
    /// * `config` - Pre-loaded EventFluxConfig
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
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
    /// * `EventFluxManager::new_with_config()` - Parallel manager method
    /// * `new_with_config_manager()` - For dynamic config loading
    pub async fn new_with_config(
        config: EventFluxConfig,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config(config);
        Self::new_with_manager(manager, app_string, out_stream).await
    }

    /// Create AppRunner from API with a pre-loaded EventFluxConfig.
    ///
    /// Mirrors EventFluxManager::new_with_config(). API variant of
    /// `new_with_config()` for pre-constructed EventFluxApp objects.
    ///
    /// # Arguments
    /// * `config` - Pre-loaded EventFluxConfig
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
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
    // TIER 3: ConfigManager-Based Constructors
    // ============================================================================

    /// Create AppRunner with a ConfigManager for dynamic config loading.
    ///
    /// Mirrors EventFluxManager::new_with_config_manager(). The ConfigManager
    /// handles loading from YAML files, environment variables, Kubernetes
    /// ConfigMaps, etc., with proper precedence and merging.
    ///
    /// # Arguments
    /// * `config_manager` - ConfigManager instance (pre-configured with loaders)
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
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
    /// * `EventFluxManager::new_with_config_manager()` - Parallel manager method
    /// * `new_from_yaml()` - Convenience method for loading from YAML files
    pub async fn new_with_config_manager(
        config_manager: ConfigManager,
        app_string: &str,
        out_stream: &str,
    ) -> Self {
        let manager = EventFluxManager::new_with_config_manager(config_manager);
        Self::new_with_manager(manager, app_string, out_stream).await
    }

    /// Create AppRunner from API with a ConfigManager for dynamic config loading.
    ///
    /// API variant of `new_with_config_manager()` for pre-constructed
    /// EventFluxApp objects.
    ///
    /// # Arguments
    /// * `config_manager` - ConfigManager instance (pre-configured with loaders)
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
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
    // TIER 4: File-Based Configuration Loaders (Convenience Methods)
    // ============================================================================

    /// Create AppRunner by loading configuration from a YAML file.
    ///
    /// This is a convenience method that combines ConfigManager::from_file() with
    /// AppRunner creation. The YAML configuration is loaded and merged with any
    /// environment variable overrides before creating the runtime.
    ///
    /// **Key Feature**: Tests SQL WITH clause priority over YAML configuration.
    /// When both YAML and SQL WITH define the same stream configuration,
    /// SQL WITH takes precedence.
    ///
    /// # Arguments
    /// * `yaml_path` - Path to YAML configuration file (accepts &str, String, Path, PathBuf)
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
    ///
    /// # Returns
    /// * `Ok(Self)` - AppRunner with loaded configuration
    /// * `Err(String)` - Configuration loading or parsing errors
    ///
    /// # Errors
    /// * File not found
    /// * YAML parsing errors
    /// * Configuration validation errors
    /// * Runtime creation errors
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
    /// * `ConfigManager::from_file()` - Underlying config loader
    /// * `new_with_config_manager()` - More control over config loading
    pub async fn new_from_yaml<P: AsRef<Path>>(
        yaml_path: P,
        app_string: &str,
        out_stream: &str,
    ) -> Result<Self, String> {
        let config_manager = ConfigManager::from_file(yaml_path.as_ref().to_path_buf());
        Ok(Self::new_with_config_manager(config_manager, app_string, out_stream).await)
    }

    /// Create AppRunner from API by loading configuration from a YAML file.
    ///
    /// API variant of `new_from_yaml()` for pre-constructed EventFluxApp objects.
    ///
    /// # Arguments
    /// * `yaml_path` - Path to YAML configuration file (accepts &str, String, Path, PathBuf)
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
    ///
    /// # Returns
    /// * `Ok(Self)` - AppRunner with loaded configuration
    /// * `Err(String)` - Configuration loading or parsing errors
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
    // TIER 5: Persistence Store Constructors
    // ============================================================================

    /// Create AppRunner with a specific persistence store.
    ///
    /// The persistence store is used for state snapshots, checkpointing,
    /// and state recovery. Common implementations include Redis, file-based
    /// stores, or in-memory stores.
    ///
    /// # Arguments
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
    /// * `store` - Persistence store implementation
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

    /// Create AppRunner from API with a specific persistence store.
    ///
    /// API variant of `new_with_store()` for pre-constructed EventFluxApp objects.
    ///
    /// # Arguments
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
    /// * `store` - Persistence store implementation
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
        // Start after adding callbacks to observe start-trigger events
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // TIER 6: Full Control Constructors (Pre-configured Manager)
    // ============================================================================

    /// Create AppRunner with a fully pre-configured EventFluxManager.
    ///
    /// This provides maximum control and flexibility. Use this when you need to:
    /// * Register custom extension factories (sources, sinks, windows, etc.)
    /// * Configure complex multi-layer configurations
    /// * Share a manager across multiple runtimes
    /// * Perform advanced setup before runtime creation
    ///
    /// All other constructors ultimately delegate to this method.
    ///
    /// # Arguments
    /// * `manager` - Pre-configured EventFluxManager (with factories, config, etc.)
    /// * `app_string` - SQL application definition string
    /// * `out_stream` - Output stream name to collect events from
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
    /// * `new_with_config()` - For configuration without factories
    /// * `new_with_config_manager()` - For dynamic config loading
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

    /// Create AppRunner WITHOUT auto-starting the runtime.
    ///
    /// This is critical for state recovery scenarios where you need to:
    /// 1. Create the runtime (which creates all processors/windows)
    /// 2. Add callbacks
    /// 3. Restore state from checkpoint
    /// 4. THEN start the runtime (caller must call runtime.start())
    ///
    /// If you start before restoring, the windows will have empty buffers!
    ///
    /// **Important**: Caller MUST call `runtime.start()` when ready.
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
        // NOTE: Explicitly do NOT call runtime.start() here!
        // The manager method no longer auto-starts, so the runtime is NOT started.
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    /// Create AppRunner from API with a fully pre-configured EventFluxManager.
    ///
    /// API variant of `new_with_manager()` for pre-constructed EventFluxApp objects.
    /// Provides maximum control over the runtime environment.
    ///
    /// # Arguments
    /// * `manager` - Pre-configured EventFluxManager (with factories, config, etc.)
    /// * `app` - Pre-constructed EventFluxApp API object
    /// * `out_stream` - Output stream name to collect events from
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
        // Start after adding callbacks to observe start-trigger events
        runtime.start().expect("Failed to start runtime");
        Self {
            runtime,
            collected,
            _manager: manager,
        }
    }

    // ============================================================================
    // Event Sending Methods
    // ============================================================================

    /// Send a single event to a stream.
    ///
    /// Events are sent with timestamp 0 (current time will be assigned by runtime).
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `data` - Event data as vector of attribute values
    pub fn send(&self, stream_id: &str, data: Vec<AttributeValue>) {
        if let Some(handler) = self.runtime.get_input_handler(stream_id) {
            handler
                .lock()
                .unwrap()
                .send_event_with_timestamp(0, data)
                .unwrap();
        }
    }

    /// Send an event with an explicit timestamp.
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `ts` - Event timestamp (milliseconds since epoch)
    /// * `data` - Event data as vector of attribute values
    pub fn send_with_ts(&self, stream_id: &str, ts: i64, data: Vec<AttributeValue>) {
        if let Some(handler) = self.runtime.get_input_handler(stream_id) {
            handler
                .lock()
                .unwrap()
                .send_event_with_timestamp(ts, data)
                .unwrap();
        }
    }

    /// Send a batch of events to a stream.
    ///
    /// More efficient than sending events individually. All events are
    /// sent with timestamp 0.
    ///
    /// # Arguments
    /// * `stream_id` - Name of the input stream
    /// * `batch` - Vector of events (each event is a vector of attribute values)
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
    // Lifecycle & State Management Methods
    // ============================================================================

    /// Shutdown the runtime and return all collected events.
    ///
    /// Consumes the AppRunner. This is typically called at the end of a test
    /// to retrieve results.
    ///
    /// # Returns
    /// All events collected from the output stream since runtime start
    pub fn shutdown(self) -> Vec<Vec<AttributeValue>> {
        self.runtime.shutdown();
        self.collected.lock().unwrap().clone()
    }

    /// Persist current runtime state and return revision ID.
    ///
    /// Requires a persistence store to be configured.
    ///
    /// # Returns
    /// Revision ID that can be used with `restore_revision()`
    pub fn persist(&self) -> String {
        self.runtime.persist().expect("persist failed").revision
    }

    /// Restore runtime state from a specific revision.
    ///
    /// # Arguments
    /// * `rev` - Revision ID from a previous `persist()` call
    pub fn restore_revision(&self, rev: &str) {
        self.runtime.restore_revision(rev).expect("restore")
    }

    /// Create a snapshot of current runtime state.
    ///
    /// # Returns
    /// Serialized snapshot as byte array
    pub fn snapshot(&self) -> Vec<u8> {
        self.runtime.snapshot().expect("snapshot")
    }

    /// Restore runtime state from a snapshot.
    ///
    /// # Arguments
    /// * `snap` - Snapshot byte array from previous `snapshot()` call
    pub fn restore(&self, snap: &[u8]) {
        self.runtime.restore(snap).expect("restore")
    }

    // ============================================================================
    // Runtime Access Methods
    // ============================================================================

    /// Get direct access to the underlying EventFluxAppRuntime.
    ///
    /// Use this for advanced operations not exposed through AppRunner.
    ///
    /// # Returns
    /// Arc-wrapped runtime instance
    pub fn runtime(&self) -> Arc<EventFluxAppRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Obtain an input handler for a specific table.
    ///
    /// # Arguments
    /// * `table_id` - Name of the table
    ///
    /// # Returns
    /// Table input handler if table exists, None otherwise
    pub fn get_table_input_handler(&self, table_id: &str) -> Option<TableInputHandler> {
        self.runtime.get_table_input_handler(table_id)
    }

    /// Retrieve aggregated rows using optional time constraints.
    ///
    /// # Arguments
    /// * `agg_id` - Aggregation identifier
    /// * `within` - Optional time range constraint
    /// * `per` - Optional time bucket duration
    ///
    /// # Returns
    /// Vector of aggregated result rows
    pub fn get_aggregation_data(
        &self,
        agg_id: &str,
        within: Option<eventflux::query_api::aggregation::Within>,
        per: Option<eventflux::query_api::aggregation::time_period::Duration>,
    ) -> Vec<Vec<AttributeValue>> {
        self.runtime.query_aggregation(agg_id, within, per)
    }
}
