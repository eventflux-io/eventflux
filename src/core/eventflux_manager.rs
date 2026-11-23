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

use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::eventflux_app_runtime::EventFluxAppRuntime;
use crate::query_api::eventflux_app::EventFluxApp as ApiEventFluxApp;
// Not needed here. EventFluxAppRuntime builds its own EventFluxAppContext.
// use crate::core::config::eventflux_app_context::EventFluxAppContext;

use crate::core::config::eventflux_context::ExtensionClassPlaceholder;
use crate::core::config::{ApplicationConfig, ConfigManager, EventFluxConfig};
use crate::core::executor::ScalarFunctionExecutor; // for UDFs
use crate::core::extension;
use crate::core::persistence::PersistenceStore;
use crate::core::DataSource;

use libloading::Library;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use rand::Rng; // For generating random app names if not specified

/// Main entry point. Creates, tracks and shuts down EventFlux app runtimes, keyed
/// by app name.
#[repr(C)]
pub struct EventFluxManager {
    eventflux_context: Arc<EventFluxContext>,
    eventflux_app_runtime_map: Arc<Mutex<HashMap<String, Arc<EventFluxAppRuntime>>>>,
    loaded_libraries: Arc<Mutex<Vec<std::mem::ManuallyDrop<libloading::Library>>>>,
    config_manager: Option<Arc<ConfigManager>>,
    cached_config: Arc<Mutex<Option<EventFluxConfig>>>,
}

impl EventFluxManager {
    pub fn new() -> Self {
        Self {
            eventflux_context: Arc::new(EventFluxContext::new()), // default context
            eventflux_app_runtime_map: Arc::new(Mutex::new(HashMap::new())),
            loaded_libraries: Arc::new(Mutex::new(Vec::new())),
            config_manager: None,
            cached_config: Arc::new(Mutex::new(None)),
        }
    }

    /// Builds a manager that uses `config` as its cached configuration.
    ///
    /// # Panics
    ///
    /// Panics if the config cache mutex is poisoned.
    pub fn new_with_config(config: EventFluxConfig) -> Self {
        let manager = Self::new();
        *manager.cached_config.lock().expect("Mutex poisoned") = Some(config);
        manager
    }

    /// Builds a manager that loads its configuration through `config_manager` on first use.
    pub fn new_with_config_manager(config_manager: ConfigManager) -> Self {
        Self {
            eventflux_context: Arc::new(EventFluxContext::new()),
            eventflux_app_runtime_map: Arc::new(Mutex::new(HashMap::new())),
            loaded_libraries: Arc::new(Mutex::new(Vec::new())),
            config_manager: Some(Arc::new(config_manager)),
            cached_config: Arc::new(Mutex::new(None)),
        }
    }

    pub fn eventflux_context(&self) -> Arc<EventFluxContext> {
        Arc::clone(&self.eventflux_context)
    }

    pub async fn create_eventflux_app_runtime_from_string(
        &self,
        eventflux_app_string: &str,
    ) -> Result<Arc<EventFluxAppRuntime>, String> {
        // 1. Parse the SQL string into a SqlApplication.
        let sql_app = crate::sql_compiler::parse_sql_application(eventflux_app_string)
            .map_err(|e| format!("SQL parse error: {}", e))?;

        // 2. Pick the app name.
        // Priority: YAML config, then "App_" plus the first stream name, then "EventFluxApp".
        let app_name = if let Some(config_name) = self.get_config_app_name().await {
            config_name
        } else {
            sql_app
                .catalog
                .get_stream_names()
                .first()
                .map(|s| format!("App_{}", s))
                .unwrap_or_else(|| "EventFluxApp".to_string())
        };

        // 3. Convert to an EventFluxApp.
        let api_eventflux_app = sql_app
            .to_eventflux_app(app_name)
            .map_err(|e| format!("Type inference error: {}", e))?;
        let api_eventflux_app_arc = Arc::new(api_eventflux_app);

        // 4. Build the runtime from it.
        self.create_eventflux_app_runtime_from_api(
            api_eventflux_app_arc,
            Some(eventflux_app_string.to_string()),
        )
        .await
    }

    // eventflux_app_str_opt is the source SQL when the app came from a string, and
    // None when an ApiEventFluxApp was passed in directly.
    /// Creates and registers an EventFluxAppRuntime from an API EventFluxApp.
    ///
    /// The runtime is NOT started. That lets callers first:
    /// 1. Add callbacks (including for start triggers)
    /// 2. Restore state from checkpoints
    /// 3. Do other setup
    /// 4. Then call `runtime.start()` when ready
    ///
    /// **Important**: Callers MUST call `runtime.start()` to begin processing events.
    ///
    /// The map key is the YAML `eventflux.application.name` if set, else the app's
    /// own name, else a generated UUID name. Errors if loading config fails, an app
    /// with that name already exists, or building the runtime fails.
    pub async fn create_eventflux_app_runtime_from_api(
        &self,
        api_eventflux_app: Arc<ApiEventFluxApp>,
        eventflux_app_str_opt: Option<String>,
    ) -> Result<Arc<EventFluxAppRuntime>, String> {
        // Load the config once for all the reads below, since get_config() deep
        // clones the cached EventFluxConfig on every call. A load failure fails app
        // creation, and get_config's error already explains itself. Starting a
        // misconfigured engine on silent defaults is worse than a startup error.
        let loaded_config = self.get_config().await?;
        let application_global = loaded_config.eventflux.application.as_ref();

        // Pick the app name from config, not from annotations.
        // Priority: YAML eventflux.application.name, then api_eventflux_app.name,
        // then a generated UUID name.
        let app_name =
            if let Some(config_name) = application_global.and_then(|app| app.name.clone()) {
                config_name
            } else if !api_eventflux_app.name.is_empty() {
                api_eventflux_app.name.clone()
            } else {
                format!("eventflux_app_{}", uuid::Uuid::new_v4().hyphenated())
            };

        if self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .contains_key(&app_name)
        {
            return Err(format!(
                "EventFluxApp with name '{app_name}' already exists."
            ));
        }

        // Set the global stream default (eventflux.application.async_default) on the
        // shared context before parsing, so junction creation sees it. Always set,
        // with a missing key meaning false, so one config's value cannot leak into
        // the next app created on this manager after the config changes.
        let async_default = application_global
            .and_then(|app| app.async_default)
            .unwrap_or(false);
        self.eventflux_context.set_default_async_mode(async_default);

        // Per app config from the config already loaded. Calling
        // get_application_config would load and clone it all again.
        let app_config = loaded_config.applications.get(&app_name).cloned();

        // The runtime builds its own EventFluxAppContext and parses the app.
        let runtime = Arc::new(EventFluxAppRuntime::new_with_config(
            Arc::clone(&api_eventflux_app),
            Arc::clone(&self.eventflux_context),
            eventflux_app_str_opt.clone(),
            app_config,
        )?);

        // Register it. Starting it is up to the caller.
        self.eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .insert(app_name.clone(), Arc::clone(&runtime));

        Ok(runtime)
    }

    /// Deprecated: Use `create_eventflux_app_runtime_from_api()` instead.
    ///
    /// Now the same as `create_eventflux_app_runtime_from_api()`, since that method
    /// no longer starts the runtime either. Kept so old callers still compile.
    #[deprecated(
        since = "0.1.0",
        note = "Use create_eventflux_app_runtime_from_api() instead - it no longer auto-starts"
    )]
    pub async fn create_eventflux_app_runtime_without_autostart(
        &self,
        api_eventflux_app: Arc<ApiEventFluxApp>,
        eventflux_app_str_opt: Option<String>,
    ) -> Result<Arc<EventFluxAppRuntime>, String> {
        self.create_eventflux_app_runtime_from_api(api_eventflux_app, eventflux_app_str_opt)
            .await
    }

    pub fn get_eventflux_app_runtime(&self, app_name: &str) -> Option<Arc<EventFluxAppRuntime>> {
        self.eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .get(app_name)
            .map(Arc::clone)
    }

    pub fn get_eventflux_app_runtimes_keys(&self) -> Vec<String> {
        self.eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .keys()
            .cloned()
            .collect()
    }

    // Removes the app and shuts it down. Returns false if no app has that name.
    pub fn shutdown_app_runtime(&self, app_name: &str) -> bool {
        // Remove it into a local first. shutdown() can block on the junction drain
        // up to its timeout, and an `if let` on the locked map would hold the lock
        // through that wait, blocking every other manager call.
        let runtime = self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .remove(app_name);
        match runtime {
            Some(runtime) => {
                runtime.shutdown();
                true
            }
            None => false,
        }
    }

    pub fn shutdown_all_app_runtimes(&self) {
        // Take the runtimes out under the lock, then shut them down without it,
        // since shutdown() can block on the junction drain.
        let runtimes: Vec<_> = self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .drain()
            .map(|(_, runtime)| runtime)
            .collect();
        for runtime in runtimes {
            runtime.shutdown();
        }
    }

    // --- Register extensions and factories on the EventFluxContext ---
    // Loads a shared library from `library_path` and calls each register function it
    // exports (extensions, windows, functions, sources, sinks, stores and mappers).
    // `name` is only used in the log line. The library is never unloaded, so the
    // registered code stays valid. Errors if the library cannot be loaded. To add a
    // single scalar function, use `add_scalar_function_factory`.
    pub fn set_extension(
        &self,
        name: &str,
        library_path: ExtensionClassPlaceholder,
    ) -> Result<(), String> {
        unsafe {
            let lib = Library::new(&library_path).map_err(|e| e.to_string())?;

            macro_rules! call_if_exists {
                ($sym:expr) => {
                    if let Ok(f) = lib.get::<extension::RegisterFn>($sym) {
                        f(self);
                    }
                };
            }

            call_if_exists!(extension::REGISTER_EXTENSION_FN);
            call_if_exists!(extension::REGISTER_WINDOWS_FN);
            call_if_exists!(extension::REGISTER_FUNCTIONS_FN);
            call_if_exists!(extension::REGISTER_SOURCES_FN);
            call_if_exists!(extension::REGISTER_SINKS_FN);
            call_if_exists!(extension::REGISTER_STORES_FN);
            call_if_exists!(extension::REGISTER_SOURCE_MAPPERS_FN);
            call_if_exists!(extension::REGISTER_SINK_MAPPERS_FN);

            self.loaded_libraries
                .lock()
                .unwrap()
                .push(std::mem::ManuallyDrop::new(lib));
        }

        println!("[EventFluxManager] dynamically loaded extension '{name}' from {library_path}");
        Ok(())
    }

    pub fn add_scalar_function_factory(
        &self,
        name: String,
        function_factory: Box<dyn ScalarFunctionExecutor>,
    ) {
        self.eventflux_context
            .add_scalar_function_factory(name, function_factory);
    }

    pub fn add_window_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::WindowProcessorFactory>,
    ) {
        self.eventflux_context.add_window_factory(name, factory);
    }

    pub fn add_attribute_aggregator_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::AttributeAggregatorFactory>,
    ) {
        self.eventflux_context
            .add_attribute_aggregator_factory(name, factory);
    }

    pub fn add_source_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::SourceFactory>,
    ) {
        self.eventflux_context.add_source_factory(name, factory);
    }

    pub fn add_sink_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::SinkFactory>,
    ) {
        self.eventflux_context.add_sink_factory(name, factory);
    }

    pub fn add_store_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::StoreFactory>,
    ) {
        self.eventflux_context.add_store_factory(name, factory);
    }

    pub fn add_source_mapper_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::SourceMapperFactory>,
    ) {
        self.eventflux_context
            .add_source_mapper_factory(name, factory);
    }

    pub fn add_sink_mapper_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::SinkMapperFactory>,
    ) {
        self.eventflux_context
            .add_sink_mapper_factory(name, factory);
    }

    pub fn add_table_factory(
        &self,
        name: String,
        factory: Box<dyn crate::core::extension::TableFactory>,
    ) {
        self.eventflux_context.add_table_factory(name, factory);
    }

    pub fn add_data_source(
        &self,
        name: String,
        data_source: Arc<dyn DataSource>,
    ) -> Result<(), String> {
        self.eventflux_context.add_data_source(name, data_source)
    }

    // set_data_source was an early placeholder. add_data_source replaced it.
    // pub fn set_data_source(&self, name: &str, ds_placeholder: DataSourcePlaceholder) -> Result<(), String> { ... }

    pub fn set_persistence_store(
        &self,
        store: Arc<dyn PersistenceStore>,
    ) -> Result<(), crate::core::exception::error::EventFluxError> {
        self.eventflux_context.set_persistence_store(store)
    }

    /// Sets the ConfigManager used to load configuration.
    ///
    /// Clears the cached config so the next read loads it again. Errors if the
    /// cache mutex is poisoned.
    pub fn set_config_manager(&mut self, config_manager: ConfigManager) -> Result<(), String> {
        self.config_manager = Some(Arc::new(config_manager));
        *self
            .cached_config
            .lock()
            .map_err(|e| format!("Mutex poisoned: {}", e))? = None;
        Ok(())
    }

    /// Returns a clone of the current configuration, loading and caching it on first use.
    ///
    /// Loads through the ConfigManager when one is set, otherwise caches and returns
    /// `EventFluxConfig::default()`. Errors if loading fails or the cache mutex is
    /// poisoned.
    pub async fn get_config(&self) -> Result<EventFluxConfig, String> {
        {
            let cached = self
                .cached_config
                .lock()
                .map_err(|e| format!("Mutex poisoned: {}", e))?;
            if let Some(ref config) = *cached {
                return Ok(config.clone());
            }
        }

        if let Some(ref config_manager) = self.config_manager {
            let config = config_manager
                .load_unified_config()
                .await
                .map_err(|e| format!("Failed to load configuration: {}", e))?;

            *self
                .cached_config
                .lock()
                .map_err(|e| format!("Mutex poisoned: {}", e))? = Some(config.clone());

            Ok(config)
        } else {
            let default_config = EventFluxConfig::default();
            *self
                .cached_config
                .lock()
                .map_err(|e| format!("Mutex poisoned: {}", e))? = Some(default_config.clone());
            Ok(default_config)
        }
    }

    /// Returns the config for one app by name, or None if the config has no entry for it.
    pub async fn get_application_config(
        &self,
        app_name: &str,
    ) -> Result<Option<ApplicationConfig>, String> {
        let config = self.get_config().await?;
        Ok(config.applications.get(app_name).cloned())
    }

    /// Returns `eventflux.application.name` from the YAML config.
    ///
    /// Returns None if it is not set or the config fails to load.
    async fn get_config_app_name(&self) -> Option<String> {
        if let Ok(config) = self.get_config().await {
            config
                .eventflux
                .application
                .as_ref()
                .and_then(|app| app.name.clone())
        } else {
            None
        }
    }

    // --- Validation ---
    // Parses the SQL and checks it through validate_eventflux_app_api. The app is
    // named "App_" plus the first stream name, or "EventFluxApp".
    pub fn validate_eventflux_app_string(&self, eventflux_app_string: &str) -> Result<(), String> {
        let sql_app = crate::sql_compiler::parse_sql_application(eventflux_app_string)
            .map_err(|e| format!("SQL parse error: {}", e))?;

        let app_name = sql_app
            .catalog
            .get_stream_names()
            .first()
            .map(|s| format!("App_{}", s))
            .unwrap_or_else(|| "EventFluxApp".to_string());
        let api_eventflux_app = sql_app
            .to_eventflux_app(app_name)
            .map_err(|e| format!("Type inference error: {}", e))?;

        self.validate_eventflux_app_api(
            &Arc::new(api_eventflux_app),
            Some(eventflux_app_string.to_string()),
        )
    }

    // Validates an app by building a temporary runtime, starting it and shutting it
    // down. The runtime is not added to the manager's map. Starting it is real, so
    // sources and sinks attached from SQL WITH or config will connect.
    pub fn validate_eventflux_app_api(
        &self,
        api_eventflux_app: &Arc<ApiEventFluxApp>,
        eventflux_app_str_opt: Option<String>,
    ) -> Result<(), String> {
        let temp_runtime = EventFluxAppRuntime::new(
            Arc::clone(api_eventflux_app),
            Arc::clone(&self.eventflux_context),
            eventflux_app_str_opt,
        )?;

        let result = temp_runtime.start().map_err(|e| e.to_string());

        // Always shut down, even if start() failed, since some parts may be running.
        temp_runtime.shutdown();

        result
    }

    // --- Persistence ---
    /// Persists the state of one app.
    ///
    /// # Returns
    ///
    /// * `Ok(revision_id)` - Every part persisted
    /// * `Err(String)` - The app is not found, persist failed, or some parts failed
    ///
    /// Any part failing gives an error, so callers can tell the persist was
    /// incomplete. Holds the manager's map lock while persisting.
    ///
    /// # Panics
    ///
    /// Panics if the runtime map mutex is poisoned.
    pub fn persist_app(&self, app_name: &str) -> Result<String, String> {
        let map = self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned");
        let rt = map
            .get(app_name)
            .ok_or_else(|| format!("EventFluxApp '{app_name}' not found"))?;
        let report = rt.persist()?;

        if report.failure_count > 0 {
            return Err(format!(
                "Partial persistence failure: {} component(s) failed to persist (revision: {}). Failed: {}",
                report.failure_count,
                report.revision,
                report.failed_components
                    .iter()
                    .map(|(id, _)| id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        Ok(report.revision)
    }

    pub fn restore_app_revision(&self, app_name: &str, revision: &str) -> Result<(), String> {
        let map = self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned");
        let rt = map
            .get(app_name)
            .ok_or_else(|| format!("EventFluxApp '{app_name}' not found"))?;
        rt.restore_revision(revision)
    }

    /// Persists every registered app and logs the result for each.
    ///
    /// Keeps going after a failure. Apps removed while this runs are skipped.
    pub fn persist_all(&self) {
        let app_names: Vec<String> = self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .keys()
            .cloned()
            .collect();

        for app_name in app_names {
            if let Some(runtime) = self
                .eventflux_app_runtime_map
                .lock()
                .expect("Mutex poisoned")
                .get(&app_name)
                .cloned()
            {
                match runtime.persist() {
                    Ok(report) => {
                        if report.failure_count > 0 {
                            log::warn!(
                                "App '{}': Partial persistence - {} succeeded, {} failed",
                                app_name,
                                report.success_count,
                                report.failure_count
                            );
                        } else {
                            log::info!(
                                "App '{}': Persisted successfully (revision: {})",
                                app_name,
                                report.revision
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("App '{}': Failed to persist - {}", app_name, e);
                    }
                }
            }
        }
    }

    // Restores each app's last persisted revision, if it has a persistence store and a
    // revision. Restore errors are ignored.
    pub fn restore_all_last_state(&self) {
        for runtime in self
            .eventflux_app_runtime_map
            .lock()
            .expect("Mutex poisoned")
            .values()
        {
            if let Some(service) = runtime.eventflux_app_context.get_snapshot_service() {
                if let Some(store) = &service.persistence_store {
                    if let Some(last) = store.get_last_revision(&runtime.name) {
                        let _ = runtime.restore_revision(&last);
                    }
                }
            }
        }
    }

    /// Level 3 API: builds an EventFluxAppRuntime straight from a SQL app string.
    ///
    /// Parses the SQL, converts it to an EventFluxApp named `app_name` (default
    /// "SqlApp"), then calls `create_eventflux_app_runtime_from_api`. A YAML
    /// `eventflux.application.name` still wins as the registered name. The runtime
    /// is not started.
    ///
    /// # Example
    /// ```rust,ignore
    /// let manager = EventFluxManager::new();
    /// let sql = r#"
    ///     CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);
    ///
    ///     SELECT symbol, price
    ///     FROM StockStream
    ///     WHERE price > 100;
    /// "#;
    ///
    /// let runtime = manager.create_runtime_from_sql(sql, Some("MyApp".to_string())).await?;
    /// runtime.start();
    /// ```
    pub async fn create_runtime_from_sql(
        &self,
        sql: &str,
        app_name: Option<String>,
    ) -> Result<Arc<EventFluxAppRuntime>, String> {
        let sql_app = crate::sql_compiler::parse_sql_application(sql)
            .map_err(|e| format!("SQL parse error: {}", e))?;

        let app_name = app_name.unwrap_or_else(|| "SqlApp".to_string());
        let eventflux_app = sql_app
            .to_eventflux_app(app_name)
            .map_err(|e| format!("Type inference error: {}", e))?;

        self.create_eventflux_app_runtime_from_api(Arc::new(eventflux_app), Some(sql.to_string()))
            .await
    }
}

impl Default for EventFluxManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventFluxManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventFluxManager")
            .field("eventflux_context", &"EventFluxContext")
            .field(
                "app_runtimes",
                &self.eventflux_app_runtime_map.lock().unwrap().len(),
            )
            .finish()
    }
}

// Reads the app name from the info annotation's name element, if present.
// Unused today.
impl ApiEventFluxApp {
    #[allow(dead_code)]
    fn get_name(&self) -> Option<String> {
        self.annotations
            .iter()
            .find(|ann| ann.name == crate::query_api::constants::ANNOTATION_INFO)
            .and_then(|ann| {
                ann.elements
                    .iter()
                    .find(|el| el.key == crate::query_api::constants::ANNOTATION_ELEMENT_NAME)
            })
            .map(|el| el.value.clone())
    }
}
