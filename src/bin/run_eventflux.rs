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

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::Parser;

use eventflux::core::config::types::global_config::PersistenceBackendType;
use eventflux::core::config::{
    apply_config_overrides, ConfigManager, EventFluxConfig, GlobalPersistenceConfig,
};
use eventflux::core::eventflux_manager::EventFluxManager;
use eventflux::core::persistence::{
    FilePersistenceStore, PersistenceStore, SqlitePersistenceStore,
};

#[derive(Parser, Debug)]
#[command(about = "Run an EventFluxQL file", author, version)]
struct Cli {
    /// EventFluxQL file to execute
    eventflux_file: PathBuf,

    /// Configuration file (YAML)
    #[arg(long, short = 'c')]
    config: Option<PathBuf>,

    /// Override a config value by its dotted key path. Repeat the flag to set more than one
    ///
    /// Examples:
    ///   --set eventflux.persistence.type=sqlite
    ///   --set eventflux.persistence.path=./data.db
    ///   --set eventflux.runtime.performance.thread_pool_size=8
    #[arg(long = "set", value_name = "KEY=VALUE")]
    overrides: Vec<String>,

    /// Shared library files with extensions to load. Repeat the flag for more than one
    #[arg(short = 'e', long)]
    extension: Vec<PathBuf>,
}

/// Build the persistence store named in the config.
///
/// Returns `None` when persistence is disabled, when the backend is `Memory`, when the
/// backend is `Redis` (not supported here yet), or when the store fails to open. Failures
/// are printed to stderr. Default locations are `./snapshots` for File, `./eventflux.db`
/// for SQLite and `redis://localhost:6379` for Redis.
fn init_persistence_store(
    persistence_config: &GlobalPersistenceConfig,
) -> Option<Arc<dyn PersistenceStore>> {
    if !persistence_config.enabled {
        return None;
    }

    match persistence_config.backend_type {
        PersistenceBackendType::File => {
            let path = persistence_config.path.as_deref().unwrap_or("./snapshots");

            match FilePersistenceStore::new(path) {
                Ok(s) => {
                    println!("Initialized file persistence store at: {}", path);
                    Some(Arc::new(s))
                }
                Err(e) => {
                    eprintln!("Failed to initialize file persistence store: {e}");
                    None
                }
            }
        }
        PersistenceBackendType::Sqlite => {
            let path = persistence_config
                .path
                .as_deref()
                .unwrap_or("./eventflux.db");

            match SqlitePersistenceStore::new(path) {
                Ok(s) => {
                    println!("Initialized SQLite persistence store at: {}", path);
                    Some(Arc::new(s))
                }
                Err(e) => {
                    eprintln!("Failed to initialize SQLite persistence store: {e}");
                    None
                }
            }
        }
        PersistenceBackendType::Redis => {
            let url = persistence_config
                .url
                .as_deref()
                .unwrap_or("redis://localhost:6379");

            eprintln!(
                "Redis persistence is not yet implemented. URL would be: {}",
                url
            );
            None
        }
        PersistenceBackendType::Memory => {
            println!("Using in-memory persistence (no state will be saved across restarts)");
            None
        }
    }
}

#[tokio::main]
async fn main() {
    // Log level comes from the RUST_LOG environment variable.
    env_logger::init();

    let cli = Cli::parse();

    // Use the YAML file from --config if given, else the defaults.
    let mut config = if let Some(ref config_path) = cli.config {
        let manager = ConfigManager::from_file(config_path);
        match manager.load_unified_config().await {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to load config from {}: {e}", config_path.display());
                std::process::exit(1);
            }
        }
    } else {
        EventFluxConfig::default()
    };

    if !cli.overrides.is_empty() {
        if let Err(e) = apply_config_overrides(&mut config, &cli.overrides) {
            eprintln!("Failed to apply config overrides: {e}");
            std::process::exit(1);
        }
    }

    let store = config
        .eventflux
        .persistence
        .as_ref()
        .and_then(init_persistence_store);

    let content = match fs::read_to_string(&cli.eventflux_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to read {}: {}", cli.eventflux_file.display(), e);
            std::process::exit(1);
        }
    };

    // Pass the config in directly so the runtime sees the --set overrides.
    // Do not call set_config_manager() here. It clears the cached config and reloads it
    // from the file, which drops the --set overrides.
    let manager = EventFluxManager::new_with_config(config);

    if let Some(s) = store {
        if let Err(e) = manager.set_persistence_store(s) {
            eprintln!("Failed to set persistence store: {e}");
            std::process::exit(1);
        }
    }

    // Each library is registered under its file stem. A path that is not valid UTF-8 is
    // skipped, and a load failure is printed but does not stop the run.
    for lib in cli.extension {
        if let Some(p) = lib.to_str() {
            let name = lib.file_stem().and_then(|s| s.to_str()).unwrap_or("ext");
            if let Err(e) = manager.set_extension(name, p.to_string()) {
                eprintln!("Failed to load extension {p}: {e}");
            }
        }
    }

    let runtime = match manager
        .create_eventflux_app_runtime_from_string(&content)
        .await
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to create runtime: {e}");
            std::process::exit(1);
        }
    };

    // No output callbacks are added here. To see events, declare a sink stream in the SQL
    // with a WITH clause such as extension = 'log'.

    if let Err(e) = runtime.start() {
        eprintln!("Failed to start runtime: {e}");
        std::process::exit(1);
    }

    println!(
        "Running EventFlux app '{}'. Press Ctrl+C to exit",
        runtime.name
    );

    // Runs until the process is killed, for example with Ctrl+C. There is no clean shutdown.
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
