// SPDX-License-Identifier: MIT OR Apache-2.0

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

    /// Override config values using dot-notation (can be used multiple times)
    ///
    /// Examples:
    ///   --set eventflux.persistence.type=sqlite
    ///   --set eventflux.persistence.path=./data.db
    ///   --set eventflux.runtime.performance.thread_pool_size=8
    #[arg(long = "set", value_name = "KEY=VALUE")]
    overrides: Vec<String>,

    /// Dynamic extension libraries to load
    #[arg(short = 'e', long)]
    extension: Vec<PathBuf>,
}

/// Initialize persistence store from configuration
fn init_persistence_store(
    persistence_config: &GlobalPersistenceConfig,
) -> Option<Arc<dyn PersistenceStore>> {
    if !persistence_config.enabled {
        return None;
    }

    match persistence_config.backend_type {
        PersistenceBackendType::File => {
            let path = persistence_config
                .path
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("./snapshots");

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
                .as_ref()
                .map(|s| s.as_str())
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
                .as_ref()
                .map(|s| s.as_str())
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
    // Initialize logging from RUST_LOG environment variable
    env_logger::init();

    let cli = Cli::parse();

    // Load configuration from file or use defaults
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

    // Apply command-line overrides
    if !cli.overrides.is_empty() {
        if let Err(e) = apply_config_overrides(&mut config, &cli.overrides) {
            eprintln!("Failed to apply config overrides: {e}");
            std::process::exit(1);
        }
    }

    // Initialize persistence store from config
    let store = config
        .eventflux
        .persistence
        .as_ref()
        .and_then(init_persistence_store);

    // Read EventFluxQL file
    let content = match fs::read_to_string(&cli.eventflux_file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to read {}: {}", cli.eventflux_file.display(), e);
            std::process::exit(1);
        }
    };

    // Create EventFlux manager with our (potentially overridden) config
    // This ensures --set overrides are used by the runtime
    // Note: We don't call set_config_manager() because it would clear the cached
    // config and reload from file, losing our --set overrides
    let manager = EventFluxManager::new_with_config(config);

    // Set persistence store if configured
    if let Some(s) = store {
        if let Err(e) = manager.set_persistence_store(s) {
            eprintln!("Failed to set persistence store: {e}");
            std::process::exit(1);
        }
    }

    // Load extensions
    for lib in cli.extension {
        if let Some(p) = lib.to_str() {
            let name = lib.file_stem().and_then(|s| s.to_str()).unwrap_or("ext");
            if let Err(e) = manager.set_extension(name, p.to_string()) {
                eprintln!("Failed to load extension {p}: {e}");
            }
        }
    }

    // Create runtime
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

    // Note: Sink streams configured via SQL WITH clause (extension = 'log')
    // already have proper logging. No need to add callbacks to all streams.

    // Start the runtime
    if let Err(e) = runtime.start() {
        eprintln!("Failed to start runtime: {e}");
        std::process::exit(1);
    }

    println!(
        "Running EventFlux app '{}'. Press Ctrl+C to exit",
        runtime.name
    );

    // Keep the main thread alive until interrupted
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
