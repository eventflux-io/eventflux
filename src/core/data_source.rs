// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/data_source.rs
// Public DataSource trait and basic SqliteDataSource implementation

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct DataSourceConfig {
    pub r#type: String,
    pub properties: std::collections::HashMap<String, String>,
}

pub trait DataSource: Debug + Send + Sync + 'static {
    fn get_type(&self) -> String;

    fn init(
        &mut self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
        data_source_id: &str,
        config: DataSourceConfig,
    ) -> Result<(), String>;

    fn get_connection(&self) -> Result<Box<dyn Any>, String>;

    fn shutdown(&mut self) -> Result<(), String>;

    fn clone_data_source(&self) -> Box<dyn DataSource>;
}

impl Clone for Box<dyn DataSource> {
    fn clone(&self) -> Self {
        self.clone_data_source()
    }
}

use rusqlite::Connection;

#[derive(Debug)]
pub struct SqliteDataSource {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDataSource {
    pub fn new(path: &str) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            conn: Arc::new(Mutex::new(Connection::open(path)?)),
        })
    }
}

impl DataSource for SqliteDataSource {
    fn get_type(&self) -> String {
        "sqlite".to_string()
    }

    fn init(
        &mut self,
        _ctx: &Arc<EventFluxAppContext>,
        _id: &str,
        cfg: DataSourceConfig,
    ) -> Result<(), String> {
        if let Some(path) = cfg.properties.get("path") {
            let conn = Connection::open(path).map_err(|e| e.to_string())?;
            self.conn = Arc::new(Mutex::new(conn));
        }
        Ok(())
    }

    fn get_connection(&self) -> Result<Box<dyn Any>, String> {
        Ok(Box::new(self.conn.clone()) as Box<dyn Any>)
    }

    fn shutdown(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn clone_data_source(&self) -> Box<dyn DataSource> {
        Box::new(SqliteDataSource {
            conn: self.conn.clone(),
        })
    }
}
