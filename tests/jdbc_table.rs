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

use eventflux::core::config::eventflux_app_context::EventFluxAppContext;
use eventflux::core::config::eventflux_context::EventFluxContext;
use eventflux::core::event::value::AttributeValue;
use eventflux::core::persistence::data_source::{DataSource, DataSourceConfig};
use eventflux::core::table::{
    InMemoryCompiledCondition, InMemoryCompiledUpdateSet, JdbcTable, Table,
};
use rusqlite::Connection;
use std::any::Any;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct SqliteDataSource {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDataSource {
    fn new(path: &str) -> Self {
        Self {
            conn: Arc::new(Mutex::new(Connection::open(path).unwrap())),
        }
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
        _cfg: DataSourceConfig,
    ) -> Result<(), String> {
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

fn setup_table(ctx: &Arc<EventFluxContext>) {
    let ds = ctx.get_data_source("DS1").unwrap();
    let conn_any = ds.get_connection().unwrap();
    let conn_arc = conn_any.downcast::<Arc<Mutex<Connection>>>().unwrap();
    let mut conn = conn_arc.lock().unwrap();
    conn.execute("CREATE TABLE test (c0 TEXT, c1 TEXT)", [])
        .unwrap();
}

#[test]
fn test_jdbc_table_crud() {
    let ctx = Arc::new(EventFluxContext::new());
    ctx.add_data_source(
        "DS1".to_string(),
        Arc::new(SqliteDataSource::new(":memory:")),
    )
    .unwrap();
    setup_table(&ctx);

    let table = JdbcTable::new("test".to_string(), "DS1".to_string(), Arc::clone(&ctx)).unwrap();
    let row1 = vec![
        AttributeValue::String("a".into()),
        AttributeValue::String("b".into()),
    ];
    table.insert(&row1).unwrap();
    assert!(table
        .contains(&InMemoryCompiledCondition {
            values: row1.clone()
        })
        .unwrap());

    let row2 = vec![
        AttributeValue::String("x".into()),
        AttributeValue::String("y".into()),
    ];
    let cond = InMemoryCompiledCondition {
        values: row1.clone(),
    };
    let us = InMemoryCompiledUpdateSet {
        values: row2.clone(),
    };
    assert!(table.update(&cond, &us).unwrap());
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: row1 })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition {
            values: row2.clone()
        })
        .unwrap());

    assert!(table
        .delete(&InMemoryCompiledCondition {
            values: row2.clone()
        })
        .unwrap());
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: row2 })
        .unwrap());
}
