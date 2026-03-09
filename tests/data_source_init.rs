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
use eventflux::core::persistence::data_source::{DataSource, DataSourceConfig};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct MockDataSource {
    called: Arc<Mutex<Option<DataSourceConfig>>>,
}

impl MockDataSource {
    fn new() -> Self {
        Self {
            called: Arc::new(Mutex::new(None)),
        }
    }
}

impl DataSource for MockDataSource {
    fn get_type(&self) -> String {
        "mock".to_string()
    }
    fn init(
        &mut self,
        _ctx: &Arc<EventFluxAppContext>,
        _id: &str,
        cfg: DataSourceConfig,
    ) -> Result<(), String> {
        *self.called.lock().unwrap() = Some(cfg);
        Ok(())
    }
    fn get_connection(&self) -> Result<Box<dyn Any>, String> {
        Ok(Box::new(()))
    }
    fn shutdown(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn clone_data_source(&self) -> Box<dyn DataSource> {
        Box::new(MockDataSource {
            called: Arc::clone(&self.called),
        })
    }
}

#[test]
fn test_add_data_source_uses_config() {
    let ctx = Arc::new(EventFluxContext::new());
    let mut props = HashMap::new();
    props.insert("url".to_string(), "mem".to_string());
    ctx.set_data_source_config(
        "DS".to_string(),
        DataSourceConfig {
            r#type: "mock".to_string(),
            properties: props.clone(),
        },
    );
    let ds = Arc::new(MockDataSource::new());
    ctx.add_data_source("DS".to_string(), ds.clone()).unwrap();
    let stored = ds.called.lock().unwrap().clone();
    assert!(stored.is_some());
    assert_eq!(
        stored.unwrap().properties.get("url"),
        Some(&"mem".to_string())
    );
}
