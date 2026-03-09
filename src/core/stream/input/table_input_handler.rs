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

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::event::Event;
use crate::core::event::value::AttributeValue;
use crate::core::table::{InMemoryCompiledCondition, InMemoryCompiledUpdateSet, Table};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableInputHandler {
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    table: Arc<dyn Table>,
}

impl TableInputHandler {
    pub fn new(table: Arc<dyn Table>, eventflux_app_context: Arc<EventFluxAppContext>) -> Self {
        Self {
            eventflux_app_context,
            table,
        }
    }

    pub fn add(&self, events: Vec<Event>) {
        for event in events {
            if let Err(e) = self.table.insert(&event.data) {
                log::error!("Failed to insert event into table: {}", e);
            }
        }
    }

    pub fn update(&self, old: Vec<AttributeValue>, new: Vec<AttributeValue>) -> bool {
        let cond = InMemoryCompiledCondition { values: old };
        let us = InMemoryCompiledUpdateSet { values: new };
        match self.table.update(&cond, &us) {
            Ok(result) => result,
            Err(e) => {
                log::error!("Failed to update table: {}", e);
                false
            }
        }
    }

    pub fn delete(&self, values: Vec<AttributeValue>) -> bool {
        let cond = InMemoryCompiledCondition { values };
        match self.table.delete(&cond) {
            Ok(result) => result,
            Err(e) => {
                log::error!("Failed to delete from table: {}", e);
                false
            }
        }
    }
}
