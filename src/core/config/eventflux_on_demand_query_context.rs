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

// Corresponds to io.eventflux.core.config.EventFluxOnDemandQueryContext
use super::eventflux_app_context::EventFluxAppContext;
use super::eventflux_query_context::EventFluxQueryContext;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventFluxOnDemandQueryContext {
    // Java extends EventFluxQueryContext. Here it wraps one and derefs to it.
    pub query_context: EventFluxQueryContext,
    pub on_demand_query_string: String,
}

impl EventFluxOnDemandQueryContext {
    pub fn new(
        eventflux_app_context: Arc<EventFluxAppContext>,
        query_name: String,
        query_string: String,
    ) -> Self {
        // Matches the Java super(eventfluxAppContext, queryName, null) call.
        // On demand queries have no partition id, so it is None.
        Self {
            query_context: EventFluxQueryContext::new(eventflux_app_context, query_name, None),
            on_demand_query_string: query_string,
        }
    }

    pub fn get_on_demand_query_string(&self) -> &str {
        &self.on_demand_query_string
    }

    // Other EventFluxQueryContext methods are reached through Deref or the public
    // query_context field. An explicit wrapper would look like this:
    // pub fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
    //     Arc::clone(&self.query_context.eventflux_app_context)
    // }
}

// Deref and DerefMut give direct access to the EventFluxQueryContext fields and methods
use std::ops::{Deref, DerefMut};

impl Deref for EventFluxOnDemandQueryContext {
    type Target = EventFluxQueryContext;
    fn deref(&self) -> &Self::Target {
        &self.query_context
    }
}

impl DerefMut for EventFluxOnDemandQueryContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.query_context
    }
}
