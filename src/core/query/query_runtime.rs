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

// src/core/query/query_runtime.rs
// Port of Siddhi's io.siddhi.core.query.QueryRuntimeImpl
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::query::processor::Processor;
use crate::query_api::execution::query::Query as ApiQuery;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub trait QueryRuntimeTrait: Debug + Send + Sync {
    fn get_query_id(&self) -> &str;
    fn is_stateful(&self) -> bool;
    fn get_query(&self) -> Option<Arc<ApiQuery>>;
}

// Runtime of a single query.
// In Java, QueryRuntimeImpl has fields for the query name, the query context, the input
// StreamJunction, QuerySelector, OutputRateLimiter and OutputCallback. Its processor
// chain is InputJunction -> QuerySelector -> OutputRateLimiter -> OutputCallback, for
// example InsertIntoStreamProcessor.
#[derive(Debug)]
pub struct QueryRuntime {
    pub query_name: String,
    pub api_query: Option<Arc<ApiQuery>>,
    pub eventflux_query_context: Option<Arc<EventFluxQueryContext>>,
    // The input junction is not stored. The query is registered with it during setup,
    // but does not own it. Java QueryRuntimeImpl does not keep it as a field either.
    // It goes to QueryParser, which links it up. The head of the processor chain is
    // what matters here.
    // input_stream_junction: Arc<Mutex<StreamJunction>>,

    // First processor in this query's chain, such as a FilterProcessor, a window
    // processor or the selector
    pub processor_chain_head: Option<Arc<Mutex<dyn Processor>>>,
    // Other QueryRuntimeImpl fields, not ported yet:
    // pub eventflux_query_context: Arc<EventFluxQueryContext>,
    // pub query_selector: Option<Arc<Mutex<SelectProcessor>>>, // Or QuerySelector if that's the struct name
    // pub output_rate_limiter: Option<Arc<Mutex<OutputRateLimiterPlaceholder>>>,
    // pub output_callback: Option<Arc<Mutex<dyn StreamCallback>>> // Or specific output processor
    // pub is_batch_table_query: bool,
    // pub is_store_query: bool,
}

impl QueryRuntime {
    // query_name usually comes from an annotation or is generated. Creates a runtime
    // with no query, context or processor chain.
    pub fn new(query_name: String) -> Self {
        Self {
            query_name,
            api_query: None,
            eventflux_query_context: None,
            processor_chain_head: None,
        }
    }

    pub fn new_with_context(
        query_name: String,
        api_query: Arc<ApiQuery>,
        eventflux_query_context: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            query_name,
            api_query: Some(api_query),
            eventflux_query_context: Some(eventflux_query_context),
            processor_chain_head: None,
        }
    }

    /// Take a snapshot through the application's `SnapshotService`. It covers every
    /// registered state holder in the app, not just this query.
    ///
    /// # Errors
    /// Returns an error message when the query context or the SnapshotService is not
    /// set.
    pub fn snapshot(&self) -> Result<Vec<u8>, String> {
        let ctx = self
            .eventflux_query_context
            .as_ref()
            .ok_or("EventFluxQueryContext not set")?;
        let service = ctx
            .get_eventflux_app_context()
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        Ok(service.snapshot())
    }

    /// Restore the given snapshot bytes through the `SnapshotService`.
    ///
    /// # Errors
    /// Returns an error message when the query context or the SnapshotService is not
    /// set.
    pub fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        let ctx = self
            .eventflux_query_context
            .as_ref()
            .ok_or("EventFluxQueryContext not set")?;
        let service = ctx
            .get_eventflux_app_context()
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        service.set_state(snapshot.to_vec());
        Ok(())
    }

    /// Restore a saved revision through the application's `SnapshotService`.
    ///
    /// # Errors
    /// Returns an error message when the query context or the SnapshotService is not
    /// set, or the error from the SnapshotService.
    pub fn restore_revision(&self, revision: &str) -> Result<(), String> {
        let ctx = self
            .eventflux_query_context
            .as_ref()
            .ok_or("EventFluxQueryContext not set")?;
        let service = ctx
            .get_eventflux_app_context()
            .get_snapshot_service()
            .ok_or("SnapshotService not set")?;
        service.restore_revision(revision)
    }

    /// Send an empty chunk (None) to the head of the processor chain, so processors
    /// that act on it can push out buffered events. Does nothing with no chain.
    pub fn flush(&self) {
        if let Some(head) = &self.processor_chain_head {
            head.lock().unwrap().process(None);
        }
    }
}

impl QueryRuntimeTrait for QueryRuntime {
    fn get_query_id(&self) -> &str {
        &self.query_name
    }

    fn is_stateful(&self) -> bool {
        self.eventflux_query_context
            .as_ref()
            .map(|ctx| ctx.is_stateful())
            .unwrap_or(false)
    }

    fn get_query(&self) -> Option<Arc<ApiQuery>> {
        self.api_query.as_ref().map(Arc::clone)
    }
}
