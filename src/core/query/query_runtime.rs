// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/query_runtime.rs
// Corresponds to io.eventflux.core.query.QueryRuntimeImpl
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::query::processor::Processor; // The Processor trait
                                              // StreamJunction is referenced in comments but not used in implementation
use crate::query_api::execution::query::Query as ApiQuery;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub trait QueryRuntimeTrait: Debug + Send + Sync {
    fn get_query_id(&self) -> &str;
    fn is_stateful(&self) -> bool;
    fn get_query(&self) -> Option<Arc<ApiQuery>>;
}

// Represents the runtime of a single query.
// In Java, QueryRuntimeImpl has fields for queryName, EventFluxQueryContext,
// StreamJunction (for input), QuerySelector, OutputRateLimiter, and OutputCallback.
// The processor chain is: InputJunction -> QuerySelector -> OutputRateLimiter -> OutputCallback (e.g., InsertIntoStreamProcessor)
#[derive(Debug)]
pub struct QueryRuntime {
    pub query_name: String,
    pub api_query: Option<Arc<ApiQuery>>,
    pub eventflux_query_context: Option<Arc<EventFluxQueryContext>>,
    // The input stream junction this query consumes from.
    // The QueryRuntime itself doesn't directly "own" the input junction,
    // but it needs to be registered with it.
    // Storing it here might be for reference or if it needs to interact with it post-setup.
    // However, Java QueryRuntimeImpl doesn't store the input StreamJunction directly as a field.
    // It's passed to QueryParser, which then sets up the links.
    // The entry point to the query's processor chain is more relevant.
    // input_stream_junction: Arc<Mutex<StreamJunction>>,

    // The first processor in this query's specific processing chain.
    // This could be a FilterProcessor, WindowProcessor, QuerySelector, etc.
    pub processor_chain_head: Option<Arc<Mutex<dyn Processor>>>,
    // Add other fields as per QueryRuntimeImpl:
    // pub eventflux_query_context: Arc<EventFluxQueryContext>,
    // pub query_selector: Option<Arc<Mutex<SelectProcessor>>>, // Or QuerySelector if that's the struct name
    // pub output_rate_limiter: Option<Arc<Mutex<OutputRateLimiterPlaceholder>>>,
    // pub output_callback: Option<Arc<Mutex<dyn StreamCallback>>> // Or specific output processor
    // pub is_batch_table_query: bool,
    // pub is_store_query: bool,
}

impl QueryRuntime {
    // query_name is usually derived from annotations or generated.
    // input_junction is needed by QueryParser to connect the processor chain.
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

    /// Snapshot the current state using the application's `SnapshotService`.
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

    /// Restore the provided snapshot bytes using the `SnapshotService`.
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

    /// Restore a persisted revision via the application's `SnapshotService`.
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

    /// Flush any buffered events through the processor chain.
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
