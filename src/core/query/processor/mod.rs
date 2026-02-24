// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/mod.rs
// This file now acts as the module root for the `processor` directory.
// Its content is based on the old `processor.rs` file.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
// MetaStreamEvent and ApiAbstractDefinition were commented out, keep as is for now.
// use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
// use crate::query_api::definition::AbstractDefinition as ApiAbstractDefinition;
// use crate::core::executor::expression_executor::ExpressionExecutor;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProcessingMode {
    #[default]
    DEFAULT,
    SLIDE,
    BATCH,
}

/// Common metadata for Processors.
#[derive(Debug, Clone)]
pub struct CommonProcessorMeta {
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub eventflux_query_context: Arc<EventFluxQueryContext>,
    pub query_name: String,
    pub next_processor: Option<Arc<Mutex<dyn Processor>>>,
}

impl CommonProcessorMeta {
    pub fn new(
        app_context: Arc<EventFluxAppContext>,
        query_context: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            eventflux_app_context: app_context,
            query_name: query_context.name.clone(),
            eventflux_query_context: query_context,
            next_processor: None,
        }
    }

    pub fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::clone(&self.eventflux_query_context)
    }
}

/// Trait for stream processors that process event chunks.
pub trait Processor: Debug + Send + Sync {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>);
    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>>;
    fn set_next_processor(&mut self, next_processor: Option<Arc<Mutex<dyn Processor>>>);
    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor>;
    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext>;
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext>;
    fn get_processing_mode(&self) -> ProcessingMode;
    fn is_stateful(&self) -> bool;

    /// Clear group states if this processor supports it (e.g., SelectProcessor)
    fn clear_group_states(&self) {
        // Default implementation does nothing
    }
}

// Declare submodules within processor directory
pub mod stream; // For StreamProcessors like FilterProcessor

// Re-export items to be accessed via `crate::core::query::processor::`
pub use self::stream::FilterProcessor; // Example re-export from stream submodule
