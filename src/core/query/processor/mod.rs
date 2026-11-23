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

// src/core/query/processor/mod.rs
// Root of the `processor` module. Holds the Processor trait and the metadata shared by
// processors.

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
// Unused imports, kept commented out
// use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
// use crate::query_api::definition::AbstractDefinition as ApiAbstractDefinition;
// use crate::core::executor::expression_executor::ExpressionExecutor;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// How a processor emits events, one by one (DEFAULT), as a sliding window (SLIDE), or
/// in batches (BATCH)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProcessingMode {
    #[default]
    DEFAULT,
    SLIDE,
    BATCH,
}

/// Metadata every processor holds, which is the app and query contexts, the query name
/// and the next processor.
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

/// A step in a query that takes a chunk of linked events and usually passes results to
/// the next processor.
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

    /// Clear per group state, for processors that keep it, such as SelectProcessor.
    fn clear_group_states(&self) {
        // Does nothing by default
    }
}

pub mod stream; // Stream processors such as FilterProcessor and the windows

pub use self::stream::FilterProcessor;
