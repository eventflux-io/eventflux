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

use crate::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext,
};
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::complex_event::ComplexEventType;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::extension::WindowProcessorFactory;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::util::scheduler::{Schedulable, Scheduler};
use crate::query_api::execution::query::input::handler::WindowHandler;
use crate::query_api::expression::{constant::ConstantValueWithFloat, Expression};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Write};
use std::sync::{Arc, Mutex};

// Import StateHolder trait and related types
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder as NewStateHolder, StateMetadata, StateSize, StateSnapshot,
};

// Window type constants
pub mod types;

// Import changelog helpers
pub(crate) mod changelog_helpers;

// Import session window processor
mod session_window_processor;
use session_window_processor::SessionWindowProcessor;

// Import session window state holder
mod session_window_state_holder;

// Import sort window processor
mod sort_window_processor;
use sort_window_processor::SortWindowProcessor;

// Import enhanced length window state holder
mod length_window_state_holder;
use length_window_state_holder::LengthWindowStateHolder;

// Import enhanced time window state holder
mod time_window_state_holder;
use time_window_state_holder::TimeWindowStateHolder;

// Import enhanced length batch window state holder
mod length_batch_window_state_holder;
use length_batch_window_state_holder::LengthBatchWindowStateHolder;

// Import enhanced time batch window state holder
mod time_batch_window_state_holder;
use time_batch_window_state_holder::TimeBatchWindowStateHolder;

// Import enhanced external time window state holder
mod external_time_window_state_holder;

pub trait WindowProcessor: Processor {}

#[derive(Debug)]
pub struct LengthWindowProcessor {
    meta: CommonProcessorMeta,
    pub length: usize,
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
}

impl LengthWindowProcessor {
    pub fn new(
        length: usize,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));

        // Create enhanced StateHolder and register it for persistence
        let component_id = format!("length_window_{}_{}", query_ctx.get_name(), length);
        let state_holder = Arc::new(LengthWindowStateHolder::new(
            Arc::clone(&buffer),
            component_id.clone(),
            length,
        ));

        // Register state holder with SnapshotService for persistence
        let state_holder_clone = (*state_holder).clone();
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder_clone));
        if let Some(snapshot_service) = app_ctx.get_snapshot_service() {
            snapshot_service.register_state_holder(component_id, state_holder_arc);
        }

        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            length,
            buffer,
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("Length window requires a parameter")?;
        if let Expression::Constant(c) = expr {
            let len = match &c.value {
                ConstantValueWithFloat::Int(i) => *i as usize,
                ConstantValueWithFloat::Long(l) => *l as usize,
                _ => return Err("Length window size must be int or long".to_string()),
            };
            Ok(Self::new(len, app_ctx, query_ctx))
        } else {
            Err("Length window size must be constant".to_string())
        }
    }
}

impl Processor for LengthWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        let mut expired: Option<Box<dyn ComplexEvent>> = None;
                        {
                            let mut buf = self.buffer.lock().unwrap();
                            if buf.len() >= self.length {
                                if let Some(old) = buf.pop_front() {
                                    let mut ex = old.as_ref().clone_without_next();
                                    ex.set_event_type(ComplexEventType::Expired);
                                    ex.set_timestamp(se.timestamp);
                                    expired = Some(Box::new(ex));
                                }
                            }
                            buf.push_back(Arc::new(se.clone_without_next()));
                        }
                        if let Some(mut ex) = expired {
                            let tail = ex.mut_next_ref_option();
                            *tail = Some(Box::new(se.clone_without_next()));
                            next.lock().unwrap().process(Some(ex));
                        } else {
                            next.lock()
                                .unwrap()
                                .process(Some(Box::new(se.clone_without_next())));
                        }
                    }
                    current_opt = ev.get_next();
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.length,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for LengthWindowProcessor {}

#[derive(Debug)]
pub struct TimeWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    scheduler: Option<Arc<Scheduler>>,
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
    state_holder: Option<TimeWindowStateHolder>,
}

impl TimeWindowProcessor {
    pub fn new(
        duration_ms: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let scheduler = app_ctx.get_scheduler();
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let component_id = format!("time_window_{}_{}", query_ctx.get_name(), duration_ms);

        let state_holder = Some(TimeWindowStateHolder::new(
            Arc::clone(&buffer),
            component_id,
            duration_ms,
        ));

        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            duration_ms,
            scheduler,
            buffer,
            state_holder,
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("Time window requires a parameter")?;
        if let Expression::Constant(c) = expr {
            let dur = match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Long(l) => *l,
                _ => return Err("Time window duration must be time constant".to_string()),
            };
            Ok(Self::new(dur, app_ctx, query_ctx))
        } else {
            Err("Time window duration must be constant".to_string())
        }
    }
}

#[derive(Debug, Clone)]
struct ExpireTask {
    event: Arc<StreamEvent>,
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
    next: Option<Arc<Mutex<dyn Processor>>>,
    state_holder: Option<TimeWindowStateHolder>,
}

impl Schedulable for ExpireTask {
    fn on_time(&self, timestamp: i64) {
        let ev_arc = {
            let mut buf = self.buffer.lock().unwrap();
            if let Some(pos) = buf.iter().position(|e| Arc::ptr_eq(e, &self.event)) {
                buf.remove(pos)
            } else {
                None
            }
        };
        if let Some(ev_arc) = ev_arc {
            let mut ev = ev_arc.as_ref().clone_without_next();
            ev.set_event_type(ComplexEventType::Expired);
            ev.set_timestamp(timestamp);

            // Track state change
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_event_expired(&ev);
            }

            if let Some(ref next) = self.next {
                next.lock().unwrap().process(Some(Box::new(ev)));
            }
        }
    }
}

#[derive(Clone)]
struct BatchFlushTask {
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    next: Option<Arc<Mutex<dyn Processor>>>,
    duration_ms: i64,
    scheduler: Arc<Scheduler>,
    start_time: Arc<Mutex<Option<i64>>>,
    /// Reset event template - emitted between expired and current to clear aggregator state
    reset_event: Arc<Mutex<Option<StreamEvent>>>,
    /// State holder for incremental checkpointing
    state_holder: Option<TimeBatchWindowStateHolder>,
}

impl Schedulable for BatchFlushTask {
    fn on_time(&self, timestamp: i64) {
        let expired_batch: Vec<StreamEvent> = {
            let mut ex = self.expired.lock().unwrap();
            std::mem::take(&mut *ex)
        };
        let current_batch: Vec<StreamEvent> = {
            let mut buf = self.buffer.lock().unwrap();
            std::mem::take(&mut *buf)
        };

        if expired_batch.is_empty() && current_batch.is_empty() {
            let old_start_time = {
                let mut start_time = self.start_time.lock().unwrap();
                let old = *start_time;
                *start_time = Some(timestamp);
                old
            };
            // Record start_time update for incremental checkpointing
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_start_time_updated(old_start_time, Some(timestamp));
            }
            self.scheduler
                .notify_at(timestamp + self.duration_ms, Arc::new(self.clone()));
            return;
        }

        // Record batch flush for incremental checkpointing
        // This must be done before processing to ensure recovery replays correctly
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_batch_flushed(&current_batch, &expired_batch, timestamp);
        }

        let mut head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail = &mut head;

        // 1. Emit expired events first
        for mut e in expired_batch {
            e.set_event_type(ComplexEventType::Expired);
            e.set_timestamp(timestamp);
            *tail = Some(Box::new(e.clone_without_next()));
            tail = tail.as_mut().unwrap().mut_next_ref_option();
        }

        // 2. Emit RESET event to clear aggregator state before processing new batch
        // This follows Siddhi's pattern: EXPIRED → RESET → CURRENT
        {
            let mut reset_guard = self.reset_event.lock().unwrap();
            if let Some(ref reset_template) = *reset_guard {
                let mut reset_ev = reset_template.clone_without_next();
                reset_ev.set_event_type(ComplexEventType::Reset);
                reset_ev.set_timestamp(timestamp);
                *tail = Some(Box::new(reset_ev));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }
            // Clear reset event after use (will be recreated from first event of next batch)
            *reset_guard = None;
            // Record reset event cleared for incremental checkpointing
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_reset_event_cleared();
            }
        }

        // 3. Emit current events
        for e in &current_batch {
            *tail = Some(Box::new(e.clone_without_next()));
            tail = tail.as_mut().unwrap().mut_next_ref_option();
        }

        {
            let mut ex = self.expired.lock().unwrap();
            ex.extend(current_batch);
        }

        if let Some(chain) = head {
            if let Some(ref next) = self.next {
                next.lock().unwrap().process(Some(chain));
            }
        }

        let old_start_time = {
            let mut start_time = self.start_time.lock().unwrap();
            let old = *start_time;
            *start_time = Some(timestamp);
            old
        };
        // Record start_time update for incremental checkpointing
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_start_time_updated(old_start_time, Some(timestamp));
        }
        self.scheduler
            .notify_at(timestamp + self.duration_ms, Arc::new(self.clone()));
    }
}

impl Processor for TimeWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref scheduler) = self.scheduler {
                if let Some(ref chunk) = complex_event_chunk {
                    let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                    while let Some(ev) = current_opt {
                        if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                            let arc = Arc::new(se.clone_without_next());
                            {
                                let mut buf = self.buffer.lock().unwrap();
                                buf.push_back(Arc::clone(&arc));
                            }

                            // Track state change
                            if let Some(ref state_holder) = self.state_holder {
                                state_holder.record_event_added(se);
                            }

                            let task = ExpireTask {
                                event: Arc::clone(&arc),
                                buffer: Arc::clone(&self.buffer),
                                next: Some(Arc::clone(next)),
                                state_holder: self.state_holder.as_ref().cloned(),
                            };
                            scheduler.notify_at(se.timestamp + self.duration_ms, Arc::new(task));
                        }
                        current_opt = ev.get_next();
                    }
                }
            }
            next.lock().unwrap().process(complex_event_chunk);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.duration_ms,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for TimeWindowProcessor {}

impl NewStateHolder for TimeWindowProcessor {
    fn schema_version(&self) -> crate::core::persistence::state_holder::SchemaVersion {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.schema_version()
        } else {
            crate::core::persistence::state_holder::SchemaVersion::new(1, 0, 0)
        }
    }

    fn serialize_state(
        &self,
        hints: &crate::core::persistence::state_holder::SerializationHints,
    ) -> Result<
        crate::core::persistence::state_holder::StateSnapshot,
        crate::core::persistence::state_holder::StateError,
    > {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.serialize_state(hints)
        } else {
            Err(
                crate::core::persistence::state_holder::StateError::InvalidStateData {
                    message: "StateHolder not initialized".to_string(),
                },
            )
        }
    }

    fn deserialize_state(
        &self,
        snapshot: &crate::core::persistence::state_holder::StateSnapshot,
    ) -> Result<(), crate::core::persistence::state_holder::StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.deserialize_state(snapshot)
        } else {
            Err(
                crate::core::persistence::state_holder::StateError::InvalidStateData {
                    message: "StateHolder not initialized".to_string(),
                },
            )
        }
    }

    fn get_changelog(
        &self,
        since: crate::core::persistence::state_holder::CheckpointId,
    ) -> Result<
        crate::core::persistence::state_holder::ChangeLog,
        crate::core::persistence::state_holder::StateError,
    > {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.get_changelog(since)
        } else {
            Err(
                crate::core::persistence::state_holder::StateError::InvalidStateData {
                    message: "StateHolder not initialized".to_string(),
                },
            )
        }
    }

    fn apply_changelog(
        &self,
        changes: &crate::core::persistence::state_holder::ChangeLog,
    ) -> Result<(), crate::core::persistence::state_holder::StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.apply_changelog(changes)
        } else {
            Err(
                crate::core::persistence::state_holder::StateError::InvalidStateData {
                    message: "StateHolder not initialized".to_string(),
                },
            )
        }
    }

    fn estimate_size(&self) -> crate::core::persistence::state_holder::StateSize {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.estimate_size()
        } else {
            crate::core::persistence::state_holder::StateSize {
                bytes: 0,
                entries: 0,
                estimated_growth_rate: 0.0,
            }
        }
    }

    fn access_pattern(&self) -> crate::core::persistence::state_holder::AccessPattern {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.access_pattern()
        } else {
            crate::core::persistence::state_holder::AccessPattern::Sequential
        }
    }

    fn component_metadata(&self) -> crate::core::persistence::state_holder::StateMetadata {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.component_metadata()
        } else {
            crate::core::persistence::state_holder::StateMetadata::new(
                format!("time_window_{}", self.duration_ms),
                "TimeWindowProcessor".to_string(),
            )
        }
    }
}

pub fn create_window_processor(
    handler: &WindowHandler,
    app_ctx: Arc<EventFluxAppContext>,
    query_ctx: Arc<EventFluxQueryContext>,
    parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
) -> Result<Arc<Mutex<dyn Processor>>, String> {
    // Look up window factory from registry
    if let Some(factory) = app_ctx
        .get_eventflux_context()
        .get_window_factory(&handler.name)
    {
        factory.create(handler, app_ctx, query_ctx, parse_ctx)
    } else {
        // Provide helpful error message with available window types
        let available = app_ctx.get_eventflux_context().list_window_factory_names();
        Err(format!(
            "Unknown window type '{}'. Available: {:?}",
            handler.name, available
        ))
    }
}

#[derive(Debug, Clone)]
pub struct LengthWindowFactory;

impl WindowProcessorFactory for LengthWindowFactory {
    fn name(&self) -> &'static str {
        "length"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(LengthWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct TimeWindowFactory;

impl WindowProcessorFactory for TimeWindowFactory {
    fn name(&self) -> &'static str {
        "time"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(TimeWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ---- LengthBatchWindowProcessor ----

#[derive(Debug)]
pub struct LengthBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub length: usize,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    state_holder: Option<LengthBatchWindowStateHolder>,
    /// Reset event template - created from first event of batch, emitted between expired and current
    reset_event: Arc<Mutex<Option<StreamEvent>>>,
}

impl LengthBatchWindowProcessor {
    pub fn new(
        length: usize,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));
        let reset_event = Arc::new(Mutex::new(None));

        // Create enhanced StateHolder and register it
        let component_id = format!("length_batch_window_{}_{}", query_ctx.get_name(), length);
        let state_holder = LengthBatchWindowStateHolder::new(
            Arc::clone(&buffer),
            Arc::clone(&expired),
            Arc::clone(&reset_event),
            component_id.clone(),
            length,
        );

        // Register state holder with SnapshotService for persistence
        let state_holder_clone = state_holder.clone();
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder_clone));
        if let Some(snapshot_service) = app_ctx.get_snapshot_service() {
            snapshot_service.register_state_holder(component_id.clone(), state_holder_arc);
        } else {
        }

        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            length,
            buffer,
            expired,
            state_holder: Some(state_holder),
            reset_event,
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("LengthBatch window requires a parameter")?;
        if let Expression::Constant(c) = expr {
            let len = match &c.value {
                ConstantValueWithFloat::Int(i) => *i as usize,
                ConstantValueWithFloat::Long(l) => *l as usize,
                _ => return Err("LengthBatch window size must be int or long".to_string()),
            };
            Ok(Self::new(len, app_ctx, query_ctx))
        } else {
            Err("LengthBatch window size must be constant".to_string())
        }
    }

    fn flush(&self, timestamp: i64) {
        if let Some(ref next) = self.meta.next_processor {
            let expired_batch: Vec<StreamEvent> = {
                let mut ex = self.expired.lock().unwrap();
                std::mem::take(&mut *ex)
            };
            let current_batch: Vec<StreamEvent> = {
                let mut buf = self.buffer.lock().unwrap();
                std::mem::take(&mut *buf)
            };

            if expired_batch.is_empty() && current_batch.is_empty() {
                return;
            }

            // Record batch flush for incremental checkpointing
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_batch_flushed(&current_batch, &expired_batch);
            }

            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;

            // 1. Emit expired events first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. Emit RESET event to clear aggregator state before processing new batch
            // This follows Siddhi's pattern: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Clear reset event after use (will be recreated from first event of next batch)
                *reset_guard = None;
                // Record reset event cleared for incremental checkpointing
                if let Some(ref state_holder) = self.state_holder {
                    state_holder.record_reset_event_cleared();
                }
            }

            // 3. Emit current events
            for e in &current_batch {
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            {
                let mut ex = self.expired.lock().unwrap();
                ex.extend(current_batch);
            }

            if let Some(chain) = head {
                next.lock().unwrap().process(Some(chain));
            }
        }
    }
}

impl Processor for LengthBatchWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(chunk) = complex_event_chunk {
            let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
            while let Some(ev) = current_opt {
                if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                    let se_clone = se.clone_without_next();

                    // Record state change for incremental checkpointing
                    if let Some(ref state_holder) = self.state_holder {
                        state_holder.record_event_added(&se_clone);
                    }

                    // Fix race condition: hold lock for entire check-and-modify operation
                    let last_ts = se.timestamp;
                    let should_flush = {
                        let mut buffer = self.buffer.lock().unwrap();
                        // Capture first event of batch as reset template
                        if buffer.is_empty() {
                            let mut reset_guard = self.reset_event.lock().unwrap();
                            *reset_guard = Some(se_clone.clone());
                            // Record reset event set for incremental checkpointing
                            if let Some(ref state_holder) = self.state_holder {
                                state_holder.record_reset_event_set(&se_clone);
                            }
                        }
                        buffer.push(se_clone);
                        buffer.len() >= self.length
                    };

                    if should_flush {
                        self.flush(last_ts);
                    }
                }
                current_opt = ev.get_next();
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.length,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for LengthBatchWindowProcessor {}

impl NewStateHolder for LengthBatchWindowProcessor {
    fn schema_version(&self) -> SchemaVersion {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.schema_version()
        } else {
            SchemaVersion::new(1, 0, 0)
        }
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.serialize_state(hints)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for serialization".to_string(),
            })
        }
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.deserialize_state(snapshot)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for deserialization".to_string(),
            })
        }
    }

    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.get_changelog(since)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for changelog".to_string(),
            })
        }
    }

    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.apply_changelog(changes)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for changelog application".to_string(),
            })
        }
    }

    fn estimate_size(&self) -> StateSize {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.estimate_size()
        } else {
            StateSize {
                bytes: 0,
                entries: 0,
                estimated_growth_rate: 0.0,
            }
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.access_pattern()
        } else {
            AccessPattern::Sequential
        }
    }

    fn component_metadata(&self) -> StateMetadata {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.component_metadata()
        } else {
            StateMetadata::new(
                "unknown_length_batch_window".to_string(),
                "LengthBatchWindowProcessor".to_string(),
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct LengthBatchWindowFactory;

impl WindowProcessorFactory for LengthBatchWindowFactory {
    fn name(&self) -> &'static str {
        "lengthBatch"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(
            LengthBatchWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ---- TimeBatchWindowProcessor ----

#[derive(Debug)]
pub struct TimeBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    scheduler: Option<Arc<Scheduler>>,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    start_time: Arc<Mutex<Option<i64>>>,
    state_holder: Option<TimeBatchWindowStateHolder>,
    /// Reset event template - created from first event, emitted between expired and current
    /// to signal aggregators to clear state before processing new batch
    reset_event: Arc<Mutex<Option<StreamEvent>>>,
}

impl TimeBatchWindowProcessor {
    pub fn new(
        duration_ms: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let scheduler = app_ctx.get_scheduler();
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let expired = Arc::new(Mutex::new(Vec::new()));
        let start_time = Arc::new(Mutex::new(None));
        let reset_event = Arc::new(Mutex::new(None));

        // Create enhanced StateHolder
        let component_id = format!("time_batch_window_{}", uuid::Uuid::new_v4());
        let state_holder = TimeBatchWindowStateHolder::new(
            Arc::clone(&buffer),
            Arc::clone(&expired),
            Arc::clone(&start_time),
            Arc::clone(&reset_event),
            component_id,
            duration_ms,
        );

        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            duration_ms,
            scheduler,
            buffer,
            expired,
            start_time,
            state_holder: Some(state_holder),
            reset_event,
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("TimeBatch window requires a parameter")?;
        if let Expression::Constant(c) = expr {
            let dur = match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Long(l) => *l,
                _ => return Err("TimeBatch window duration must be time constant".to_string()),
            };
            Ok(Self::new(dur, app_ctx, query_ctx))
        } else {
            Err("TimeBatch window duration must be constant".to_string())
        }
    }

    fn flush(&self, timestamp: i64) {
        if let Some(ref next) = self.meta.next_processor {
            let expired_batch: Vec<StreamEvent> = {
                let mut ex = self.expired.lock().unwrap();
                std::mem::take(&mut *ex)
            };
            let current_batch: Vec<StreamEvent> = {
                let mut buf = self.buffer.lock().unwrap();
                std::mem::take(&mut *buf)
            };

            if expired_batch.is_empty() && current_batch.is_empty() {
                return;
            }

            // Record batch flush for incremental checkpointing
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_batch_flushed(&current_batch, &expired_batch, timestamp);
            }

            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;

            // 1. Emit expired events first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. Emit RESET event to clear aggregator state before processing new batch
            // This follows Siddhi's pattern: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Clear reset event after use (will be recreated from first event of next batch)
                *reset_guard = None;
                // Record reset event cleared for incremental checkpointing
                if let Some(ref state_holder) = self.state_holder {
                    state_holder.record_reset_event_cleared();
                }
            }

            // 3. Emit current events
            for e in &current_batch {
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            {
                let mut ex = self.expired.lock().unwrap();
                ex.extend(current_batch);
            }

            if let Some(chain) = head {
                next.lock().unwrap().process(Some(chain));
            }
        }
        let old_start_time = {
            let mut start_time = self.start_time.lock().unwrap();
            let old = *start_time;
            *start_time = Some(timestamp);
            old
        };
        // Record start_time update for incremental checkpointing
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_start_time_updated(old_start_time, Some(timestamp));
        }
        if let Some(ref scheduler) = self.scheduler {
            let task = BatchFlushTask {
                buffer: Arc::clone(&self.buffer),
                expired: Arc::clone(&self.expired),
                next: self.meta.next_processor.as_ref().map(Arc::clone),
                duration_ms: self.duration_ms,
                scheduler: Arc::clone(scheduler),
                start_time: Arc::clone(&self.start_time),
                reset_event: Arc::clone(&self.reset_event),
                state_holder: self.state_holder.clone(),
            };
            scheduler.notify_at(timestamp + self.duration_ms, Arc::new(task));
        }
    }
}

impl Processor for TimeBatchWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(chunk) = complex_event_chunk {
            let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
            while let Some(ev) = current_opt {
                if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                    let mut start = self.start_time.lock().unwrap();
                    let old_start_time = *start;

                    if start.is_none() {
                        *start = Some(se.timestamp);

                        // Record start time change for incremental checkpointing
                        if let Some(ref state_holder) = self.state_holder {
                            state_holder
                                .record_start_time_updated(old_start_time, Some(se.timestamp));
                        }

                        if let Some(ref scheduler) = self.scheduler {
                            let task = BatchFlushTask {
                                buffer: Arc::clone(&self.buffer),
                                expired: Arc::clone(&self.expired),
                                next: self.meta.next_processor.as_ref().map(Arc::clone),
                                duration_ms: self.duration_ms,
                                scheduler: Arc::clone(scheduler),
                                start_time: Arc::clone(&self.start_time),
                                reset_event: Arc::clone(&self.reset_event),
                                state_holder: self.state_holder.clone(),
                            };
                            scheduler.notify_at(se.timestamp + self.duration_ms, Arc::new(task));
                        }
                    } else if se.timestamp - start.unwrap() >= self.duration_ms {
                        let ts = se.timestamp;
                        drop(start);
                        self.flush(ts);
                    }

                    let se_clone = se.clone_without_next();

                    // Create reset event from first event of each batch
                    // This will be emitted between expired and current events to reset aggregators
                    {
                        let mut reset_guard = self.reset_event.lock().unwrap();
                        if reset_guard.is_none() {
                            *reset_guard = Some(se_clone.clone());
                            // Record reset event set for incremental checkpointing
                            if let Some(ref state_holder) = self.state_holder {
                                state_holder.record_reset_event_set(&se_clone);
                            }
                        }
                    }

                    // Record state change for incremental checkpointing
                    if let Some(ref state_holder) = self.state_holder {
                        state_holder.record_event_added(&se_clone);
                    }

                    self.buffer.lock().unwrap().push(se_clone);
                }
                current_opt = ev.get_next();
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.duration_ms,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for TimeBatchWindowProcessor {}

impl NewStateHolder for TimeBatchWindowProcessor {
    fn schema_version(&self) -> SchemaVersion {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.schema_version()
        } else {
            SchemaVersion::new(1, 0, 0)
        }
    }

    fn serialize_state(&self, hints: &SerializationHints) -> Result<StateSnapshot, StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.serialize_state(hints)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for serialization".to_string(),
            })
        }
    }

    fn deserialize_state(&self, snapshot: &StateSnapshot) -> Result<(), StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.deserialize_state(snapshot)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for deserialization".to_string(),
            })
        }
    }

    fn get_changelog(&self, since: CheckpointId) -> Result<ChangeLog, StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.get_changelog(since)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for changelog".to_string(),
            })
        }
    }

    fn apply_changelog(&self, changes: &ChangeLog) -> Result<(), StateError> {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.apply_changelog(changes)
        } else {
            Err(StateError::InvalidStateData {
                message: "No state holder available for changelog application".to_string(),
            })
        }
    }

    fn estimate_size(&self) -> StateSize {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.estimate_size()
        } else {
            StateSize {
                bytes: 0,
                entries: 0,
                estimated_growth_rate: 0.0,
            }
        }
    }

    fn access_pattern(&self) -> AccessPattern {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.access_pattern()
        } else {
            AccessPattern::Sequential
        }
    }

    fn component_metadata(&self) -> StateMetadata {
        if let Some(ref state_holder) = self.state_holder {
            state_holder.component_metadata()
        } else {
            StateMetadata::new(
                "unknown_time_batch_window".to_string(),
                "TimeBatchWindowProcessor".to_string(),
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeBatchWindowFactory;

impl WindowProcessorFactory for TimeBatchWindowFactory {
    fn name(&self) -> &'static str {
        "timeBatch"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(
            TimeBatchWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ---- ExternalTimeWindowProcessor ----

#[derive(Debug)]
pub struct ExternalTimeWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
}

impl ExternalTimeWindowProcessor {
    pub fn new(
        duration_ms: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            duration_ms,
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .get(1)
            .ok_or("externalTime window requires a duration parameter")?;
        if let Expression::Constant(c) = expr {
            let dur = match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Long(l) => *l,
                _ => return Err("externalTime window duration must be time constant".to_string()),
            };
            Ok(Self::new(dur, app_ctx, query_ctx))
        } else {
            Err("externalTime window duration must be constant".to_string())
        }
    }
}

impl Processor for ExternalTimeWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        let ts = se.timestamp;
                        let mut expired_head: Option<Box<dyn ComplexEvent>> = None;
                        let mut tail = &mut expired_head;
                        {
                            let mut buf = self.buffer.lock().unwrap();
                            while let Some(front) = buf.front() {
                                if ts - front.timestamp >= self.duration_ms {
                                    if let Some(old) = buf.pop_front() {
                                        let mut ex = old.as_ref().clone_without_next();
                                        ex.set_event_type(ComplexEventType::Expired);
                                        ex.set_timestamp(ts);
                                        *tail = Some(Box::new(ex));
                                        tail = tail.as_mut().unwrap().mut_next_ref_option();
                                    }
                                } else {
                                    break;
                                }
                            }
                            buf.push_back(Arc::new(se.clone_without_next()));
                        }

                        *tail = Some(Box::new(se.clone_without_next()));
                        next.lock().unwrap().process(expired_head);
                    }
                    current_opt = ev.get_next();
                }
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.duration_ms,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for ExternalTimeWindowProcessor {}

#[derive(Debug, Clone)]
pub struct ExternalTimeWindowFactory;

impl WindowProcessorFactory for ExternalTimeWindowFactory {
    fn name(&self) -> &'static str {
        "externalTime"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(
            ExternalTimeWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ---- ExternalTimeBatchWindowProcessor ----

#[derive(Debug)]
pub struct ExternalTimeBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    start_time: Arc<Mutex<Option<i64>>>,
    /// Reset event template - emitted between expired and current to clear aggregator state
    reset_event: Arc<Mutex<Option<StreamEvent>>>,
}

impl ExternalTimeBatchWindowProcessor {
    pub fn new(
        duration_ms: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            duration_ms,
            buffer: Arc::new(Mutex::new(Vec::new())),
            expired: Arc::new(Mutex::new(Vec::new())),
            start_time: Arc::new(Mutex::new(None)),
            reset_event: Arc::new(Mutex::new(None)),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .get(1)
            .ok_or("externalTimeBatch window requires a duration parameter")?;
        if let Expression::Constant(c) = expr {
            let dur = match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Long(l) => *l,
                _ => {
                    return Err(
                        "externalTimeBatch window duration must be time constant".to_string()
                    )
                }
            };
            Ok(Self::new(dur, app_ctx, query_ctx))
        } else {
            Err("externalTimeBatch window duration must be constant".to_string())
        }
    }

    fn flush(&self, timestamp: i64) {
        if let Some(ref next) = self.meta.next_processor {
            let expired_batch: Vec<StreamEvent> = {
                let mut ex = self.expired.lock().unwrap();
                std::mem::take(&mut *ex)
            };
            let current_batch: Vec<StreamEvent> = {
                let mut buf = self.buffer.lock().unwrap();
                std::mem::take(&mut *buf)
            };

            if expired_batch.is_empty() && current_batch.is_empty() {
                return;
            }

            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;

            // 1. Emit expired events first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. Emit RESET event to clear aggregator state before processing new batch
            // This follows Siddhi's pattern: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Clear reset event after use (will be recreated from first event of next batch)
                *reset_guard = None;
            }

            // 3. Emit current events
            for e in &current_batch {
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            {
                let mut ex = self.expired.lock().unwrap();
                ex.extend(current_batch);
            }

            if let Some(chain) = head {
                next.lock().unwrap().process(Some(chain));
            }
        }
        *self.start_time.lock().unwrap() = Some(timestamp);
    }
}

impl Processor for ExternalTimeBatchWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(chunk) = complex_event_chunk {
            let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
            while let Some(ev) = current_opt {
                if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                    let ts = se.timestamp;
                    let mut start = self.start_time.lock().unwrap();
                    if start.is_none() {
                        *start = Some(ts);
                    }
                    while ts - start.unwrap() >= self.duration_ms {
                        let flush_ts = start.unwrap() + self.duration_ms;
                        drop(start);
                        self.flush(flush_ts);
                        start = self.start_time.lock().unwrap();
                    }
                    let se_clone = se.clone_without_next();
                    {
                        let mut buffer = self.buffer.lock().unwrap();
                        // Capture reset event template from first event of each batch
                        if buffer.is_empty() {
                            let mut reset_guard = self.reset_event.lock().unwrap();
                            *reset_guard = Some(se_clone.clone());
                        }
                        buffer.push(se_clone);
                    }
                }
                current_opt = ev.get_next();
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.duration_ms,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for ExternalTimeBatchWindowProcessor {}

// ---- LossyCountingWindowProcessor ----

#[derive(Debug)]
pub struct LossyCountingWindowProcessor {
    meta: CommonProcessorMeta,
}

impl LossyCountingWindowProcessor {
    pub fn new(app_ctx: Arc<EventFluxAppContext>, query_ctx: Arc<EventFluxQueryContext>) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
        }
    }

    pub fn from_handler(
        _handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        Ok(Self::new(app_ctx, query_ctx))
    }
}

impl Processor for LossyCountingWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            next.lock().unwrap().process(complex_event_chunk);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        false
    }
}

impl WindowProcessor for LossyCountingWindowProcessor {}

#[derive(Debug, Clone)]
pub struct LossyCountingWindowFactory;

impl WindowProcessorFactory for LossyCountingWindowFactory {
    fn name(&self) -> &'static str {
        "lossyCounting"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(
            LossyCountingWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ---- CronWindowProcessor ----

#[derive(Debug)]
pub struct CronWindowProcessor {
    meta: CommonProcessorMeta,
    cron: String,
    scheduler: Option<Arc<Scheduler>>,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    scheduled: Arc<Mutex<bool>>,
}

impl CronWindowProcessor {
    pub fn new(
        cron: String,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let scheduler = app_ctx.get_scheduler();
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            cron,
            scheduler,
            buffer: Arc::new(Mutex::new(Vec::new())),
            expired: Arc::new(Mutex::new(Vec::new())),
            scheduled: Arc::new(Mutex::new(false)),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("cron window requires a cron expression")?;
        if let Expression::Constant(c) = expr {
            if let ConstantValueWithFloat::String(s) = &c.value {
                Ok(Self::new(s.clone(), app_ctx, query_ctx))
            } else {
                Err("cron expression must be a string".to_string())
            }
        } else {
            Err("cron expression must be constant".to_string())
        }
    }

    fn schedule(&self) {
        if let Some(ref sched) = self.scheduler {
            if !*self.scheduled.lock().unwrap() {
                let task = CronFlushTask {
                    buffer: Arc::clone(&self.buffer),
                    expired: Arc::clone(&self.expired),
                    next: self.meta.next_processor.as_ref().map(Arc::clone),
                };
                let _ = sched.schedule_cron(&self.cron, Arc::new(task), None, None);
                *self.scheduled.lock().unwrap() = true;
            }
        }
    }
}

#[derive(Clone)]
struct CronFlushTask {
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    next: Option<Arc<Mutex<dyn Processor>>>,
}

impl Schedulable for CronFlushTask {
    fn on_time(&self, timestamp: i64) {
        let expired_batch: Vec<StreamEvent> = {
            let mut ex = self.expired.lock().unwrap();
            std::mem::take(&mut *ex)
        };
        let current_batch: Vec<StreamEvent> = {
            let mut buf = self.buffer.lock().unwrap();
            std::mem::take(&mut *buf)
        };

        if expired_batch.is_empty() && current_batch.is_empty() {
            return;
        }

        let mut head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail = &mut head;

        for mut e in expired_batch {
            e.set_event_type(ComplexEventType::Expired);
            e.set_timestamp(timestamp);
            *tail = Some(Box::new(e.clone_without_next()));
            tail = tail.as_mut().unwrap().mut_next_ref_option();
        }

        for e in &current_batch {
            *tail = Some(Box::new(e.clone_without_next()));
            tail = tail.as_mut().unwrap().mut_next_ref_option();
        }

        {
            let mut ex = self.expired.lock().unwrap();
            ex.extend(current_batch);
        }

        if let Some(chain) = head {
            if let Some(ref next) = self.next {
                next.lock().unwrap().process(Some(chain));
            }
        }
    }
}

impl Processor for CronWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(chunk) = complex_event_chunk {
            let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
            while let Some(ev) = current_opt {
                if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                    self.buffer.lock().unwrap().push(se.clone_without_next());
                }
                current_opt = ev.get_next();
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
        self.schedule();
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.cron.clone(),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }
    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for CronWindowProcessor {}

#[derive(Debug, Clone)]
pub struct CronWindowFactory;

impl WindowProcessorFactory for CronWindowFactory {
    fn name(&self) -> &'static str {
        "cron"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(CronWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct ExternalTimeBatchWindowFactory;

impl WindowProcessorFactory for ExternalTimeBatchWindowFactory {
    fn name(&self) -> &'static str {
        "externalTimeBatch"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(
            ExternalTimeBatchWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct SessionWindowFactory;

impl WindowProcessorFactory for SessionWindowFactory {
    fn name(&self) -> &'static str {
        "session"
    }
    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(SessionWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct SortWindowFactory;

impl WindowProcessorFactory for SortWindowFactory {
    fn name(&self) -> &'static str {
        "sort"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(SortWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// HELPER FUNCTIONS FOR WINDOW PROCESSORS
// ============================================================================

/// Resolve attribute name expressions to their indices in before_window_data.
fn resolve_key_indices(
    params: &[Expression],
    parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
) -> Result<Vec<usize>, String> {
    if params.is_empty() {
        return Ok(Vec::new());
    }
    let meta = parse_ctx.stream_meta_map.get(&parse_ctx.default_source)
        .ok_or_else(|| format!("No stream metadata for '{}'", parse_ctx.default_source))?;
    let mut indices = Vec::new();
    for param in params {
        if let Expression::Variable(var) = param {
            let (idx, _) = meta.find_attribute_info(&var.attribute_name)
                .ok_or_else(|| format!("Attribute '{}' not found in stream", var.attribute_name))?;
            indices.push(*idx);
        }
    }
    Ok(indices)
}

/// Generate a composite key from stream attribute values.
/// If `key_indices` is non-empty, uses only those attribute positions.
/// Otherwise, uses all attributes from before_window_data.
/// Uses a null byte separator to avoid collisions.
fn generate_composite_key(event: &StreamEvent, key_indices: &[usize]) -> String {
    let mut buf = String::new();
    if key_indices.is_empty() {
        for (i, v) in event.before_window_data.iter().enumerate() {
            if i > 0 { buf.push('\0'); }
            let _ = write!(buf, "{}", v);
        }
    } else {
        let mut first = true;
        for &idx in key_indices {
            if let Some(v) = event.before_window_data.get(idx) {
                if !first { buf.push('\0'); }
                let _ = write!(buf, "{}", v);
                first = false;
            }
        }
    }
    buf
}

// ============================================================================
// UNIQUE WINDOW PROCESSOR
// Keeps only one event per unique key value (the latest occurrence)
// Reference: Similar to Siddhi's unique extension
// ============================================================================

/// UniqueWindowProcessor keeps only the latest event per unique key value.
/// When a new event arrives with a key that already exists, the old event is expired.
/// Supports composite keys (e.g., `unique(symbol, exchange)`).
///
/// **Warning**: The `unique_map` grows with each distinct key value seen. For
/// high-cardinality key spaces (e.g., UUIDs), this will consume increasing memory.
/// This matches Java Siddhi's unique window behavior.
#[derive(Debug)]
pub struct UniqueWindowProcessor {
    meta: CommonProcessorMeta,
    /// Resolved indices of key attributes in before_window_data
    key_attribute_indices: Vec<usize>,
    /// Map from key value to stored event
    unique_map: Arc<Mutex<HashMap<String, Arc<StreamEvent>>>>,
}

impl UniqueWindowProcessor {
    pub fn new(
        key_attribute_indices: Vec<usize>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            key_attribute_indices,
            unique_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();
        if params.is_empty() {
            return Err("Unique window requires at least one attribute parameter".to_string());
        }
        let key_attribute_indices = resolve_key_indices(params, parse_ctx)?;
        if key_attribute_indices.is_empty() {
            return Err("Unique window requires at least one attribute parameter".to_string());
        }
        Ok(Self::new(key_attribute_indices, app_ctx, query_ctx))
    }
}

impl Processor for UniqueWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut output_head: Option<Box<dyn ComplexEvent>> = None;
                let mut output_tail = &mut output_head;
                let mut map = self.unique_map.lock().unwrap();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        let key = generate_composite_key(se, &self.key_attribute_indices);

                        if let Some(old_event) = map.insert(key, Arc::new(se.clone_without_next())) {
                            // Expire the old event with the same key
                            let mut ex = old_event.as_ref().clone_without_next();
                            ex.set_event_type(ComplexEventType::Expired);
                            ex.set_timestamp(se.timestamp);
                            *output_tail = Some(Box::new(ex));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        }

                        *output_tail = Some(Box::new(se.clone_without_next()));
                        output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                    }
                    current_opt = ev.get_next();
                }
                drop(map);

                if let Some(chain) = output_head {
                    next.lock().unwrap().process(Some(chain));
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.key_attribute_indices.clone(),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for UniqueWindowProcessor {}

#[derive(Debug, Clone)]
pub struct UniqueWindowFactory;

impl WindowProcessorFactory for UniqueWindowFactory {
    fn name(&self) -> &'static str {
        "unique"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(UniqueWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// FIRST UNIQUE WINDOW PROCESSOR
// Keeps only the first occurrence per unique key value
// Reference: Siddhi's firstUnique extension
// ============================================================================

/// FirstUniqueWindowProcessor keeps only the first event per unique key value.
/// Subsequent events with the same key are ignored (not even emitted).
/// Supports composite keys (e.g., `firstUnique(user_id, session_id)`).
///
/// **Warning**: The `seen_keys` set grows unbounded over the lifetime of the window.
/// For high-cardinality key spaces (e.g., UUIDs, transaction IDs), this will
/// consume increasing memory. This matches Java Siddhi's firstUnique behavior.
#[derive(Debug)]
pub struct FirstUniqueWindowProcessor {
    meta: CommonProcessorMeta,
    /// Resolved indices of key attributes in before_window_data
    key_attribute_indices: Vec<usize>,
    /// Set of keys we've already seen
    seen_keys: Arc<Mutex<HashSet<String>>>,
}

impl FirstUniqueWindowProcessor {
    pub fn new(
        key_attribute_indices: Vec<usize>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            key_attribute_indices,
            seen_keys: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();
        if params.is_empty() {
            return Err("FirstUnique window requires at least one attribute parameter".to_string());
        }
        let key_attribute_indices = resolve_key_indices(params, parse_ctx)?;
        if key_attribute_indices.is_empty() {
            return Err("FirstUnique window requires at least one attribute parameter".to_string());
        }
        Ok(Self::new(key_attribute_indices, app_ctx, query_ctx))
    }
}

impl Processor for FirstUniqueWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut output_head: Option<Box<dyn ComplexEvent>> = None;
                let mut output_tail = &mut output_head;
                let mut keys = self.seen_keys.lock().unwrap();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        let key = generate_composite_key(se, &self.key_attribute_indices);

                        if keys.insert(key) {
                            *output_tail = Some(Box::new(se.clone_without_next()));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        }
                    }
                    current_opt = ev.get_next();
                }
                drop(keys);

                if let Some(chain) = output_head {
                    next.lock().unwrap().process(Some(chain));
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.key_attribute_indices.clone(),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for FirstUniqueWindowProcessor {}

#[derive(Debug, Clone)]
pub struct FirstUniqueWindowFactory;

impl WindowProcessorFactory for FirstUniqueWindowFactory {
    fn name(&self) -> &'static str {
        "firstUnique"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(FirstUniqueWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// DELAY WINDOW PROCESSOR
// Holds events for a specific delay period before processing them
// Reference: DelayWindowProcessor.java
// ============================================================================

/// DelayWindowProcessor delays event emission by a specified time period.
/// Each event is individually scheduled for emission after the delay.
#[derive(Debug)]
pub struct DelayWindowProcessor {
    meta: CommonProcessorMeta,
    /// Delay duration in milliseconds
    delay_ms: i64,
    scheduler: Option<Arc<Scheduler>>,
}

#[derive(Clone)]
struct DelayEmitTask {
    event: Arc<StreamEvent>,
    next: Option<Arc<Mutex<dyn Processor>>>,
}

impl Schedulable for DelayEmitTask {
    fn on_time(&self, timestamp: i64) {
        let mut event = self.event.as_ref().clone_without_next();
        event.set_timestamp(timestamp);

        if let Some(ref next) = self.next {
            next.lock().unwrap().process(Some(Box::new(event)));
        }
    }
}

impl DelayWindowProcessor {
    pub fn new(
        delay_ms: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let scheduler = app_ctx.get_scheduler();
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            delay_ms,
            scheduler,
        }
    }

    fn new_cloned(
        delay_ms: i64,
        scheduler: Arc<Scheduler>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            delay_ms,
            scheduler: Some(scheduler),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("Delay window requires a delay duration parameter")?;

        if let Expression::Constant(c) = expr {
            let delay = match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Int(i) => *i as i64,
                ConstantValueWithFloat::Long(l) => *l,
                _ => return Err("Delay window duration must be time or integer".to_string()),
            };
            let processor = Self::new(delay, app_ctx, query_ctx);
            if processor.scheduler.is_none() {
                return Err("Delay window requires a scheduler (not available in this context)".to_string());
            }
            Ok(processor)
        } else {
            Err("Delay window duration must be constant".to_string())
        }
    }
}

impl Processor for DelayWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref scheduler) = self.scheduler {
                if let Some(ref chunk) = complex_event_chunk {
                    let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                    while let Some(ev) = current_opt {
                        if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                            let task = DelayEmitTask {
                                event: Arc::new(se.clone_without_next()),
                                next: Some(Arc::clone(next)),
                            };
                            scheduler.notify_at(se.timestamp + self.delay_ms, Arc::new(task));
                        }
                        current_opt = ev.get_next();
                    }
                }
            } else {
                next.lock().unwrap().process(complex_event_chunk);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        let scheduler = self.scheduler.as_ref()
            .expect("DelayWindowProcessor invariant violated: scheduler missing during clone")
            .clone();
        Box::new(Self::new_cloned(
            self.delay_ms,
            scheduler,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for DelayWindowProcessor {}

#[derive(Debug, Clone)]
pub struct DelayWindowFactory;

impl WindowProcessorFactory for DelayWindowFactory {
    fn name(&self) -> &'static str {
        "delay"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(DelayWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// FREQUENT WINDOW PROCESSOR
// Tracks most frequently occurring events using Misra-Gries algorithm
// Reference: FrequentWindowProcessor.java
// ============================================================================

/// FrequentWindowProcessor tracks the most frequently occurring events.
/// Uses a counting algorithm based on Misra-Gries counting algorithm.
///
/// Usage: `WINDOW('frequent', count)` groups by all attributes.
/// `WINDOW('frequent', count, attr1, attr2)` groups by specific attributes.
#[derive(Debug)]
pub struct FrequentWindowProcessor {
    meta: CommonProcessorMeta,
    /// Number of most frequent events to track
    most_frequent_count: usize,
    /// Attribute indices for key generation (empty = use all attributes)
    key_attribute_indices: Vec<usize>,
    state: Arc<Mutex<FrequentWindowState>>,
}

#[derive(Debug)]
struct FrequentWindowState {
    event_map: HashMap<String, Arc<StreamEvent>>,
    count_map: HashMap<String, usize>,
}

impl FrequentWindowProcessor {
    pub fn new(
        most_frequent_count: usize,
        key_attribute_indices: Vec<usize>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            most_frequent_count,
            key_attribute_indices,
            state: Arc::new(Mutex::new(FrequentWindowState {
                event_map: HashMap::new(),
                count_map: HashMap::new(),
            })),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();
        let expr = params
            .first()
            .ok_or("Frequent window requires event count parameter")?;

        let count = if let Expression::Constant(c) = expr {
            match &c.value {
                ConstantValueWithFloat::Int(i) => *i as usize,
                ConstantValueWithFloat::Long(l) => *l as usize,
                _ => return Err("Frequent window count must be integer".to_string()),
            }
        } else {
            return Err("Frequent window count must be constant".to_string());
        };

        // Resolve optional key attribute indices from remaining parameters
        let key_attribute_indices = resolve_key_indices(&params[1..], parse_ctx)?;

        Ok(Self::new(count, key_attribute_indices, app_ctx, query_ctx))
    }
}

impl Processor for FrequentWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut output_head: Option<Box<dyn ComplexEvent>> = None;
                let mut output_tail = &mut output_head;
                let mut state = self.state.lock().unwrap();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        let key = generate_composite_key(se, &self.key_attribute_indices);
                        let cloned = Arc::new(se.clone_without_next());

                        if let Some(old) = state.event_map.insert(key.clone(), Arc::clone(&cloned)) {
                            // Key exists — replacement policy: expire old, emit new
                            *state.count_map.entry(key).or_insert(0) += 1;
                            let mut ex = old.as_ref().clone_without_next();
                            ex.set_event_type(ComplexEventType::Expired);
                            ex.set_timestamp(se.timestamp);
                            *output_tail = Some(Box::new(ex));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            *output_tail = Some(Box::new(se.clone_without_next()));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        } else {
                            // New key
                            if state.event_map.len() > self.most_frequent_count {
                                // Over capacity — decrement all counts, evict zeros
                                let mut keys_to_remove = Vec::new();
                                for (k, count) in state.count_map.iter_mut() {
                                    if *count <= 1 {
                                        keys_to_remove.push(k.clone());
                                    } else {
                                        *count -= 1;
                                    }
                                }
                                for k in &keys_to_remove {
                                    state.count_map.remove(k);
                                    if let Some(expired) = state.event_map.remove(k) {
                                        let mut ex = expired.as_ref().clone_without_next();
                                        ex.set_event_type(ComplexEventType::Expired);
                                        ex.set_timestamp(se.timestamp);
                                        *output_tail = Some(Box::new(ex));
                                        output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                                    }
                                }

                                if state.event_map.len() > self.most_frequent_count {
                                    // Still over limit, remove the new event
                                    state.event_map.remove(&key);
                                } else {
                                    state.count_map.insert(key, 1);
                                    *output_tail = Some(Box::new(se.clone_without_next()));
                                    output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                                }
                            } else {
                                state.count_map.insert(key, 1);
                                *output_tail = Some(Box::new(se.clone_without_next()));
                                output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            }
                        }
                    }
                    current_opt = ev.get_next();
                }
                drop(state);

                if let Some(chain) = output_head {
                    next.lock().unwrap().process(Some(chain));
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.most_frequent_count,
            self.key_attribute_indices.clone(),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for FrequentWindowProcessor {}

#[derive(Debug, Clone)]
pub struct FrequentWindowFactory;

impl WindowProcessorFactory for FrequentWindowFactory {
    fn name(&self) -> &'static str {
        "frequent"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(FrequentWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// LOSSY FREQUENT WINDOW PROCESSOR
// Approximate frequent item tracking using Lossy Counting algorithm
// Reference: LossyFrequentWindowProcessor.java
// ============================================================================

/// LossyCount tracks count and bucket ID for lossy counting algorithm
#[derive(Debug, Clone)]
struct LossyCount {
    count: usize,
    bucket_id: usize,
}

/// LossyFrequentWindowProcessor identifies events whose frequency exceeds
/// a support threshold using the Lossy Counting algorithm.
#[derive(Debug)]
pub struct LossyFrequentWindowProcessor {
    meta: CommonProcessorMeta,
    /// Support threshold (0.0 - 1.0)
    support: f64,
    /// Error bound (0.0 - 1.0)
    error: f64,
    /// Window width = ceil(1/error), stored as integer for precision
    window_width: usize,
    /// Attribute indices for key generation (empty = use all attributes)
    key_attribute_indices: Vec<usize>,
    state: Arc<Mutex<LossyFrequentWindowState>>,
}

#[derive(Debug)]
struct LossyFrequentWindowState {
    total_count: usize,
    current_bucket_id: usize,
    count_map: HashMap<String, LossyCount>,
    event_map: HashMap<String, Arc<StreamEvent>>,
}

impl LossyFrequentWindowProcessor {
    pub fn new(
        support: f64,
        error: f64,
        key_attribute_indices: Vec<usize>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let window_width = (1.0 / error).ceil() as usize;
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            support,
            error,
            window_width,
            key_attribute_indices,
            state: Arc::new(Mutex::new(LossyFrequentWindowState {
                total_count: 0,
                current_bucket_id: 1,
                count_map: HashMap::new(),
                event_map: HashMap::new(),
            })),
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();

        let support = match params.first() {
            Some(Expression::Constant(c)) => match &c.value {
                ConstantValueWithFloat::Float(f) => *f as f64,
                ConstantValueWithFloat::Double(d) => *d,
                _ => return Err("LossyFrequent support threshold must be a float".to_string()),
            },
            _ => return Err("LossyFrequent window requires support threshold".to_string()),
        };

        // Find where key attributes start (after support and optional error params)
        let (error, key_params_start) = if params.len() > 1 {
            match &params[1] {
                Expression::Constant(c) => match &c.value {
                    ConstantValueWithFloat::Float(f) => (*f as f64, 2),
                    ConstantValueWithFloat::Double(d) => (*d, 2),
                    _ => (support / 10.0, 1),
                },
                Expression::Variable(_) => (support / 10.0, 1),
                _ => (support / 10.0, 1),
            }
        } else {
            (support / 10.0, 1)
        };

        if !(0.0..=1.0).contains(&support) || error <= 0.0 || error > 1.0 {
            return Err("Support must be between 0 and 1, error must be > 0 and <= 1".to_string());
        }

        // Resolve optional key attribute indices from remaining parameters
        let key_attribute_indices = resolve_key_indices(&params[key_params_start..], parse_ctx)?;

        Ok(Self::new(support, error, key_attribute_indices, app_ctx, query_ctx))
    }
}

impl Processor for LossyFrequentWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut output_head: Option<Box<dyn ComplexEvent>> = None;
                let mut output_tail = &mut output_head;
                let mut state = self.state.lock().unwrap();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        state.total_count += 1;
                        if state.total_count > 1 {
                            state.current_bucket_id = state.total_count.div_ceil(self.window_width);
                        }

                        let key = generate_composite_key(se, &self.key_attribute_indices);
                        let cloned = Arc::new(se.clone_without_next());

                        // Update counts (always, independent of window membership)
                        if let Some(lc) = state.count_map.get_mut(&key) {
                            lc.count += 1;
                        } else {
                            let bucket_id = state.current_bucket_id.saturating_sub(1);
                            state.count_map.insert(key.clone(), LossyCount {
                                count: 1,
                                bucket_id,
                            });
                        }

                        // Check threshold to decide window membership
                        let threshold = (self.support - self.error) * (state.total_count as f64);
                        let meets_threshold = state.count_map.get(&key)
                            .map_or(false, |lc| lc.count as f64 >= threshold);
                        let was_in_window = state.event_map.contains_key(&key);

                        if meets_threshold {
                            if was_in_window {
                                // Replace: expire old, emit new
                                let old = state.event_map.insert(key, Arc::clone(&cloned)).unwrap();
                                let mut ex = old.as_ref().clone_without_next();
                                ex.set_event_type(ComplexEventType::Expired);
                                ex.set_timestamp(se.timestamp);
                                *output_tail = Some(Box::new(ex));
                                output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            } else {
                                // New entry into window
                                state.event_map.insert(key, Arc::clone(&cloned));
                            }
                            *output_tail = Some(Box::new(se.clone_without_next()));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        } else if was_in_window {
                            // Fell below threshold — expire from window
                            if let Some(old) = state.event_map.remove(&key) {
                                let mut ex = old.as_ref().clone_without_next();
                                ex.set_event_type(ComplexEventType::Expired);
                                ex.set_timestamp(se.timestamp);
                                *output_tail = Some(Box::new(ex));
                                output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            }
                        }

                        // Prune if at window boundary
                        if state.total_count % self.window_width == 0 {
                            let bucket_id = state.current_bucket_id;
                            let keys_to_remove: Vec<String> = state.count_map.iter()
                                .filter(|(_, lc)| lc.count + lc.bucket_id <= bucket_id)
                                .map(|(k, _)| k.clone())
                                .collect();
                            for k in keys_to_remove {
                                state.count_map.remove(&k);
                                if let Some(expired) = state.event_map.remove(&k) {
                                    let mut ex = expired.as_ref().clone_without_next();
                                    ex.set_event_type(ComplexEventType::Expired);
                                    ex.set_timestamp(se.timestamp);
                                    *output_tail = Some(Box::new(ex));
                                    output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                                }
                            }
                        }
                    }
                    current_opt = ev.get_next();
                }
                drop(state);

                if let Some(chain) = output_head {
                    next.lock().unwrap().process(Some(chain));
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.support,
            self.error,
            self.key_attribute_indices.clone(),
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for LossyFrequentWindowProcessor {}

#[derive(Debug, Clone)]
pub struct LossyFrequentWindowFactory;

impl WindowProcessorFactory for LossyFrequentWindowFactory {
    fn name(&self) -> &'static str {
        "lossyFrequent"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(LossyFrequentWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// EXPRESSION WINDOW PROCESSOR
// Count-based sliding window parsed from an expression string
// Reference: ExpressionWindowProcessor.java (simplified version)
// ============================================================================

/// ExpressionWindowProcessor is a sliding window sized by a count expression
/// parsed at construction time (e.g., `count() <= 20` keeps at most 20 events).
///
/// **Supported expressions**: `count() <= N` and `count() < N` only.
/// The expression is parsed once during initialization — it is NOT evaluated
/// dynamically per event. Unsupported expressions are rejected at construction.
#[derive(Debug)]
pub struct ExpressionWindowProcessor {
    meta: CommonProcessorMeta,
    /// The expression string (e.g., "count() <= 20")
    expression: String,
    /// Max count extracted from simple expressions
    max_count: Option<usize>,
    /// Buffer of events
    buffer: Arc<Mutex<VecDeque<Arc<StreamEvent>>>>,
}

impl ExpressionWindowProcessor {
    pub fn new(
        expression: String,
        max_count: Option<usize>,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            expression,
            max_count,
            buffer: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn parse_count_expression(expr: &str) -> Option<usize> {
        // Simple regex-free parsing for "count() <= N" or "count() < N"
        // Remove all whitespace to tolerate "count( ) <= 5" or extra spacing
        let expr = expr.replace(' ', "").to_lowercase();
        if expr.starts_with("count()") {
            let rest = expr.trim_start_matches("count()").trim();
            if rest.starts_with("<=") {
                rest.trim_start_matches("<=").trim().parse().ok()
            } else if rest.starts_with("<") {
                rest.trim_start_matches("<").trim().parse::<usize>().ok()
                    .map(|n| n.saturating_sub(1))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let expr = handler
            .get_parameters()
            .first()
            .ok_or("Expression window requires an expression parameter")?;

        if let Expression::Constant(c) = expr {
            if let ConstantValueWithFloat::String(s) = &c.value {
                let max_count = Self::parse_count_expression(s);
                if max_count.is_none() {
                    return Err(format!(
                        "Unsupported expression window condition: '{}'. \
                         Currently supported: 'count() <= N' and 'count() < N'",
                        s
                    ));
                }
                Ok(Self::new(s.clone(), max_count, app_ctx, query_ctx))
            } else {
                Err("Expression must be a string".to_string())
            }
        } else {
            Err("Expression window parameter must be constant string".to_string())
        }
    }
}

impl Processor for ExpressionWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(ref chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut output_head: Option<Box<dyn ComplexEvent>> = None;
                let mut output_tail = &mut output_head;

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        {
                            let mut buf = self.buffer.lock().unwrap();
                            buf.push_back(Arc::new(se.clone_without_next()));

                            // Expire events that violate the expression
                            if let Some(max) = self.max_count {
                                while buf.len() > max {
                                    if let Some(old) = buf.pop_front() {
                                        let mut ex = old.as_ref().clone_without_next();
                                        ex.set_event_type(ComplexEventType::Expired);
                                        ex.set_timestamp(se.timestamp);
                                        *output_tail = Some(Box::new(ex));
                                        output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                                    }
                                }
                            }
                        }

                        // Add current event
                        *output_tail = Some(Box::new(se.clone_without_next()));
                        output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                    }
                    current_opt = ev.get_next();
                }

                if let Some(chain) = output_head {
                    next.lock().unwrap().process(Some(chain));
                }
            } else {
                next.lock().unwrap().process(None);
            }
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next;
    }

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.expression.clone(),
            self.max_count,
            Arc::clone(&self.meta.eventflux_app_context),
            Arc::clone(query_ctx),
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::SLIDE
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl WindowProcessor for ExpressionWindowProcessor {}

#[derive(Debug, Clone)]
pub struct ExpressionWindowFactory;

impl WindowProcessorFactory for ExpressionWindowFactory {
    fn name(&self) -> &'static str {
        "expression"
    }

    fn create(
        &self,
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Arc<Mutex<dyn Processor>>, String> {
        Ok(Arc::new(Mutex::new(ExpressionWindowProcessor::from_handler(
            handler, app_ctx, query_ctx, parse_ctx,
        )?)))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}
