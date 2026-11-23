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

// StateHolder trait and its types, for checkpointing window state
use crate::core::persistence::state_holder::{
    AccessPattern, ChangeLog, CheckpointId, SchemaVersion, SerializationHints, StateError,
    StateHolder as NewStateHolder, StateMetadata, StateSize, StateSnapshot,
};

// Window type name constants
pub mod types;

// Shared changelog replay helpers
pub(crate) mod changelog_helpers;

// Session window processor
mod session_window_processor;
use session_window_processor::SessionWindowProcessor;

// Session window state holder
mod session_window_state_holder;

// Sort window processor
mod sort_window_processor;
use sort_window_processor::SortWindowProcessor;

// Length window state holder
mod length_window_state_holder;
use length_window_state_holder::LengthWindowStateHolder;

// Time window state holder
mod time_window_state_holder;
use time_window_state_holder::TimeWindowStateHolder;

// Length batch window state holder
mod length_batch_window_state_holder;
use length_batch_window_state_holder::LengthBatchWindowStateHolder;

// Time batch window state holder
mod time_batch_window_state_holder;
use time_batch_window_state_holder::TimeBatchWindowStateHolder;

// External time window state holder
mod external_time_window_state_holder;

/// Marker trait for processors that are windows
pub trait WindowProcessor: Processor {}

/// Sliding window over the last `length` events. Each new event pushes the oldest one
/// out as an expired event once the window is full.
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

        // The state holder shares the buffer, so snapshots see the live window
        let component_id = format!("length_window_{}_{}", query_ctx.get_name(), length);
        let state_holder = Arc::new(LengthWindowStateHolder::new(
            Arc::clone(&buffer),
            component_id.clone(),
            length,
        ));

        // Register with the SnapshotService, when there is one, so it is checkpointed
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

            // Log the expiry for incremental checkpoints
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
    /// Reset event template. Sent between the expired and current events so
    /// aggregators clear their state.
    reset_event: Arc<Mutex<Option<StreamEvent>>>,
    /// Logs changes for incremental checkpoints
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
            // Log the new start time for incremental checkpoints
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_start_time_updated(old_start_time, Some(timestamp));
            }
            self.scheduler
                .notify_at(timestamp + self.duration_ms, Arc::new(self.clone()));
            return;
        }

        // Log the flush before emitting anything, so a recovery replays it in the right
        // order
        if let Some(ref state_holder) = self.state_holder {
            state_holder.record_batch_flushed(&current_batch, &expired_batch, timestamp);
        }

        let mut head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail = &mut head;

        // 1. Expired events go first
        for mut e in expired_batch {
            e.set_event_type(ComplexEventType::Expired);
            e.set_timestamp(timestamp);
            *tail = Some(Box::new(e.clone_without_next()));
            tail = tail.as_mut().unwrap().mut_next_ref_option();
        }

        // 2. A RESET event, so aggregators clear their state before the new batch.
        // Same order as Siddhi: EXPIRED → RESET → CURRENT
        {
            let mut reset_guard = self.reset_event.lock().unwrap();
            if let Some(ref reset_template) = *reset_guard {
                let mut reset_ev = reset_template.clone_without_next();
                reset_ev.set_event_type(ComplexEventType::Reset);
                reset_ev.set_timestamp(timestamp);
                *tail = Some(Box::new(reset_ev));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }
            // Used up. The first event of the next batch becomes the new template.
            *reset_guard = None;
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_reset_event_cleared();
            }
        }

        // 3. Current events go last
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
        // Log the new start time for incremental checkpoints
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

                            // Log the add for incremental checkpoints
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
    if let Some(factory) = app_ctx
        .get_eventflux_context()
        .get_window_factory(&handler.name)
    {
        factory.create(handler, app_ctx, query_ctx, parse_ctx)
    } else {
        // List the registered window names in the error to help the user
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

/// Batch window that collects `length` events and then emits them together.
#[derive(Debug)]
pub struct LengthBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub length: usize,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    state_holder: Option<LengthBatchWindowStateHolder>,
    /// Reset event template, copied from the first event of a batch. Sent between the
    /// expired and current events.
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

        // The state holder shares the buffers, so snapshots see the live window
        let component_id = format!("length_batch_window_{}_{}", query_ctx.get_name(), length);
        let state_holder = LengthBatchWindowStateHolder::new(
            Arc::clone(&buffer),
            Arc::clone(&expired),
            Arc::clone(&reset_event),
            component_id.clone(),
            length,
        );

        // Register with the SnapshotService, when there is one, so it is checkpointed
        let state_holder_clone = state_holder.clone();
        let state_holder_arc: Arc<Mutex<dyn crate::core::persistence::StateHolder>> =
            Arc::new(Mutex::new(state_holder_clone));
        if let Some(snapshot_service) = app_ctx.get_snapshot_service() {
            snapshot_service.register_state_holder(component_id.clone(), state_holder_arc);
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

            // Log the flush for incremental checkpoints
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_batch_flushed(&current_batch, &expired_batch);
            }

            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;

            // 1. Expired events go first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. A RESET event, so aggregators clear their state before the new batch.
            // Same order as Siddhi: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Used up. The first event of the next batch becomes the new template.
                *reset_guard = None;
                // Log the clear for incremental checkpoints
                if let Some(ref state_holder) = self.state_holder {
                    state_holder.record_reset_event_cleared();
                }
            }

            // 3. Current events go last
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

                    // Log the add for incremental checkpoints
                    if let Some(ref state_holder) = self.state_holder {
                        state_holder.record_event_added(&se_clone);
                    }

                    // Hold the buffer lock across the check and the push, so two threads
                    // cannot both see a non full batch
                    let last_ts = se.timestamp;
                    let should_flush = {
                        let mut buffer = self.buffer.lock().unwrap();
                        // The first event of a batch becomes the reset template
                        if buffer.is_empty() {
                            let mut reset_guard = self.reset_event.lock().unwrap();
                            *reset_guard = Some(se_clone.clone());
                            // Log the new template for incremental checkpoints
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

/// Batch window that collects events for `duration_ms` and then emits them together.
/// A batch is flushed by the scheduler, or by the first event that arrives after the
/// batch time is up.
#[derive(Debug)]
pub struct TimeBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    scheduler: Option<Arc<Scheduler>>,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    start_time: Arc<Mutex<Option<i64>>>,
    state_holder: Option<TimeBatchWindowStateHolder>,
    /// Reset event template, copied from the first event of a batch. Sent between the
    /// expired and current events so aggregators clear their state before the new batch.
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

        // The state holder shares the buffers. Its id is a random UUID and it is not
        // registered with the SnapshotService here.
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

            // Log the flush for incremental checkpoints
            if let Some(ref state_holder) = self.state_holder {
                state_holder.record_batch_flushed(&current_batch, &expired_batch, timestamp);
            }

            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;

            // 1. Expired events go first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. A RESET event, so aggregators clear their state before the new batch.
            // Same order as Siddhi: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Used up. The first event of the next batch becomes the new template.
                *reset_guard = None;
                // Log the clear for incremental checkpoints
                if let Some(ref state_holder) = self.state_holder {
                    state_holder.record_reset_event_cleared();
                }
            }

            // 3. Current events go last
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
        // Log the new start time for incremental checkpoints
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

                        // Log the new start time for incremental checkpoints
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

                    // The first event of each batch becomes the reset template, sent between
                    // the expired and current events to reset aggregators
                    {
                        let mut reset_guard = self.reset_event.lock().unwrap();
                        if reset_guard.is_none() {
                            *reset_guard = Some(se_clone.clone());
                            // Log the new template for incremental checkpoints
                            if let Some(ref state_holder) = self.state_holder {
                                state_holder.record_reset_event_set(&se_clone);
                            }
                        }
                    }

                    // Log the add for incremental checkpoints
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

/// Sliding time window driven by event timestamps instead of the clock. Each event
/// expires the buffered events that are `duration_ms` or more older than it. The
/// duration is the second handler parameter.
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

/// Batch window driven by event timestamps. A batch closes when an event arrives
/// `duration_ms` or more after the batch start. The duration is the second handler
/// parameter. Keeps no state holder, so it is not checkpointed.
#[derive(Debug)]
pub struct ExternalTimeBatchWindowProcessor {
    meta: CommonProcessorMeta,
    pub duration_ms: i64,
    buffer: Arc<Mutex<Vec<StreamEvent>>>,
    expired: Arc<Mutex<Vec<StreamEvent>>>,
    start_time: Arc<Mutex<Option<i64>>>,
    /// Reset event template. Sent between the expired and current events so
    /// aggregators clear their state.
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

            // 1. Expired events go first
            for mut e in expired_batch {
                e.set_event_type(ComplexEventType::Expired);
                e.set_timestamp(timestamp);
                *tail = Some(Box::new(e.clone_without_next()));
                tail = tail.as_mut().unwrap().mut_next_ref_option();
            }

            // 2. A RESET event, so aggregators clear their state before the new batch.
            // Same order as Siddhi: EXPIRED → RESET → CURRENT
            {
                let mut reset_guard = self.reset_event.lock().unwrap();
                if let Some(ref reset_template) = *reset_guard {
                    let mut reset_ev = reset_template.clone_without_next();
                    reset_ev.set_event_type(ComplexEventType::Reset);
                    reset_ev.set_timestamp(timestamp);
                    *tail = Some(Box::new(reset_ev));
                    tail = tail.as_mut().unwrap().mut_next_ref_option();
                }
                // Used up. The first event of the next batch becomes the new template.
                *reset_guard = None;
            }

            // 3. Current events go last
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
                        // The first event of each batch becomes the reset template
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

/// Placeholder for a lossy counting window. It passes every chunk through unchanged
/// and keeps no state. See LossyFrequentWindowProcessor for the working version.
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

/// Batch window flushed on a cron schedule. The schedule is registered once, when the
/// next processor is set. No RESET event is sent between batches.
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
///
/// Looks names up in the default source stream. Parameters that are not variables are
/// skipped. Returns an empty Vec for no parameters.
///
/// # Errors
/// Returns an error message when there is no metadata for the default source, or when
/// an attribute is not in the stream.
fn resolve_key_indices(
    params: &[Expression],
    parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
) -> Result<Vec<usize>, String> {
    if params.is_empty() {
        return Ok(Vec::new());
    }
    let meta = parse_ctx
        .stream_meta_map
        .get(&parse_ctx.default_source)
        .ok_or_else(|| format!("No stream metadata for '{}'", parse_ctx.default_source))?;
    let mut indices = Vec::new();
    for param in params {
        if let Expression::Variable(var) = param {
            let (idx, _) = meta
                .find_attribute_info(&var.attribute_name)
                .ok_or_else(|| format!("Attribute '{}' not found in stream", var.attribute_name))?;
            indices.push(*idx);
        }
    }
    Ok(indices)
}

/// Build a key string from the event's attribute values.
/// With `key_indices` set, uses only those positions, skipping any out of range.
/// With it empty, uses every value in before_window_data.
/// Values are joined with a null character so different splits cannot give the same
/// key.
fn generate_composite_key(event: &StreamEvent, key_indices: &[usize]) -> String {
    let mut buf = String::new();
    if key_indices.is_empty() {
        for (i, v) in event.before_window_data.iter().enumerate() {
            if i > 0 {
                buf.push('\0');
            }
            let _ = write!(buf, "{}", v);
        }
    } else {
        let mut first = true;
        for &idx in key_indices {
            if let Some(v) = event.before_window_data.get(idx) {
                if !first {
                    buf.push('\0');
                }
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

/// Keeps only the latest event for each key value.
/// When an event arrives with a key already held, the old event is expired.
/// Keys can span several attributes, for example `unique(symbol, exchange)`.
///
/// **Warning**: `unique_map` gains an entry for every new key value. With many
/// distinct keys, such as UUIDs, memory keeps growing. Java Siddhi's unique window
/// behaves the same way.
#[derive(Debug)]
pub struct UniqueWindowProcessor {
    meta: CommonProcessorMeta,
    /// Positions of the key attributes in before_window_data
    key_attribute_indices: Vec<usize>,
    /// Latest event for each key
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

                        if let Some(old_event) = map.insert(key, Arc::new(se.clone_without_next()))
                        {
                            // Send the replaced event out as expired, ahead of the new one
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

/// Keeps only the first event for each key value.
/// Later events with the same key are dropped and not emitted at all.
/// Keys can span several attributes, for example `firstUnique(user_id, session_id)`.
///
/// **Warning**: `seen_keys` never shrinks while the window lives. With many distinct
/// keys, such as UUIDs or transaction ids, memory keeps growing. Java Siddhi's
/// firstUnique behaves the same way.
#[derive(Debug)]
pub struct FirstUniqueWindowProcessor {
    meta: CommonProcessorMeta,
    /// Positions of the key attributes in before_window_data
    key_attribute_indices: Vec<usize>,
    /// Every key seen so far
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
        Ok(Arc::new(Mutex::new(
            FirstUniqueWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// DELAY WINDOW PROCESSOR
// Holds each event for a set delay before emitting it
// Reference: DelayWindowProcessor.java
// ============================================================================

/// Holds each event back for `delay_ms` and then emits it on its own, timestamped with
/// the time it fires. Without a scheduler, chunks pass straight through, but
/// `from_handler` refuses to build one without a scheduler.
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
                return Err(
                    "Delay window requires a scheduler (not available in this context)".to_string(),
                );
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
        let scheduler = self
            .scheduler
            .as_ref()
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

/// Keeps the most frequent events, using Misra-Gries counting.
///
/// Usage: `WINDOW('frequent', count)` keys on all attributes.
/// `WINDOW('frequent', count, attr1, attr2)` keys on the listed attributes.
///
/// A repeat of a held key expires the old event and emits the new one. A new key past
/// capacity lowers every count by one and expires keys whose count runs out. If there
/// is still no room, the new event is dropped.
#[derive(Debug)]
pub struct FrequentWindowProcessor {
    meta: CommonProcessorMeta,
    /// Most keys to hold at once
    most_frequent_count: usize,
    /// Positions of the key attributes. Empty means every attribute.
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

        // Any parameters after the count name the key attributes
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

                        if let Some(old) = state.event_map.insert(key.clone(), Arc::clone(&cloned))
                        {
                            // Key already held. Count it, expire the old event, emit the new.
                            *state.count_map.entry(key).or_insert(0) += 1;
                            let mut ex = old.as_ref().clone_without_next();
                            ex.set_event_type(ComplexEventType::Expired);
                            ex.set_timestamp(se.timestamp);
                            *output_tail = Some(Box::new(ex));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            *output_tail = Some(Box::new(se.clone_without_next()));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        } else {
                            // New key. It is already in event_map, so the length counts it.
                            if state.event_map.len() > self.most_frequent_count {
                                // Over capacity. Lower every count and expire keys at zero.
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
                                        output_tail =
                                            output_tail.as_mut().unwrap().mut_next_ref_option();
                                    }
                                }

                                if state.event_map.len() > self.most_frequent_count {
                                    // Still no room, so drop the new event unseen
                                    state.event_map.remove(&key);
                                } else {
                                    state.count_map.insert(key, 1);
                                    *output_tail = Some(Box::new(se.clone_without_next()));
                                    output_tail =
                                        output_tail.as_mut().unwrap().mut_next_ref_option();
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

/// Count for one key in lossy counting, with the bucket it was first counted in
#[derive(Debug, Clone)]
struct LossyCount {
    count: usize,
    bucket_id: usize,
}

/// Keeps the events whose key appears more often than a support threshold, using the
/// Lossy Counting method.
///
/// Takes the support, then an optional error (default support / 10), then optional
/// key attributes. A key enters the window once its count reaches
/// (support - error) * total events, and is expired when it falls below that or is
/// pruned at a bucket boundary.
#[derive(Debug)]
pub struct LossyFrequentWindowProcessor {
    meta: CommonProcessorMeta,
    /// Support threshold, from 0.0 to 1.0
    support: f64,
    /// Allowed error, above 0.0 and at most 1.0
    error: f64,
    /// Bucket width, ceil(1 / error). Kept as an integer so bucket math is exact.
    window_width: usize,
    /// Positions of the key attributes. Empty means every attribute.
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

        // A float second parameter is the error, and key attributes start after it.
        // Otherwise the error defaults to support / 10 and keys start at index 1.
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

        // The remaining parameters name the key attributes
        let key_attribute_indices = resolve_key_indices(&params[key_params_start..], parse_ctx)?;

        Ok(Self::new(
            support,
            error,
            key_attribute_indices,
            app_ctx,
            query_ctx,
        ))
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

                        // Count every event, whether or not its key is in the window
                        if let Some(lc) = state.count_map.get_mut(&key) {
                            lc.count += 1;
                        } else {
                            let bucket_id = state.current_bucket_id.saturating_sub(1);
                            state.count_map.insert(
                                key.clone(),
                                LossyCount {
                                    count: 1,
                                    bucket_id,
                                },
                            );
                        }

                        // The threshold decides whether the key belongs in the window
                        let threshold = (self.support - self.error) * (state.total_count as f64);
                        let meets_threshold = state
                            .count_map
                            .get(&key)
                            .is_some_and(|lc| lc.count as f64 >= threshold);
                        let was_in_window = state.event_map.contains_key(&key);

                        if meets_threshold {
                            if was_in_window {
                                // Already in the window. Expire the old event, emit the new.
                                let old = state.event_map.insert(key, Arc::clone(&cloned)).unwrap();
                                let mut ex = old.as_ref().clone_without_next();
                                ex.set_event_type(ComplexEventType::Expired);
                                ex.set_timestamp(se.timestamp);
                                *output_tail = Some(Box::new(ex));
                                output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            } else {
                                // Enters the window for the first time
                                state.event_map.insert(key, Arc::clone(&cloned));
                            }
                            *output_tail = Some(Box::new(se.clone_without_next()));
                            output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                        } else if was_in_window {
                            // Fell below the threshold, so expire it from the window
                            if let Some(old) = state.event_map.remove(&key) {
                                let mut ex = old.as_ref().clone_without_next();
                                ex.set_event_type(ComplexEventType::Expired);
                                ex.set_timestamp(se.timestamp);
                                *output_tail = Some(Box::new(ex));
                                output_tail = output_tail.as_mut().unwrap().mut_next_ref_option();
                            }
                        }

                        // At each bucket boundary, drop keys with count + bucket_id at or
                        // below the current bucket, and expire their events
                        if state.total_count % self.window_width == 0 {
                            let bucket_id = state.current_bucket_id;
                            let keys_to_remove: Vec<String> = state
                                .count_map
                                .iter()
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
                                    output_tail =
                                        output_tail.as_mut().unwrap().mut_next_ref_option();
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
        Ok(Arc::new(Mutex::new(
            LossyFrequentWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}

// ============================================================================
// EXPRESSION WINDOW PROCESSOR
// Sliding window sized by a count read from an expression string
// Reference: ExpressionWindowProcessor.java (simplified version)
// ============================================================================

/// Sliding window sized by a count expression that is read once when the window is
/// built. For example `count() <= 20` keeps at most 20 events.
///
/// **Supported expressions**: only `count() <= N` and `count() < N`.
/// The expression is not run per event. `from_handler` rejects anything else.
#[derive(Debug)]
pub struct ExpressionWindowProcessor {
    meta: CommonProcessorMeta,
    /// The expression text, for example "count() <= 20"
    expression: String,
    /// Most events to keep, read from the expression
    max_count: Option<usize>,
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
        // Plain string parsing for "count() <= N" or "count() < N", no regex. Spaces are
        // removed first, so "count( ) <= 5" also works. Tabs are not removed.
        // "count() < N" gives N - 1.
        let expr = expr.replace(' ', "").to_lowercase();
        if expr.starts_with("count()") {
            let rest = expr.trim_start_matches("count()").trim();
            if rest.starts_with("<=") {
                rest.trim_start_matches("<=").trim().parse().ok()
            } else if rest.starts_with("<") {
                rest.trim_start_matches("<")
                    .trim()
                    .parse::<usize>()
                    .ok()
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

                            // Expire the oldest events until the count fits again
                            if let Some(max) = self.max_count {
                                while buf.len() > max {
                                    if let Some(old) = buf.pop_front() {
                                        let mut ex = old.as_ref().clone_without_next();
                                        ex.set_event_type(ComplexEventType::Expired);
                                        ex.set_timestamp(se.timestamp);
                                        *output_tail = Some(Box::new(ex));
                                        output_tail =
                                            output_tail.as_mut().unwrap().mut_next_ref_option();
                                    }
                                }
                            }
                        }

                        // The new event goes after any expired ones
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
        Ok(Arc::new(Mutex::new(
            ExpressionWindowProcessor::from_handler(handler, app_ctx, query_ctx, parse_ctx)?,
        )))
    }

    fn clone_box(&self) -> Box<dyn WindowProcessorFactory> {
        Box::new(Self {})
    }
}
