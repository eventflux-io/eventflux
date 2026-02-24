// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/processor/stream/window/session_window_processor.rs
// Rust implementation of EventFlux SessionWindowProcessor

use crate::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext,
};
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::stream::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::stream::window::WindowProcessor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::core::util::scheduler::{Schedulable, Scheduler};
use crate::query_api::execution::query::input::handler::WindowHandler;
use crate::query_api::expression::{constant::ConstantValueWithFloat, Expression};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const DEFAULT_SESSION_KEY: &str = "default-key";

/// A session window groups events based on a session key and maintains sessions
/// that expire after a configurable gap period.
#[derive(Debug)]
pub struct SessionWindowProcessor {
    /// Common processor metadata
    meta: CommonProcessorMeta,
    /// Time gap in milliseconds after which a session expires
    session_gap: i64,
    /// Additional time to accept late events after session expiry
    allowed_latency: i64,
    /// Executor for extracting session key from events
    session_key_executor: Option<Box<dyn ExpressionExecutor>>,
    /// Scheduler for handling session timeouts
    scheduler: Option<Arc<Scheduler>>,
    /// Window state containing all sessions
    state: Arc<Mutex<SessionWindowState>>,
}

/// Internal state for the session window
#[derive(Debug)]
pub(super) struct SessionWindowState {
    /// Map of session key -> session container
    pub(super) session_map: HashMap<String, SessionContainer>,
    /// Chunk to collect expired events
    pub(super) expired_event_chunk: SessionEventChunk,
}

/// Container for managing current and previous sessions for a session key
#[derive(Debug)]
pub(super) struct SessionContainer {
    pub(super) current_session: SessionEventChunk,
    pub(super) previous_session: SessionEventChunk,
}

/// Event chunk with session-specific timestamps
#[derive(Debug)]
pub(super) struct SessionEventChunk {
    pub(super) events: Vec<Arc<StreamEvent>>,
    pub(super) start_timestamp: i64,
    pub(super) end_timestamp: i64,
    pub(super) alive_timestamp: i64,
}

impl SessionWindowProcessor {
    /// Create a new session window processor
    pub fn new(
        session_gap: i64,
        session_key_executor: Option<Box<dyn ExpressionExecutor>>,
        allowed_latency: i64,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let scheduler = app_ctx.get_scheduler();
        SessionWindowProcessor {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            session_gap,
            allowed_latency,
            session_key_executor,
            scheduler,
            state: Arc::new(Mutex::new(SessionWindowState::new())),
        }
    }

    /// Create from window handler (standard factory pattern)
    pub fn from_handler(
        handler: &WindowHandler,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
        _parse_ctx: &crate::core::util::parser::expression_parser::ExpressionParserContext,
    ) -> Result<Self, String> {
        let params = handler.get_parameters();

        // First parameter: session gap (required)
        let session_gap = match params.first() {
            Some(Expression::Constant(c)) => match &c.value {
                ConstantValueWithFloat::Time(t) => *t,
                ConstantValueWithFloat::Long(l) => *l,
                ConstantValueWithFloat::Int(i) => *i as i64,
                _ => return Err("Session gap must be a time/long/int constant".to_string()),
            },
            _ => return Err("Session window requires session gap parameter".to_string()),
        };

        if session_gap <= 0 {
            return Err("Session gap must be positive".to_string());
        }

        // Second parameter: session key (optional, dynamic)
        let session_key_executor = if params.len() >= 2 {
            // TODO: Parse session key expression executor
            None // For now, use default key
        } else {
            None
        };

        // Third parameter: allowed latency (optional)
        let allowed_latency = if params.len() >= 3 {
            match &params[2] {
                Expression::Constant(c) => match &c.value {
                    ConstantValueWithFloat::Time(t) => *t,
                    ConstantValueWithFloat::Long(l) => *l,
                    ConstantValueWithFloat::Int(i) => *i as i64,
                    _ => return Err("Allowed latency must be a time/long/int constant".to_string()),
                },
                _ => return Err("Allowed latency must be constant".to_string()),
            }
        } else {
            0
        };

        if allowed_latency < 0 {
            return Err("Allowed latency cannot be negative".to_string());
        }

        if allowed_latency > session_gap {
            return Err("Allowed latency cannot be greater than session gap".to_string());
        }

        Ok(Self::new(
            session_gap,
            session_key_executor,
            allowed_latency,
            app_ctx,
            query_ctx,
        ))
    }

    /// Get the session key from an event
    fn get_session_key(&self, event: &dyn ComplexEvent) -> String {
        if let Some(ref executor) = self.session_key_executor {
            if let Some(AttributeValue::String(key)) = executor.execute(Some(event)) {
                key
            } else {
                DEFAULT_SESSION_KEY.to_string()
            }
        } else {
            DEFAULT_SESSION_KEY.to_string()
        }
    }

    /// Process events and maintain sessions
    fn process_event(
        &self,
        event: Box<dyn ComplexEvent>,
    ) -> Result<Vec<Box<dyn ComplexEvent>>, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Failed to acquire session window state lock".to_string())?;

        let event_timestamp = event.get_timestamp();
        let session_key = self.get_session_key(event.as_ref());
        let max_timestamp = event_timestamp + self.session_gap;
        let alive_timestamp = max_timestamp + self.allowed_latency;

        // Get or create session container for this key
        let session_container = state
            .session_map
            .entry(session_key.clone())
            .or_insert_with(SessionContainer::new);

        // Convert event to StreamEvent for storage
        let stream_event = if let Some(se) = event.as_any().downcast_ref::<StreamEvent>() {
            Arc::new(se.clone_without_next())
        } else {
            return Err("Session window only supports StreamEvent".to_string());
        };

        // Handle event based on session state
        let is_empty = session_container.current_session.is_empty();
        let current_start = session_container.current_session.start_timestamp;
        let current_end = session_container.current_session.end_timestamp;

        if is_empty {
            // First event in session
            session_container
                .current_session
                .add_event(Arc::clone(&stream_event));
            session_container.current_session.set_timestamps(
                event_timestamp,
                max_timestamp,
                alive_timestamp,
            );
            self.schedule_timeout(max_timestamp)?;
        } else if event_timestamp >= current_start {
            // Event is not late relative to current session start
            if event_timestamp <= current_end {
                // Event extends current session
                session_container
                    .current_session
                    .add_event(Arc::clone(&stream_event));
                session_container.current_session.set_timestamps(
                    current_start,
                    max_timestamp,
                    alive_timestamp,
                );
                self.schedule_timeout(max_timestamp)?;
            } else {
                // Event starts a new session
                if self.allowed_latency > 0 {
                    // Move current to previous session
                    session_container.move_current_to_previous();
                    session_container.current_session.clear();
                    session_container
                        .current_session
                        .add_event(Arc::clone(&stream_event));
                    session_container.current_session.set_timestamps(
                        event_timestamp,
                        max_timestamp,
                        alive_timestamp,
                    );
                    self.schedule_timeout(max_timestamp)?;
                }
            }
        } else {
            // Late event - try to add to appropriate session
            self.handle_late_event(
                event_timestamp,
                Arc::clone(&stream_event),
                session_container,
            )?;
        }

        // Process any session timeouts
        self.process_session_timeouts(event_timestamp, &mut state)?;

        // Return any expired events, converting to Box<dyn ComplexEvent>
        let mut result = Vec::new();
        if !state.expired_event_chunk.is_empty() {
            let expired_events = state.expired_event_chunk.drain_events();
            for event in expired_events {
                let mut expired_event = event.as_ref().clone_without_next();
                expired_event.set_event_type(ComplexEventType::Expired);
                result.push(Box::new(expired_event) as Box<dyn ComplexEvent>);
            }
        }

        Ok(result)
    }

    /// Handle late arriving events
    fn handle_late_event(
        &self,
        event_timestamp: i64,
        event: Arc<StreamEvent>,
        session_container: &mut SessionContainer,
    ) -> Result<(), String> {
        if self.allowed_latency > 0 {
            let current_start = session_container.current_session.start_timestamp;
            let previous_start = session_container.previous_session.start_timestamp;
            let previous_empty = session_container.previous_session.is_empty();

            // Check if event can extend current session backwards
            if event_timestamp >= (current_start - self.session_gap) {
                session_container
                    .current_session
                    .add_event_at_start(Arc::clone(&event));
                session_container.current_session.start_timestamp = event_timestamp;
                // Merge sessions if needed
                self.merge_sessions_in_container(session_container);
            } else if !previous_empty && event_timestamp >= (previous_start - self.session_gap) {
                // Event belongs to previous session
                session_container
                    .previous_session
                    .add_event(Arc::clone(&event));

                if event_timestamp < previous_start {
                    session_container.previous_session.start_timestamp = event_timestamp;
                } else {
                    // Event might extend previous session
                    let new_end = event_timestamp + self.session_gap;
                    let new_alive = new_end + self.allowed_latency;
                    session_container.previous_session.set_timestamps(
                        previous_start,
                        new_end,
                        new_alive,
                    );
                    self.merge_sessions_in_container(session_container);
                }
            } else {
                // Event is too late - log and discard
                println!("Event {event_timestamp} is too late for any session");
            }
        } else {
            // No latency allowed - check only current session
            let current_start = session_container.current_session.start_timestamp;
            if event_timestamp >= (current_start - self.session_gap) {
                session_container
                    .current_session
                    .add_event_at_start(Arc::clone(&event));
                session_container.current_session.start_timestamp = event_timestamp;
            } else {
                println!("Event {event_timestamp} is too late for current session");
            }
        }

        Ok(())
    }

    /// Merge sessions within a container
    fn merge_sessions_in_container(&self, container: &mut SessionContainer) {
        if !container.previous_session.is_empty()
            && container.previous_session.end_timestamp
                >= (container.current_session.start_timestamp - self.session_gap)
        {
            // Sessions should be merged
            container
                .current_session
                .merge_from(&mut container.previous_session);
            container.previous_session.clear();
        }
    }

    /// Process expired sessions based on current timestamp
    fn process_session_timeouts(
        &self,
        current_time: i64,
        state: &mut SessionWindowState,
    ) -> Result<(), String> {
        // Find all current sessions that have expired
        let mut expired_sessions = Vec::new();

        for (key, container) in &state.session_map {
            if !container.current_session.is_empty()
                && current_time >= container.current_session.end_timestamp
            {
                expired_sessions.push(key.clone());
            }
        }

        // Process expired current sessions
        for key in expired_sessions {
            if let Some(container) = state.session_map.get_mut(&key) {
                if self.allowed_latency > 0 {
                    // Move to previous session
                    container.move_current_to_previous();
                    self.schedule_timeout(container.previous_session.alive_timestamp)?;
                } else {
                    // Expire immediately
                    state
                        .expired_event_chunk
                        .add_session_events(&container.current_session);
                    container.current_session.clear();
                }
            }
        }

        // Process expired previous sessions (if latency allowed)
        if self.allowed_latency > 0 {
            let mut expired_previous = Vec::new();

            for (key, container) in &state.session_map {
                if !container.previous_session.is_empty()
                    && current_time >= container.previous_session.alive_timestamp
                {
                    expired_previous.push(key.clone());
                }
            }

            for key in expired_previous {
                if let Some(container) = state.session_map.get_mut(&key) {
                    state
                        .expired_event_chunk
                        .add_session_events(&container.previous_session);
                    container.previous_session.clear();
                }
            }
        }

        Ok(())
    }

    /// Schedule a timeout notification
    fn schedule_timeout(&self, timestamp: i64) -> Result<(), String> {
        if let Some(ref _scheduler) = self.scheduler {
            // For now, we'll handle timeouts in the main processing loop
            // TODO: Implement proper schedulable task
            println!("Session timeout scheduled for timestamp: {timestamp}");
        }
        Ok(())
    }
}

impl Processor for SessionWindowProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.meta.next_processor {
            if let Some(chunk) = complex_event_chunk {
                let mut current_opt = Some(chunk.as_ref() as &dyn ComplexEvent);
                let mut all_expired: Vec<Box<dyn ComplexEvent>> = Vec::new();

                while let Some(ev) = current_opt {
                    if let Some(se) = ev.as_any().downcast_ref::<StreamEvent>() {
                        match self.process_event(Box::new(se.clone_without_next())) {
                            Ok(expired_events) => {
                                all_expired.extend(expired_events);
                            }
                            Err(e) => {
                                eprintln!("Error processing session window event: {e}");
                            }
                        }
                    }
                    current_opt = ev.get_next();
                }

                // Send expired events first, then current events
                if !all_expired.is_empty() {
                    let mut head: Option<Box<dyn ComplexEvent>> = None;
                    let mut tail = &mut head;

                    for event in all_expired {
                        *tail = Some(event);
                        tail = tail.as_mut().unwrap().mut_next_ref_option();
                    }

                    // Add current events to the chain
                    *tail = Some(chunk);

                    next.lock().unwrap().process(head);
                } else {
                    next.lock().unwrap().process(Some(chunk));
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

    fn clone_processor(&self, query_ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        Box::new(Self::new(
            self.session_gap,
            // TODO: Clone session key executor properly
            None,
            self.allowed_latency,
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

impl WindowProcessor for SessionWindowProcessor {}

impl Schedulable for SessionWindowProcessor {
    fn on_time(&self, timestamp: i64) {
        // Handle scheduled session timeouts
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                eprintln!("Failed to acquire session window state lock for scheduled timeout");
                return;
            }
        };

        if let Err(e) = self.process_session_timeouts(timestamp, &mut state) {
            eprintln!("Error processing scheduled session timeout: {e}");
        }
    }
}

impl SessionWindowState {
    pub(super) fn new() -> Self {
        SessionWindowState {
            session_map: HashMap::new(),
            expired_event_chunk: SessionEventChunk::new(),
        }
    }
}

impl SessionContainer {
    pub(super) fn new() -> Self {
        SessionContainer {
            current_session: SessionEventChunk::new(),
            previous_session: SessionEventChunk::new(),
        }
    }

    pub(super) fn move_current_to_previous(&mut self) {
        // Move current session to previous, clearing current
        std::mem::swap(&mut self.current_session, &mut self.previous_session);
        self.current_session.clear();
    }
}

impl SessionEventChunk {
    pub(super) fn new() -> Self {
        SessionEventChunk {
            events: Vec::new(),
            start_timestamp: -1,
            end_timestamp: -1,
            alive_timestamp: -1,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(super) fn add_event(&mut self, event: Arc<StreamEvent>) {
        self.events.push(event);
    }

    pub(super) fn add_event_at_start(&mut self, event: Arc<StreamEvent>) {
        self.events.insert(0, event);
    }

    pub(super) fn set_timestamps(&mut self, start: i64, end: i64, alive: i64) {
        self.start_timestamp = start;
        self.end_timestamp = end;
        self.alive_timestamp = alive;
    }

    pub(super) fn clear(&mut self) {
        self.events.clear();
        self.start_timestamp = -1;
        self.end_timestamp = -1;
        self.alive_timestamp = -1;
    }

    pub(super) fn merge_from(&mut self, other: &mut SessionEventChunk) {
        // Insert other's events at the beginning and update start timestamp
        let mut other_events = std::mem::take(&mut other.events);
        other_events.extend(std::mem::take(&mut self.events));
        self.events = other_events;
        self.start_timestamp = other.start_timestamp;
    }

    pub(super) fn add_session_events(&mut self, session: &SessionEventChunk) {
        for event in &session.events {
            self.events.push(Arc::clone(event));
        }
    }

    pub(super) fn drain_events(&mut self) -> Vec<Arc<StreamEvent>> {
        std::mem::take(&mut self.events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::stream::StreamEvent;
    use crate::core::event::value::AttributeValue;
    use std::sync::Arc;

    #[test]
    fn test_session_event_chunk() {
        let mut chunk = SessionEventChunk::new();
        assert!(chunk.is_empty());

        let event = Arc::new(StreamEvent::new(1000, 1, 0, 0));
        chunk.add_event(event);
        assert!(!chunk.is_empty());
        assert_eq!(chunk.events.len(), 1);

        chunk.set_timestamps(1000, 6000, 6000);
        assert_eq!(chunk.start_timestamp, 1000);
        assert_eq!(chunk.end_timestamp, 6000);
        assert_eq!(chunk.alive_timestamp, 6000);

        chunk.clear();
        assert!(chunk.is_empty());
        assert_eq!(chunk.start_timestamp, -1);
    }

    #[test]
    fn test_session_container() {
        let container = SessionContainer::new();
        assert!(container.current_session.is_empty());
        assert!(container.previous_session.is_empty());
    }

    #[test]
    fn test_session_window_state() {
        let state = SessionWindowState::new();
        assert!(state.session_map.is_empty());
        assert!(state.expired_event_chunk.is_empty());
    }
}
