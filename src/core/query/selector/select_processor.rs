// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/select_processor.rs
use super::attribute::OutputAttributeProcessor;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::{ComplexEvent, ComplexEventType};
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::query_api::definition::StreamDefinition as ApiStreamDefinition;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::core::persistence::StateHolder;

use super::{GroupByKeyGenerator, OrderByEventComparator};
use crate::core::executor::expression_executor::ExpressionExecutor;

#[derive(Debug)]
struct GroupState {
    oaps: Vec<OutputAttributeProcessor>,
    having_exec: Option<Box<dyn ExpressionExecutor>>,
}
// Simplified OutputRateLimiter supporting event-count based throttling.
#[derive(Debug)]
pub struct OutputRateLimiter {
    pub next_processor: Option<Arc<Mutex<dyn Processor>>>,
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub eventflux_query_context: Arc<EventFluxQueryContext>,
    batch_size: usize,
    behavior: crate::query_api::execution::query::output::ratelimit::OutputRateBehavior,
    buffer: Arc<Mutex<Vec<Box<dyn ComplexEvent>>>>,
    counter: Arc<Mutex<usize>>,
}

#[derive(Debug)]
struct OutputRateLimiterStateHolder {
    buffer: Arc<Mutex<Vec<Box<dyn ComplexEvent>>>>,
    counter: Arc<Mutex<usize>>,
}

#[derive(Serialize, Deserialize)]
struct StoredEvent {
    ts: i64,
    data: Vec<AttributeValue>,
    expired: bool,
}

#[derive(Serialize, Deserialize)]
struct LimiterSnapshot {
    count: usize,
    events: Vec<StoredEvent>,
}

impl StateHolder for OutputRateLimiterStateHolder {
    fn schema_version(&self) -> crate::core::persistence::SchemaVersion {
        crate::core::persistence::SchemaVersion::new(1, 0, 0)
    }

    fn serialize_state(
        &self,
        _hints: &crate::core::persistence::SerializationHints,
    ) -> Result<crate::core::persistence::StateSnapshot, crate::core::persistence::StateError> {
        let count = *self.counter.lock().unwrap();
        let events = self
            .buffer
            .lock()
            .unwrap()
            .iter()
            .map(|e| StoredEvent {
                ts: e.get_timestamp(),
                data: e.get_output_data().map_or(Vec::new(), |d| d.to_vec()),
                expired: e.get_event_type() == ComplexEventType::Expired,
            })
            .collect();
        let snap = LimiterSnapshot { count, events };
        let data = crate::core::util::to_bytes(&snap).map_err(|e| {
            crate::core::persistence::StateError::SerializationError {
                message: format!("Failed to serialize OutputRateLimiter state: {e}"),
            }
        })?;

        let checksum = crate::core::persistence::StateSnapshot::calculate_checksum(&data);

        Ok(crate::core::persistence::StateSnapshot {
            version: self.schema_version(),
            checkpoint_id: 0,
            data,
            compression: crate::core::persistence::CompressionType::None,
            checksum,
            metadata: self.component_metadata(),
        })
    }

    fn deserialize_state(
        &self,
        snapshot: &crate::core::persistence::StateSnapshot,
    ) -> Result<(), crate::core::persistence::StateError> {
        if !snapshot.verify_integrity() {
            return Err(crate::core::persistence::StateError::ChecksumMismatch);
        }

        let snap: LimiterSnapshot = crate::core::util::from_bytes(&snapshot.data).map_err(|e| {
            crate::core::persistence::StateError::DeserializationError {
                message: format!("Failed to deserialize OutputRateLimiter state: {e}"),
            }
        })?;

        *self.counter.lock().unwrap() = snap.count;
        let mut buf = self.buffer.lock().unwrap();
        buf.clear();
        for ev in snap.events {
            let mut se = StreamEvent::new(ev.ts, 0, 0, ev.data.len());
            se.output_data = Some(ev.data);
            se.event_type = if ev.expired {
                ComplexEventType::Expired
            } else {
                ComplexEventType::Current
            };
            buf.push(Box::new(se));
        }
        Ok(())
    }

    fn get_changelog(
        &self,
        _since: crate::core::persistence::CheckpointId,
    ) -> Result<crate::core::persistence::ChangeLog, crate::core::persistence::StateError> {
        // OutputRateLimiter doesn't support incremental changes
        Err(crate::core::persistence::StateError::SerializationError {
            message: "OutputRateLimiter doesn't support incremental checkpointing".to_string(),
        })
    }

    fn apply_changelog(
        &self,
        _changes: &crate::core::persistence::ChangeLog,
    ) -> Result<(), crate::core::persistence::StateError> {
        // OutputRateLimiter doesn't support incremental changes
        Err(crate::core::persistence::StateError::DeserializationError {
            message: "OutputRateLimiter doesn't support incremental changes".to_string(),
        })
    }

    fn estimate_size(&self) -> crate::core::persistence::StateSize {
        let buffer_size = self.buffer.lock().unwrap().len();
        crate::core::persistence::StateSize {
            bytes: buffer_size * std::mem::size_of::<StoredEvent>() + std::mem::size_of::<usize>(),
            entries: buffer_size + 1, // events + counter
            estimated_growth_rate: 0.0,
        }
    }

    fn access_pattern(&self) -> crate::core::persistence::AccessPattern {
        crate::core::persistence::AccessPattern::Hot // Rate limiter is frequently accessed
    }

    fn component_metadata(&self) -> crate::core::persistence::StateMetadata {
        crate::core::persistence::StateMetadata::new(
            "output_rate_limiter".to_string(),
            "OutputRateLimiter".to_string(),
        )
    }
}

impl OutputRateLimiter {
    pub fn new(
        next_processor: Option<Arc<Mutex<dyn Processor>>>,
        eventflux_app_context: Arc<EventFluxAppContext>,
        eventflux_query_context: Arc<EventFluxQueryContext>,
        batch_size: usize,
        behavior: crate::query_api::execution::query::output::ratelimit::OutputRateBehavior,
    ) -> Self {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(Mutex::new(0usize));
        let holder = Arc::new(Mutex::new(OutputRateLimiterStateHolder {
            buffer: Arc::clone(&buffer),
            counter: Arc::clone(&counter),
        }));
        eventflux_query_context.register_state_holder("output_rate_limiter".into(), holder);
        Self {
            next_processor,
            eventflux_app_context,
            eventflux_query_context,
            batch_size,
            behavior,
            buffer,
            counter,
        }
    }

    fn emit(&self, events: Vec<Box<dyn ComplexEvent>>) {
        if let Some(ref next) = self.next_processor {
            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;
            for ev in events {
                *tail = Some(ev);
                if let Some(ref mut t) = *tail {
                    tail = t.mut_next_ref_option();
                }
            }
            next.lock().unwrap().process(head);
        }
    }
}

impl Processor for OutputRateLimiter {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        if complex_event_chunk.is_none() {
            let mut out = Vec::new();
            {
                let mut buf = self.buffer.lock().unwrap();
                if !buf.is_empty() {
                    out.extend(buf.drain(..));
                    *self.counter.lock().unwrap() = 0;
                }
            }
            if !out.is_empty() {
                self.emit(out);
            }
            return;
        }
        let mut current = complex_event_chunk;
        while let Some(mut ev) = current {
            let next = ev.set_next(None);
            {
                let mut count = self.counter.lock().unwrap();
                let mut buf = self.buffer.lock().unwrap();
                *count += 1;
                match self.behavior {
                    crate::query_api::execution::query::output::ratelimit::OutputRateBehavior::All => {
                        buf.push(ev);
                    }
                    crate::query_api::execution::query::output::ratelimit::OutputRateBehavior::First => {
                        if *count == 1 {
                            buf.push(ev);
                        }
                    }
                    crate::query_api::execution::query::output::ratelimit::OutputRateBehavior::Last => {
                        buf.clear();
                        buf.push(ev);
                    }
                }
                if *count >= self.batch_size {
                    let out: Vec<Box<dyn ComplexEvent>> = buf.drain(..).collect();
                    *count = 0;
                    drop(buf);
                    drop(count);
                    self.emit(out);
                }
            }
            current = next;
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next_processor: Option<Arc<Mutex<dyn Processor>>>) {
        self.next_processor = next_processor;
    }

    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(OutputRateLimiter::new(
            self.next_processor.as_ref().map(Arc::clone),
            Arc::clone(&self.eventflux_app_context),
            Arc::clone(eventflux_query_context),
            self.batch_size,
            self.behavior,
        ))
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        Arc::clone(&self.eventflux_query_context)
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::BATCH
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

impl Drop for OutputRateLimiter {
    fn drop(&mut self) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.is_empty() {
            return;
        }
        let events: Vec<_> = buf.drain(..).collect();
        *self.counter.lock().unwrap() = 0;
        drop(buf);
        if let Some(ref next) = self.next_processor {
            let mut head: Option<Box<dyn ComplexEvent>> = None;
            let mut tail = &mut head;
            for ev in events {
                *tail = Some(ev);
                if let Some(ref mut t) = *tail {
                    tail = t.mut_next_ref_option();
                }
            }
            next.lock().unwrap().process(head);
        }
    }
}

/// A stream processor that handles SELECT clause projections.
#[derive(Debug)]
pub struct SelectProcessor {
    meta: CommonProcessorMeta,
    current_on: bool,
    expired_on: bool,
    contains_aggregator: bool,
    output_attribute_processors: Vec<OutputAttributeProcessor>,
    pub output_stream_definition: Arc<ApiStreamDefinition>,
    having_condition_executor:
        Option<Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>>, // Changed placeholder
    is_group_by: bool,
    group_by_key_generator: Option<GroupByKeyGenerator>,
    is_order_by: bool,
    order_by_event_comparator: Option<OrderByEventComparator>,
    batching_enabled: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    /// Per-group aggregator state when both group-by and aggregators are used.
    group_states: Mutex<std::collections::HashMap<String, GroupState>>,
}

impl SelectProcessor {
    pub fn new(
        api_selector: &crate::query_api::execution::query::selection::Selector,
        current_on: bool,
        expired_on: bool,
        eventflux_app_context: Arc<EventFluxAppContext>,
        eventflux_query_context: Arc<EventFluxQueryContext>,
        output_attribute_processors: Vec<OutputAttributeProcessor>,
        output_stream_definition: Arc<ApiStreamDefinition>,
        having_executor: Option<
            Box<dyn crate::core::executor::expression_executor::ExpressionExecutor>,
        >,
        group_by_key_generator: Option<GroupByKeyGenerator>,
        order_by_comparator: Option<OrderByEventComparator>,
        batching_enabled: Option<bool>,
    ) -> Self {
        let _query_name = eventflux_query_context.name.clone();
        let contains_aggregator_flag = output_attribute_processors
            .iter()
            .any(|oap| oap.is_aggregator());

        Self {
            meta: CommonProcessorMeta::new(eventflux_app_context, eventflux_query_context),
            current_on,
            expired_on,
            contains_aggregator: contains_aggregator_flag,
            output_attribute_processors,
            output_stream_definition,
            having_condition_executor: having_executor,
            is_group_by: group_by_key_generator.is_some(),
            group_by_key_generator,
            is_order_by: order_by_comparator.is_some(),
            order_by_event_comparator: order_by_comparator, // Corrected field init
            batching_enabled: batching_enabled.unwrap_or(true),
            limit: api_selector
                .limit
                .as_ref()
                .and_then(|c| c.value.to_u64_for_limit_offset()),
            offset: api_selector
                .offset
                .as_ref()
                .and_then(|c| c.value.to_u64_for_limit_offset()),
            group_states: Mutex::new(HashMap::new()),
        }
    }

    /// Clear group states - called during state restoration to ensure clean state
    pub fn clear_group_states(&self) {
        if let Ok(mut group_states) = self.group_states.lock() {
            group_states.clear();
        }
    }

    fn apply_limit_offset(&self, events: Vec<Box<dyn ComplexEvent>>) -> Vec<Box<dyn ComplexEvent>> {
        let mut final_events = Vec::new();
        let mut seen = 0u64;
        let offset = self.offset.unwrap_or(0);
        let mut remaining = self.limit.unwrap_or(u64::MAX);

        for ev in events.into_iter() {
            let etype = ev.get_event_type();
            let countable = matches!(etype, ComplexEventType::Current | ComplexEventType::Expired);
            if countable {
                if seen < offset {
                    seen += 1;
                    continue;
                }
                if remaining == 0 {
                    break;
                }
                remaining -= 1;
            }
            final_events.push(ev);
        }

        final_events
    }
}

impl Processor for SelectProcessor {
    fn process(&self, complex_event_chunk: Option<Box<dyn ComplexEvent>>) {
        let mut input_event_opt = complex_event_chunk;
        let mut collected: Vec<Box<dyn ComplexEvent>> = Vec::new();
        let mut group_map: HashMap<String, Box<dyn ComplexEvent>> = HashMap::new();
        let mut state_lock = if self.contains_aggregator && self.is_group_by {
            Some(self.group_states.lock().unwrap())
        } else {
            None
        };

        while let Some(mut event_box) = input_event_opt {
            let next = event_box.set_next(None);
            let etype = event_box.get_event_type();

            let allowed = match etype {
                ComplexEventType::Current => self.current_on,
                ComplexEventType::Expired => self.expired_on,
                ComplexEventType::Reset => true,
                _ => false,
            };
            if !allowed && etype != ComplexEventType::Reset {
                input_event_opt = next;
                continue;
            }

            let mut out = Vec::with_capacity(self.output_attribute_processors.len());
            if let Some(ref mut map) = state_lock {
                let key = self
                    .group_by_key_generator
                    .as_ref()
                    .and_then(|g| g.construct_event_key(event_box.as_ref()))
                    .unwrap_or_default();
                let state = map.entry(key.clone()).or_insert_with(|| GroupState {
                    oaps: self
                        .output_attribute_processors
                        .iter()
                        .map(|oap| oap.clone_oap(&self.meta.eventflux_app_context))
                        .collect(),
                    having_exec: self
                        .having_condition_executor
                        .as_ref()
                        .map(|e| e.clone_executor(&self.meta.eventflux_app_context)),
                });
                for oap in &state.oaps {
                    out.push(oap.process(Some(event_box.as_ref())));
                }
                event_box.set_output_data(Some(out));
                if etype != ComplexEventType::Reset {
                    event_box.set_event_type(etype);
                }
                if let Some(ref h) = state.having_exec {
                    let pass = matches!(
                        h.execute(Some(event_box.as_ref())),
                        Some(AttributeValue::Bool(true))
                    );
                    if !pass {
                        input_event_opt = next;
                        continue;
                    }
                }
                // RESET events are consumed (processed by aggregators) but NOT output
                // They only signal aggregators to clear state before new batch
                if etype != ComplexEventType::Reset {
                    group_map.insert(key, event_box);
                }
            } else {
                for oap in &self.output_attribute_processors {
                    out.push(oap.process(Some(event_box.as_ref())));
                }
                event_box.set_output_data(Some(out));
                if etype != ComplexEventType::Reset {
                    event_box.set_event_type(etype);
                }
                if let Some(ref having_exec) = self.having_condition_executor {
                    let pass = matches!(
                        having_exec.execute(Some(event_box.as_ref())),
                        Some(AttributeValue::Bool(true))
                    );
                    if !pass {
                        input_event_opt = next;
                        continue;
                    }
                }
                // RESET events are consumed (processed by aggregators) but NOT output
                // They only signal aggregators to clear state before new batch
                if etype != ComplexEventType::Reset {
                    if self.is_group_by {
                        let key = self
                            .group_by_key_generator
                            .as_ref()
                            .and_then(|g| g.construct_event_key(event_box.as_ref()))
                            .unwrap_or_default();
                        group_map.insert(key, event_box);
                    } else {
                        collected.push(event_box);
                    }
                }
            }

            input_event_opt = next;
        }

        if self.is_group_by {
            for (_, ev) in group_map.into_iter() {
                collected.push(ev);
            }
        }

        if self.is_order_by {
            if let Some(ref cmp) = self.order_by_event_comparator {
                collected.sort_by(|a, b| cmp.compare(a.as_ref(), b.as_ref()));
            }
        }

        let final_events = self.apply_limit_offset(collected);

        // Re-link chain
        let mut head: Option<Box<dyn ComplexEvent>> = None;
        let mut tail_ref = &mut head;
        for ev in final_events {
            *tail_ref = Some(ev);
            if let Some(ref mut t) = *tail_ref {
                tail_ref = t.mut_next_ref_option();
            }
        }

        if let Some(ref next_proc) = self.meta.next_processor {
            next_proc.lock().unwrap().process(head);
        }
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.meta.next_processor.as_ref().map(Arc::clone)
    }

    fn set_next_processor(&mut self, next_processor: Option<Arc<Mutex<dyn Processor>>>) {
        self.meta.next_processor = next_processor;
    }

    fn clone_processor(
        &self,
        eventflux_query_context: &Arc<EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        let cloned_oaps = self
            .output_attribute_processors
            .iter()
            .map(|oap| oap.clone_oap(&self.meta.eventflux_app_context))
            .collect();
        let cloned_having = self
            .having_condition_executor
            .as_ref()
            .map(|exec| exec.clone_executor(&self.meta.eventflux_app_context));

        Box::new(SelectProcessor {
            meta: CommonProcessorMeta::new(
                Arc::clone(&self.meta.eventflux_app_context),
                Arc::clone(eventflux_query_context),
            ),
            current_on: self.current_on,
            expired_on: self.expired_on,
            contains_aggregator: self.contains_aggregator,
            output_attribute_processors: cloned_oaps,
            output_stream_definition: Arc::clone(&self.output_stream_definition),
            having_condition_executor: cloned_having,
            is_group_by: self.is_group_by,
            group_by_key_generator: self.group_by_key_generator.clone(),
            is_order_by: self.is_order_by,
            order_by_event_comparator: self.order_by_event_comparator.clone(),
            batching_enabled: self.batching_enabled,
            limit: self.limit,
            offset: self.offset,
            group_states: Mutex::new(HashMap::new()),
        })
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        Arc::clone(&self.meta.eventflux_app_context)
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.meta.get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        if self.contains_aggregator
            || self.is_group_by
            || self.is_order_by
            || self.limit.is_some()
            || self.offset.is_some()
        {
            ProcessingMode::BATCH
        } else {
            ProcessingMode::DEFAULT
        }
    }

    fn is_stateful(&self) -> bool {
        self.contains_aggregator || self.is_group_by // Simplified
    }

    fn clear_group_states(&self) {
        // Clear group states for SelectProcessor
        if let Ok(mut group_states) = self.group_states.lock() {
            group_states.clear();
        }
    }
}

// Helper on query_api::ConstantValueWithFloat for limit/offset
use crate::query_api::expression::constant::ConstantValueWithFloat as ApiConstantValue;
impl ApiConstantValue {
    fn to_u64_for_limit_offset(&self) -> Option<u64> {
        match self {
            ApiConstantValue::Int(i) if *i >= 0 => Some(*i as u64),
            ApiConstantValue::Long(l) if *l >= 0 => Some(*l as u64),
            _ => None, // Or error for invalid type/negative value
        }
    }
}
