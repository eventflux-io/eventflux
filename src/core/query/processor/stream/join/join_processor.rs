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

use std::sync::{Arc, Mutex};

use crate::core::config::{
    eventflux_app_context::EventFluxAppContext, eventflux_query_context::EventFluxQueryContext,
};
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::event::{
    complex_event::{ComplexEvent, ComplexEventType},
    state::{MetaStateEvent, StateEvent, StateEventFactory},
};
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::query_api::execution::query::input::stream::join_input_stream::Type as JoinType;

#[derive(Debug, Clone, Copy)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Debug)]
pub struct JoinProcessor {
    meta: CommonProcessorMeta,
    pub join_type: JoinType,
    pub condition_executor: Option<Box<dyn ExpressionExecutor>>,
    pub left_buffer: Vec<StreamEvent>,
    pub right_buffer: Vec<StreamEvent>,
    state_event_factory: StateEventFactory,
}

impl JoinProcessor {
    pub fn new(
        join_type: JoinType,
        condition_executor: Option<Box<dyn ExpressionExecutor>>,
        meta_state_event: MetaStateEvent,
        app_ctx: Arc<EventFluxAppContext>,
        query_ctx: Arc<EventFluxQueryContext>,
    ) -> Self {
        let factory = StateEventFactory::new_from_meta(&meta_state_event);
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            join_type,
            condition_executor,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            state_event_factory: factory,
        }
    }

    fn build_joined_event(
        &self,
        left: Option<&StreamEvent>,
        right: Option<&StreamEvent>,
        event_type: ComplexEventType,
    ) -> StateEvent {
        let mut event = self.state_event_factory.new_instance();
        if let Some(l) = left {
            event.stream_events[0] = Some(l.clone_without_next());
            event.timestamp = l.timestamp;
        }
        if let Some(r) = right {
            event.stream_events[1] = Some(r.clone_without_next());
            if left.is_none() {
                event.timestamp = r.timestamp;
            }
        }
        event.event_type = event_type;
        event
    }

    fn forward(&self, se: StateEvent) {
        if let Some(ref next) = self.meta.next_processor {
            next.lock().unwrap().process(Some(Box::new(se)));
        }
    }

    fn process_event(&mut self, side: JoinSide, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                let se_clone = se.clone_without_next();
                let event_type = se.get_event_type();
                match side {
                    JoinSide::Left => {
                        let mut matched = false;
                        for r in &self.right_buffer {
                            let joined =
                                self.build_joined_event(Some(&se_clone), Some(r), event_type);
                            if let Some(ref cond) = self.condition_executor {
                                if let Some(AttributeValue::Bool(true)) =
                                    cond.execute(Some(&joined))
                                {
                                    matched = true;
                                    self.forward(joined);
                                }
                            } else {
                                matched = true;
                                self.forward(joined);
                            }
                        }
                        if !matched
                            && matches!(
                                self.join_type,
                                JoinType::LeftOuterJoin | JoinType::FullOuterJoin
                            )
                        {
                            let joined = self.build_joined_event(Some(&se_clone), None, event_type);
                            self.forward(joined);
                        }
                        self.left_buffer.push(se_clone);
                    }
                    JoinSide::Right => {
                        let mut matched = false;
                        for l in &self.left_buffer {
                            let joined =
                                self.build_joined_event(Some(l), Some(&se_clone), event_type);
                            if let Some(ref cond) = self.condition_executor {
                                if let Some(AttributeValue::Bool(true)) =
                                    cond.execute(Some(&joined))
                                {
                                    matched = true;
                                    self.forward(joined);
                                }
                            } else {
                                matched = true;
                                self.forward(joined);
                            }
                        }
                        if !matched
                            && matches!(
                                self.join_type,
                                JoinType::RightOuterJoin | JoinType::FullOuterJoin
                            )
                        {
                            let joined = self.build_joined_event(None, Some(&se_clone), event_type);
                            self.forward(joined);
                        }
                        self.right_buffer.push(se_clone);
                    }
                }
            }
        }
    }

    pub fn create_side_processor(
        self_arc: &Arc<Mutex<Self>>,
        side: JoinSide,
    ) -> Arc<Mutex<JoinProcessorSide>> {
        Arc::new(Mutex::new(JoinProcessorSide {
            parent: Arc::clone(self_arc),
            side,
        }))
    }
}

#[derive(Debug)]
pub struct JoinProcessorSide {
    parent: Arc<Mutex<JoinProcessor>>,
    side: JoinSide,
}

impl Processor for JoinProcessorSide {
    fn process(&self, chunk: Option<Box<dyn ComplexEvent>>) {
        self.parent.lock().unwrap().process_event(self.side, chunk);
    }

    fn next_processor(&self) -> Option<Arc<Mutex<dyn Processor>>> {
        self.parent.lock().unwrap().meta.next_processor.clone()
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.parent.lock().unwrap().meta.next_processor = next;
    }

    fn clone_processor(&self, ctx: &Arc<EventFluxQueryContext>) -> Box<dyn Processor> {
        let parent = self.parent.lock().unwrap();
        let cloned = JoinProcessor::new(
            parent.join_type,
            parent
                .condition_executor
                .as_ref()
                .map(|c| c.clone_executor(&parent.meta.eventflux_app_context)),
            MetaStateEvent::default(),
            Arc::clone(&parent.meta.eventflux_app_context),
            Arc::clone(ctx),
        );
        let arc = Arc::new(Mutex::new(cloned));
        Box::new(JoinProcessorSide {
            parent: arc,
            side: self.side,
        })
    }

    fn get_eventflux_app_context(&self) -> Arc<EventFluxAppContext> {
        self.parent
            .lock()
            .unwrap()
            .meta
            .eventflux_app_context
            .clone()
    }

    fn get_eventflux_query_context(&self) -> Arc<EventFluxQueryContext> {
        self.parent
            .lock()
            .unwrap()
            .meta
            .get_eventflux_query_context()
    }

    fn get_processing_mode(&self) -> ProcessingMode {
        ProcessingMode::DEFAULT
    }

    fn is_stateful(&self) -> bool {
        true
    }
}
