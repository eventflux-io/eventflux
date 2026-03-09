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

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::query::processor::{CommonProcessorMeta, ProcessingMode, Processor};
use crate::query_api::execution::query::input::stream::join_input_stream::Type as JoinType;
use std::sync::{Arc, Mutex};

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
    pub left_attr_count: usize,
    pub right_attr_count: usize,
    pub next_processor: Option<Arc<Mutex<dyn Processor>>>,
    pub left_buffer: Vec<StreamEvent>,
    pub right_buffer: Vec<StreamEvent>,
}

impl JoinProcessor {
    pub fn new(
        join_type: JoinType,
        condition_executor: Option<Box<dyn ExpressionExecutor>>,
        left_attr_count: usize,
        right_attr_count: usize,
        app_ctx: Arc<crate::core::config::eventflux_app_context::EventFluxAppContext>,
        query_ctx: Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
    ) -> Self {
        Self {
            meta: CommonProcessorMeta::new(app_ctx, query_ctx),
            join_type,
            condition_executor,
            left_attr_count,
            right_attr_count,
            next_processor: None,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }

    fn build_joined_event(
        &self,
        left: Option<&StreamEvent>,
        right: Option<&StreamEvent>,
    ) -> StreamEvent {
        let mut event = StreamEvent::new(
            left.or(right).map(|e| e.timestamp).unwrap_or(0),
            self.left_attr_count + self.right_attr_count,
            0,
            0,
        );
        for i in 0..self.left_attr_count {
            let val = left
                .and_then(|l| l.before_window_data.get(i).cloned())
                .unwrap_or(AttributeValue::Null);
            event.before_window_data[i] = val;
        }
        for j in 0..self.right_attr_count {
            let val = right
                .and_then(|r| r.before_window_data.get(j).cloned())
                .unwrap_or(AttributeValue::Null);
            event.before_window_data[self.left_attr_count + j] = val;
        }
        event
    }

    fn forward(&self, se: StreamEvent) {
        if let Some(ref next) = self.next_processor {
            next.lock().unwrap().process(Some(Box::new(se)));
        }
    }

    fn process_event(&mut self, side: JoinSide, mut chunk: Option<Box<dyn ComplexEvent>>) {
        while let Some(mut ce) = chunk {
            chunk = ce.set_next(None);
            if let Some(se) = ce.as_any().downcast_ref::<StreamEvent>() {
                let se_clone = se.clone_without_next();
                match side {
                    JoinSide::Left => {
                        let mut matched = false;
                        for r in &self.right_buffer {
                            let joined = self.build_joined_event(Some(&se_clone), Some(r));
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
                            let joined = self.build_joined_event(Some(&se_clone), None);
                            self.forward(joined);
                        }
                        self.left_buffer.push(se_clone);
                    }
                    JoinSide::Right => {
                        let mut matched = false;
                        for l in &self.left_buffer {
                            let joined = self.build_joined_event(Some(l), Some(&se_clone));
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
                            let joined = self.build_joined_event(None, Some(&se_clone));
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
        self.parent.lock().unwrap().next_processor.clone()
    }

    fn set_next_processor(&mut self, next: Option<Arc<Mutex<dyn Processor>>>) {
        self.parent.lock().unwrap().next_processor = next;
    }

    fn clone_processor(
        &self,
        _ctx: &Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext>,
    ) -> Box<dyn Processor> {
        Box::new(JoinProcessorSide {
            parent: Arc::clone(&self.parent),
            side: self.side,
        })
    }

    fn get_eventflux_app_context(
        &self,
    ) -> Arc<crate::core::config::eventflux_app_context::EventFluxAppContext> {
        self.parent
            .lock()
            .unwrap()
            .meta
            .eventflux_app_context
            .clone()
    }
    fn get_eventflux_query_context(
        &self,
    ) -> Arc<crate::core::config::eventflux_query_context::EventFluxQueryContext> {
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
