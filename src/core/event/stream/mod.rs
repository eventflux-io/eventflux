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

// src/core/event/stream/mod.rs

pub mod meta_stream_event;
pub mod operation;
pub mod populater;
pub mod stream_event;
pub mod stream_event_cloner;
pub mod stream_event_factory;

pub use self::meta_stream_event::{MetaStreamEvent, MetaStreamEventType}; // Also export MetaStreamEventType
pub use self::operation::{Operation, Operator};
pub use self::populater::*;
pub use self::stream_event::StreamEvent;
pub use self::stream_event_cloner::StreamEventCloner;
pub use self::stream_event_factory::StreamEventFactory;
// ComplexEventType should be used via crate::core::event::ComplexEventType
