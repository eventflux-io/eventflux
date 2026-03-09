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

// src/core/event/stream/populater/mod.rs
pub mod complex_event_populater;
pub mod selective_complex_event_populater;
pub mod stream_event_populator_factory;
pub mod stream_mapping_element;

pub use complex_event_populater::ComplexEventPopulater;
pub use selective_complex_event_populater::SelectiveComplexEventPopulater;
pub use stream_event_populator_factory::construct_event_populator;
pub use stream_mapping_element::StreamMappingElement;
