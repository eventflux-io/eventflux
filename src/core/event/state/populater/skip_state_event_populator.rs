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

// src/core/event/state/populater/skip_state_event_populator.rs
use super::state_event_populator::StateEventPopulator;
use crate::core::event::complex_event::ComplexEvent;

#[derive(Debug, Clone)]
pub struct SkipStateEventPopulator;

impl StateEventPopulator for SkipStateEventPopulator {
    fn populate_state_event(&self, _complex_event: &mut dyn ComplexEvent) {
        // intentionally no-op
    }
}
