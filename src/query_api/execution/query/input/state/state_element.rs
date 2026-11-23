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

use crate::query_api::eventflux_element::EventFluxElement;

use super::absent_stream_state_element::AbsentStreamStateElement;
use super::count_state_element::CountStateElement;
use super::every_state_element::EveryStateElement;
use super::logical_state_element::LogicalStateElement;
use super::next_state_element::NextStateElement;
use super::stream_state_element::StreamStateElement;

/// One node in a pattern or sequence tree.
#[derive(Clone, Debug, PartialEq)]
pub enum StateElement {
    Stream(StreamStateElement),
    AbsentStream(AbsentStreamStateElement),
    Logical(LogicalStateElement), // boxes its two sides itself
    Next(Box<NextStateElement>),
    Count(CountStateElement), // boxes its stream element itself
    Every(Box<EveryStateElement>),
}

// Reach the EventFluxElement of whichever variant this is. Unused for now.
impl StateElement {
    #[allow(dead_code)]
    fn eventflux_element_ref(&self) -> &EventFluxElement {
        match self {
            StateElement::Stream(s) => &s.eventflux_element,
            StateElement::AbsentStream(a) => a.eventflux_element(),
            StateElement::Logical(l) => &l.eventflux_element,
            StateElement::Next(n) => &n.eventflux_element,
            StateElement::Count(c) => &c.eventflux_element,
            StateElement::Every(e) => &e.eventflux_element,
        }
    }

    #[allow(dead_code)]
    fn eventflux_element_mut_ref(&mut self) -> &mut EventFluxElement {
        match self {
            StateElement::Stream(s) => &mut s.eventflux_element,
            StateElement::AbsentStream(a) => a.eventflux_element_mut(),
            StateElement::Logical(l) => &mut l.eventflux_element,
            StateElement::Next(n) => &mut n.eventflux_element,
            StateElement::Count(c) => &mut c.eventflux_element,
            StateElement::Every(e) => &mut e.eventflux_element,
        }
    }
}
