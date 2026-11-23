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

use super::state_element::StateElement;
use crate::query_api::eventflux_element::EventFluxElement;

/// Two pattern steps in order, as in `A -> B`. `next_state_element` must follow
/// `state_element`.
///
/// Has no `Default` because StateElement has no default value.
#[derive(Clone, Debug, PartialEq)]
pub struct NextStateElement {
    pub eventflux_element: EventFluxElement,

    pub state_element: Box<StateElement>,
    pub next_state_element: Box<StateElement>,
}

impl NextStateElement {
    pub fn new(state_element: StateElement, next_state_element: StateElement) -> Self {
        NextStateElement {
            eventflux_element: EventFluxElement::default(),
            state_element: Box::new(state_element),
            next_state_element: Box::new(next_state_element),
        }
    }
}
