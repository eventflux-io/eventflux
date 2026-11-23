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

/// An EVERY pattern step. Restarts matching of the inner element after each match.
///
/// Has no `Default` because the inner element is required.
#[derive(Clone, Debug, PartialEq)]
pub struct EveryStateElement {
    pub eventflux_element: EventFluxElement,

    pub state_element: Box<StateElement>,
    // No WITHIN here. It applies to the whole pattern and lives on StateInputStream.
}

impl EveryStateElement {
    pub fn new(state_element: StateElement) -> Self {
        EveryStateElement {
            eventflux_element: EventFluxElement::default(),
            state_element: Box::new(state_element),
        }
    }
}
