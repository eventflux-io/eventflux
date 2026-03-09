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

// Corresponds to io.eventflux.query.api.execution.query.input.handler.Filter
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

// Corrected structure (previous duplicate removed):
#[derive(Clone, Debug, PartialEq)]
pub struct Filter {
    pub eventflux_element: EventFluxElement,
    pub filter_expression: Expression,
}

impl Filter {
    pub fn new(filter_expression: Expression) -> Self {
        Filter {
            eventflux_element: EventFluxElement::default(),
            filter_expression,
        }
    }

    // For StreamHandlerTrait's get_parameters_as_option_vec
    pub(super) fn get_parameters_ref_internal(&self) -> Vec<&Expression> {
        vec![&self.filter_expression]
    }
}

// No Default derive for Filter as filter_expression is mandatory.
