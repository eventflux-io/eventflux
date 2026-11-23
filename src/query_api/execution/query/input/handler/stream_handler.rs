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
use crate::query_api::expression::Expression;

use super::filter::Filter;
use super::stream_function::StreamFunction;
use super::window::Window as WindowHandler;

/// Methods shared by all stream handlers.
pub trait StreamHandlerTrait {
    /// The handler's parameter expressions. A filter always gives its one condition.
    /// A function or window gives `None` when it has no parameters.
    fn get_parameters_as_option_vec(&self) -> Option<Vec<&Expression>>;
}

/// One handler applied to an input stream.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamHandler {
    Filter(Filter),
    Function(StreamFunction),
    // Boxed because Window is larger than the other variants.
    Window(Box<WindowHandler>),
}

impl StreamHandler {
    // The EventFluxElement of whichever handler this wraps. Unused for now.
    #[allow(dead_code)]
    fn eventflux_element_ref(&self) -> &EventFluxElement {
        match self {
            StreamHandler::Filter(f) => &f.eventflux_element,
            StreamHandler::Function(sf) => &sf.eventflux_element,
            StreamHandler::Window(w) => &w.eventflux_element,
        }
    }

    #[allow(dead_code)]
    fn eventflux_element_mut_ref(&mut self) -> &mut EventFluxElement {
        match self {
            StreamHandler::Filter(f) => &mut f.eventflux_element,
            StreamHandler::Function(sf) => &mut sf.eventflux_element,
            StreamHandler::Window(w) => &mut w.eventflux_element,
        }
    }
}

impl StreamHandlerTrait for StreamHandler {
    fn get_parameters_as_option_vec(&self) -> Option<Vec<&Expression>> {
        match self {
            StreamHandler::Filter(f) => Some(f.get_parameters_ref_internal()),
            StreamHandler::Function(sf) => sf.get_parameters_ref_internal(),
            StreamHandler::Window(w) => w.get_parameters_ref_internal(),
        }
    }
}

// A new handler type needs an `eventflux_element: EventFluxElement` field and a
// `get_parameters_ref_internal()` method for the dispatch above.
