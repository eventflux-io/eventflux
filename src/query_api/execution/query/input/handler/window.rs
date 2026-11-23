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

/// A window applied to an input stream, such as `length(10)`. Exported as `WindowHandler`
/// from the handler module.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Window {
    pub eventflux_element: EventFluxElement,

    pub namespace: Option<String>,
    /// Window type name, such as `length` or `time`.
    pub name: String,

    pub parameters: Vec<Expression>,
}

impl Window {
    pub fn new(name: String, namespace: Option<String>, parameters: Vec<Expression>) -> Self {
        Window {
            eventflux_element: EventFluxElement::default(),
            namespace,
            name,
            parameters,
        }
    }

    pub fn get_parameters(&self) -> &[Expression] {
        &self.parameters
    }

    // Used by StreamHandlerTrait::get_parameters_as_option_vec. Gives None when there are
    // no parameters.
    pub(super) fn get_parameters_ref_internal(&self) -> Option<Vec<&Expression>> {
        if self.parameters.is_empty() {
            None
        } else {
            Some(self.parameters.iter().collect())
        }
    }
}

// Possible Extension trait shared with StreamFunction. Not used.
// pub trait Extension {
//     fn get_namespace(&self) -> Option<&str>;
//     fn get_name(&self) -> &str;
// }
// impl Extension for Window {
//     fn get_namespace(&self) -> Option<&str> { self.namespace.as_deref() }
//     fn get_name(&self) -> &str { &self.name }
// }
