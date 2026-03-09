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
use crate::query_api::expression::Variable;

// Corrected structure (previous duplicate removed):
#[derive(Clone, Debug, PartialEq)]
pub struct OutputAttribute {
    pub eventflux_element: EventFluxElement,
    pub rename: Option<String>,
    pub expression: Expression,
}

impl OutputAttribute {
    // Constructor for `OutputAttribute(String rename, Expression expression)`
    // Making rename Option<String> as per prompt and current code.
    pub fn new(rename: Option<String>, expression: Expression) -> Self {
        OutputAttribute {
            eventflux_element: EventFluxElement::default(),
            rename,
            expression,
        }
    }

    // Constructor for `OutputAttribute(Variable variable)`
    pub fn new_from_variable(variable: Variable) -> Self {
        OutputAttribute {
            eventflux_element: EventFluxElement::default(), // Or copy from variable.eventflux_element?
            rename: Some(variable.attribute_name.clone()),
            expression: Expression::Variable(variable),
        }
    }

    pub fn get_rename(&self) -> &Option<String> {
        &self.rename
    }

    pub fn get_expression(&self) -> &Expression {
        &self.expression
    }
}

// No Default derive.
