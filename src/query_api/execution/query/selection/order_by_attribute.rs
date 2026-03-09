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
use crate::query_api::expression::Variable;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)] // Default requires Variable to be Default
#[derive(Default)]
pub struct OrderByAttribute {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    // OrderByAttribute fields
    pub variable: Variable, // Variable struct has Default derive
    pub order: Order,
}

impl OrderByAttribute {
    // Constructor for `OrderByAttribute(Variable variable, Order order)`
    pub fn new(variable: Variable, order: Order) -> Self {
        OrderByAttribute {
            eventflux_element: EventFluxElement::default(), // Or copy from variable.eventflux_element?
            variable,
            order,
        }
    }

    // Constructor for `OrderByAttribute(Variable variable)` which defaults to ASC
    pub fn new_default_order(variable: Variable) -> Self {
        Self::new(variable, Order::default())
    }

    pub fn get_variable(&self) -> &Variable {
        &self.variable
    }

    pub fn get_order(&self) -> &Order {
        &self.order
    }
}

// If Variable is Default, OrderByAttribute can derive Default.
// Variable's Default requires attribute_name to be String::default().
