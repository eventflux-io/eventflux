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

/// Sort direction. Defaults to `Asc`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

/// One ORDER BY entry. The attribute to sort on and the direction.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct OrderByAttribute {
    pub eventflux_element: EventFluxElement,

    pub variable: Variable,
    pub order: Order,
}

impl OrderByAttribute {
    /// The query positions start unset. They are not copied from `variable`.
    pub fn new(variable: Variable, order: Order) -> Self {
        OrderByAttribute {
            eventflux_element: EventFluxElement::default(),
            variable,
            order,
        }
    }

    /// Sort on `variable` in ascending order.
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
