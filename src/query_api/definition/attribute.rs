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

/// Data type of an attribute. Defaults to `OBJECT`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Type {
    STRING,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOL,
    #[default]
    OBJECT,
}

/// A named, typed field of a stream, table or other definition.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Attribute {
    pub eventflux_element: EventFluxElement,

    pub name: String,
    pub attribute_type: Type,
}

impl Attribute {
    pub fn new(name: String, attribute_type: Type) -> Self {
        Attribute {
            eventflux_element: EventFluxElement::default(),
            name,
            attribute_type,
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_type(&self) -> &Type {
        &self.attribute_type
    }
}

// Possible Deref impls, if callers ever need direct access to the EventFluxElement fields.
// impl std::ops::Deref for Attribute {
//     type Target = EventFluxElement;
//     fn deref(&self) -> &Self::Target { &self.eventflux_element }
// }
// impl std::ops::DerefMut for Attribute {
//     fn deref_mut(&mut self) -> &mut Self::Target { &mut self.eventflux_element }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_creation() {
        let attr = Attribute::new("timestamp".to_string(), Type::LONG);
        assert_eq!(attr.get_name(), "timestamp");
        assert_eq!(attr.get_type(), &Type::LONG);
        // The query positions start unset.
        assert_eq!(attr.eventflux_element.query_context_start_index, None);
        assert_eq!(attr.eventflux_element.query_context_end_index, None);
    }

    #[test]
    fn test_attribute_type_default() {
        assert_eq!(Type::default(), Type::OBJECT);
    }
}
