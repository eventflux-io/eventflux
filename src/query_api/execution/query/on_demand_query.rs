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
// Annotation is not used here as Java OnDemandQuery doesn't have annotations field.
// use crate::query_api::annotation::Annotation;
use super::{OutputEventType, OutputStream}; // Use parent module's re-exports
use crate::query_api::execution::query::input::InputStore;
use crate::query_api::execution::query::output::output_stream::{
    DeleteStreamAction, OutputStreamAction, UpdateOrInsertStreamAction, UpdateStreamAction,
};
use crate::query_api::execution::query::output::stream::UpdateSet;
use crate::query_api::execution::query::selection::Selector;
use crate::query_api::expression::Expression;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)] // Added Eq, Hash, Copy
#[derive(Default)]
pub enum OnDemandQueryType {
    Insert,
    Delete,
    Update,
    #[default]
    Select,
    UpdateOrInsert,
    Find,
}

#[derive(Clone, Debug, PartialEq)] // Default will be custom via new()
pub struct OnDemandQuery {
    pub eventflux_element: EventFluxElement, // Composed EventFluxElement

    pub input_store: Option<InputStore>,
    pub selector: Selector,
    pub output_stream: OutputStream,
    pub on_demand_query_type: Option<OnDemandQueryType>,
    // No annotations field in Java OnDemandQuery
}

impl OnDemandQuery {
    pub fn new() -> Self {
        OnDemandQuery {
            eventflux_element: EventFluxElement::default(),
            input_store: None,
            selector: Selector::new(),
            output_stream: OutputStream::default_return_stream(),
            on_demand_query_type: None, // Or Some(OnDemandQueryType::default())
        }
    }

    // Static factory from Java
    pub fn query() -> Self {
        Self::new()
    }

    // Builder methods
    pub fn from(mut self, input_store: InputStore) -> Self {
        self.input_store = Some(input_store);
        self
    }

    pub fn select(mut self, selector: Selector) -> Self {
        self.selector = selector;
        self
    }

    pub fn out_stream(mut self, output_stream: OutputStream) -> Self {
        self.output_stream = output_stream;
        if self.output_stream.get_output_event_type().is_none() {
            self.output_stream
                .set_output_event_type_if_none(OutputEventType::CurrentEvents);
        }
        self
    }

    pub fn set_type(mut self, query_type: OnDemandQueryType) -> Self {
        self.on_demand_query_type = Some(query_type);
        self
    }

    pub fn delete_by(
        mut self,
        output_table_id: String,
        on_deleting_expression: Expression,
    ) -> Self {
        let action = DeleteStreamAction {
            target_id: output_table_id,
            on_delete_expression: on_deleting_expression,
            target_alias: None, // Programmatic API, no SQL alias
            source_alias: None,
        };
        self.output_stream = OutputStream::new(
            OutputStreamAction::Delete(action),
            Some(OutputEventType::CurrentEvents),
        );
        self
    }

    pub fn update_by(mut self, output_table_id: String, on_update_expression: Expression) -> Self {
        let action = UpdateStreamAction {
            target_id: output_table_id,
            on_update_expression,
            update_set_clause: None,
            target_alias: None, // Programmatic API, no SQL alias
            source_alias: None,
        };
        self.output_stream = OutputStream::new(
            OutputStreamAction::Update(action),
            Some(OutputEventType::CurrentEvents),
        );
        self
    }

    pub fn update_by_with_set(
        mut self,
        output_table_id: String,
        update_set_attributes: UpdateSet,
        on_update_expression: Expression,
    ) -> Self {
        let action = UpdateStreamAction {
            target_id: output_table_id,
            on_update_expression,
            update_set_clause: Some(update_set_attributes),
            target_alias: None, // Programmatic API, no SQL alias
            source_alias: None,
        };
        self.output_stream = OutputStream::new(
            OutputStreamAction::Update(action),
            Some(OutputEventType::CurrentEvents),
        );
        self
    }

    pub fn update_or_insert_by(
        mut self,
        output_table_id: String,
        update_set_attributes: UpdateSet,
        on_update_expression: Expression,
    ) -> Self {
        let action = UpdateOrInsertStreamAction {
            target_id: output_table_id,
            on_update_expression,
            update_set_clause: Some(update_set_attributes),
            target_alias: None, // Programmatic API, no SQL alias
            source_alias: None,
        };
        self.output_stream = OutputStream::new(
            OutputStreamAction::UpdateOrInsert(action),
            Some(OutputEventType::CurrentEvents),
        );
        self
    }

    pub fn get_input_store(&self) -> Option<&InputStore> {
        self.input_store.as_ref()
    }

    pub fn get_selector(&self) -> &Selector {
        &self.selector
    }

    pub fn get_output_stream(&self) -> &OutputStream {
        &self.output_stream
    }

    pub fn get_type(&self) -> Option<OnDemandQueryType> {
        self.on_demand_query_type
    }
}

impl Default for OnDemandQuery {
    fn default() -> Self {
        Self::new()
    }
}

// EventFluxElement is composed. Access via self.eventflux_element.
// No direct impl of EventFluxElement trait needed on OnDemandQuery itself if using composition and public field.
// However, Java's OnDemandQuery *is* a EventFluxElement. So, if we want to pass OnDemandQuery
// where a dyn EventFluxElement is expected, it should implement the trait (by delegating).
// For now, assuming direct access to composed `eventflux_element` is sufficient, or this will be added later.
