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

use super::stream::UpdateSet;
use crate::query_api::eventflux_element::EventFluxElement;
use crate::query_api::expression::Expression;

/// Which events a query emits. Defaults to `CurrentEvents`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Default)]
pub enum OutputEventType {
    ExpiredEvents,
    #[default]
    CurrentEvents,
    AllEvents,
    AllRawEvents,
    ExpiredRawEvents,
}

// The action structs hold no EventFluxElement. The OutputStream that wraps them holds the
// query positions.

/// INSERT INTO a stream. The default has an empty target id.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct InsertIntoStreamAction {
    pub target_id: String,
    pub is_inner_stream: bool,
    pub is_fault_stream: bool,
}

/// Return the results to the caller instead of sending them to a stream.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct ReturnStreamAction {
    // No fields
}

/// Delete matching rows from a table. Has no `Default` because the condition is required.
#[derive(Clone, Debug, PartialEq)]
pub struct DeleteStreamAction {
    pub target_id: String,
    pub on_delete_expression: Expression,
    /// Alias for the target table, such as "s" in "DELETE FROM stockTable AS s".
    pub target_alias: Option<String>,
    /// Alias for the source stream, such as "d" in "USING deleteStream AS d".
    pub source_alias: Option<String>,
}

/// Update matching rows in a table. Has no `Default` because the condition is required.
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateStreamAction {
    pub target_id: String,
    /// The ON condition that picks the rows to update.
    pub on_update_expression: Expression,
    /// The SET assignments.
    pub update_set_clause: Option<UpdateSet>,
    /// Alias for the target table, such as "s" in "UPDATE stockTable AS s".
    pub target_alias: Option<String>,
    /// Alias for the source stream, such as "u" in "FROM updateStream AS u".
    pub source_alias: Option<String>,
}

/// Update matching rows in a table, or insert when none match. Has no `Default` because
/// the condition is required.
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateOrInsertStreamAction {
    pub target_id: String,
    /// The ON condition that picks the rows to update.
    pub on_update_expression: Expression,
    /// The SET assignments.
    pub update_set_clause: Option<UpdateSet>,
    /// Alias for the target table. The SQL compiler always leaves it `None` because
    /// UPSERT INTO has no table alias syntax.
    pub target_alias: Option<String>,
    /// Alias for the source stream, from SELECT ... FROM stream AS alias.
    pub source_alias: Option<String>,
}

/// Where a query sends its results. Defaults to `Return`.
#[derive(Clone, Debug, PartialEq)]
pub enum OutputStreamAction {
    InsertInto(InsertIntoStreamAction),
    Return(ReturnStreamAction),
    Delete(DeleteStreamAction),
    Update(UpdateStreamAction),
    UpdateOrInsert(UpdateOrInsertStreamAction),
}

impl Default for OutputStreamAction {
    fn default() -> Self {
        OutputStreamAction::Return(ReturnStreamAction::default())
    }
}

/// The output part of a query. An action plus the event type to emit.
///
/// The derived default has a Return action and no event type.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct OutputStream {
    pub eventflux_element: EventFluxElement,

    pub action: OutputStreamAction,
    pub output_event_type: Option<OutputEventType>,
}

impl OutputStream {
    /// A `None` event type becomes `CurrentEvents`, so the result always has one set.
    pub fn new(action: OutputStreamAction, initial_event_type: Option<OutputEventType>) -> Self {
        OutputStream {
            eventflux_element: EventFluxElement::default(),
            action,
            output_event_type: initial_event_type.or(Some(OutputEventType::default())),
        }
    }

    /// A Return action that emits `CurrentEvents`.
    pub fn default_return_stream() -> Self {
        Self {
            eventflux_element: EventFluxElement::default(),
            action: OutputStreamAction::Return(ReturnStreamAction::default()),
            output_event_type: Some(OutputEventType::CurrentEvents),
        }
    }

    /// The target stream or table id, or `None` for a Return action.
    pub fn get_target_id(&self) -> Option<&str> {
        match &self.action {
            OutputStreamAction::InsertInto(a) => Some(&a.target_id),
            OutputStreamAction::Delete(a) => Some(&a.target_id),
            OutputStreamAction::Update(a) => Some(&a.target_id),
            OutputStreamAction::UpdateOrInsert(a) => Some(&a.target_id),
            OutputStreamAction::Return(_) => None,
        }
    }

    pub fn get_output_event_type(&self) -> Option<OutputEventType> {
        self.output_event_type
    }

    pub fn set_output_event_type(&mut self, event_type: OutputEventType) {
        self.output_event_type = Some(event_type);
    }

    pub fn set_output_event_type_if_none(&mut self, event_type: OutputEventType) {
        if self.output_event_type.is_none() {
            self.output_event_type = Some(event_type);
        }
    }
}

// Read the query positions through the public eventflux_element field.
