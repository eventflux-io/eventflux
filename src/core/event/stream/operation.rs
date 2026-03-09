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

// src/core/event/stream/operation.rs
// Corresponds to io.eventflux.core.event.stream.Operation
#[derive(Debug)]
pub struct Operation {
    pub operation: Operator,
    pub parameters: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl Operation {
    pub fn new(operation: Operator) -> Self {
        Self {
            operation,
            parameters: None,
        }
    }

    pub fn with_parameters(
        operation: Operator,
        parameters: Box<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            operation,
            parameters: Some(parameters),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Add,
    Remove,
    Clear,
    Overwrite,
    DeleteByOperator,
    DeleteByIndex,
}
