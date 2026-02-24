// SPDX-License-Identifier: MIT OR Apache-2.0

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
