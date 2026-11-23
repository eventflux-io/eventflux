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

// Corresponds to io.eventflux.core.executor.ConstantExpressionExecutor
use super::expression_executor::ExpressionExecutor;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::query_api::definition::attribute::Type as ApiAttributeType;

/// Executor that returns a constant value. It ignores the event.
#[derive(Debug, Clone)]
pub struct ConstantExpressionExecutor {
    value: AttributeValue,
    return_type: ApiAttributeType,
}

impl ConstantExpressionExecutor {
    /// Does not check that `value` matches `return_type`. The caller, such as
    /// ExpressionParser, must pass a matching pair, for example AttributeValue::Int with
    /// Attribute::Type::INT.
    pub fn new(value: AttributeValue, return_type: ApiAttributeType) -> Self {
        Self { value, return_type }
    }

    // Not part of the Java ExpressionExecutor interface
    pub fn get_value(&self) -> &AttributeValue {
        &self.value
    }
}

impl ExpressionExecutor for ConstantExpressionExecutor {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        Some(self.value.clone())
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        _eventflux_app_context: &std::sync::Arc<
            crate::core::config::eventflux_app_context::EventFluxAppContext,
        >,
    ) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::value::AttributeValue;
    // ApiAttributeType comes from the parent module through super::*
    use crate::core::config::eventflux_app_context::EventFluxAppContext;
    use crate::core::executor::expression_executor::ExpressionExecutor;
    use std::sync::Arc;

    #[test]
    fn test_constant_string() {
        let exec = ConstantExpressionExecutor::new(
            AttributeValue::String("hello".to_string()),
            ApiAttributeType::STRING,
        );
        let result = exec.execute(None);
        assert_eq!(result, Some(AttributeValue::String("hello".to_string())));
        assert_eq!(exec.get_return_type(), ApiAttributeType::STRING);
    }

    #[test]
    fn test_constant_int() {
        let exec = ConstantExpressionExecutor::new(AttributeValue::Int(123), ApiAttributeType::INT);
        let result = exec.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(123)));
        assert_eq!(exec.get_return_type(), ApiAttributeType::INT);
    }

    #[test]
    fn test_constant_clone() {
        let exec = ConstantExpressionExecutor::new(
            AttributeValue::String("clone_me".to_string()),
            ApiAttributeType::STRING,
        );
        let app_ctx_placeholder = Arc::new(EventFluxAppContext::default_for_testing());
        let cloned_exec = exec.clone_executor(&app_ctx_placeholder);

        let result = cloned_exec.execute(None);
        assert_eq!(result, Some(AttributeValue::String("clone_me".to_string())));
        assert_eq!(cloned_exec.get_return_type(), ApiAttributeType::STRING);
    }
}
