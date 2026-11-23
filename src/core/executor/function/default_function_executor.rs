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

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// The default(value, defaultValue) function. Returns defaultValue when value is NULL or
/// gives no value, otherwise returns value. A two argument coalesce.
///
/// The declared return type is the wider of the two numeric types, but the value that
/// comes back is not converted to it. It keeps the type of whichever argument produced it.
#[derive(Debug)]
pub struct DefaultFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    default_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType,
}

impl DefaultFunctionExecutor {
    /// # Errors
    ///
    /// Returns an error unless there are exactly 2 arguments whose types are the same or
    /// both numeric.
    pub fn new(args: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if args.len() != 2 {
            return Err(format!(
                "default() requires exactly 2 arguments, got {}",
                args.len()
            ));
        }

        let mut args = args;
        let default_executor = args.pop().unwrap();
        let value_executor = args.pop().unwrap();

        let value_type = value_executor.get_return_type();
        let default_type = default_executor.get_return_type();

        if value_type == default_type {
            return Ok(Self {
                value_executor,
                default_executor,
                return_type: value_type,
            });
        }

        // Types differ, so both must be numeric. Widen INT -> LONG -> FLOAT -> DOUBLE.
        let return_type = Self::widen_numeric_types(value_type, default_type)?;

        Ok(Self {
            value_executor,
            default_executor,
            return_type,
        })
    }

    /// Returns the wider of two numeric types, in the order INT < LONG < FLOAT < DOUBLE.
    /// Returns an error if either type is not numeric.
    fn widen_numeric_types(
        t1: ApiAttributeType,
        t2: ApiAttributeType,
    ) -> Result<ApiAttributeType, String> {
        use ApiAttributeType::*;

        let is_numeric = |t: ApiAttributeType| matches!(t, INT | LONG | FLOAT | DOUBLE);

        if !is_numeric(t1) || !is_numeric(t2) {
            return Err(format!(
                "default() requires compatible types. Found {:?} and {:?}",
                t1, t2
            ));
        }

        let type_rank = |t: ApiAttributeType| match t {
            INT => 1,
            LONG => 2,
            FLOAT => 3,
            DOUBLE => 4,
            _ => 0,
        };

        if type_rank(t1) >= type_rank(t2) {
            Ok(t1)
        } else {
            Ok(t2)
        }
    }
}

impl ExpressionExecutor for DefaultFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.value_executor.execute(event) {
            Some(AttributeValue::Null) | None => {
                // NULL or no value, so use the default
                self.default_executor.execute(event)
            }
            Some(value) => {
                // Not NULL, so return it unchanged
                Some(value)
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(
            DefaultFunctionExecutor::new(vec![
                self.value_executor.clone_executor(ctx),
                self.default_executor.clone_executor(ctx),
            ])
            .expect("Cloning DefaultFunctionExecutor failed"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;

    #[test]
    fn test_default_returns_value_when_not_null() {
        let executor = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(42),
                ApiAttributeType::INT,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(0),
                ApiAttributeType::INT,
            )),
        ])
        .unwrap();

        let result = executor.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(42)));
    }

    #[test]
    fn test_default_returns_default_when_null() {
        let executor = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Null,
                ApiAttributeType::INT,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(99),
                ApiAttributeType::INT,
            )),
        ])
        .unwrap();

        let result = executor.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(99)));
    }

    #[test]
    fn test_default_with_string() {
        let executor = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Null,
                ApiAttributeType::STRING,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::String("fallback".to_string()),
                ApiAttributeType::STRING,
            )),
        ])
        .unwrap();

        let result = executor.execute(None);
        assert_eq!(result, Some(AttributeValue::String("fallback".to_string())));
    }

    #[test]
    fn test_default_requires_two_args() {
        let result = DefaultFunctionExecutor::new(vec![Box::new(ConstantExpressionExecutor::new(
            AttributeValue::Int(1),
            ApiAttributeType::INT,
        ))]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("2 arguments"));
    }

    #[test]
    fn test_default_requires_compatible_types() {
        let result = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(1),
                ApiAttributeType::INT,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::String("text".to_string()),
                ApiAttributeType::STRING,
            )),
        ]);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("compatible types"));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_default_allows_numeric_widening() {
        // INT + LONG should work
        let result = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(1),
                ApiAttributeType::INT,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Long(999),
                ApiAttributeType::LONG,
            )),
        ]);
        assert!(result.is_ok());

        // INT + DOUBLE should work
        let result = DefaultFunctionExecutor::new(vec![
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Int(1),
                ApiAttributeType::INT,
            )),
            Box::new(ConstantExpressionExecutor::new(
                AttributeValue::Double(3.14),
                ApiAttributeType::DOUBLE,
            )),
        ]);
        assert!(result.is_ok());
    }
}
