// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/default_function_executor.rs
// Implements default(value, defaultValue) - returns defaultValue if value is null

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// DefaultFunctionExecutor implements the default(value, defaultValue) function.
/// Returns defaultValue if value is null, otherwise returns value.
/// This is essentially a 2-argument version of coalesce.
#[derive(Debug)]
pub struct DefaultFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    default_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType,
}

impl DefaultFunctionExecutor {
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

        // Allow exact type match
        if value_type == default_type {
            return Ok(Self {
                value_executor,
                default_executor,
                return_type: value_type,
            });
        }

        // Allow numeric type widening (INT -> LONG -> FLOAT -> DOUBLE)
        let return_type = Self::widen_numeric_types(value_type, default_type)?;

        Ok(Self {
            value_executor,
            default_executor,
            return_type,
        })
    }

    /// Determines the widest compatible numeric type for two types.
    /// Returns an error if types are incompatible.
    fn widen_numeric_types(
        t1: ApiAttributeType,
        t2: ApiAttributeType,
    ) -> Result<ApiAttributeType, String> {
        use ApiAttributeType::*;

        // Check if both are numeric
        let is_numeric = |t: ApiAttributeType| matches!(t, INT | LONG | FLOAT | DOUBLE);

        if !is_numeric(t1) || !is_numeric(t2) {
            return Err(format!(
                "default() requires compatible types. Found {:?} and {:?}",
                t1, t2
            ));
        }

        // Numeric widening hierarchy: INT < LONG < FLOAT < DOUBLE
        let type_rank = |t: ApiAttributeType| match t {
            INT => 1,
            LONG => 2,
            FLOAT => 3,
            DOUBLE => 4,
            _ => 0,
        };

        // Return the wider type
        if type_rank(t1) >= type_rank(t2) {
            Ok(t1)
        } else {
            Ok(t2)
        }
    }
}

impl ExpressionExecutor for DefaultFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        // Execute the value expression
        match self.value_executor.execute(event) {
            Some(AttributeValue::Null) | None => {
                // Value is null, return the default
                self.default_executor.execute(event)
            }
            Some(value) => {
                // Value is not null, return it
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
