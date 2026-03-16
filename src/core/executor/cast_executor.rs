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

//! Cast Expression Executor
//!
//! Executes CAST expressions for type conversion between:
//! - String to numeric types (INT, LONG, FLOAT, DOUBLE)
//! - Numeric types to String
//! - Numeric type widening (INT -> LONG, FLOAT -> DOUBLE)
//! - Numeric type narrowing (LONG -> INT, DOUBLE -> FLOAT)

use super::expression_executor::ExpressionExecutor;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

/// Executor for CAST expressions
#[derive(Debug)]
pub struct CastExecutor {
    /// The inner expression to evaluate before casting
    inner_executor: Box<dyn ExpressionExecutor>,
    /// The target type to cast to
    target_type: ApiAttributeType,
}

impl CastExecutor {
    /// Create a new CastExecutor
    pub fn new(inner_executor: Box<dyn ExpressionExecutor>, target_type: ApiAttributeType) -> Self {
        Self {
            inner_executor,
            target_type,
        }
    }

    /// Perform the actual type conversion
    fn cast_value(&self, value: &AttributeValue) -> Option<AttributeValue> {
        match (&self.target_type, value) {
            // === String conversions ===
            (ApiAttributeType::STRING, AttributeValue::Int(i)) => {
                Some(AttributeValue::String(i.to_string()))
            }
            (ApiAttributeType::STRING, AttributeValue::Long(l)) => {
                Some(AttributeValue::String(l.to_string()))
            }
            (ApiAttributeType::STRING, AttributeValue::Float(f)) => {
                Some(AttributeValue::String(f.to_string()))
            }
            (ApiAttributeType::STRING, AttributeValue::Double(d)) => {
                Some(AttributeValue::String(d.to_string()))
            }
            (ApiAttributeType::STRING, AttributeValue::Bool(b)) => {
                Some(AttributeValue::String(b.to_string()))
            }
            (ApiAttributeType::STRING, AttributeValue::String(s)) => {
                Some(AttributeValue::String(s.clone()))
            }

            // === String to numeric conversions ===
            (ApiAttributeType::INT, AttributeValue::String(s)) => {
                s.trim().parse::<i32>().ok().map(AttributeValue::Int)
            }
            (ApiAttributeType::LONG, AttributeValue::String(s)) => {
                s.trim().parse::<i64>().ok().map(AttributeValue::Long)
            }
            (ApiAttributeType::FLOAT, AttributeValue::String(s)) => {
                s.trim().parse::<f32>().ok().map(AttributeValue::Float)
            }
            (ApiAttributeType::DOUBLE, AttributeValue::String(s)) => {
                s.trim().parse::<f64>().ok().map(AttributeValue::Double)
            }

            // === Numeric widening conversions ===
            (ApiAttributeType::LONG, AttributeValue::Int(i)) => {
                Some(AttributeValue::Long(*i as i64))
            }
            (ApiAttributeType::DOUBLE, AttributeValue::Float(f)) => {
                Some(AttributeValue::Double(*f as f64))
            }
            (ApiAttributeType::DOUBLE, AttributeValue::Int(i)) => {
                Some(AttributeValue::Double(*i as f64))
            }
            (ApiAttributeType::DOUBLE, AttributeValue::Long(l)) => {
                Some(AttributeValue::Double(*l as f64))
            }
            (ApiAttributeType::FLOAT, AttributeValue::Int(i)) => {
                Some(AttributeValue::Float(*i as f32))
            }
            (ApiAttributeType::FLOAT, AttributeValue::Long(l)) => {
                Some(AttributeValue::Float(*l as f32))
            }

            // === Numeric narrowing conversions (with overflow checking) ===
            (ApiAttributeType::INT, AttributeValue::Long(l)) => {
                // Check if long value fits in i32 range
                i32::try_from(*l).ok().map(AttributeValue::Int)
            }
            (ApiAttributeType::FLOAT, AttributeValue::Double(d)) => {
                // Check if double is within f32 range (not infinite after conversion)
                let f = *d as f32;
                if f.is_finite() || d.is_nan() {
                    Some(AttributeValue::Float(f))
                } else {
                    None // Overflow: value too large for f32
                }
            }
            (ApiAttributeType::INT, AttributeValue::Double(d)) => {
                // Check if double is within i32 range
                if *d >= (i32::MIN as f64) && *d <= (i32::MAX as f64) {
                    Some(AttributeValue::Int(*d as i32))
                } else {
                    None // Overflow: value out of i32 range
                }
            }
            (ApiAttributeType::INT, AttributeValue::Float(f)) => {
                // Check if float is within i32 range
                if *f >= (i32::MIN as f32) && *f <= (i32::MAX as f32) {
                    Some(AttributeValue::Int(*f as i32))
                } else {
                    None // Overflow: value out of i32 range
                }
            }
            (ApiAttributeType::LONG, AttributeValue::Double(d)) => {
                // Check if double is within i64 range
                if *d >= (i64::MIN as f64) && *d <= (i64::MAX as f64) {
                    Some(AttributeValue::Long(*d as i64))
                } else {
                    None // Overflow: value out of i64 range
                }
            }
            (ApiAttributeType::LONG, AttributeValue::Float(f)) => {
                // Check if float is within i64 range
                if *f >= (i64::MIN as f32) && *f <= (i64::MAX as f32) {
                    Some(AttributeValue::Long(*f as i64))
                } else {
                    None // Overflow: value out of i64 range
                }
            }

            // === Boolean conversions ===
            (ApiAttributeType::BOOL, AttributeValue::String(s)) => {
                let lower = s.trim().to_lowercase();
                match lower.as_str() {
                    "true" | "1" | "yes" => Some(AttributeValue::Bool(true)),
                    "false" | "0" | "no" => Some(AttributeValue::Bool(false)),
                    _ => None,
                }
            }
            (ApiAttributeType::BOOL, AttributeValue::Int(i)) => Some(AttributeValue::Bool(*i != 0)),
            (ApiAttributeType::BOOL, AttributeValue::Long(l)) => {
                Some(AttributeValue::Bool(*l != 0))
            }

            // === Same type (no-op) ===
            (ApiAttributeType::INT, AttributeValue::Int(i)) => Some(AttributeValue::Int(*i)),
            (ApiAttributeType::LONG, AttributeValue::Long(l)) => Some(AttributeValue::Long(*l)),
            (ApiAttributeType::FLOAT, AttributeValue::Float(f)) => Some(AttributeValue::Float(*f)),
            (ApiAttributeType::DOUBLE, AttributeValue::Double(d)) => {
                Some(AttributeValue::Double(*d))
            }
            (ApiAttributeType::BOOL, AttributeValue::Bool(b)) => Some(AttributeValue::Bool(*b)),

            // === Null handling ===
            (_, AttributeValue::Null) => Some(AttributeValue::Null),

            // === Unsupported conversions ===
            _ => None,
        }
    }
}

impl ExpressionExecutor for CastExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        // First evaluate the inner expression
        let inner_value = self.inner_executor.execute(event)?;

        // Then cast the result to the target type
        self.cast_value(&inner_value)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.target_type
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(CastExecutor {
            inner_executor: self.inner_executor.clone_executor(ctx),
            target_type: self.target_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::executor::constant_expression_executor::ConstantExpressionExecutor;

    fn make_constant_executor(value: AttributeValue) -> Box<dyn ExpressionExecutor> {
        let return_type = match &value {
            AttributeValue::String(_) => ApiAttributeType::STRING,
            AttributeValue::Int(_) => ApiAttributeType::INT,
            AttributeValue::Long(_) => ApiAttributeType::LONG,
            AttributeValue::Float(_) => ApiAttributeType::FLOAT,
            AttributeValue::Double(_) => ApiAttributeType::DOUBLE,
            AttributeValue::Bool(_) => ApiAttributeType::BOOL,
            AttributeValue::Null => ApiAttributeType::OBJECT,
            _ => ApiAttributeType::OBJECT,
        };
        Box::new(ConstantExpressionExecutor::new(value, return_type))
    }

    #[test]
    fn test_string_to_double() {
        let inner = make_constant_executor(AttributeValue::String("123.45".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::DOUBLE);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Double(123.45)));
    }

    #[test]
    fn test_string_to_int() {
        let inner = make_constant_executor(AttributeValue::String("42".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(42)));
    }

    #[test]
    fn test_string_to_long() {
        let inner = make_constant_executor(AttributeValue::String("9876543210".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::LONG);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Long(9876543210)));
    }

    #[test]
    fn test_int_to_string() {
        let inner = make_constant_executor(AttributeValue::Int(42));
        let cast = CastExecutor::new(inner, ApiAttributeType::STRING);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::String("42".to_string())));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_double_to_string() {
        let inner = make_constant_executor(AttributeValue::Double(3.14));
        let cast = CastExecutor::new(inner, ApiAttributeType::STRING);

        let result = cast.execute(None);
        // Note: floating point to string might have precision variations
        assert!(matches!(result, Some(AttributeValue::String(_))));
    }

    #[test]
    fn test_int_to_long() {
        let inner = make_constant_executor(AttributeValue::Int(42));
        let cast = CastExecutor::new(inner, ApiAttributeType::LONG);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Long(42)));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_float_to_double() {
        let inner = make_constant_executor(AttributeValue::Float(3.14_f32));
        let cast = CastExecutor::new(inner, ApiAttributeType::DOUBLE);

        let result = cast.execute(None);
        if let Some(AttributeValue::Double(d)) = result {
            assert!((d - 3.14).abs() < 0.01);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_invalid_string_to_int() {
        let inner = make_constant_executor(AttributeValue::String("not_a_number".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_null_passthrough() {
        let inner = make_constant_executor(AttributeValue::Null);
        let cast = CastExecutor::new(inner, ApiAttributeType::DOUBLE);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Null));
    }

    #[test]
    fn test_string_with_whitespace() {
        let inner = make_constant_executor(AttributeValue::String("  123.45  ".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::DOUBLE);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Double(123.45)));
    }

    #[test]
    fn test_return_type() {
        let inner = make_constant_executor(AttributeValue::String("123".to_string()));
        let cast = CastExecutor::new(inner, ApiAttributeType::DOUBLE);

        assert_eq!(cast.get_return_type(), ApiAttributeType::DOUBLE);
    }

    // === Overflow tests for narrowing conversions ===

    #[test]
    fn test_long_to_int_overflow() {
        // Value larger than i32::MAX should return None
        let inner = make_constant_executor(AttributeValue::Long(9876543210_i64));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_long_to_int_underflow() {
        // Value smaller than i32::MIN should return None
        let inner = make_constant_executor(AttributeValue::Long(i64::MIN));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_long_to_int_valid() {
        // Value within i32 range should work
        let inner = make_constant_executor(AttributeValue::Long(42_i64));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, Some(AttributeValue::Int(42)));
    }

    #[test]
    fn test_double_to_int_overflow() {
        // Value larger than i32::MAX should return None
        let inner = make_constant_executor(AttributeValue::Double(3_000_000_000.0));
        let cast = CastExecutor::new(inner, ApiAttributeType::INT);

        let result = cast.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_double_to_float_overflow() {
        // Value larger than f32::MAX should return None
        let inner = make_constant_executor(AttributeValue::Double(f64::MAX));
        let cast = CastExecutor::new(inner, ApiAttributeType::FLOAT);

        let result = cast.execute(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_double_to_float_valid() {
        // Normal value should work
        let inner = make_constant_executor(AttributeValue::Double(123.45));
        let cast = CastExecutor::new(inner, ApiAttributeType::FLOAT);

        let result = cast.execute(None);
        if let Some(AttributeValue::Float(f)) = result {
            assert!((f - 123.45_f32).abs() < 0.01);
        } else {
            panic!("Expected Float");
        }
    }
}
