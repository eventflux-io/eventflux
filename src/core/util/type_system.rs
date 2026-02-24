// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/type_system.rs
// Comprehensive type system for EventFlux Rust implementing Java-compatible type conversion behavior

use crate::core::event::value::AttributeValue;
use crate::core::exception::EventFluxError;
use crate::query_api::definition::attribute::Type as AttributeType;

/// Type precedence for arithmetic operations following Java EventFlux rules
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypePrecedence {
    Int = 0,
    Long = 1,
    Float = 2,
    Double = 3,
}

impl TypePrecedence {
    /// Get type precedence from AttributeType
    pub fn from_attribute_type(attr_type: AttributeType) -> Option<Self> {
        match attr_type {
            AttributeType::INT => Some(TypePrecedence::Int),
            AttributeType::LONG => Some(TypePrecedence::Long),
            AttributeType::FLOAT => Some(TypePrecedence::Float),
            AttributeType::DOUBLE => Some(TypePrecedence::Double),
            _ => None,
        }
    }

    /// Convert to AttributeType
    pub fn to_attribute_type(self) -> AttributeType {
        match self {
            TypePrecedence::Int => AttributeType::INT,
            TypePrecedence::Long => AttributeType::LONG,
            TypePrecedence::Float => AttributeType::FLOAT,
            TypePrecedence::Double => AttributeType::DOUBLE,
        }
    }
}

/// Determine the result type for arithmetic operations between two types
pub fn get_arithmetic_result_type(
    left: AttributeType,
    right: AttributeType,
) -> Result<AttributeType, EventFluxError> {
    let left_precedence = TypePrecedence::from_attribute_type(left);
    let right_precedence = TypePrecedence::from_attribute_type(right);

    match (left_precedence, right_precedence) {
        (Some(l), Some(r)) => {
            // Return the type with higher precedence
            let result_precedence = std::cmp::max(l, r);
            Ok(result_precedence.to_attribute_type())
        }
        _ => Err(EventFluxError::type_error(
            "Arithmetic operations only supported on numeric types",
            format!("{left:?}"),
            format!("{right:?}"),
        )),
    }
}

/// Comprehensive type conversion following Java EventFlux rules
pub struct TypeConverter;

impl TypeConverter {
    /// Convert AttributeValue to target type with Java-compatible behavior
    /// Returns None for invalid conversions (matching Java's null return behavior)
    pub fn convert(value: AttributeValue, target_type: AttributeType) -> Option<AttributeValue> {
        // Handle null values first
        if value.is_null() {
            return match target_type {
                AttributeType::STRING => Some(AttributeValue::String("null".to_string())),
                _ => Some(AttributeValue::Null),
            };
        }

        match target_type {
            AttributeType::BOOL => Self::to_bool(value),
            AttributeType::INT => Self::to_int(value),
            AttributeType::LONG => Self::to_long(value),
            AttributeType::FLOAT => Self::to_float(value),
            AttributeType::DOUBLE => Self::to_double(value),
            AttributeType::STRING => Self::to_string(value),
            AttributeType::OBJECT => Some(value), // Object type accepts any value
        }
    }

    /// Convert to boolean following Java rules
    fn to_bool(value: AttributeValue) -> Option<AttributeValue> {
        match value {
            AttributeValue::Bool(b) => Some(AttributeValue::Bool(b)),
            AttributeValue::Int(i) => Some(AttributeValue::Bool(i == 1)),
            AttributeValue::Long(l) => Some(AttributeValue::Bool(l == 1)),
            AttributeValue::Float(f) => Some(AttributeValue::Bool((f - 1.0).abs() < f32::EPSILON)),
            AttributeValue::Double(d) => Some(AttributeValue::Bool((d - 1.0).abs() < f64::EPSILON)),
            AttributeValue::String(s) => {
                // Java Boolean.parseBoolean() behavior: case-insensitive "true" returns true, everything else is false
                // However, for explicit false, we should handle it too for better UX
                let lower = s.to_lowercase();
                if lower == "true" {
                    Some(AttributeValue::Bool(true))
                } else if lower == "false" {
                    Some(AttributeValue::Bool(false))
                } else {
                    None // Invalid string to boolean conversion
                }
            }
            _ => None,
        }
    }

    /// Convert to int with type widening
    fn to_int(value: AttributeValue) -> Option<AttributeValue> {
        match value {
            AttributeValue::Int(i) => Some(AttributeValue::Int(i)),
            AttributeValue::Bool(b) => Some(AttributeValue::Int(if b { 1 } else { 0 })),
            AttributeValue::String(s) => s.parse::<i32>().ok().map(AttributeValue::Int),
            _ => None, // No narrowing conversions for safety
        }
    }

    /// Convert to long with type widening
    fn to_long(value: AttributeValue) -> Option<AttributeValue> {
        match value {
            AttributeValue::Long(l) => Some(AttributeValue::Long(l)),
            AttributeValue::Int(i) => Some(AttributeValue::Long(i as i64)), // Widening conversion
            AttributeValue::Bool(b) => Some(AttributeValue::Long(if b { 1 } else { 0 })),
            AttributeValue::String(s) => s.parse::<i64>().ok().map(AttributeValue::Long),
            _ => None,
        }
    }

    /// Convert to float with type widening
    fn to_float(value: AttributeValue) -> Option<AttributeValue> {
        match value {
            AttributeValue::Float(f) => Some(AttributeValue::Float(f)),
            AttributeValue::Int(i) => Some(AttributeValue::Float(i as f32)), // Widening conversion
            AttributeValue::Long(l) => Some(AttributeValue::Float(l as f32)), // Widening conversion
            AttributeValue::Bool(b) => Some(AttributeValue::Float(if b { 1.0 } else { 0.0 })),
            AttributeValue::String(s) => s.parse::<f32>().ok().map(AttributeValue::Float),
            _ => None,
        }
    }

    /// Convert to double with type widening
    fn to_double(value: AttributeValue) -> Option<AttributeValue> {
        match value {
            AttributeValue::Double(d) => Some(AttributeValue::Double(d)),
            AttributeValue::Float(f) => Some(AttributeValue::Double(f as f64)), // Widening conversion
            AttributeValue::Long(l) => Some(AttributeValue::Double(l as f64)), // Widening conversion
            AttributeValue::Int(i) => Some(AttributeValue::Double(i as f64)), // Widening conversion
            AttributeValue::Bool(b) => Some(AttributeValue::Double(if b { 1.0 } else { 0.0 })),
            AttributeValue::String(s) => s.parse::<f64>().ok().map(AttributeValue::Double),
            _ => None,
        }
    }

    /// Convert to string (always succeeds)
    fn to_string(value: AttributeValue) -> Option<AttributeValue> {
        let string_value = match value {
            AttributeValue::String(s) => s,
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Long(l) => l.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Double(d) => d.to_string(),
            AttributeValue::Bool(b) => b.to_string(),
            AttributeValue::Bytes(bytes) => format!("<bytes:{}>", bytes.len()),
            AttributeValue::Null => "null".to_string(),
            AttributeValue::Object(_) => "<object>".to_string(), // TODO: Implement special handling for Throwable and arrays
        };
        Some(AttributeValue::String(string_value))
    }

    /// Cast numeric value to target type for arithmetic operations
    pub fn cast_for_arithmetic(
        value: &AttributeValue,
        target_type: AttributeType,
    ) -> Option<AttributeValue> {
        match target_type {
            AttributeType::INT => match value {
                AttributeValue::Int(i) => Some(AttributeValue::Int(*i)),
                AttributeValue::Long(l) => Some(AttributeValue::Int(*l as i32)),
                AttributeValue::Float(f) => Some(AttributeValue::Int(*f as i32)),
                AttributeValue::Double(d) => Some(AttributeValue::Int(*d as i32)),
                _ => None,
            },
            AttributeType::LONG => match value {
                AttributeValue::Int(i) => Some(AttributeValue::Long(*i as i64)),
                AttributeValue::Long(l) => Some(AttributeValue::Long(*l)),
                AttributeValue::Float(f) => Some(AttributeValue::Long(*f as i64)),
                AttributeValue::Double(d) => Some(AttributeValue::Long(*d as i64)),
                _ => None,
            },
            AttributeType::FLOAT => match value {
                AttributeValue::Int(i) => Some(AttributeValue::Float(*i as f32)),
                AttributeValue::Long(l) => Some(AttributeValue::Float(*l as f32)),
                AttributeValue::Float(f) => Some(AttributeValue::Float(*f)),
                AttributeValue::Double(d) => Some(AttributeValue::Float(*d as f32)),
                _ => None,
            },
            AttributeType::DOUBLE => match value {
                AttributeValue::Int(i) => Some(AttributeValue::Double(*i as f64)),
                AttributeValue::Long(l) => Some(AttributeValue::Double(*l as f64)),
                AttributeValue::Float(f) => Some(AttributeValue::Double(*f as f64)),
                AttributeValue::Double(d) => Some(AttributeValue::Double(*d)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Get numeric value as f64 for arithmetic operations
    pub fn get_numeric_value(value: &AttributeValue) -> Option<f64> {
        match value {
            AttributeValue::Int(i) => Some(*i as f64),
            AttributeValue::Long(l) => Some(*l as f64),
            AttributeValue::Float(f) => Some(*f as f64),
            AttributeValue::Double(d) => Some(*d),
            _ => None,
        }
    }

    /// Check if a value is numeric
    pub fn is_numeric(value: &AttributeValue) -> bool {
        matches!(
            value,
            AttributeValue::Int(_)
                | AttributeValue::Long(_)
                | AttributeValue::Float(_)
                | AttributeValue::Double(_)
        )
    }

    /// Validate type conversion at parse time
    pub fn validate_conversion(
        from_type: AttributeType,
        to_type: AttributeType,
    ) -> Result<(), EventFluxError> {
        use AttributeType::*;

        // These conversions are always valid
        match (from_type, to_type) {
            // Same type
            (a, b) if a == b => Ok(()),
            // Any type to object or string
            (_, OBJECT | STRING) => Ok(()),
            // Numeric widening conversions
            (INT, LONG | FLOAT | DOUBLE) => Ok(()),
            (LONG, FLOAT | DOUBLE) => Ok(()),
            (FLOAT, DOUBLE) => Ok(()),
            // Boolean conversions (excluding STRING which is handled above)
            (BOOL, INT | LONG | FLOAT | DOUBLE) => Ok(()),
            (INT | LONG | FLOAT | DOUBLE, BOOL) => Ok(()),
            // String conversions (validated at runtime)
            (STRING, _) => Ok(()),
            // Invalid conversions
            _ => Err(EventFluxError::type_error(
                "Invalid type conversion",
                format!("{from_type:?}"),
                format!("{to_type:?}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_precedence() {
        assert_eq!(
            get_arithmetic_result_type(AttributeType::INT, AttributeType::DOUBLE).unwrap(),
            AttributeType::DOUBLE
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::FLOAT, AttributeType::LONG).unwrap(),
            AttributeType::FLOAT
        );
    }

    #[test]
    fn test_boolean_conversion() {
        // Integer to boolean
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(1), AttributeType::BOOL),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(0), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );

        // String to boolean
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("true".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("false".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(false))
        );
    }

    #[test]
    fn test_numeric_widening() {
        // Int to Long
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::LONG),
            Some(AttributeValue::Long(42))
        );

        // Long to Double
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(42), AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.0))
        );
    }

    #[test]
    fn test_string_conversions() {
        // Number to String
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::STRING),
            Some(AttributeValue::String("42".to_string()))
        );

        // String to Number
        assert_eq!(
            TypeConverter::convert(AttributeValue::String("42".to_string()), AttributeType::INT),
            Some(AttributeValue::Int(42))
        );
    }

    #[test]
    fn test_null_handling() {
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::STRING),
            Some(AttributeValue::String("null".to_string()))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::INT),
            Some(AttributeValue::Null)
        );
    }

    #[test]
    fn test_invalid_conversions() {
        // String that can't be parsed as number
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("not_a_number".to_string()),
                AttributeType::INT
            ),
            None
        );
    }
}
