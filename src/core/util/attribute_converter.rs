// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/attribute_converter.rs
// Utility helpers for converting between arbitrary values and AttributeValue
// according to the desired Attribute::Type.  This is a comprehensive port of
// io.eventflux.core.util.AttributeConverter with Java-compatible behavior.

use crate::core::event::value::AttributeValue;
use crate::core::util::type_system::TypeConverter;
use crate::query_api::definition::attribute::Type as AttributeType;

/// Convert a generic AttributeValue to the requested `AttributeType`.
/// The conversion rules exactly mirror the Java implementation.
/// Returns None for invalid conversions (matching Java's null return behavior).
pub fn get_property_value(
    value: AttributeValue,
    attribute_type: AttributeType,
) -> Option<AttributeValue> {
    TypeConverter::convert(value, attribute_type)
}

/// Legacy version that returns Result<AttributeValue, String> for backward compatibility
pub fn get_property_value_result(
    value: AttributeValue,
    attribute_type: AttributeType,
) -> Result<AttributeValue, String> {
    let value_debug = format!("{value:?}");
    TypeConverter::convert(value, attribute_type)
        .ok_or_else(|| format!("Cannot convert {value_debug} to {attribute_type:?}"))
}

/// Parse a string and convert it directly to an `AttributeValue` of the given type.
pub fn get_property_value_from_str(
    text: &str,
    attribute_type: AttributeType,
) -> Option<AttributeValue> {
    let value = AttributeValue::String(text.to_string());
    get_property_value(value, attribute_type)
}

/// Legacy version that returns Result for backward compatibility
pub fn get_property_value_from_str_result(
    text: &str,
    attribute_type: AttributeType,
) -> Result<AttributeValue, String> {
    get_property_value_from_str(text, attribute_type)
        .ok_or_else(|| format!("Cannot convert string '{text}' to {attribute_type:?}"))
}

/// Get property value with type validation
pub fn get_validated_property_value(
    value: AttributeValue,
    attribute_type: AttributeType,
) -> Result<AttributeValue, crate::core::exception::EventFluxError> {
    use crate::core::exception::EventFluxError;
    use crate::core::util::type_system::TypeConverter;

    // Store the type before moving the value
    let value_type = value.get_type();

    // Validate conversion is possible
    crate::core::util::type_system::TypeConverter::validate_conversion(value_type, attribute_type)?;

    // Perform conversion
    TypeConverter::convert(value, attribute_type).ok_or_else(|| {
        EventFluxError::type_error(
            "Type conversion failed at runtime",
            format!("{value_type:?}"),
            format!("{attribute_type:?}"),
        )
    })
}

/// Helper function for getting numeric value for arithmetic operations
pub fn get_numeric_value_for_arithmetic(value: &AttributeValue) -> Option<f64> {
    crate::core::util::type_system::TypeConverter::get_numeric_value(value)
}

/// Helper function to cast value for arithmetic operations
pub fn cast_for_arithmetic(
    value: &AttributeValue,
    target_type: AttributeType,
) -> Option<AttributeValue> {
    crate::core::util::type_system::TypeConverter::cast_for_arithmetic(value, target_type)
}

/// Check if value is numeric type
pub fn is_numeric_type(value: &AttributeValue) -> bool {
    crate::core::util::type_system::TypeConverter::is_numeric(value)
}

/// Get the arithmetic result type for two operand types
pub fn get_arithmetic_result_type(
    left: AttributeType,
    right: AttributeType,
) -> Result<AttributeType, crate::core::exception::EventFluxError> {
    crate::core::util::type_system::get_arithmetic_result_type(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_string_to_int() {
        let v = get_property_value_from_str("42", AttributeType::INT).unwrap();
        assert!(matches!(v, AttributeValue::Int(42)));
    }

    #[test]
    fn test_convert_int_to_long() {
        let v = get_property_value(AttributeValue::Int(7), AttributeType::LONG).unwrap();
        assert!(matches!(v, AttributeValue::Long(7)));
    }

    #[test]
    fn test_boolean_conversion() {
        // Test Java-style boolean conversion
        let v = get_property_value(AttributeValue::Int(1), AttributeType::BOOL).unwrap();
        assert!(matches!(v, AttributeValue::Bool(true)));

        let v = get_property_value(AttributeValue::Int(0), AttributeType::BOOL).unwrap();
        assert!(matches!(v, AttributeValue::Bool(false)));

        let v = get_property_value(
            AttributeValue::String("true".to_string()),
            AttributeType::BOOL,
        )
        .unwrap();
        assert!(matches!(v, AttributeValue::Bool(true)));
    }

    #[test]
    fn test_null_conversion() {
        let v = get_property_value(AttributeValue::Null, AttributeType::STRING).unwrap();
        assert!(matches!(v, AttributeValue::String(s) if s == "null"));

        let v = get_property_value(AttributeValue::Null, AttributeType::INT).unwrap();
        assert!(matches!(v, AttributeValue::Null));
    }

    #[test]
    fn test_invalid_conversion() {
        // Test that invalid conversions return None (Java behavior)
        let v = get_property_value(
            AttributeValue::String("not_a_number".to_string()),
            AttributeType::INT,
        );
        assert!(v.is_none());
    }

    #[test]
    fn test_type_widening() {
        // Test automatic type widening
        let v = get_property_value(AttributeValue::Int(42), AttributeType::DOUBLE).unwrap();
        assert!(matches!(v, AttributeValue::Double(d) if (d - 42.0).abs() < f64::EPSILON));
    }
}
