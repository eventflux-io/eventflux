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

// Helpers that convert values to an AttributeValue of a given Attribute::Type. A port of
// io.eventflux.core.util.AttributeConverter that keeps the Java behaviour. The actual rules
// live in TypeConverter in type_system.rs.

use crate::core::event::value::AttributeValue;
use crate::core::util::type_system::TypeConverter;
use crate::query_api::definition::attribute::Type as AttributeType;

/// Converts an AttributeValue to the requested `AttributeType` with
/// `TypeConverter::convert`, which follows the Java rules.
/// Returns None when the conversion is not possible, where Java returns null.
pub fn get_property_value(
    value: AttributeValue,
    attribute_type: AttributeType,
) -> Option<AttributeValue> {
    TypeConverter::convert(value, attribute_type)
}

/// Same as [`get_property_value`], but returns an error message instead of None.
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

/// Same as [`get_property_value_from_str`], but returns an error message instead of None.
pub fn get_property_value_from_str_result(
    text: &str,
    attribute_type: AttributeType,
) -> Result<AttributeValue, String> {
    get_property_value_from_str(text, attribute_type)
        .ok_or_else(|| format!("Cannot convert string '{text}' to {attribute_type:?}"))
}

/// Converts `value` to `attribute_type` after checking that the type pair is allowed.
///
/// # Errors
///
/// Returns the error from `TypeConverter::validate_conversion` when the pair is not
/// allowed, or a type error when the conversion itself fails, such as a string that does
/// not parse.
pub fn get_validated_property_value(
    value: AttributeValue,
    attribute_type: AttributeType,
) -> Result<AttributeValue, crate::core::exception::EventFluxError> {
    use crate::core::exception::EventFluxError;
    use crate::core::util::type_system::TypeConverter;

    // Read the type before the value is moved
    let value_type = value.get_type();

    crate::core::util::type_system::TypeConverter::validate_conversion(value_type, attribute_type)?;

    TypeConverter::convert(value, attribute_type).ok_or_else(|| {
        EventFluxError::type_error(
            "Type conversion failed at runtime",
            format!("{value_type:?}"),
            format!("{attribute_type:?}"),
        )
    })
}

/// Returns the value as f64 for arithmetic. See `TypeConverter::get_numeric_value`.
pub fn get_numeric_value_for_arithmetic(value: &AttributeValue) -> Option<f64> {
    crate::core::util::type_system::TypeConverter::get_numeric_value(value)
}

/// Casts a value to `target_type` for arithmetic. See `TypeConverter::cast_for_arithmetic`.
pub fn cast_for_arithmetic(
    value: &AttributeValue,
    target_type: AttributeType,
) -> Option<AttributeValue> {
    crate::core::util::type_system::TypeConverter::cast_for_arithmetic(value, target_type)
}

/// True if the value is a numeric type. See `TypeConverter::is_numeric`.
pub fn is_numeric_type(value: &AttributeValue) -> bool {
    crate::core::util::type_system::TypeConverter::is_numeric(value)
}

/// Returns the result type of an arithmetic operation on the two operand types, or an
/// error if they cannot be combined.
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
        // Java style boolean conversion
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
        // Invalid conversions return None, as Java returns null
        let v = get_property_value(
            AttributeValue::String("not_a_number".to_string()),
            AttributeType::INT,
        );
        assert!(v.is_none());
    }

    #[test]
    fn test_type_widening() {
        let v = get_property_value(AttributeValue::Int(42), AttributeType::DOUBLE).unwrap();
        assert!(matches!(v, AttributeValue::Double(d) if (d - 42.0).abs() < f64::EPSILON));
    }
}
