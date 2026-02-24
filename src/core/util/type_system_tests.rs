// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/type_system_tests.rs
// Comprehensive tests for the type system implementation

#[cfg(test)]
mod type_system_tests {
    use crate::core::event::value::AttributeValue;
    use crate::core::util::type_system::{
        get_arithmetic_result_type, TypeConverter, TypePrecedence,
    };
    use crate::query_api::definition::attribute::Type as AttributeType;

    #[test]
    fn test_type_precedence_ordering() {
        use TypePrecedence::*;
        assert!(Double > Float);
        assert!(Float > Long);
        assert!(Long > Int);
        assert!(Double > Int);
    }

    #[test]
    fn test_arithmetic_result_types() {
        // Test all combinations of numeric types
        assert_eq!(
            get_arithmetic_result_type(AttributeType::INT, AttributeType::INT).unwrap(),
            AttributeType::INT
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::INT, AttributeType::LONG).unwrap(),
            AttributeType::LONG
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::INT, AttributeType::FLOAT).unwrap(),
            AttributeType::FLOAT
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::INT, AttributeType::DOUBLE).unwrap(),
            AttributeType::DOUBLE
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::LONG, AttributeType::FLOAT).unwrap(),
            AttributeType::FLOAT
        );
        assert_eq!(
            get_arithmetic_result_type(AttributeType::FLOAT, AttributeType::DOUBLE).unwrap(),
            AttributeType::DOUBLE
        );

        // Test symmetric property
        assert_eq!(
            get_arithmetic_result_type(AttributeType::FLOAT, AttributeType::LONG).unwrap(),
            get_arithmetic_result_type(AttributeType::LONG, AttributeType::FLOAT).unwrap()
        );

        // Test error cases
        assert!(get_arithmetic_result_type(AttributeType::STRING, AttributeType::INT).is_err());
        assert!(get_arithmetic_result_type(AttributeType::BOOL, AttributeType::INT).is_err());
    }

    #[test]
    fn test_boolean_conversions_comprehensive() {
        // Int to Boolean (Java rules: 1 = true, everything else = false)
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(1), AttributeType::BOOL),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(0), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(-1), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );

        // Long to Boolean
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(1), AttributeType::BOOL),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(0), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );

        // Float to Boolean
        assert_eq!(
            TypeConverter::convert(AttributeValue::Float(1.0), AttributeType::BOOL),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Float(0.0), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );

        // Double to Boolean
        assert_eq!(
            TypeConverter::convert(AttributeValue::Double(1.0), AttributeType::BOOL),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Double(0.0), AttributeType::BOOL),
            Some(AttributeValue::Bool(false))
        );

        // String to Boolean (case insensitive)
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("true".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(true))
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("TRUE".to_string()),
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
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("FALSE".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(false))
        );

        // Invalid string to boolean
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("maybe".to_string()),
                AttributeType::BOOL
            ),
            None
        );

        // Boolean to Numeric (Java rules: true = 1, false = 0)
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(true), AttributeType::INT),
            Some(AttributeValue::Int(1))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(false), AttributeType::INT),
            Some(AttributeValue::Int(0))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(true), AttributeType::LONG),
            Some(AttributeValue::Long(1))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(true), AttributeType::FLOAT),
            Some(AttributeValue::Float(1.0))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(true), AttributeType::DOUBLE),
            Some(AttributeValue::Double(1.0))
        );
    }

    #[test]
    fn test_numeric_widening_conversions() {
        // Int to wider types
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::LONG),
            Some(AttributeValue::Long(42))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::FLOAT),
            Some(AttributeValue::Float(42.0))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.0))
        );

        // Long to wider types
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(42), AttributeType::FLOAT),
            Some(AttributeValue::Float(42.0))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(42), AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.0))
        );

        // Float to wider types
        assert_eq!(
            TypeConverter::convert(AttributeValue::Float(42.5), AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.5))
        );

        // No narrowing conversions (should return None)
        assert_eq!(
            TypeConverter::convert(AttributeValue::Long(42), AttributeType::INT),
            None
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Float(42.5), AttributeType::INT),
            None
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Double(42.5), AttributeType::FLOAT),
            None
        );
    }

    #[test]
    fn test_string_conversions() {
        // String to numeric (valid)
        assert_eq!(
            TypeConverter::convert(AttributeValue::String("42".to_string()), AttributeType::INT),
            Some(AttributeValue::Int(42))
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("42".to_string()),
                AttributeType::LONG
            ),
            Some(AttributeValue::Long(42))
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("42.5".to_string()),
                AttributeType::FLOAT
            ),
            Some(AttributeValue::Float(42.5))
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("42.5".to_string()),
                AttributeType::DOUBLE
            ),
            Some(AttributeValue::Double(42.5))
        );

        // String to numeric (invalid - return None like Java)
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("not_a_number".to_string()),
                AttributeType::INT
            ),
            None
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("abc".to_string()),
                AttributeType::FLOAT
            ),
            None
        );

        // Numeric to string (always succeeds)
        assert_eq!(
            TypeConverter::convert(AttributeValue::Int(42), AttributeType::STRING),
            Some(AttributeValue::String("42".to_string()))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Float(42.5), AttributeType::STRING),
            Some(AttributeValue::String("42.5".to_string()))
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Bool(true), AttributeType::STRING),
            Some(AttributeValue::String("true".to_string()))
        );
    }

    #[test]
    fn test_null_handling() {
        // Null to any non-string type returns Null
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::INT),
            Some(AttributeValue::Null)
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::BOOL),
            Some(AttributeValue::Null)
        );
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::OBJECT),
            Some(AttributeValue::Null)
        );

        // Null to string returns "null" string
        assert_eq!(
            TypeConverter::convert(AttributeValue::Null, AttributeType::STRING),
            Some(AttributeValue::String("null".to_string()))
        );
    }

    #[test]
    fn test_object_type_handling() {
        let obj = AttributeValue::Object(None);

        // Object to Object (identity) - Note: Objects don't have proper equality, so we just check the type
        let converted = TypeConverter::convert(obj.clone(), AttributeType::OBJECT).unwrap();
        assert!(matches!(converted, AttributeValue::Object(_)));

        // Object to String
        assert_eq!(
            TypeConverter::convert(obj.clone(), AttributeType::STRING),
            Some(AttributeValue::String("<object>".to_string()))
        );

        // Object to other types (should return None)
        assert_eq!(
            TypeConverter::convert(obj.clone(), AttributeType::INT),
            None
        );
    }

    #[test]
    fn test_arithmetic_casting() {
        let int_val = AttributeValue::Int(42);
        let long_val = AttributeValue::Long(42);
        let float_val = AttributeValue::Float(42.5);
        let double_val = AttributeValue::Double(42.5);

        // Test casting for arithmetic operations
        assert_eq!(
            TypeConverter::cast_for_arithmetic(&int_val, AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.0))
        );
        assert_eq!(
            TypeConverter::cast_for_arithmetic(&long_val, AttributeType::FLOAT),
            Some(AttributeValue::Float(42.0))
        );
        assert_eq!(
            TypeConverter::cast_for_arithmetic(&float_val, AttributeType::DOUBLE),
            Some(AttributeValue::Double(42.5))
        );

        // Test numeric value extraction
        assert_eq!(TypeConverter::get_numeric_value(&int_val), Some(42.0));
        assert_eq!(TypeConverter::get_numeric_value(&long_val), Some(42.0));
        assert_eq!(TypeConverter::get_numeric_value(&float_val), Some(42.5));
        assert_eq!(TypeConverter::get_numeric_value(&double_val), Some(42.5));

        // Non-numeric types - strings should NOT be converted by get_numeric_value
        // (that's for to_number() method on AttributeValue)
        assert_eq!(
            TypeConverter::get_numeric_value(&AttributeValue::String("42".to_string())),
            None // get_numeric_value only extracts from already-numeric types
        );
        assert_eq!(
            TypeConverter::get_numeric_value(&AttributeValue::Bool(true)),
            None // booleans are not considered numeric for arithmetic
        );
        assert_eq!(
            TypeConverter::get_numeric_value(&AttributeValue::Null),
            None
        );
    }

    #[test]
    fn test_is_numeric() {
        assert!(TypeConverter::is_numeric(&AttributeValue::Int(42)));
        assert!(TypeConverter::is_numeric(&AttributeValue::Long(42)));
        assert!(TypeConverter::is_numeric(&AttributeValue::Float(42.5)));
        assert!(TypeConverter::is_numeric(&AttributeValue::Double(42.5)));

        assert!(!TypeConverter::is_numeric(&AttributeValue::String(
            "42".to_string()
        )));
        assert!(!TypeConverter::is_numeric(&AttributeValue::Bool(true)));
        assert!(!TypeConverter::is_numeric(&AttributeValue::Null));
        assert!(!TypeConverter::is_numeric(&AttributeValue::Object(None)));
    }

    #[test]
    fn test_attribute_value_methods() {
        let int_val = AttributeValue::Int(42);
        let string_val = AttributeValue::String("test".to_string());
        let null_val = AttributeValue::Null;

        // Test get_type()
        assert_eq!(int_val.get_type(), AttributeType::INT);
        assert_eq!(string_val.get_type(), AttributeType::STRING);
        assert_eq!(null_val.get_type(), AttributeType::OBJECT);

        // Test is_null()
        assert!(!int_val.is_null());
        assert!(!string_val.is_null());
        assert!(null_val.is_null());

        // Test accessor methods
        assert_eq!(int_val.as_i32(), Some(42));
        assert_eq!(string_val.as_string(), Some(&"test".to_string()));
        assert_eq!(null_val.as_i32(), None);

        // Test to_number()
        assert_eq!(int_val.to_number(), Some(42.0));
        assert_eq!(
            AttributeValue::String("42.5".to_string()).to_number(),
            Some(42.5)
        );
        assert_eq!(
            AttributeValue::String("not_a_number".to_string()).to_number(),
            None
        );

        // Test to_boolean()
        assert_eq!(AttributeValue::Int(1).to_boolean(), Some(true));
        assert_eq!(AttributeValue::Int(0).to_boolean(), Some(false));
        assert_eq!(
            AttributeValue::String("true".to_string()).to_boolean(),
            Some(true)
        );
        assert_eq!(
            AttributeValue::String("maybe".to_string()).to_boolean(),
            None
        );

        // Test to_string_value()
        assert_eq!(int_val.to_string_value(), "42");
        assert_eq!(null_val.to_string_value(), "null");
    }

    #[test]
    fn test_is_convertible_to() {
        let int_val = AttributeValue::Int(42);
        let string_val = AttributeValue::String("42".to_string());
        let invalid_string = AttributeValue::String("not_a_number".to_string());

        // Valid conversions
        assert!(int_val.is_convertible_to(AttributeType::LONG));
        assert!(int_val.is_convertible_to(AttributeType::DOUBLE));
        assert!(int_val.is_convertible_to(AttributeType::STRING));
        assert!(string_val.is_convertible_to(AttributeType::INT));

        // Invalid conversions
        assert!(!invalid_string.is_convertible_to(AttributeType::INT));

        // Null is convertible to any type
        assert!(AttributeValue::Null.is_convertible_to(AttributeType::INT));
        assert!(AttributeValue::Null.is_convertible_to(AttributeType::STRING));

        // Object type accepts any value
        assert!(int_val.is_convertible_to(AttributeType::OBJECT));
        assert!(string_val.is_convertible_to(AttributeType::OBJECT));
    }

    #[test]
    fn test_java_compatibility_edge_cases() {
        // Test specific Java behavior edge cases

        // Boolean parsing in Java is case-sensitive and only "true" (case-insensitive) returns true
        // Actually, Java's Boolean.parseBoolean() is case-insensitive for "true" but anything else is false
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("True".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(true)) // Our implementation should be case-insensitive for "true"
        );
        assert_eq!(
            TypeConverter::convert(
                AttributeValue::String("False".to_string()),
                AttributeType::BOOL
            ),
            Some(AttributeValue::Bool(false))
        );

        // Numeric precision edge cases
        let max_int = AttributeValue::Int(i32::MAX);
        assert_eq!(
            TypeConverter::convert(max_int, AttributeType::LONG),
            Some(AttributeValue::Long(i32::MAX as i64))
        );

        // Float precision when converting to double
        let float_val = AttributeValue::Float(0.1);
        if let Some(AttributeValue::Double(d)) =
            TypeConverter::convert(float_val, AttributeType::DOUBLE)
        {
            // Due to floating point precision, 0.1f as f64 is not exactly 0.1
            assert!((d - 0.1).abs() < 0.01); // Allow some precision loss
        } else {
            panic!("Float to Double conversion should succeed");
        }
    }
}
