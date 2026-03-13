// SPDX-License-Identifier: MIT OR Apache-2.0

/// Utility functions for compliance, including data redaction for GDPR
pub mod compliance_util {
    use crate::core::event::value::AttributeValue;

    /// Redact potentially sensitive fields in an AttributeValue
    /// For GDPR compliance, mask PII like emails, names, etc.
    pub fn redact_attribute_value(value: &AttributeValue, field_name: &str) -> AttributeValue {
        match value {
            AttributeValue::String(s) => {
                if is_pii_field(field_name) {
                    AttributeValue::String(mask_string(s))
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }

    /// Check if a field name indicates personally identifiable information
    fn is_pii_field(field_name: &str) -> bool {
        let pii_fields = ["email", "name", "phone", "address", "ssn", "id"];
        pii_fields.iter().any(|&f| field_name.to_lowercase().contains(f))
    }

    /// Mask a string for logging
    fn mask_string(s: &str) -> String {
        if s.len() <= 4 {
            "*".repeat(s.len())
        } else {
            format!("{}****{}", &s[0..2], &s[s.len()-2..])
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_redact_pii() {
            let value = AttributeValue::String("john.doe@example.com".to_string());
            let redacted = redact_attribute_value(&value, "email");
            assert_eq!(redacted, AttributeValue::String("jo****om".to_string()));
        }

        #[test]
        fn test_no_redact_non_pii() {
            let value = AttributeValue::String("some data".to_string());
            let redacted = redact_attribute_value(&value, "amount");
            assert_eq!(redacted, value);
        }
    }
}