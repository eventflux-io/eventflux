// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/event/value.rs
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::Any;
use std::fmt;

// This enum will represent the possible types of attribute values in a EventFlux event.
// Java Event uses Object[], allowing any type. Rust needs to be more explicit.
// query_api::definition::attribute::Type enum has STRING, INT, LONG, FLOAT, DOUBLE, BOOL, OBJECT.
// This enum should reflect those types for data carrying.

#[derive(Default)]
pub enum AttributeValue {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    /// Raw binary data for passthrough scenarios (protobuf, msgpack, etc.)
    /// Unlike Object, this variant can be cloned and serialized.
    Bytes(Vec<u8>),
    Object(Option<Box<dyn Any + Send + Sync>>), // For OBJECT type, ensure thread safety
    #[default]
    Null,                         // To represent null values explicitly
}

// Manual implementation of Debug to handle Box<dyn Any>
impl fmt::Debug for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::String(s) => write!(f, "String({s:?})"),
            AttributeValue::Int(i) => write!(f, "Int({i:?})"),
            AttributeValue::Long(l) => write!(f, "Long({l:?})"),
            AttributeValue::Float(fl) => write!(f, "Float({fl:?})"),
            AttributeValue::Double(d) => write!(f, "Double({d:?})"),
            AttributeValue::Bool(b) => write!(f, "Bool({b:?})"),
            AttributeValue::Bytes(bytes) => write!(f, "Bytes({} bytes)", bytes.len()),
            AttributeValue::Object(_) => write!(f, "Object(<opaque>)"), // Cannot inspect Box<dyn Any> easily
            AttributeValue::Null => write!(f, "Null"),
        }
    }
}

// Manual implementation of PartialEq
impl PartialEq for AttributeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (AttributeValue::String(a), AttributeValue::String(b)) => a == b,
            (AttributeValue::Int(a), AttributeValue::Int(b)) => a == b,
            (AttributeValue::Long(a), AttributeValue::Long(b)) => a == b,
            (AttributeValue::Float(a), AttributeValue::Float(b)) => a == b, // Note: float comparison issues
            (AttributeValue::Double(a), AttributeValue::Double(b)) => a == b, // Note: float comparison issues
            (AttributeValue::Bool(a), AttributeValue::Bool(b)) => a == b,
            (AttributeValue::Bytes(a), AttributeValue::Bytes(b)) => a == b,
            (AttributeValue::Null, AttributeValue::Null) => true,
            // Comparing Box<dyn Any> is problematic.
            // Typically, objects are compared by reference or specific methods, not direct equality.
            // For now, objects are not equal unless they are the same instance (which this comparison doesn't check)
            // or if we had a way to downcast and compare known types.
            (AttributeValue::Object(_), AttributeValue::Object(_)) => false,
            _ => false, // Different enum variants
        }
    }
}

impl Clone for AttributeValue {
    fn clone(&self) -> Self {
        match self {
            AttributeValue::String(s) => AttributeValue::String(s.clone()),
            AttributeValue::Int(i) => AttributeValue::Int(*i),
            AttributeValue::Long(l) => AttributeValue::Long(*l),
            AttributeValue::Float(f) => AttributeValue::Float(*f),
            AttributeValue::Double(d) => AttributeValue::Double(*d),
            AttributeValue::Bool(b) => AttributeValue::Bool(*b),
            AttributeValue::Bytes(bytes) => AttributeValue::Bytes(bytes.clone()),
            AttributeValue::Object(_) => AttributeValue::Object(None),
            AttributeValue::Null => AttributeValue::Null,
        }
    }
}

// --- serde support ---
#[derive(Serialize, Deserialize)]
pub(crate) enum AttrSer {
    String(String),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

impl Serialize for AttributeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let repr = match self {
            AttributeValue::String(s) => AttrSer::String(s.clone()),
            AttributeValue::Int(i) => AttrSer::Int(*i),
            AttributeValue::Long(l) => AttrSer::Long(*l),
            AttributeValue::Float(f) => AttrSer::Float(*f),
            AttributeValue::Double(d) => AttrSer::Double(*d),
            AttributeValue::Bool(b) => AttrSer::Bool(*b),
            AttributeValue::Bytes(bytes) => AttrSer::Bytes(bytes.clone()),
            _ => AttrSer::Null,
        };
        repr.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = AttrSer::deserialize(deserializer)?;
        Ok(match repr {
            AttrSer::String(s) => AttributeValue::String(s),
            AttrSer::Int(i) => AttributeValue::Int(i),
            AttrSer::Long(l) => AttributeValue::Long(l),
            AttrSer::Float(f) => AttributeValue::Float(f),
            AttrSer::Double(d) => AttributeValue::Double(d),
            AttrSer::Bool(b) => AttributeValue::Bool(b),
            AttrSer::Bytes(bytes) => AttributeValue::Bytes(bytes),
            AttrSer::Null => AttributeValue::Null,
        })
    }
}

impl AttributeValue {
    /// Get string representation for debugging and display
    pub fn as_string(&self) -> Option<&String> {
        match self {
            AttributeValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            AttributeValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            AttributeValue::Long(l) => Some(*l),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            AttributeValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            AttributeValue::Double(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AttributeValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, AttributeValue::Null)
    }

    /// Get the type of this value
    pub fn get_type(&self) -> crate::query_api::definition::attribute::Type {
        use crate::query_api::definition::attribute::Type;
        match self {
            AttributeValue::String(_) => Type::STRING,
            AttributeValue::Int(_) => Type::INT,
            AttributeValue::Long(_) => Type::LONG,
            AttributeValue::Float(_) => Type::FLOAT,
            AttributeValue::Double(_) => Type::DOUBLE,
            AttributeValue::Bool(_) => Type::BOOL,
            AttributeValue::Bytes(_) => Type::OBJECT, // Bytes stored as OBJECT type
            AttributeValue::Object(_) => Type::OBJECT,
            AttributeValue::Null => Type::OBJECT, // Null can be any type
        }
    }

    /// Convert to numeric value as f64 (for arithmetic operations)
    pub fn to_number(&self) -> Option<f64> {
        match self {
            AttributeValue::Int(i) => Some(*i as f64),
            AttributeValue::Long(l) => Some(*l as f64),
            AttributeValue::Float(f) => Some(*f as f64),
            AttributeValue::Double(d) => Some(*d),
            AttributeValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            AttributeValue::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Convert to boolean following Java EventFlux rules
    pub fn to_boolean(&self) -> Option<bool> {
        match self {
            AttributeValue::Bool(b) => Some(*b),
            AttributeValue::Int(i) => Some(*i == 1),
            AttributeValue::Long(l) => Some(*l == 1),
            AttributeValue::Float(f) => Some((*f - 1.0).abs() < f32::EPSILON),
            AttributeValue::Double(d) => Some((*d - 1.0).abs() < f64::EPSILON),
            AttributeValue::String(s) => {
                let lower = s.to_lowercase();
                if lower == "true" {
                    Some(true)
                } else if lower == "false" {
                    Some(false)
                } else {
                    None // Invalid string
                }
            }
            _ => None,
        }
    }

    /// Convert value to string representation (Java toString() equivalent)
    pub fn to_string_value(&self) -> String {
        match self {
            AttributeValue::String(s) => s.clone(),
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Long(l) => l.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Double(d) => d.to_string(),
            AttributeValue::Bool(b) => b.to_string(),
            AttributeValue::Bytes(bytes) => format!("<bytes:{}>", bytes.len()),
            AttributeValue::Object(_) => "<object>".to_string(),
            AttributeValue::Null => "null".to_string(),
        }
    }

    /// Check if this value can be converted to target type
    pub fn is_convertible_to(
        &self,
        target_type: crate::query_api::definition::attribute::Type,
    ) -> bool {
        use crate::query_api::definition::attribute::Type;

        match (self, target_type) {
            (AttributeValue::Null, _) => true, // Null can convert to any type
            (_, Type::OBJECT) => true,         // Any value can become object
            (_, Type::STRING) => true,         // Any value can become string
            (AttributeValue::String(s), Type::INT) => s.parse::<i32>().is_ok(),
            (AttributeValue::String(s), Type::LONG) => s.parse::<i64>().is_ok(),
            (AttributeValue::String(s), Type::FLOAT) => s.parse::<f32>().is_ok(),
            (AttributeValue::String(s), Type::DOUBLE) => s.parse::<f64>().is_ok(),
            (AttributeValue::String(s), Type::BOOL) => {
                let lower = s.to_lowercase();
                lower == "true" || lower == "false"
            }
            // Numeric type widening
            (AttributeValue::Int(_), Type::LONG | Type::FLOAT | Type::DOUBLE) => true,
            (AttributeValue::Long(_), Type::FLOAT | Type::DOUBLE) => true,
            (AttributeValue::Float(_), Type::DOUBLE) => true,
            // Boolean to numeric
            (AttributeValue::Bool(_), Type::INT | Type::LONG | Type::FLOAT | Type::DOUBLE) => true,
            // Numeric to boolean (always valid)
            (
                AttributeValue::Int(_)
                | AttributeValue::Long(_)
                | AttributeValue::Float(_)
                | AttributeValue::Double(_),
                Type::BOOL,
            ) => true,
            // Same type
            (v, target) => v.get_type() == target,
        }
    }
}

impl fmt::Display for AttributeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttributeValue::String(s) => write!(f, "{s}"),
            AttributeValue::Int(i) => write!(f, "{i}"),
            AttributeValue::Long(l) => write!(f, "{l}"),
            AttributeValue::Float(v) => write!(f, "{v}"),
            AttributeValue::Double(v) => write!(f, "{v}"),
            AttributeValue::Bool(b) => write!(f, "{b}"),
            AttributeValue::Bytes(bytes) => write!(f, "<bytes:{}>", bytes.len()),
            AttributeValue::Object(_) => write!(f, "<object>"),
            AttributeValue::Null => write!(f, "null"),
        }
    }
}
