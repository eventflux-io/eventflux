// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/math/common.rs
use crate::core::event::value::AttributeValue;

// Helper trait for AttributeValue to handle coercion for math operations
pub(super) trait CoerceNumeric {
    // pub(super) to be visible only within math module
    fn to_i32_or_err_str(&self, op_name: &str) -> Option<i32>;
    fn to_i64_or_err_str(&self, op_name: &str) -> Option<i64>;
    fn to_f32_or_err_str(&self, op_name: &str) -> Option<f32>;
    fn to_f64_or_err_str(&self, op_name: &str) -> Option<f64>;
}

impl CoerceNumeric for AttributeValue {
    fn to_i32_or_err_str(&self, _op_name: &str) -> Option<i32> {
        match self {
            AttributeValue::Int(v) => Some(*v),
            AttributeValue::Long(v) => Some(*v as i32),
            AttributeValue::Float(v) => Some(*v as i32),
            AttributeValue::Double(v) => Some(*v as i32),
            _ => {
                // TODO: Log warning: log_warn!("Type mismatch for {}: expected numeric, found {:?}", op_name, self);
                None
            }
        }
    }
    fn to_i64_or_err_str(&self, _op_name: &str) -> Option<i64> {
        match self {
            AttributeValue::Int(v) => Some(*v as i64),
            AttributeValue::Long(v) => Some(*v),
            AttributeValue::Float(v) => Some(*v as i64),
            AttributeValue::Double(v) => Some(*v as i64),
            _ => None,
        }
    }
    fn to_f32_or_err_str(&self, _op_name: &str) -> Option<f32> {
        match self {
            AttributeValue::Int(v) => Some(*v as f32),
            AttributeValue::Long(v) => Some(*v as f32),
            AttributeValue::Float(v) => Some(*v),
            AttributeValue::Double(v) => Some(*v as f32),
            _ => None,
        }
    }
    fn to_f64_or_err_str(&self, _op_name: &str) -> Option<f64> {
        match self {
            AttributeValue::Int(v) => Some(*v as f64),
            AttributeValue::Long(v) => Some(*v as f64),
            AttributeValue::Float(v) => Some(*v as f64),
            AttributeValue::Double(v) => Some(*v),
            _ => None,
        }
    }
}
