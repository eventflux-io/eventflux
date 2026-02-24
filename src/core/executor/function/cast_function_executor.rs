// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/cast_function_executor.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug)]
pub struct CastFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    return_type: ApiAttributeType,
}

impl CastFunctionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        type_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if type_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("Cast function type argument must be STRING".to_string());
        }
        let type_val = match type_executor.execute(None) {
            Some(AttributeValue::String(s)) => s.to_lowercase(),
            _ => {
                return Err("Cast function requires a constant string as type argument".to_string())
            }
        };
        let return_type = match type_val.as_str() {
            "string" => ApiAttributeType::STRING,
            "int" => ApiAttributeType::INT,
            "long" => ApiAttributeType::LONG,
            "float" => ApiAttributeType::FLOAT,
            "double" => ApiAttributeType::DOUBLE,
            "bool" | "boolean" => ApiAttributeType::BOOL,
            "object" => ApiAttributeType::OBJECT,
            _ => return Err(format!("Unsupported cast target type: {type_val}")),
        };
        Ok(Self {
            value_executor,
            return_type,
        })
    }

    fn cast_value(&self, value: AttributeValue) -> Option<AttributeValue> {
        if matches!(value, AttributeValue::Null) {
            return Some(AttributeValue::Null);
        }
        match self.return_type {
            ApiAttributeType::STRING => Some(AttributeValue::String(match value {
                AttributeValue::String(s) => s,
                AttributeValue::Int(v) => v.to_string(),
                AttributeValue::Long(v) => v.to_string(),
                AttributeValue::Float(v) => v.to_string(),
                AttributeValue::Double(v) => v.to_string(),
                AttributeValue::Bool(v) => v.to_string(),
                AttributeValue::Bytes(bytes) => format!("<bytes:{}>", bytes.len()),
                AttributeValue::Object(_) => "<object>".to_string(),
                AttributeValue::Null => String::new(),
            })),
            ApiAttributeType::INT => match value {
                AttributeValue::Int(v) => Some(AttributeValue::Int(v)),
                AttributeValue::Long(v) => Some(AttributeValue::Int(v as i32)),
                AttributeValue::Float(v) => Some(AttributeValue::Int(v as i32)),
                AttributeValue::Double(v) => Some(AttributeValue::Int(v as i32)),
                AttributeValue::Bool(v) => Some(AttributeValue::Int(if v { 1 } else { 0 })),
                AttributeValue::String(s) => s.parse::<i32>().ok().map(AttributeValue::Int),
                _ => None,
            },
            ApiAttributeType::LONG => match value {
                AttributeValue::Int(v) => Some(AttributeValue::Long(v as i64)),
                AttributeValue::Long(v) => Some(AttributeValue::Long(v)),
                AttributeValue::Float(v) => Some(AttributeValue::Long(v as i64)),
                AttributeValue::Double(v) => Some(AttributeValue::Long(v as i64)),
                AttributeValue::Bool(v) => Some(AttributeValue::Long(if v { 1 } else { 0 })),
                AttributeValue::String(s) => s.parse::<i64>().ok().map(AttributeValue::Long),
                _ => None,
            },
            ApiAttributeType::FLOAT => match value {
                AttributeValue::Int(v) => Some(AttributeValue::Float(v as f32)),
                AttributeValue::Long(v) => Some(AttributeValue::Float(v as f32)),
                AttributeValue::Float(v) => Some(AttributeValue::Float(v)),
                AttributeValue::Double(v) => Some(AttributeValue::Float(v as f32)),
                AttributeValue::Bool(v) => Some(AttributeValue::Float(if v { 1.0 } else { 0.0 })),
                AttributeValue::String(s) => s.parse::<f32>().ok().map(AttributeValue::Float),
                _ => None,
            },
            ApiAttributeType::DOUBLE => match value {
                AttributeValue::Int(v) => Some(AttributeValue::Double(v as f64)),
                AttributeValue::Long(v) => Some(AttributeValue::Double(v as f64)),
                AttributeValue::Float(v) => Some(AttributeValue::Double(v as f64)),
                AttributeValue::Double(v) => Some(AttributeValue::Double(v)),
                AttributeValue::Bool(v) => Some(AttributeValue::Double(if v { 1.0 } else { 0.0 })),
                AttributeValue::String(s) => s.parse::<f64>().ok().map(AttributeValue::Double),
                _ => None,
            },
            ApiAttributeType::BOOL => match value {
                AttributeValue::Bool(b) => Some(AttributeValue::Bool(b)),
                AttributeValue::String(s) => s.parse::<bool>().ok().map(AttributeValue::Bool),
                AttributeValue::Int(i) => Some(AttributeValue::Bool(i != 0)),
                AttributeValue::Long(l) => Some(AttributeValue::Bool(l != 0)),
                AttributeValue::Float(f) => Some(AttributeValue::Bool(f != 0.0)),
                AttributeValue::Double(d) => Some(AttributeValue::Bool(d != 0.0)),
                _ => None,
            },
            ApiAttributeType::OBJECT => Some(value),
        }
    }
}

impl ExpressionExecutor for CastFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value = self.value_executor.execute(event)?;
        self.cast_value(value)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(CastFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            return_type: self.return_type,
        })
    }
}
