// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/date_functions.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use chrono::{NaiveDateTime, Utc};
use std::sync::Arc;

/// NOW() function - returns current timestamp in milliseconds
#[derive(Debug, Default, Clone)]
pub struct NowFunctionExecutor;

impl ExpressionExecutor for NowFunctionExecutor {
    fn execute(&self, _event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let millis = Utc::now().timestamp_millis();
        Some(AttributeValue::Long(millis))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(&self, _ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(self.clone())
    }
}

#[derive(Debug)]
pub struct FormatDateFunctionExecutor {
    timestamp_executor: Box<dyn ExpressionExecutor>,
    pattern: String,
}

#[derive(Debug)]
pub struct ParseDateFunctionExecutor {
    date_executor: Box<dyn ExpressionExecutor>,
    pattern: String,
}

impl ParseDateFunctionExecutor {
    pub fn new(
        date_executor: Box<dyn ExpressionExecutor>,
        pattern_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if pattern_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("parseDate pattern must be STRING".to_string());
        }
        let pattern = match pattern_executor.execute(None) {
            Some(AttributeValue::String(s)) => s,
            _ => return Err("parseDate pattern must be constant string".to_string()),
        };
        Ok(Self {
            date_executor,
            pattern,
        })
    }
}

impl ExpressionExecutor for ParseDateFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let date_val = self.date_executor.execute(event)?;
        let s = match date_val {
            AttributeValue::String(v) => v,
            AttributeValue::Null => return Some(AttributeValue::Null),
            _ => return None,
        };
        let ndt = if let Ok(d) = chrono::NaiveDate::parse_from_str(&s, &self.pattern) {
            d.and_hms_opt(0, 0, 0)?
        } else {
            NaiveDateTime::parse_from_str(&s, &self.pattern).ok()?
        };
        let ts = ndt.and_utc().timestamp_millis();
        Some(AttributeValue::Long(ts))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ParseDateFunctionExecutor {
            date_executor: self.date_executor.clone_executor(ctx),
            pattern: self.pattern.clone(),
        })
    }
}

#[derive(Debug)]
pub struct DateAddFunctionExecutor {
    timestamp_executor: Box<dyn ExpressionExecutor>,
    increment_executor: Box<dyn ExpressionExecutor>,
    unit: String,
}

impl DateAddFunctionExecutor {
    pub fn new(
        timestamp_executor: Box<dyn ExpressionExecutor>,
        increment_executor: Box<dyn ExpressionExecutor>,
        unit_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if unit_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("dateAdd unit must be STRING".to_string());
        }
        let unit = match unit_executor.execute(None) {
            Some(AttributeValue::String(s)) => s.to_lowercase(),
            _ => return Err("dateAdd unit must be constant string".to_string()),
        };
        Ok(Self {
            timestamp_executor,
            increment_executor,
            unit,
        })
    }
}

impl ExpressionExecutor for DateAddFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let ts_val = self.timestamp_executor.execute(event)?;
        let base_ts = match ts_val {
            AttributeValue::Long(v) => v,
            AttributeValue::Int(v) => v as i64,
            AttributeValue::Null => return Some(AttributeValue::Null),
            _ => return None,
        };
        let inc_val = self.increment_executor.execute(event)?;
        let inc = match inc_val {
            AttributeValue::Int(v) => v as i64,
            AttributeValue::Long(v) => v,
            AttributeValue::Float(v) => v as i64,
            AttributeValue::Double(v) => v as i64,
            AttributeValue::Null => return Some(AttributeValue::Null),
            _ => return None,
        };
        let dt = chrono::DateTime::<Utc>::from_timestamp_millis(base_ts)?;
        let result = match self.unit.as_str() {
            "seconds" => dt + chrono::Duration::seconds(inc),
            "minutes" => dt + chrono::Duration::minutes(inc),
            "hours" => dt + chrono::Duration::hours(inc),
            "days" => dt + chrono::Duration::days(inc),
            _ => return None,
        };
        Some(AttributeValue::Long(result.timestamp_millis()))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::LONG
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(DateAddFunctionExecutor {
            timestamp_executor: self.timestamp_executor.clone_executor(ctx),
            increment_executor: self.increment_executor.clone_executor(ctx),
            unit: self.unit.clone(),
        })
    }
}

impl FormatDateFunctionExecutor {
    pub fn new(
        timestamp_executor: Box<dyn ExpressionExecutor>,
        pattern_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        if pattern_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("formatDate pattern must be STRING".to_string());
        }
        let pattern = match pattern_executor.execute(None) {
            Some(AttributeValue::String(s)) => s,
            _ => return Err("formatDate pattern must be constant string".to_string()),
        };
        Ok(Self {
            timestamp_executor,
            pattern,
        })
    }
}

impl ExpressionExecutor for FormatDateFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let ts_val = self.timestamp_executor.execute(event)?;
        let ts = match ts_val {
            AttributeValue::Long(v) => v,
            AttributeValue::Int(v) => v as i64,
            AttributeValue::Null => return Some(AttributeValue::Null),
            _ => return None,
        };
        let ndt = chrono::DateTime::<Utc>::from_timestamp_millis(ts)?.naive_utc();
        let formatted = ndt.format(&self.pattern).to_string();
        Some(AttributeValue::String(formatted))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(FormatDateFunctionExecutor {
            timestamp_executor: self.timestamp_executor.clone_executor(ctx),
            pattern: self.pattern.clone(),
        })
    }
}
