// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/math_functions.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

fn to_f64(val: &AttributeValue) -> Option<f64> {
    match val {
        AttributeValue::Int(v) => Some(*v as f64),
        AttributeValue::Long(v) => Some(*v as f64),
        AttributeValue::Float(v) => Some(*v as f64),
        AttributeValue::Double(v) => Some(*v),
        _ => None,
    }
}

#[derive(Debug)]
pub struct SqrtFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl SqrtFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for SqrtFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.sqrt()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(SqrtFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct RoundFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    precision_executor: Option<Box<dyn ExpressionExecutor>>,
}

impl RoundFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            precision_executor: None,
        })
    }

    pub fn new_with_precision(
        value_executor: Box<dyn ExpressionExecutor>,
        precision_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            precision_executor: Some(precision_executor),
        })
    }
}

#[derive(Debug)]
pub struct LogFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl LogFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for LogFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.ln()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LogFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct SinFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl SinFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for SinFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.sin()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(SinFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct TanFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl TanFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for TanFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.tan()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(TanFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct AsinFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl AsinFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for AsinFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                // asin is only defined for values in [-1, 1]
                if num < -1.0 || num > 1.0 {
                    Some(AttributeValue::Double(f64::NAN))
                } else {
                    Some(AttributeValue::Double(num.asin()))
                }
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AsinFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct AcosFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl AcosFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for AcosFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                // acos is only defined for values in [-1, 1]
                if num < -1.0 || num > 1.0 {
                    Some(AttributeValue::Double(f64::NAN))
                } else {
                    Some(AttributeValue::Double(num.acos()))
                }
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AcosFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct AtanFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl AtanFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for AtanFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.atan()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AtanFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

impl ExpressionExecutor for RoundFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                let result = if let Some(ref precision_exec) = self.precision_executor {
                    let precision_val = precision_exec.execute(event)?;
                    let precision = match precision_val {
                        AttributeValue::Int(p) => p,
                        AttributeValue::Long(p) => p as i32,
                        _ => 0,
                    };
                    let multiplier = 10_f64.powi(precision);
                    (num * multiplier).round() / multiplier
                } else {
                    num.round()
                };
                Some(AttributeValue::Double(result))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(RoundFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            precision_executor: self
                .precision_executor
                .as_ref()
                .map(|e| e.clone_executor(ctx)),
        })
    }
}

#[derive(Debug)]
pub struct AbsFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl AbsFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for AbsFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            AttributeValue::Int(v) => Some(AttributeValue::Int(v.abs())),
            AttributeValue::Long(v) => Some(AttributeValue::Long(v.abs())),
            AttributeValue::Float(v) => Some(AttributeValue::Float(v.abs())),
            AttributeValue::Double(v) => Some(AttributeValue::Double(v.abs())),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.value_executor.get_return_type()
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AbsFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct FloorFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl FloorFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for FloorFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.floor()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(FloorFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct CeilFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl CeilFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for CeilFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.ceil()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(CeilFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct CosFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl CosFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for CosFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.cos()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(CosFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct ExpFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl ExpFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for ExpFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.exp()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ExpFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct PowerFunctionExecutor {
    base_executor: Box<dyn ExpressionExecutor>,
    exponent_executor: Box<dyn ExpressionExecutor>,
}

impl PowerFunctionExecutor {
    pub fn new(
        base_executor: Box<dyn ExpressionExecutor>,
        exponent_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            base_executor,
            exponent_executor,
        })
    }
}

impl ExpressionExecutor for PowerFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let base_val = self.base_executor.execute(event)?;
        let exp_val = self.exponent_executor.execute(event)?;
        match (&base_val, &exp_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            _ => {
                let base = to_f64(&base_val)?;
                let exp = to_f64(&exp_val)?;
                Some(AttributeValue::Double(base.powf(exp)))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(PowerFunctionExecutor {
            base_executor: self.base_executor.clone_executor(ctx),
            exponent_executor: self.exponent_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct Log10FunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl Log10FunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for Log10FunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let val = self.value_executor.execute(event)?;
        match val {
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => {
                let num = to_f64(&val)?;
                Some(AttributeValue::Double(num.log10()))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(Log10FunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

/// MaximumFunctionExecutor implements maximum(a, b, c, ...) - returns the maximum of all values.
/// All arguments must be numeric. Returns DOUBLE.
#[derive(Debug)]
pub struct MaximumFunctionExecutor {
    executors: Vec<Box<dyn ExpressionExecutor>>,
}

impl MaximumFunctionExecutor {
    pub fn new(executors: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if executors.is_empty() {
            return Err("maximum() requires at least one argument".to_string());
        }
        Ok(Self { executors })
    }
}

impl ExpressionExecutor for MaximumFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let mut max_val: Option<f64> = None;

        for exec in &self.executors {
            match exec.execute(event)? {
                AttributeValue::Null => continue,
                val => {
                    let num = to_f64(&val)?;
                    max_val = Some(match max_val {
                        None => num,
                        Some(current) => current.max(num),
                    });
                }
            }
        }

        match max_val {
            Some(v) => Some(AttributeValue::Double(v)),
            None => Some(AttributeValue::Null),
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(MaximumFunctionExecutor {
            executors: self
                .executors
                .iter()
                .map(|e| e.clone_executor(ctx))
                .collect(),
        })
    }
}

/// MinimumFunctionExecutor implements minimum(a, b, c, ...) - returns the minimum of all values.
/// All arguments must be numeric. Returns DOUBLE.
#[derive(Debug)]
pub struct MinimumFunctionExecutor {
    executors: Vec<Box<dyn ExpressionExecutor>>,
}

impl MinimumFunctionExecutor {
    pub fn new(executors: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if executors.is_empty() {
            return Err("minimum() requires at least one argument".to_string());
        }
        Ok(Self { executors })
    }
}

impl ExpressionExecutor for MinimumFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let mut min_val: Option<f64> = None;

        for exec in &self.executors {
            match exec.execute(event)? {
                AttributeValue::Null => continue,
                val => {
                    let num = to_f64(&val)?;
                    min_val = Some(match min_val {
                        None => num,
                        Some(current) => current.min(num),
                    });
                }
            }
        }

        match min_val {
            Some(v) => Some(AttributeValue::Double(v)),
            None => Some(AttributeValue::Null),
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(MinimumFunctionExecutor {
            executors: self
                .executors
                .iter()
                .map(|e| e.clone_executor(ctx))
                .collect(),
        })
    }
}

/// ModFunctionExecutor implements mod(a, b) - returns a % b (modulo operation).
#[derive(Debug)]
pub struct ModFunctionExecutor {
    a_executor: Box<dyn ExpressionExecutor>,
    b_executor: Box<dyn ExpressionExecutor>,
}

impl ModFunctionExecutor {
    pub fn new(
        a_executor: Box<dyn ExpressionExecutor>,
        b_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            a_executor,
            b_executor,
        })
    }
}

impl ExpressionExecutor for ModFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let a_val = self.a_executor.execute(event)?;
        let b_val = self.b_executor.execute(event)?;

        match (&a_val, &b_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            _ => {
                let a = to_f64(&a_val)?;
                let b = to_f64(&b_val)?;
                if b == 0.0 {
                    Some(AttributeValue::Null) // Division by zero returns null
                } else {
                    Some(AttributeValue::Double(a % b))
                }
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ModFunctionExecutor {
            a_executor: self.a_executor.clone_executor(ctx),
            b_executor: self.b_executor.clone_executor(ctx),
        })
    }
}

/// SignFunctionExecutor implements sign(num) - returns -1, 0, or 1.
#[derive(Debug)]
pub struct SignFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
}

impl SignFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { value_executor })
    }
}

impl ExpressionExecutor for SignFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.value_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            val => {
                let num = to_f64(&val)?;
                let sign = if num > 0.0 {
                    1
                } else if num < 0.0 {
                    -1
                } else {
                    0
                };
                Some(AttributeValue::Int(sign))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::INT
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(SignFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
        })
    }
}

/// TruncFunctionExecutor implements trunc(num) or trunc(num, precision).
/// Truncates decimal places (towards zero).
#[derive(Debug)]
pub struct TruncFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    precision_executor: Option<Box<dyn ExpressionExecutor>>,
}

impl TruncFunctionExecutor {
    pub fn new(value_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            precision_executor: None,
        })
    }

    pub fn new_with_precision(
        value_executor: Box<dyn ExpressionExecutor>,
        precision_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            precision_executor: Some(precision_executor),
        })
    }
}

impl ExpressionExecutor for TruncFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.value_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            val => {
                let num = to_f64(&val)?;
                let precision = match &self.precision_executor {
                    Some(exec) => match exec.execute(event)? {
                        AttributeValue::Null => return Some(AttributeValue::Null),
                        p => match &p {
                            AttributeValue::Int(i) => *i,
                            AttributeValue::Long(l) => *l as i32,
                            _ => 0,
                        },
                    },
                    None => 0,
                };

                let multiplier = 10_f64.powi(precision);
                let truncated = (num * multiplier).trunc() / multiplier;
                Some(AttributeValue::Double(truncated))
            }
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::DOUBLE
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(TruncFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            precision_executor: self
                .precision_executor
                .as_ref()
                .map(|e| e.clone_executor(ctx)),
        })
    }
}
