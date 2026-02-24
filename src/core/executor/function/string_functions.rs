// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/executor/function/string_functions.rs
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;

#[derive(Debug)]
pub struct LengthFunctionExecutor {
    expr: Box<dyn ExpressionExecutor>,
}

impl LengthFunctionExecutor {
    pub fn new(expr: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if expr.get_return_type() != ApiAttributeType::STRING {
            return Err("length() requires a STRING argument".to_string());
        }
        Ok(Self { expr })
    }
}

impl ExpressionExecutor for LengthFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.expr.execute(event)? {
            AttributeValue::String(s) => Some(AttributeValue::Int(s.len() as i32)),
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::INT
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LengthFunctionExecutor {
            expr: self.expr.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct ConcatFunctionExecutor {
    executors: Vec<Box<dyn ExpressionExecutor>>,
}

impl ConcatFunctionExecutor {
    pub fn new(executors: Vec<Box<dyn ExpressionExecutor>>) -> Result<Self, String> {
        if executors.is_empty() {
            return Err("concat() requires at least one argument".to_string());
        }
        for e in &executors {
            if e.get_return_type() != ApiAttributeType::STRING {
                return Err("concat() arguments must be STRING".to_string());
            }
        }
        Ok(Self { executors })
    }
}

impl ExpressionExecutor for ConcatFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let mut result = String::new();
        for e in &self.executors {
            match e.execute(event)? {
                AttributeValue::String(s) => result.push_str(&s),
                AttributeValue::Null => return Some(AttributeValue::Null),
                _ => return None,
            }
        }
        Some(AttributeValue::String(result))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ConcatFunctionExecutor {
            executors: self
                .executors
                .iter()
                .map(|e| e.clone_executor(ctx))
                .collect(),
        })
    }
}

fn to_i32(val: &AttributeValue) -> Option<i32> {
    match val {
        AttributeValue::Int(v) => Some(*v),
        AttributeValue::Long(v) => Some(*v as i32),
        AttributeValue::Float(v) => Some(*v as i32),
        AttributeValue::Double(v) => Some(*v as i32),
        _ => None,
    }
}

#[derive(Debug)]
pub struct LowerFunctionExecutor {
    expr: Box<dyn ExpressionExecutor>,
}

impl LowerFunctionExecutor {
    pub fn new(expr: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if expr.get_return_type() != ApiAttributeType::STRING {
            return Err("lower() requires a STRING argument".to_string());
        }
        Ok(Self { expr })
    }
}

impl ExpressionExecutor for LowerFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.expr.execute(event)? {
            AttributeValue::String(s) => Some(AttributeValue::String(s.to_lowercase())),
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LowerFunctionExecutor {
            expr: self.expr.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct UpperFunctionExecutor {
    expr: Box<dyn ExpressionExecutor>,
}

impl UpperFunctionExecutor {
    pub fn new(expr: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        if expr.get_return_type() != ApiAttributeType::STRING {
            return Err("upper() requires a STRING argument".to_string());
        }
        Ok(Self { expr })
    }
}

impl ExpressionExecutor for UpperFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.expr.execute(event)? {
            AttributeValue::String(s) => Some(AttributeValue::String(s.to_uppercase())),
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(UpperFunctionExecutor {
            expr: self.expr.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct SubstringFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    start_executor: Box<dyn ExpressionExecutor>,
    length_executor: Option<Box<dyn ExpressionExecutor>>,
}

impl SubstringFunctionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        start_executor: Box<dyn ExpressionExecutor>,
        length_executor: Option<Box<dyn ExpressionExecutor>>,
    ) -> Result<Self, String> {
        if value_executor.get_return_type() != ApiAttributeType::STRING {
            return Err("substring() requires STRING as first argument".to_string());
        }
        if start_executor.get_return_type() == ApiAttributeType::STRING {
            return Err("substring() start index must be numeric".to_string());
        }
        if let Some(le) = &length_executor {
            if le.get_return_type() == ApiAttributeType::STRING {
                return Err("substring() length must be numeric".to_string());
            }
        }
        Ok(Self {
            value_executor,
            start_executor,
            length_executor,
        })
    }
}

impl ExpressionExecutor for SubstringFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value = self.value_executor.execute(event)?;
        let s = match value {
            AttributeValue::String(v) => v,
            AttributeValue::Null => return Some(AttributeValue::Null),
            _ => return None,
        };

        let start_val = self.start_executor.execute(event)?;
        let start_idx = to_i32(&start_val)?;
        // Use 0-based indexing (Rust native) - SQL 1-based conversion happens at converter level
        let start = if start_idx < 0 { 0 } else { start_idx as usize };

        let substr = if let Some(le) = &self.length_executor {
            let len_val = le.execute(event)?;
            let len = to_i32(&len_val)? as usize;
            if start >= s.len() {
                String::new()
            } else {
                let end = usize::min(start + len, s.len());
                s[start..end].to_string()
            }
        } else if start >= s.len() {
            String::new()
        } else {
            s[start..].to_string()
        };

        Some(AttributeValue::String(substr))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(SubstringFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            start_executor: self.start_executor.clone_executor(ctx),
            length_executor: self.length_executor.as_ref().map(|e| e.clone_executor(ctx)),
        })
    }
}

#[derive(Debug)]
pub struct TrimFunctionExecutor {
    expr: Box<dyn ExpressionExecutor>,
}

impl TrimFunctionExecutor {
    pub fn new(expr: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { expr })
    }
}

impl ExpressionExecutor for TrimFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.expr.execute(event)? {
            AttributeValue::String(s) => Some(AttributeValue::String(s.trim().to_string())),
            AttributeValue::Null => Some(AttributeValue::Null),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(TrimFunctionExecutor {
            expr: self.expr.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct LikeFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    pattern_executor: Box<dyn ExpressionExecutor>,
}

impl LikeFunctionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        pattern_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            pattern_executor,
        })
    }

    /// Convert SQL LIKE pattern to regex pattern
    /// % matches any sequence of characters
    /// _ matches any single character
    fn like_to_regex(pattern: &str) -> String {
        let mut regex = String::from("^");
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                '\\' => {
                    // Escape next character
                    if let Some(&next) = chars.peek() {
                        chars.next();
                        regex.push_str(&regex::escape(&next.to_string()));
                    }
                }
                _ => regex.push_str(&regex::escape(&c.to_string())),
            }
        }

        regex.push('$');
        regex
    }
}

impl ExpressionExecutor for LikeFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value = self.value_executor.execute(event)?;
        let pattern = self.pattern_executor.execute(event)?;

        match (&value, &pattern) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(s), AttributeValue::String(p)) => {
                let regex_pattern = Self::like_to_regex(p);
                match regex::Regex::new(&regex_pattern) {
                    Ok(re) => Some(AttributeValue::Bool(re.is_match(s))),
                    Err(_) => None,
                }
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::BOOL
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LikeFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            pattern_executor: self.pattern_executor.clone_executor(ctx),
        })
    }
}

#[derive(Debug)]
pub struct ReplaceFunctionExecutor {
    value_executor: Box<dyn ExpressionExecutor>,
    from_executor: Box<dyn ExpressionExecutor>,
    to_executor: Box<dyn ExpressionExecutor>,
}

impl ReplaceFunctionExecutor {
    pub fn new(
        value_executor: Box<dyn ExpressionExecutor>,
        from_executor: Box<dyn ExpressionExecutor>,
        to_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            value_executor,
            from_executor,
            to_executor,
        })
    }
}

impl ExpressionExecutor for ReplaceFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let value = self.value_executor.execute(event)?;
        let from = self.from_executor.execute(event)?;
        let to = self.to_executor.execute(event)?;

        match (&value, &from, &to) {
            (AttributeValue::Null, _, _)
            | (_, AttributeValue::Null, _)
            | (_, _, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(s), AttributeValue::String(f), AttributeValue::String(t)) => {
                Some(AttributeValue::String(s.replace(f.as_str(), t.as_str())))
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ReplaceFunctionExecutor {
            value_executor: self.value_executor.clone_executor(ctx),
            from_executor: self.from_executor.clone_executor(ctx),
            to_executor: self.to_executor.clone_executor(ctx),
        })
    }
}

/// LeftFunctionExecutor implements left(str, n) - returns leftmost n characters.
#[derive(Debug)]
pub struct LeftFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
    n_executor: Box<dyn ExpressionExecutor>,
}

impl LeftFunctionExecutor {
    pub fn new(
        str_executor: Box<dyn ExpressionExecutor>,
        n_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            str_executor,
            n_executor,
        })
    }
}

impl ExpressionExecutor for LeftFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let str_val = self.str_executor.execute(event)?;
        let n_val = self.n_executor.execute(event)?;

        match (&str_val, &n_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(s), n) => {
                let count = match n {
                    AttributeValue::Int(i) => *i as usize,
                    AttributeValue::Long(l) => *l as usize,
                    _ => return None,
                };
                let result: String = s.chars().take(count).collect();
                Some(AttributeValue::String(result))
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LeftFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
            n_executor: self.n_executor.clone_executor(ctx),
        })
    }
}

/// RightFunctionExecutor implements right(str, n) - returns rightmost n characters.
#[derive(Debug)]
pub struct RightFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
    n_executor: Box<dyn ExpressionExecutor>,
}

impl RightFunctionExecutor {
    pub fn new(
        str_executor: Box<dyn ExpressionExecutor>,
        n_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            str_executor,
            n_executor,
        })
    }
}

impl ExpressionExecutor for RightFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let str_val = self.str_executor.execute(event)?;
        let n_val = self.n_executor.execute(event)?;

        match (&str_val, &n_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(s), n) => {
                let count = match n {
                    AttributeValue::Int(i) => *i as usize,
                    AttributeValue::Long(l) => *l as usize,
                    _ => return None,
                };
                let len = s.chars().count();
                let skip = len.saturating_sub(count);
                let result: String = s.chars().skip(skip).collect();
                Some(AttributeValue::String(result))
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(RightFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
            n_executor: self.n_executor.clone_executor(ctx),
        })
    }
}

/// LtrimFunctionExecutor implements ltrim(str) - trims leading whitespace.
#[derive(Debug)]
pub struct LtrimFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
}

impl LtrimFunctionExecutor {
    pub fn new(str_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { str_executor })
    }
}

impl ExpressionExecutor for LtrimFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.str_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            AttributeValue::String(s) => Some(AttributeValue::String(s.trim_start().to_string())),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LtrimFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
        })
    }
}

/// RtrimFunctionExecutor implements rtrim(str) - trims trailing whitespace.
#[derive(Debug)]
pub struct RtrimFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
}

impl RtrimFunctionExecutor {
    pub fn new(str_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { str_executor })
    }
}

impl ExpressionExecutor for RtrimFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.str_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            AttributeValue::String(s) => Some(AttributeValue::String(s.trim_end().to_string())),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(RtrimFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
        })
    }
}

/// ReverseFunctionExecutor implements reverse(str) - reverses the string.
#[derive(Debug)]
pub struct ReverseFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
}

impl ReverseFunctionExecutor {
    pub fn new(str_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { str_executor })
    }
}

impl ExpressionExecutor for ReverseFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.str_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            AttributeValue::String(s) => Some(AttributeValue::String(s.chars().rev().collect())),
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ReverseFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
        })
    }
}

/// RepeatFunctionExecutor implements repeat(str, n) - repeats the string n times.
#[derive(Debug)]
pub struct RepeatFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
    n_executor: Box<dyn ExpressionExecutor>,
}

impl RepeatFunctionExecutor {
    pub fn new(
        str_executor: Box<dyn ExpressionExecutor>,
        n_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            str_executor,
            n_executor,
        })
    }
}

impl ExpressionExecutor for RepeatFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let str_val = self.str_executor.execute(event)?;
        let n_val = self.n_executor.execute(event)?;

        match (&str_val, &n_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(s), n) => {
                let count = match n {
                    AttributeValue::Int(i) => *i as usize,
                    AttributeValue::Long(l) => *l as usize,
                    _ => return None,
                };
                Some(AttributeValue::String(s.repeat(count)))
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(RepeatFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
            n_executor: self.n_executor.clone_executor(ctx),
        })
    }
}

/// PositionFunctionExecutor implements position(substr, str) - returns 1-based position of substring.
/// Returns 0 if substring is not found (SQL convention).
#[derive(Debug)]
pub struct PositionFunctionExecutor {
    substr_executor: Box<dyn ExpressionExecutor>,
    str_executor: Box<dyn ExpressionExecutor>,
}

impl PositionFunctionExecutor {
    pub fn new(
        substr_executor: Box<dyn ExpressionExecutor>,
        str_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            substr_executor,
            str_executor,
        })
    }
}

impl ExpressionExecutor for PositionFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let substr_val = self.substr_executor.execute(event)?;
        let str_val = self.str_executor.execute(event)?;

        match (&substr_val, &str_val) {
            (AttributeValue::Null, _) | (_, AttributeValue::Null) => Some(AttributeValue::Null),
            (AttributeValue::String(substr), AttributeValue::String(s)) => {
                // SQL position is 1-based, returns 0 if not found
                match s.find(substr.as_str()) {
                    Some(idx) => Some(AttributeValue::Int((idx + 1) as i32)),
                    None => Some(AttributeValue::Int(0)),
                }
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::INT
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(PositionFunctionExecutor {
            substr_executor: self.substr_executor.clone_executor(ctx),
            str_executor: self.str_executor.clone_executor(ctx),
        })
    }
}

/// AsciiFunctionExecutor implements ascii(str) - returns ASCII code of first character.
/// Returns NULL if string is empty.
#[derive(Debug)]
pub struct AsciiFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
}

impl AsciiFunctionExecutor {
    pub fn new(str_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { str_executor })
    }
}

impl ExpressionExecutor for AsciiFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        match self.str_executor.execute(event)? {
            AttributeValue::Null => Some(AttributeValue::Null),
            AttributeValue::String(s) => {
                if s.is_empty() {
                    Some(AttributeValue::Null)
                } else {
                    // Get the first character's ASCII/Unicode code point
                    let first_char = s.chars().next()?;
                    Some(AttributeValue::Int(first_char as i32))
                }
            }
            _ => None,
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::INT
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(AsciiFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
        })
    }
}

/// ChrFunctionExecutor implements chr(code) - returns character from ASCII/Unicode code point.
#[derive(Debug)]
pub struct ChrFunctionExecutor {
    code_executor: Box<dyn ExpressionExecutor>,
}

impl ChrFunctionExecutor {
    pub fn new(code_executor: Box<dyn ExpressionExecutor>) -> Result<Self, String> {
        Ok(Self { code_executor })
    }
}

impl ExpressionExecutor for ChrFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let code_val = self.code_executor.execute(event)?;

        let code = match code_val {
            AttributeValue::Null => return Some(AttributeValue::Null),
            AttributeValue::Int(i) => i as u32,
            AttributeValue::Long(l) => l as u32,
            _ => return None,
        };

        // Convert code point to character
        match char::from_u32(code) {
            Some(c) => Some(AttributeValue::String(c.to_string())),
            None => Some(AttributeValue::Null), // Invalid code point
        }
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(ChrFunctionExecutor {
            code_executor: self.code_executor.clone_executor(ctx),
        })
    }
}

/// LpadFunctionExecutor implements lpad(str, len, pad) - left-pads string to specified length.
/// If string is longer than len, it is truncated to len characters.
#[derive(Debug)]
pub struct LpadFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
    len_executor: Box<dyn ExpressionExecutor>,
    pad_executor: Box<dyn ExpressionExecutor>,
}

impl LpadFunctionExecutor {
    pub fn new(
        str_executor: Box<dyn ExpressionExecutor>,
        len_executor: Box<dyn ExpressionExecutor>,
        pad_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            str_executor,
            len_executor,
            pad_executor,
        })
    }
}

impl ExpressionExecutor for LpadFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let str_val = self.str_executor.execute(event)?;
        let len_val = self.len_executor.execute(event)?;
        let pad_val = self.pad_executor.execute(event)?;

        // Handle NULL inputs
        if matches!(str_val, AttributeValue::Null)
            || matches!(len_val, AttributeValue::Null)
            || matches!(pad_val, AttributeValue::Null)
        {
            return Some(AttributeValue::Null);
        }

        let s = match str_val {
            AttributeValue::String(s) => s,
            _ => return None,
        };
        let target_len = match len_val {
            AttributeValue::Int(i) => i as usize,
            AttributeValue::Long(l) => l as usize,
            _ => return None,
        };
        let pad = match pad_val {
            AttributeValue::String(p) => p,
            _ => return None,
        };

        if pad.is_empty() {
            return Some(AttributeValue::String(s));
        }

        let current_len = s.chars().count();
        if current_len >= target_len {
            // Truncate to target length
            let truncated: String = s.chars().take(target_len).collect();
            return Some(AttributeValue::String(truncated));
        }

        // Need to pad
        let pad_len = target_len - current_len;
        let pad_chars: Vec<char> = pad.chars().collect();
        let mut result = String::with_capacity(target_len);

        // Add padding characters
        for i in 0..pad_len {
            result.push(pad_chars[i % pad_chars.len()]);
        }
        result.push_str(&s);

        Some(AttributeValue::String(result))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(LpadFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
            len_executor: self.len_executor.clone_executor(ctx),
            pad_executor: self.pad_executor.clone_executor(ctx),
        })
    }
}

/// RpadFunctionExecutor implements rpad(str, len, pad) - right-pads string to specified length.
/// If string is longer than len, it is truncated to len characters.
#[derive(Debug)]
pub struct RpadFunctionExecutor {
    str_executor: Box<dyn ExpressionExecutor>,
    len_executor: Box<dyn ExpressionExecutor>,
    pad_executor: Box<dyn ExpressionExecutor>,
}

impl RpadFunctionExecutor {
    pub fn new(
        str_executor: Box<dyn ExpressionExecutor>,
        len_executor: Box<dyn ExpressionExecutor>,
        pad_executor: Box<dyn ExpressionExecutor>,
    ) -> Result<Self, String> {
        Ok(Self {
            str_executor,
            len_executor,
            pad_executor,
        })
    }
}

impl ExpressionExecutor for RpadFunctionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue> {
        let str_val = self.str_executor.execute(event)?;
        let len_val = self.len_executor.execute(event)?;
        let pad_val = self.pad_executor.execute(event)?;

        // Handle NULL inputs
        if matches!(str_val, AttributeValue::Null)
            || matches!(len_val, AttributeValue::Null)
            || matches!(pad_val, AttributeValue::Null)
        {
            return Some(AttributeValue::Null);
        }

        let s = match str_val {
            AttributeValue::String(s) => s,
            _ => return None,
        };
        let target_len = match len_val {
            AttributeValue::Int(i) => i as usize,
            AttributeValue::Long(l) => l as usize,
            _ => return None,
        };
        let pad = match pad_val {
            AttributeValue::String(p) => p,
            _ => return None,
        };

        if pad.is_empty() {
            return Some(AttributeValue::String(s));
        }

        let current_len = s.chars().count();
        if current_len >= target_len {
            // Truncate to target length
            let truncated: String = s.chars().take(target_len).collect();
            return Some(AttributeValue::String(truncated));
        }

        // Need to pad
        let pad_len = target_len - current_len;
        let pad_chars: Vec<char> = pad.chars().collect();
        let mut result = s.clone();

        // Add padding characters
        for i in 0..pad_len {
            result.push(pad_chars[i % pad_chars.len()]);
        }

        Some(AttributeValue::String(result))
    }

    fn get_return_type(&self) -> ApiAttributeType {
        ApiAttributeType::STRING
    }

    fn clone_executor(&self, ctx: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        Box::new(RpadFunctionExecutor {
            str_executor: self.str_executor.clone_executor(ctx),
            len_executor: self.len_executor.clone_executor(ctx),
            pad_executor: self.pad_executor.clone_executor(ctx),
        })
    }
}
