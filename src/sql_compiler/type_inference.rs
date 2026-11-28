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

//! Type inference for the EventFlux SQL compiler.
//!
//! Works out the type of each SQL expression when the query is compiled. That gives the
//! output stream schemas and catches type errors early.
//!
//! # Design Principles
//!
//! 1. **Fail Fast**: Catch type errors when compiling, not while running
//! 2. **Borrow, do not share**: The engine borrows the catalog instead of holding an Arc
//! 3. **Extensible**: Function return types come from a lookup table, not a big match

use crate::query_api::definition::attribute::Type as AttributeType;
use crate::query_api::expression::constant::Constant;
use crate::query_api::expression::expression::Expression;
use crate::query_api::expression::variable::Variable;
use crate::sql_compiler::catalog::SqlCatalog;
use crate::sql_compiler::error::TypeError;

/// The streams and tables an unqualified column may come from.
#[derive(Debug, Clone, Default)]
pub struct TypeContext {
    /// Streams and tables to search, in order. Includes every JOIN source.
    pub available_streams: Vec<String>,
}

impl TypeContext {
    #[inline]
    pub const fn new() -> Self {
        TypeContext {
            available_streams: Vec::new(),
        }
    }

    #[inline]
    pub fn from_stream(stream_name: String) -> Self {
        TypeContext {
            available_streams: vec![stream_name],
        }
    }

    /// Add one more stream to search, such as the other side of a JOIN.
    #[inline]
    pub fn with_stream(mut self, stream_name: String) -> Self {
        self.available_streams.push(stream_name);
        self
    }

    #[inline]
    pub fn from_streams(streams: Vec<String>) -> Self {
        TypeContext {
            available_streams: streams,
        }
    }
}

/// Return type rule for one built in function.
struct FunctionSignature {
    /// Lower case name.
    name: &'static str,
    /// Fewest arguments allowed. Extra arguments are not rejected.
    min_args: usize,
    /// Maps the argument types to the result type, or an error for bad argument types.
    return_type: fn(&[AttributeType]) -> Result<AttributeType, TypeError>,
}

impl FunctionSignature {
    const fn new(
        name: &'static str,
        min_args: usize,
        return_type: fn(&[AttributeType]) -> Result<AttributeType, TypeError>,
    ) -> Self {
        Self {
            name,
            min_args,
            return_type,
        }
    }
}

// Rank of numeric types for arithmetic. DOUBLE > FLOAT > LONG > INT. Other types rank 0.
#[inline]
const fn type_precedence(t: AttributeType) -> u8 {
    match t {
        AttributeType::DOUBLE => 4,
        AttributeType::FLOAT => 3,
        AttributeType::LONG => 2,
        AttributeType::INT => 1,
        _ => 0,
    }
}

#[inline]
fn is_numeric(t: AttributeType) -> bool {
    matches!(
        t,
        AttributeType::INT | AttributeType::LONG | AttributeType::FLOAT | AttributeType::DOUBLE
    )
}

// Return type rules for built in functions, kept as a table instead of a big match.
// `name` must be lower case. Unknown names get None.
fn get_function_signature(name: &str) -> Option<&'static FunctionSignature> {
    static FUNCTIONS: &[FunctionSignature] = &[
        // Aggregations
        FunctionSignature::new("count", 0, |_| Ok(AttributeType::LONG)),
        FunctionSignature::new("sum", 1, |args| match args[0] {
            AttributeType::INT | AttributeType::LONG => Ok(AttributeType::LONG),
            AttributeType::FLOAT | AttributeType::DOUBLE => Ok(AttributeType::DOUBLE),
            _ => Err(TypeError::ConversionFailed(
                "SUM requires numeric argument".into(),
            )),
        }),
        FunctionSignature::new("avg", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "AVG requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("min", 1, |args| Ok(args[0])),
        FunctionSignature::new("max", 1, |args| Ok(args[0])),
        // distinctCount counts unique values
        FunctionSignature::new("distinctcount", 1, |_| Ok(AttributeType::LONG)),
        // stdDev is the standard deviation, always a DOUBLE
        FunctionSignature::new("stddev", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "STDDEV requires numeric argument".into(),
                ))
            }
        }),
        // Boolean aggregators
        FunctionSignature::new("and", 1, |args| {
            if args[0] == AttributeType::BOOL {
                Ok(AttributeType::BOOL)
            } else {
                Err(TypeError::ConversionFailed(
                    "AND aggregator requires boolean argument".into(),
                ))
            }
        }),
        FunctionSignature::new("or", 1, |args| {
            if args[0] == AttributeType::BOOL {
                Ok(AttributeType::BOOL)
            } else {
                Err(TypeError::ConversionFailed(
                    "OR aggregator requires boolean argument".into(),
                ))
            }
        }),
        // Math functions
        FunctionSignature::new("round", 1, |args| match args[0] {
            AttributeType::FLOAT | AttributeType::DOUBLE => Ok(AttributeType::DOUBLE),
            AttributeType::INT | AttributeType::LONG => Ok(AttributeType::LONG),
            _ => Err(TypeError::ConversionFailed(
                "ROUND requires numeric argument".into(),
            )),
        }),
        FunctionSignature::new("abs", 1, |args| {
            if is_numeric(args[0]) {
                Ok(args[0])
            } else {
                Err(TypeError::ConversionFailed(
                    "ABS requires numeric argument".into(),
                ))
            }
        }),
        // String functions
        FunctionSignature::new("upper", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "UPPER requires STRING argument".into(),
                ))
            }
        }),
        FunctionSignature::new("lower", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "LOWER requires STRING argument".into(),
                ))
            }
        }),
        FunctionSignature::new("length", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "LENGTH requires STRING argument".into(),
                ))
            }
        }),
        FunctionSignature::new("concat", 0, |_| Ok(AttributeType::STRING)),
        // Utility functions
        // coalesce gives the first non null value. Typed as its first argument.
        FunctionSignature::new("coalesce", 1, |args| Ok(args[0])),
        // default gives the second argument when the first is null. Typed as the first.
        FunctionSignature::new("default", 2, |args| Ok(args[0])),
        // nullif gives NULL when both arguments are equal, else the first
        FunctionSignature::new("nullif", 2, |args| Ok(args[0])),
        // uuid makes a random UUID string
        FunctionSignature::new("uuid", 0, |_| Ok(AttributeType::STRING)),
        // eventTimestamp gives the event timestamp as a LONG
        FunctionSignature::new("eventtimestamp", 0, |_| Ok(AttributeType::LONG)),
        // now gives the current time in milliseconds
        FunctionSignature::new("now", 0, |_| Ok(AttributeType::LONG)),
        // Additional math functions
        FunctionSignature::new("floor", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "FLOOR requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("ceil", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "CEIL requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("sqrt", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "SQRT requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("sin", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "SIN requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("cos", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "COS requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("tan", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "TAN requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("asin", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "ASIN requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("acos", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "ACOS requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("atan", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "ATAN requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("exp", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "EXP requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("power", 2, |args| {
            if is_numeric(args[0]) && is_numeric(args[1]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "POWER requires numeric arguments".into(),
                ))
            }
        }),
        FunctionSignature::new("log", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "LOG requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("ln", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "LN requires numeric argument".into(),
                ))
            }
        }),
        FunctionSignature::new("log10", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "LOG10 requires numeric argument".into(),
                ))
            }
        }),
        // maximum gives the largest of its numeric arguments
        FunctionSignature::new("maximum", 1, |args| {
            if args.iter().all(|a| is_numeric(*a)) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "MAXIMUM requires numeric arguments".into(),
                ))
            }
        }),
        // minimum gives the smallest of its numeric arguments
        FunctionSignature::new("minimum", 1, |args| {
            if args.iter().all(|a| is_numeric(*a)) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "MINIMUM requires numeric arguments".into(),
                ))
            }
        }),
        // mod gives the remainder, a % b
        FunctionSignature::new("mod", 2, |args| {
            if is_numeric(args[0]) && is_numeric(args[1]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "MOD requires numeric arguments".into(),
                ))
            }
        }),
        // sign gives -1, 0 or 1
        FunctionSignature::new("sign", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "SIGN requires numeric argument".into(),
                ))
            }
        }),
        // trunc drops decimal places
        FunctionSignature::new("trunc", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "TRUNC requires numeric argument".into(),
                ))
            }
        }),
        // truncate is another name for trunc
        FunctionSignature::new("truncate", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::DOUBLE)
            } else {
                Err(TypeError::ConversionFailed(
                    "TRUNCATE requires numeric argument".into(),
                ))
            }
        }),
        // Additional string functions
        FunctionSignature::new("trim", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "TRIM requires STRING argument".into(),
                ))
            }
        }),
        FunctionSignature::new("replace", 3, |args| {
            if args[0] == AttributeType::STRING
                && args[1] == AttributeType::STRING
                && args[2] == AttributeType::STRING
            {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "REPLACE requires STRING arguments".into(),
                ))
            }
        }),
        // left gives the first n characters
        FunctionSignature::new("left", 2, |args| {
            if args[0] == AttributeType::STRING && is_numeric(args[1]) {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "LEFT requires (STRING, INT) arguments".into(),
                ))
            }
        }),
        // right gives the last n characters
        FunctionSignature::new("right", 2, |args| {
            if args[0] == AttributeType::STRING && is_numeric(args[1]) {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "RIGHT requires (STRING, INT) arguments".into(),
                ))
            }
        }),
        // ltrim drops leading whitespace
        FunctionSignature::new("ltrim", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "LTRIM requires STRING argument".into(),
                ))
            }
        }),
        // rtrim drops trailing whitespace
        FunctionSignature::new("rtrim", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "RTRIM requires STRING argument".into(),
                ))
            }
        }),
        // reverse reverses a string
        FunctionSignature::new("reverse", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "REVERSE requires STRING argument".into(),
                ))
            }
        }),
        // repeat repeats a string n times
        FunctionSignature::new("repeat", 2, |args| {
            if args[0] == AttributeType::STRING && is_numeric(args[1]) {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "REPEAT requires (STRING, INT) arguments".into(),
                ))
            }
        }),
        FunctionSignature::new("substring", 1, |_| Ok(AttributeType::STRING)),
        // substr is another name for substring
        FunctionSignature::new("substr", 1, |_| Ok(AttributeType::STRING)),
        // like gives a BOOL for a pattern match
        FunctionSignature::new("like", 2, |args| {
            if args[0] == AttributeType::STRING && args[1] == AttributeType::STRING {
                Ok(AttributeType::BOOL)
            } else {
                Err(TypeError::ConversionFailed(
                    "LIKE requires STRING arguments".into(),
                ))
            }
        }),
        // position gives where a substring starts, counting from 1, or 0 if not found
        FunctionSignature::new("position", 2, |args| {
            if args[0] == AttributeType::STRING && args[1] == AttributeType::STRING {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "POSITION requires (STRING, STRING) arguments".into(),
                ))
            }
        }),
        // locate is another name for position, as in MySQL
        FunctionSignature::new("locate", 2, |args| {
            if args[0] == AttributeType::STRING && args[1] == AttributeType::STRING {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "LOCATE requires (STRING, STRING) arguments".into(),
                ))
            }
        }),
        // instr is another name for position, as in MySQL and Oracle
        FunctionSignature::new("instr", 2, |args| {
            if args[0] == AttributeType::STRING && args[1] == AttributeType::STRING {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "INSTR requires (STRING, STRING) arguments".into(),
                ))
            }
        }),
        // ascii gives the ASCII code of the first character
        FunctionSignature::new("ascii", 1, |args| {
            if args[0] == AttributeType::STRING {
                Ok(AttributeType::INT)
            } else {
                Err(TypeError::ConversionFailed(
                    "ASCII requires STRING argument".into(),
                ))
            }
        }),
        // chr gives the character for an ASCII or Unicode code point
        FunctionSignature::new("chr", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "CHR requires INT argument".into(),
                ))
            }
        }),
        // char is another name for chr, as in MySQL
        FunctionSignature::new("char", 1, |args| {
            if is_numeric(args[0]) {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "CHAR requires INT argument".into(),
                ))
            }
        }),
        // lpad pads a string on the left to the given length
        FunctionSignature::new("lpad", 3, |args| {
            if args[0] == AttributeType::STRING
                && is_numeric(args[1])
                && args[2] == AttributeType::STRING
            {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "LPAD requires (STRING, INT, STRING) arguments".into(),
                ))
            }
        }),
        // rpad pads a string on the right to the given length
        FunctionSignature::new("rpad", 3, |args| {
            if args[0] == AttributeType::STRING
                && is_numeric(args[1])
                && args[2] == AttributeType::STRING
            {
                Ok(AttributeType::STRING)
            } else {
                Err(TypeError::ConversionFailed(
                    "RPAD requires (STRING, INT, STRING) arguments".into(),
                ))
            }
        }),
    ];

    FUNCTIONS.iter().find(|f| f.name == name)
}

/// Works out expression types by looking up columns in a borrowed catalog.
pub struct TypeInferenceEngine<'a> {
    catalog: &'a SqlCatalog,
}

impl<'a> TypeInferenceEngine<'a> {
    #[inline]
    pub const fn new(catalog: &'a SqlCatalog) -> Self {
        TypeInferenceEngine { catalog }
    }

    /// The result type of an expression.
    ///
    /// Also checks operand types along the way. Comparisons, AND, OR, NOT, IS NULL and IN
    /// give BOOL. NULL gives OBJECT. Unknown functions give OBJECT so user defined
    /// functions still compile.
    ///
    /// # Errors
    ///
    /// Returns `TypeError::ConversionFailed` for an unknown column, arithmetic on non
    /// numeric types, a non BOOL operand to AND, OR or NOT, a bad comparison, CASE
    /// branches of different types, an unsupported CAST, or bad function arguments.
    pub fn infer_type(
        &self,
        expr: &Expression,
        context: &TypeContext,
    ) -> Result<AttributeType, TypeError> {
        match expr {
            Expression::Constant(c) => Self::infer_constant_type(c),
            Expression::Variable(v) => self.infer_variable_type(v, context),
            Expression::IndexedVariable(iv) => {
                // e1[0].attr has the same type as the attribute itself, so look it up as a
                // plain Variable on the same stream.
                let var = crate::query_api::expression::Variable {
                    eventflux_element: iv.eventflux_element.clone(),
                    stream_id: iv.stream_id.clone(),
                    stream_index: iv.stream_index,
                    attribute_name: iv.attribute_name.clone(),
                    is_inner_stream: false,
                    function_id: None,
                    function_index: None,
                };
                self.infer_variable_type(&var, context)
            }
            Expression::Add(add) => {
                self.infer_arithmetic_type(&add.left_value, &add.right_value, context)
            }
            Expression::Subtract(sub) => {
                self.infer_arithmetic_type(&sub.left_value, &sub.right_value, context)
            }
            Expression::Multiply(mul) => {
                self.infer_arithmetic_type(&mul.left_value, &mul.right_value, context)
            }
            Expression::Divide(div) => {
                self.infer_arithmetic_type(&div.left_value, &div.right_value, context)
            }
            Expression::Mod(m) => {
                self.infer_arithmetic_type(&m.left_value, &m.right_value, context)
            }
            Expression::Compare(cmp) => {
                self.validate_comparison_types(
                    &cmp.left_expression,
                    &cmp.right_expression,
                    &cmp.operator,
                    context,
                )?;
                Ok(AttributeType::BOOL)
            }
            Expression::And(and) => {
                self.validate_boolean_operand(&and.left_expression, context, "AND (left)")?;
                self.validate_boolean_operand(&and.right_expression, context, "AND (right)")?;
                Ok(AttributeType::BOOL)
            }
            Expression::Or(or) => {
                self.validate_boolean_operand(&or.left_expression, context, "OR (left)")?;
                self.validate_boolean_operand(&or.right_expression, context, "OR (right)")?;
                Ok(AttributeType::BOOL)
            }
            Expression::Not(not) => {
                self.validate_boolean_operand(&not.expression, context, "NOT")?;
                Ok(AttributeType::BOOL)
            }
            Expression::IsNull(_) | Expression::In(_) => Ok(AttributeType::BOOL),
            Expression::AttributeFunction(func) => self.infer_function_type(func, context),
            Expression::Case(case) => {
                // The CASE type comes from its results. Every WHEN result and the ELSE
                // must share one type, except that NULL (OBJECT) fits any type.
                if case.when_clauses.is_empty() {
                    return Err(TypeError::ConversionFailed(
                        "CASE expression must have at least one WHEN clause".to_string(),
                    ));
                }

                // The first WHEN result that is not NULL sets the type. If all are NULL,
                // the ELSE sets it. If that is NULL too, the type is OBJECT.
                let mut result_type = AttributeType::OBJECT;
                for when_clause in &case.when_clauses {
                    let when_type = self.infer_type(&when_clause.result, context)?;
                    if when_type != AttributeType::OBJECT {
                        result_type = when_type;
                        break;
                    }
                }
                if result_type == AttributeType::OBJECT {
                    let else_type = self.infer_type(&case.else_result, context)?;
                    if else_type != AttributeType::OBJECT {
                        result_type = else_type;
                    }
                }

                for (idx, when_clause) in case.when_clauses.iter().enumerate() {
                    let when_type = self.infer_type(&when_clause.result, context)?;
                    if when_type != result_type && when_type != AttributeType::OBJECT {
                        return Err(TypeError::ConversionFailed(format!(
                            "CASE expression type mismatch: WHEN clause {} returns {:?}, expected {:?}",
                            idx + 1,
                            when_type,
                            result_type
                        )));
                    }
                }

                // NULL is allowed here because a missing ELSE is filled in as NULL.
                let else_type = self.infer_type(&case.else_result, context)?;
                if else_type != result_type && else_type != AttributeType::OBJECT {
                    return Err(TypeError::ConversionFailed(format!(
                        "CASE expression type mismatch: ELSE clause returns {:?}, expected {:?}",
                        else_type, result_type
                    )));
                }

                Ok(result_type)
            }
            Expression::Cast(cast) => {
                let source_type = self.infer_type(&cast.expression, context)?;
                let target_type = cast.target_type;

                if !Self::is_cast_supported(source_type, target_type) {
                    return Err(TypeError::ConversionFailed(format!(
                        "Cannot cast {:?} to {:?}",
                        source_type, target_type
                    )));
                }

                Ok(target_type)
            }
        }
    }

    /// The type of a constant. Time constants are LONG and NULL is OBJECT. Never fails.
    #[inline]
    fn infer_constant_type(constant: &Constant) -> Result<AttributeType, TypeError> {
        use crate::query_api::expression::constant::ConstantValueWithFloat;

        Ok(match &constant.value {
            ConstantValueWithFloat::Int(_) => AttributeType::INT,
            ConstantValueWithFloat::Long(_) | ConstantValueWithFloat::Time(_) => {
                AttributeType::LONG
            }
            ConstantValueWithFloat::Float(_) => AttributeType::FLOAT,
            ConstantValueWithFloat::Double(_) => AttributeType::DOUBLE,
            ConstantValueWithFloat::String(_) => AttributeType::STRING,
            ConstantValueWithFloat::Bool(_) => AttributeType::BOOL,
            ConstantValueWithFloat::Null => AttributeType::OBJECT,
        })
    }

    /// Whether CAST from `source` to `target` is allowed. Must match the conversions in
    /// CastExecutor.
    #[inline]
    fn is_cast_supported(source: AttributeType, target: AttributeType) -> bool {
        use AttributeType::*;

        // Casting to the same type does nothing.
        if source == target {
            return true;
        }

        // NULL and OBJECT pass through to any type.
        if source == OBJECT {
            return true;
        }

        match (source, target) {
            (STRING, INT | LONG | FLOAT | DOUBLE | BOOL) => true,

            (INT | LONG | FLOAT | DOUBLE, STRING) => true,

            (BOOL, STRING) => true,

            // Widening. INT to LONG, FLOAT or DOUBLE. LONG to FLOAT or DOUBLE. FLOAT to
            // DOUBLE.
            (INT, LONG | FLOAT | DOUBLE) => true,
            (LONG, FLOAT | DOUBLE) => true,
            (FLOAT, DOUBLE) => true,

            // Narrowing. LONG to INT. DOUBLE to FLOAT, INT or LONG. FLOAT to INT or LONG.
            (LONG, INT) => true,
            (DOUBLE, FLOAT | INT | LONG) => true,
            (FLOAT, INT | LONG) => true,

            // Any non zero value becomes true.
            (INT | LONG, BOOL) => true,

            _ => false,
        }
    }

    /// The type of a column reference.
    ///
    /// A qualified column such as `s.price` is looked up on that stream, table or alias.
    /// An unqualified one takes the first match among the context streams, in order.
    fn infer_variable_type(
        &self,
        variable: &Variable,
        context: &TypeContext,
    ) -> Result<AttributeType, TypeError> {
        let attr_name = variable.get_attribute_name();

        if let Some(stream_id) = variable.get_stream_id() {
            return self
                .catalog
                .get_column_type(stream_id, attr_name)
                .map_err(|e| {
                    TypeError::ConversionFailed(format!(
                        "Column {}.{} not found: {}",
                        stream_id, attr_name, e
                    ))
                });
        }

        // No ambiguity check. The first stream with the column wins.
        for stream_name in &context.available_streams {
            if let Ok(attr_type) = self.catalog.get_column_type(stream_name, attr_name) {
                return Ok(attr_type);
            }
        }

        Err(TypeError::ConversionFailed(format!(
            "Column '{}' not found in available streams",
            attr_name
        )))
    }

    /// The result type of arithmetic. Both sides must be numeric, and the result is the
    /// wider of the two types.
    #[inline]
    fn infer_arithmetic_type(
        &self,
        left: &Expression,
        right: &Expression,
        context: &TypeContext,
    ) -> Result<AttributeType, TypeError> {
        let left_type = self.infer_type(left, context)?;
        let right_type = self.infer_type(right, context)?;

        if !is_numeric(left_type) || !is_numeric(right_type) {
            return Err(TypeError::ConversionFailed(format!(
                "Cannot perform arithmetic on {:?} and {:?}",
                left_type, right_type
            )));
        }

        Ok(
            if type_precedence(left_type) >= type_precedence(right_type) {
                left_type
            } else {
                right_type
            },
        )
    }

    /// The result type of a function call, from the function table.
    ///
    /// Names are matched without regard to letter case. The namespace is ignored. An
    /// unknown function gives OBJECT so user defined functions still compile.
    fn infer_function_type(
        &self,
        func: &crate::query_api::expression::attribute_function::AttributeFunction,
        context: &TypeContext,
    ) -> Result<AttributeType, TypeError> {
        let func_name = func.function_name.to_lowercase();

        let param_types: Result<Vec<AttributeType>, TypeError> = func
            .parameters
            .iter()
            .map(|param| self.infer_type(param, context))
            .collect();
        let param_types = param_types?;

        if let Some(sig) = get_function_signature(&func_name) {
            if param_types.len() < sig.min_args {
                return Err(TypeError::ConversionFailed(format!(
                    "{} requires at least {} argument(s), found {}",
                    func_name.to_uppercase(),
                    sig.min_args,
                    param_types.len()
                )));
            }

            (sig.return_type)(&param_types)
        } else {
            Ok(AttributeType::OBJECT)
        }
    }

    /// Check that an expression gives a BOOL, for clauses such as WHERE and HAVING.
    ///
    /// # Errors
    ///
    /// Returns `TypeError::ConversionFailed` naming `clause_name` and the type found, with
    /// a hint on how to fix it. Also returns any error from inferring the type.
    pub fn validate_boolean_expression(
        &self,
        expr: &Expression,
        context: &TypeContext,
        clause_name: &str,
    ) -> Result<(), TypeError> {
        let expr_type = self.infer_type(expr, context)?;

        if expr_type != AttributeType::BOOL {
            let hint = match expr {
                Expression::Variable(_) if is_numeric(expr_type) => {
                    "Did you mean to use a comparison? Try adding '> 0' or '!= 0'".to_string()
                }
                Expression::Add(_)
                | Expression::Subtract(_)
                | Expression::Multiply(_)
                | Expression::Divide(_) => {
                    "Arithmetic expressions must be compared: try '(...) > 0'".to_string()
                }
                Expression::AttributeFunction(f) => {
                    format!(
                        "Function '{}' returns {:?}, not BOOL - use a comparison",
                        f.function_name, expr_type
                    )
                }
                _ => format!("Expected BOOL expression, found {:?}", expr_type),
            };

            return Err(TypeError::ConversionFailed(format!(
                "{} clause must return BOOL type, found {:?}. Hint: {}",
                clause_name, expr_type, hint
            )));
        }

        Ok(())
    }

    /// Build a TypeContext from the input streams of a query.
    ///
    /// Collects stream ids and their aliases, without repeats, so qualified references
    /// such as "R.column" in table joins resolve. A query with no input gives an empty
    /// context.
    pub fn build_context_from_query(
        &self,
        query: &crate::query_api::execution::query::Query,
    ) -> TypeContext {
        let mut available_sources = Vec::new();

        if let Some(input_stream) = query.get_input_stream() {
            Self::collect_stream_identifiers(input_stream, &mut available_sources);
        }

        TypeContext::from_streams(available_sources)
    }

    /// Append the stream ids and aliases of a single, join or pattern input.
    fn collect_stream_identifiers(
        input: &crate::query_api::execution::query::input::stream::input_stream::InputStream,
        identifiers: &mut Vec<String>,
    ) {
        use crate::query_api::execution::query::input::stream::input_stream::InputStream;

        match input {
            InputStream::Single(single) => {
                Self::collect_from_single_stream(single, identifiers);
            }
            InputStream::Join(join) => {
                Self::collect_from_single_stream(&join.left_input_stream, identifiers);
                Self::collect_from_single_stream(&join.right_input_stream, identifiers);
            }
            InputStream::State(state) => {
                // Walk the pattern tree for both stream ids and aliases.
                Self::collect_from_state_element(&state.state_element, identifiers);
            }
        }
    }

    /// Walk a pattern tree and append every stream id and alias.
    fn collect_from_state_element(
        element: &crate::query_api::execution::query::input::state::StateElement,
        identifiers: &mut Vec<String>,
    ) {
        use crate::query_api::execution::query::input::state::StateElement;

        match element {
            StateElement::Stream(stream_state) => {
                Self::collect_from_single_stream(
                    stream_state.get_single_input_stream(),
                    identifiers,
                );
            }
            StateElement::Next(next) => {
                Self::collect_from_state_element(&next.state_element, identifiers);
                Self::collect_from_state_element(&next.next_state_element, identifiers);
            }
            StateElement::Logical(logical) => {
                Self::collect_from_state_element(&logical.stream_state_element_1, identifiers);
                Self::collect_from_state_element(&logical.stream_state_element_2, identifiers);
            }
            StateElement::Every(every) => {
                Self::collect_from_state_element(&every.state_element, identifiers);
            }
            StateElement::Count(count) => {
                Self::collect_from_single_stream(
                    count.stream_state_element.get_single_input_stream(),
                    identifiers,
                );
            }
            StateElement::AbsentStream(absent) => {
                Self::collect_from_single_stream(
                    absent.stream_state_element.get_single_input_stream(),
                    identifiers,
                );
            }
        }
    }

    /// Append the stream id, then its alias if any. Skips names already in the list.
    fn collect_from_single_stream(
        single: &crate::query_api::execution::query::input::stream::single_input_stream::SingleInputStream,
        identifiers: &mut Vec<String>,
    ) {
        let stream_id = single.get_stream_id_str().to_string();
        if !identifiers.contains(&stream_id) {
            identifiers.push(stream_id);
        }

        if let Some(ref_id) = single.get_stream_reference_id_str() {
            let ref_id = ref_id.to_string();
            if !identifiers.contains(&ref_id) {
                identifiers.push(ref_id);
            }
        }
    }

    /// Check the types in a whole query.
    ///
    /// Checks:
    /// - WHERE gives BOOL, on a single stream, JOIN or pattern input
    /// - HAVING gives BOOL
    ///
    /// Join and pattern aliases of the query resolve. JOIN ON conditions, per element
    /// pattern filters such as `e1=A[v > 10]` and the SELECT list are not checked here.
    /// The select expressions are typed later, when output streams are built.
    ///
    /// # Errors
    ///
    /// Returns the first `TypeError` found.
    pub fn validate_query(
        &self,
        query: &crate::query_api::execution::query::Query,
    ) -> Result<(), TypeError> {
        use crate::query_api::execution::query::input::handler::StreamHandler;
        use crate::query_api::execution::query::input::stream::input_stream::InputStream;

        // Join and pattern aliases (a.price, e1.price) must resolve, and callers pass a
        // catalog that does not have them.
        let aliased_catalog = self.catalog.with_query_aliases(query);
        let engine = TypeInferenceEngine::new(&aliased_catalog);
        let context = engine.build_context_from_query(query);

        // WHERE is stored as Filter handlers on a single input stream, and as the filter
        // of a join or pattern input.
        match query.get_input_stream() {
            Some(InputStream::Single(single_stream)) => {
                for handler in single_stream.get_stream_handlers() {
                    if let StreamHandler::Filter(filter) = handler {
                        engine.validate_boolean_expression(
                            &filter.filter_expression,
                            &context,
                            "WHERE",
                        )?;
                    }
                }
            }
            Some(InputStream::Join(join)) => {
                if let Some(filter) = &join.filter {
                    engine.validate_boolean_expression(filter, &context, "WHERE")?;
                }
            }
            Some(InputStream::State(state)) => {
                if let Some(filter) = &state.filter {
                    engine.validate_boolean_expression(filter, &context, "WHERE")?;
                }
            }
            None => {}
        }

        if let Some(having) = query.get_selector().get_having_expression() {
            engine.validate_boolean_expression(having, &context, "HAVING")?;
        }

        Ok(())
    }

    /// Check that the two sides of a comparison fit together.
    ///
    /// Rules:
    /// - BOOL only compares with BOOL, and only with == and !=
    /// - STRING only compares with STRING
    /// - Numeric types (INT, LONG, FLOAT, DOUBLE) compare with each other
    ///
    /// NULL counts as OBJECT, so comparing a BOOL or STRING with NULL is an error.
    fn validate_comparison_types(
        &self,
        left: &Expression,
        right: &Expression,
        operator: &crate::query_api::expression::condition::CompareOperator,
        context: &TypeContext,
    ) -> Result<(), TypeError> {
        use crate::query_api::expression::condition::CompareOperator;

        let left_type = self.infer_type(left, context)?;
        let right_type = self.infer_type(right, context)?;

        if left_type == AttributeType::BOOL || right_type == AttributeType::BOOL {
            if left_type != right_type {
                return Err(TypeError::ConversionFailed(format!(
                    "Cannot compare BOOL with {:?}. Boolean values can only be compared with other boolean values.",
                    if left_type == AttributeType::BOOL { right_type } else { left_type }
                )));
            }
            match operator {
                CompareOperator::Equal | CompareOperator::NotEqual => {}
                _ => {
                    return Err(TypeError::ConversionFailed(format!(
                        "Boolean values can only use == and != operators, not {:?}",
                        operator
                    )));
                }
            }
        }

        if left_type == AttributeType::STRING || right_type == AttributeType::STRING {
            // A STRING can only be compared with another STRING
            if left_type != right_type {
                return Err(TypeError::ConversionFailed(format!(
                    "Cannot compare STRING with {:?}. String values can only be compared with other string values.",
                    if left_type == AttributeType::STRING { right_type } else { left_type }
                )));
            }
        }

        // Any other pair passes, including mixed numeric types, which are converted when
        // compared. OBJECT against a numeric type also passes.

        Ok(())
    }

    /// Check that an AND, OR or NOT operand gives a BOOL. `operator_name` goes into the
    /// error message.
    fn validate_boolean_operand(
        &self,
        expr: &Expression,
        context: &TypeContext,
        operator_name: &str,
    ) -> Result<(), TypeError> {
        let expr_type = self.infer_type(expr, context)?;

        if expr_type != AttributeType::BOOL {
            return Err(TypeError::ConversionFailed(format!(
                "{} operator requires BOOL operand, but found {:?}. \
                 Hint: Use a comparison expression (e.g., 'value > 0') instead of a raw value.",
                operator_name, expr_type
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::definition::stream_definition::StreamDefinition;

    fn create_test_catalog() -> SqlCatalog {
        let mut catalog = SqlCatalog::new();

        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("symbol".to_string(), AttributeType::STRING)
            .attribute("price".to_string(), AttributeType::DOUBLE)
            .attribute("volume".to_string(), AttributeType::LONG)
            .attribute("change".to_string(), AttributeType::FLOAT)
            .attribute("count".to_string(), AttributeType::INT)
            .attribute("active".to_string(), AttributeType::BOOL);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();

        catalog
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_constant_type_inference() {
        let catalog = create_test_catalog();
        let _engine = TypeInferenceEngine::new(&catalog);

        assert_eq!(
            TypeInferenceEngine::infer_constant_type(&Constant::int(42)).unwrap(),
            AttributeType::INT
        );
        assert_eq!(
            TypeInferenceEngine::infer_constant_type(&Constant::long(42)).unwrap(),
            AttributeType::LONG
        );
        assert_eq!(
            TypeInferenceEngine::infer_constant_type(&Constant::double(3.14)).unwrap(),
            AttributeType::DOUBLE
        );
    }

    #[test]
    fn test_variable_type_inference() {
        let catalog = create_test_catalog();
        let engine = TypeInferenceEngine::new(&catalog);
        let context = TypeContext::from_stream("TestStream".to_string());

        let var = Variable::new("price".to_string());
        assert_eq!(
            engine.infer_variable_type(&var, &context).unwrap(),
            AttributeType::DOUBLE
        );
    }

    #[test]
    fn test_arithmetic_type_precedence() {
        let catalog = create_test_catalog();
        let engine = TypeInferenceEngine::new(&catalog);
        let context = TypeContext::from_stream("TestStream".to_string());

        // DOUBLE + INT → DOUBLE
        let expr = Expression::add(
            Expression::variable("price".to_string()),
            Expression::value_int(10),
        );
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::DOUBLE
        );

        // LONG * INT → LONG
        let expr = Expression::multiply(
            Expression::variable("volume".to_string()),
            Expression::value_int(2),
        );
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::LONG
        );
    }

    #[test]
    fn test_function_registry() {
        let catalog = create_test_catalog();
        let engine = TypeInferenceEngine::new(&catalog);
        let context = TypeContext::from_stream("TestStream".to_string());

        // COUNT → LONG
        let expr = Expression::function_no_ns("count".to_string(), vec![]);
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::LONG
        );

        // AVG → DOUBLE
        let expr = Expression::function_no_ns(
            "avg".to_string(),
            vec![Expression::variable("price".to_string())],
        );
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::DOUBLE
        );
    }

    #[test]
    fn test_cast_supported_conversions() {
        // String to numeric
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::STRING,
            AttributeType::INT
        ));
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::STRING,
            AttributeType::DOUBLE
        ));

        // Numeric to String
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::INT,
            AttributeType::STRING
        ));
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::DOUBLE,
            AttributeType::STRING
        ));

        // Numeric widening
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::INT,
            AttributeType::LONG
        ));
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::FLOAT,
            AttributeType::DOUBLE
        ));

        // Numeric narrowing
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::LONG,
            AttributeType::INT
        ));
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::DOUBLE,
            AttributeType::FLOAT
        ));

        // Same type (no-op)
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::INT,
            AttributeType::INT
        ));
    }

    #[test]
    fn test_cast_unsupported_conversions() {
        // Bool to numeric (not directly supported)
        assert!(!TypeInferenceEngine::is_cast_supported(
            AttributeType::BOOL,
            AttributeType::INT
        ));
        assert!(!TypeInferenceEngine::is_cast_supported(
            AttributeType::BOOL,
            AttributeType::DOUBLE
        ));

        // Bool to String is supported
        assert!(TypeInferenceEngine::is_cast_supported(
            AttributeType::BOOL,
            AttributeType::STRING
        ));
    }

    #[test]
    fn test_cast_type_inference_valid() {
        let catalog = create_test_catalog();
        let engine = TypeInferenceEngine::new(&catalog);
        let context = TypeContext::from_stream("TestStream".to_string());

        // CAST(symbol AS INT). STRING to INT is allowed.
        let expr = Expression::cast(
            Expression::variable("symbol".to_string()),
            AttributeType::INT,
        );
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::INT
        );

        // CAST(price AS INT). DOUBLE to INT is allowed.
        let expr = Expression::cast(
            Expression::variable("price".to_string()),
            AttributeType::INT,
        );
        assert_eq!(
            engine.infer_type(&expr, &context).unwrap(),
            AttributeType::INT
        );
    }

    #[test]
    fn test_cast_type_inference_invalid() {
        let catalog = create_test_catalog();
        let engine = TypeInferenceEngine::new(&catalog);
        let context = TypeContext::from_stream("TestStream".to_string());

        // CAST(active AS DOUBLE). BOOL to DOUBLE is NOT allowed.
        let expr = Expression::cast(
            Expression::variable("active".to_string()),
            AttributeType::DOUBLE,
        );
        let result = engine.infer_type(&expr, &context);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot cast BOOL to DOUBLE"));
    }
}
