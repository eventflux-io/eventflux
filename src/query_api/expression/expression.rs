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

// Corresponds to io.eventflux.query.api.expression.Expression (the abstract class)
// In Rust, this is represented as an enum.

// Import all the specific expression types
use super::attribute_function::AttributeFunction;
use super::case::{Case, WhenClause};
use super::cast::Cast;
use super::condition::{And, Compare, CompareOperator, InOp, IsNull, Not, Or};
use super::constant::{Constant, TimeUtil as ConstantTimeUtil}; // Corrected ConstantValue path
use super::indexed_variable::IndexedVariable;
use super::math::{Add, Divide, ModOp, Multiply, Subtract};
use super::variable::Variable; // Renamed Operator to CompareOperator
use crate::query_api::definition::attribute::Type as AttributeType;

/// Represents various types of expressions in a EventFlux query.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Constant(Constant),
    Variable(Variable),
    IndexedVariable(Box<IndexedVariable>),
    AttributeFunction(Box<AttributeFunction>),
    Add(Box<Add>),
    Subtract(Box<Subtract>),
    Multiply(Box<Multiply>),
    Divide(Box<Divide>),
    Mod(Box<ModOp>),
    And(Box<And>),
    Or(Box<Or>),
    Not(Box<Not>),
    Compare(Box<Compare>),
    In(Box<InOp>),
    IsNull(Box<IsNull>),
    Case(Box<Case>),
    Cast(Box<Cast>),
}

// Static factory methods from Java's Expression class
impl Expression {
    // Constants
    pub fn value_string(value: String) -> Self {
        Expression::Constant(Constant::string(value))
    }
    pub fn value_int(value: i32) -> Self {
        Expression::Constant(Constant::int(value))
    }
    pub fn value_long(value: i64) -> Self {
        Expression::Constant(Constant::long(value))
    }
    pub fn value_double(value: f64) -> Self {
        Expression::Constant(Constant::double(value))
    }
    pub fn value_float(value: f32) -> Self {
        Expression::Constant(Constant::float(value))
    }
    pub fn value_bool(value: bool) -> Self {
        Expression::Constant(Constant::bool(value))
    }
    pub fn value_null() -> Self {
        Expression::Constant(Constant::null())
    }

    // Variable
    pub fn variable(attribute_name: String) -> Self {
        Expression::Variable(Variable::new(attribute_name))
    }

    // IndexedVariable - array access: e[0].attr, e[last].attr
    pub fn indexed_variable_with_index(
        attribute_name: String,
        index: usize,
        stream_id: Option<String>,
        stream_index: Option<i32>,
    ) -> Self {
        let mut var = IndexedVariable::new_with_index(attribute_name, index);
        if let (Some(sid), Some(sidx)) = (stream_id, stream_index) {
            var = var.of_stream_with_index(sid, sidx);
        }
        Expression::IndexedVariable(Box::new(var))
    }

    pub fn indexed_variable_with_last(
        attribute_name: String,
        stream_id: Option<String>,
        stream_index: Option<i32>,
    ) -> Self {
        let mut var = IndexedVariable::new_with_last(attribute_name);
        if let (Some(sid), Some(sidx)) = (stream_id, stream_index) {
            var = var.of_stream_with_index(sid, sidx);
        }
        Expression::IndexedVariable(Box::new(var))
    }

    // AttributeFunction
    pub fn function(namespace: Option<String>, name: String, params: Vec<Expression>) -> Self {
        Expression::AttributeFunction(Box::new(AttributeFunction::new(namespace, name, params)))
    }
    pub fn function_no_ns(name: String, params: Vec<Expression>) -> Self {
        Expression::AttributeFunction(Box::new(AttributeFunction::new(None, name, params)))
    }

    // Math
    #[allow(clippy::should_implement_trait)]
    pub fn add(left: Expression, right: Expression) -> Self {
        Expression::Add(Box::new(Add::new(left, right)))
    }
    pub fn subtract(left: Expression, right: Expression) -> Self {
        Expression::Subtract(Box::new(Subtract::new(left, right)))
    }
    pub fn multiply(left: Expression, right: Expression) -> Self {
        Expression::Multiply(Box::new(Multiply::new(left, right)))
    }
    pub fn divide(left: Expression, right: Expression) -> Self {
        Expression::Divide(Box::new(Divide::new(left, right)))
    }
    pub fn modulus(left: Expression, right: Expression) -> Self {
        Expression::Mod(Box::new(ModOp::new(left, right)))
    }

    // Condition
    pub fn compare(left: Expression, op: CompareOperator, right: Expression) -> Self {
        Expression::Compare(Box::new(Compare::new(left, op, right)))
    }
    pub fn and(left: Expression, right: Expression) -> Self {
        Expression::And(Box::new(And::new(left, right)))
    }
    pub fn or(left: Expression, right: Expression) -> Self {
        Expression::Or(Box::new(Or::new(left, right)))
    }
    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expression) -> Self {
        Expression::Not(Box::new(Not::new(expr)))
    }

    pub fn in_op(expr: Expression, source_id: String) -> Self {
        Expression::In(Box::new(InOp::new(expr, source_id)))
    }

    pub fn is_null(expr: Expression) -> Self {
        Expression::IsNull(Box::new(IsNull::new_with_expression(expr)))
    }
    pub fn is_null_stream(
        stream_id: String,
        stream_index: Option<i32>,
        is_inner: bool,
        is_fault: bool,
    ) -> Self {
        Expression::IsNull(Box::new(IsNull::new_with_stream_details(
            stream_id,
            stream_index,
            is_inner,
            is_fault,
        )))
    }

    // CASE expression
    pub fn case(
        operand: Option<Expression>,
        when_clauses: Vec<WhenClause>,
        else_result: Expression,
    ) -> Self {
        Expression::Case(Box::new(Case::new(
            operand.map(Box::new),
            when_clauses,
            Box::new(else_result),
        )))
    }

    // CAST expression
    pub fn cast(expression: Expression, target_type: AttributeType) -> Self {
        Expression::Cast(Box::new(Cast::new(expression, target_type)))
    }

    // Time constants
    pub fn time_millisec(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::millisec(val))
    }
    pub fn time_sec(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::sec(val))
    }
    pub fn time_minute(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::minute(val))
    }
    pub fn time_hour(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::hour(val))
    }
    pub fn time_day(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::day(val))
    }
    pub fn time_week(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::week(val))
    }
    pub fn time_month(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::month(val))
    }
    pub fn time_year(val: i64) -> Self {
        Expression::Constant(ConstantTimeUtil::year(val))
    }
}

// Accessing EventFluxElement context from the composed field in each variant's struct.
impl Expression {
    pub fn get_query_context_start_index(&self) -> Option<(i32, i32)> {
        match self {
            Expression::Constant(c) => c.eventflux_element.query_context_start_index,
            Expression::Variable(v) => v.eventflux_element.query_context_start_index,
            Expression::IndexedVariable(iv) => iv.eventflux_element.query_context_start_index,
            Expression::AttributeFunction(af) => af.eventflux_element.query_context_start_index,
            Expression::Add(a) => a.eventflux_element.query_context_start_index,
            Expression::Subtract(s) => s.eventflux_element.query_context_start_index,
            Expression::Multiply(m) => m.eventflux_element.query_context_start_index,
            Expression::Divide(d) => d.eventflux_element.query_context_start_index,
            Expression::Mod(m) => m.eventflux_element.query_context_start_index,
            Expression::And(a) => a.eventflux_element.query_context_start_index,
            Expression::Or(o) => o.eventflux_element.query_context_start_index,
            Expression::Not(n) => n.eventflux_element.query_context_start_index,
            Expression::Compare(c) => c.eventflux_element.query_context_start_index,
            Expression::In(i) => i.eventflux_element.query_context_start_index,
            Expression::IsNull(i) => i.eventflux_element.query_context_start_index,
            Expression::Case(c) => c.eventflux_element.query_context_start_index,
            Expression::Cast(c) => c.eventflux_element.query_context_start_index,
        }
    }

    pub fn set_query_context_start_index(&mut self, index: Option<(i32, i32)>) {
        match self {
            Expression::Constant(c) => c.eventflux_element.query_context_start_index = index,
            Expression::Variable(v) => v.eventflux_element.query_context_start_index = index,
            Expression::IndexedVariable(iv) => {
                iv.eventflux_element.query_context_start_index = index
            }
            Expression::AttributeFunction(af) => {
                af.eventflux_element.query_context_start_index = index
            }
            Expression::Add(a) => a.eventflux_element.query_context_start_index = index,
            Expression::Subtract(s) => s.eventflux_element.query_context_start_index = index,
            Expression::Multiply(m) => m.eventflux_element.query_context_start_index = index,
            Expression::Divide(d) => d.eventflux_element.query_context_start_index = index,
            Expression::Mod(m) => m.eventflux_element.query_context_start_index = index,
            Expression::And(a) => a.eventflux_element.query_context_start_index = index,
            Expression::Or(o) => o.eventflux_element.query_context_start_index = index,
            Expression::Not(n) => n.eventflux_element.query_context_start_index = index,
            Expression::Compare(c) => c.eventflux_element.query_context_start_index = index,
            Expression::In(i) => i.eventflux_element.query_context_start_index = index,
            Expression::IsNull(i) => i.eventflux_element.query_context_start_index = index,
            Expression::Case(c) => c.eventflux_element.query_context_start_index = index,
            Expression::Cast(c) => c.eventflux_element.query_context_start_index = index,
        }
    }

    pub fn get_query_context_end_index(&self) -> Option<(i32, i32)> {
        match self {
            Expression::Constant(c) => c.eventflux_element.query_context_end_index,
            Expression::Variable(v) => v.eventflux_element.query_context_end_index,
            Expression::IndexedVariable(iv) => iv.eventflux_element.query_context_end_index,
            Expression::AttributeFunction(af) => af.eventflux_element.query_context_end_index,
            Expression::Add(a) => a.eventflux_element.query_context_end_index,
            Expression::Subtract(s) => s.eventflux_element.query_context_end_index,
            Expression::Multiply(m) => m.eventflux_element.query_context_end_index,
            Expression::Divide(d) => d.eventflux_element.query_context_end_index,
            Expression::Mod(m) => m.eventflux_element.query_context_end_index,
            Expression::And(a) => a.eventflux_element.query_context_end_index,
            Expression::Or(o) => o.eventflux_element.query_context_end_index,
            Expression::Not(n) => n.eventflux_element.query_context_end_index,
            Expression::Compare(c) => c.eventflux_element.query_context_end_index,
            Expression::In(i) => i.eventflux_element.query_context_end_index,
            Expression::IsNull(i) => i.eventflux_element.query_context_end_index,
            Expression::Case(c) => c.eventflux_element.query_context_end_index,
            Expression::Cast(c) => c.eventflux_element.query_context_end_index,
        }
    }

    pub fn set_query_context_end_index(&mut self, index: Option<(i32, i32)>) {
        match self {
            Expression::Constant(c) => c.eventflux_element.query_context_end_index = index,
            Expression::Variable(v) => v.eventflux_element.query_context_end_index = index,
            Expression::IndexedVariable(iv) => iv.eventflux_element.query_context_end_index = index,
            Expression::AttributeFunction(af) => {
                af.eventflux_element.query_context_end_index = index
            }
            Expression::Add(a) => a.eventflux_element.query_context_end_index = index,
            Expression::Subtract(s) => s.eventflux_element.query_context_end_index = index,
            Expression::Multiply(m) => m.eventflux_element.query_context_end_index = index,
            Expression::Divide(d) => d.eventflux_element.query_context_end_index = index,
            Expression::Mod(m) => m.eventflux_element.query_context_end_index = index,
            Expression::And(a) => a.eventflux_element.query_context_end_index = index,
            Expression::Or(o) => o.eventflux_element.query_context_end_index = index,
            Expression::Not(n) => n.eventflux_element.query_context_end_index = index,
            Expression::Compare(c) => c.eventflux_element.query_context_end_index = index,
            Expression::In(i) => i.eventflux_element.query_context_end_index = index,
            Expression::IsNull(i) => i.eventflux_element.query_context_end_index = index,
            Expression::Case(c) => c.eventflux_element.query_context_end_index = index,
            Expression::Cast(c) => c.eventflux_element.query_context_end_index = index,
        }
    }
}
