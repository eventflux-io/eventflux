// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::{ExpressionExecutor, create_expression_executor};
use crate::query_api::expression::case::{Case, WhenClause};
use crate::core::error::error::Error;
use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::query_api::definition::attribute::Type as ApiAttributeType;
use std::sync::Arc;
use std::any::Any;

/// Executor for CASE expressions
pub struct CaseExecutor {
    operand_executor: Option<Box<dyn ExpressionExecutor>>,
    when_condition_executors: Vec<Box<dyn ExpressionExecutor>>,
    when_result_executors: Vec<Box<dyn ExpressionExecutor>>,
    else_executor: Box<dyn ExpressionExecutor>,
}

impl CaseExecutor {
    pub fn new(case: &Case, context: &Arc<EventFluxAppContext>) -> Result<Self, Error> {
        let operand_executor = if let Some(ref operand) = case.operand {
            Some(create_expression_executor(operand, context)?)
        } else {
            None
        };

        let mut when_condition_executors = Vec::new();
        let mut when_result_executors = Vec::new();

        for when_clause in &case.when_clauses {
            when_condition_executors.push(create_expression_executor(&when_clause.condition, context)?);
            when_result_executors.push(create_expression_executor(&when_clause.result, context)?);
        }

        let else_executor = create_expression_executor(&case.else_result, context)?;

        Ok(Self {
            operand_executor,
            when_condition_executors,
            when_result_executors,
            else_executor,
        })
    }
}

impl ExpressionExecutor for CaseExecutor {
    fn execute(&self, event: Option<&dyn crate::core::event::complex_event::ComplexEvent>) -> Option<AttributeValue> {
        if let Some(ref operand_exec) = self.operand_executor {
            // Simple CASE
            let operand_value = operand_exec.execute(event)?;
            for (cond_exec, res_exec) in self.when_condition_executors.iter().zip(&self.when_result_executors) {
                let when_value = cond_exec.execute(event)?;
                if operand_value == when_value {
                    return res_exec.execute(event);
                }
            }
        } else {
            // Searched CASE
            for (cond_exec, res_exec) in self.when_condition_executors.iter().zip(&self.when_result_executors) {
                let condition_result = cond_exec.execute(event)?;
                if let AttributeValue::Boolean(true) = condition_result {
                    return res_exec.execute(event);
                }
            }
        }

        // ELSE
        self.else_executor.execute(event)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        // Assume all results have the same type
        self.else_executor.get_return_type()
    }

    fn clone_executor(&self, context: &Arc<EventFluxAppContext>) -> Box<dyn ExpressionExecutor> {
        // For simplicity, assume we can recreate
        // In real impl, would need to store the original Case
        Box::new(Self {
            operand_executor: self.operand_executor.as_ref().map(|e| e.clone_executor(context)),
            when_condition_executors: self.when_condition_executors.iter().map(|e| e.clone_executor(context)).collect(),
            when_result_executors: self.when_result_executors.iter().map(|e| e.clone_executor(context)).collect(),
            else_executor: self.else_executor.clone_executor(context),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::event::Event;
    use crate::query_api::expression::constant::Constant;
    use crate::query_api::expression::Expression;
    use crate::core::event::value::AttributeValue;

    #[test]
    fn test_simple_case() {
        let operand = Box::new(Expression::Constant(Constant::new(AttributeValue::Integer(1))));
        let when_clause = WhenClause::new(
            Box::new(Expression::Constant(Constant::new(AttributeValue::Integer(1)))),
            Box::new(Expression::Constant(Constant::new(AttributeValue::String("one".to_string())))),
        );
        let else_result = Box::new(Expression::Constant(Constant::new(AttributeValue::String("other".to_string()))));
        let case = Case::new(Some(operand), vec![when_clause], else_result);

        let executor = CaseExecutor::new(&case).unwrap();
        let event = Event::new(); // Empty event for test

        let result = executor.execute(&event).unwrap();
        assert_eq!(result, AttributeValue::String("one".to_string()));
    }

    #[test]
    fn test_searched_case() {
        let condition = Box::new(Expression::Constant(Constant::new(AttributeValue::Boolean(true))));
        let when_clause = WhenClause::new(
            condition,
            Box::new(Expression::Constant(Constant::new(AttributeValue::String("true".to_string())))),
        );
        let else_result = Box::new(Expression::Constant(Constant::new(AttributeValue::String("false".to_string()))));
        let case = Case::new(None, vec![when_clause], else_result);

        let executor = CaseExecutor::new(&case).unwrap();
        let event = Event::new();

        let result = executor.execute(&event).unwrap();
        assert_eq!(result, AttributeValue::String("true".to_string()));
    }
}