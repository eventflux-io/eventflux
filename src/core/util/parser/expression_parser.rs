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

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::meta_state_event::MetaStateEvent;
use crate::core::event::stream::meta_stream_event::MetaStreamEvent;
use crate::core::event::value::AttributeValue as CoreAttributeValue;
use crate::core::executor::function::scalar_function_executor::ScalarFunctionExecutor;
use crate::core::executor::{
    cast_executor::CastExecutor,
    condition::*,
    constant_expression_executor::ConstantExpressionExecutor,
    expression_executor::ExpressionExecutor,
    function::*,
    math::*,
    variable_expression_executor::{
        VariableExpressionExecutor, /*, VariablePosition, EventDataArrayType */
    },
    EventVariableFunctionExecutor, MultiValueVariableFunctionExecutor,
};
use crate::core::query::processor::ProcessingMode;
use crate::core::query::selector::attribute::aggregator::AttributeAggregatorExpressionExecutor;
use crate::query_api::{
    definition::attribute::Type as ApiAttributeType,
    expression::{
        constant::ConstantValueWithFloat as ApiConstantValue, Expression as ApiExpression,
    },
};

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Error from parsing an expression, with its position in the query when known.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionParseError {
    pub message: String,
    pub line: Option<i32>,
    pub column: Option<i32>,
    pub query_name: String,
}

impl ExpressionParseError {
    pub fn new(
        message: String,
        element: &crate::query_api::eventflux_element::EventFluxElement,
        query: &str,
    ) -> Self {
        let (line, column) = element
            .query_context_start_index
            .map(|(l, c)| (Some(l), Some(c)))
            .unwrap_or((None, None));
        ExpressionParseError {
            message,
            line,
            column,
            query_name: query.to_string(),
        }
    }
}

impl fmt::Display for ExpressionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.line, self.column) {
            (Some(l), Some(c)) => write!(
                f,
                "{} at line {}, column {} in query '{}'",
                self.message, l, c, self.query_name
            ),
            _ => write!(f, "{} in query '{}'", self.message, self.query_name),
        }
    }
}

impl std::error::Error for ExpressionParseError {}

pub type ExpressionParseResult<T> = Result<T, ExpressionParseError>;

// Wraps a ScalarFunctionExecutor (a UDF or a more complex built in) so it runs as an
// expression. It calls `init` when built and `destroy` when dropped.
#[derive(Debug)]
pub struct AttributeFunctionExpressionExecutor {
    scalar_function_executor: Box<dyn ScalarFunctionExecutor>,
    argument_executors: Vec<Box<dyn ExpressionExecutor>>,
    // eventflux_app_context: Arc<EventFluxAppContext>, // Stored by scalar_function_executor if needed after init
    return_type: ApiAttributeType,
}

impl AttributeFunctionExpressionExecutor {
    pub fn new(
        mut scalar_func_impl: Box<dyn ScalarFunctionExecutor>,
        arg_execs: Vec<Box<dyn ExpressionExecutor>>,
        app_ctx: Arc<EventFluxAppContext>,
    ) -> Result<Self, String> {
        scalar_func_impl.init(&arg_execs, &app_ctx)?;
        let return_type = scalar_func_impl.get_return_type();
        Ok(Self {
            scalar_function_executor: scalar_func_impl,
            argument_executors: arg_execs,
            return_type,
        })
    }
}

impl ExpressionExecutor for AttributeFunctionExpressionExecutor {
    fn execute(&self, event: Option<&dyn ComplexEvent>) -> Option<CoreAttributeValue> {
        // The function computes its own value from the event, using whatever it kept in
        // `init`, such as its argument executors. This wrapper does not run
        // `argument_executors` itself. Another design would run them here and pass the
        // values to a method like `eval(args: Vec<AttributeValue>)`, but the UDF keeps the
        // plain ExpressionExecutor::execute signature.
        self.scalar_function_executor.execute(event)
    }

    fn get_return_type(&self) -> ApiAttributeType {
        self.return_type
    }

    fn clone_executor(
        &self,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Box<dyn ExpressionExecutor> {
        let cloned_args = self
            .argument_executors
            .iter()
            .map(|e| e.clone_executor(eventflux_app_context))
            .collect();
        let cloned_scalar_fn = self.scalar_function_executor.clone_scalar_function();
        // `new` runs `init` again on the fresh function copy
        match AttributeFunctionExpressionExecutor::new(
            cloned_scalar_fn,
            cloned_args,
            Arc::clone(eventflux_app_context),
        ) {
            Ok(exec) => Box::new(exec),
            Err(e) => {
                // If `init` fails, print the error to stderr and fall back to a constant
                // NULL executor
                eprintln!("Failed to clone AttributeFunctionExpressionExecutor: {e}");
                Box::new(ConstantExpressionExecutor::new(
                    CoreAttributeValue::Null,
                    ApiAttributeType::OBJECT,
                ))
            }
        }
    }
}

impl Drop for AttributeFunctionExpressionExecutor {
    fn drop(&mut self) {
        self.scalar_function_executor.destroy();
    }
}

/// Context for `ExpressionParser`. Holds the metadata used to resolve variables.
///
/// The meta maps hold a `MetaStreamEvent` per stream, table, window and aggregation id,
/// and a `MetaStateEvent` per pattern state id. `default_source` names the entry to try
/// first when a variable does not name its source.
pub struct ExpressionParserContext<'a> {
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub eventflux_query_context: Arc<EventFluxQueryContext>,
    pub stream_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub table_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub window_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub aggregation_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub state_meta_map: HashMap<String, Arc<MetaStateEvent>>,
    /// Position of each source id (stream or table) in a StateEvent chain.
    /// Single stream queries map their input id to position 0, while joins and
    /// patterns map each source id to its own index. A missing id counts as 0.
    pub stream_positions: HashMap<String, i32>,
    pub default_source: String,
    pub query_name: &'a str,
    /// True for a mutation (UPDATE/DELETE/UPSERT). Unqualified columns then resolve
    /// to the target table without an ambiguity error, even if the source stream
    /// has a column with the same name.
    pub is_mutation_context: bool,
}

/// Parses a query_api::Expression into a core::ExpressionExecutor.
///
/// Variable resolution is basic. Variables always point at before window data, and some
/// expression kinds or contexts are not fully handled, such as aggregations within
/// HAVING.
///
/// # Errors
///
/// Returns an error when a variable is not found or is found in more than one source,
/// or when a sub expression, function or operator cannot be built.
pub fn parse_expression(
    api_expr: &ApiExpression,
    context: &ExpressionParserContext<'_>,
) -> ExpressionParseResult<Box<dyn ExpressionExecutor>> {
    match api_expr {
        ApiExpression::Constant(api_const) => {
            let (core_value, core_type) =
                convert_api_constant_to_core_attribute_value(&api_const.value);
            Ok(Box::new(ConstantExpressionExecutor::new(
                core_value, core_type,
            )))
        }
        ApiExpression::Variable(api_var) => {
            let attribute_name = &api_var.attribute_name;
            let stream_id_opt = api_var.stream_id.as_deref();

            let mut found: Option<([i32; 4], ApiAttributeType)> = None;
            let mut _found_id: Option<String> = None;

            // Looks the attribute up in one source and records it. Errors if a different
            // source already matched.
            let mut check_meta = |id: &str, meta: &MetaStreamEvent| {
                if let Some((idx, t)) = meta.find_attribute_info(attribute_name) {
                    if found.is_some() && _found_id.as_deref() != Some(id) {
                        return Err(ExpressionParseError::new(
                            format!("Attribute '{attribute_name}' found in multiple sources"),
                            &api_var.eventflux_element,
                            context.query_name,
                        ));
                    }
                    let pos = *context.stream_positions.get(id).unwrap_or(&0);
                    found = Some((
                        [
                            pos,
                            api_var.stream_index.unwrap_or(0),
                            crate::core::util::eventflux_constants::BEFORE_WINDOW_DATA_INDEX as i32,
                            *idx as i32,
                        ],
                        *t,
                    ));
                    _found_id = Some(id.to_string());
                }
                Ok(())
            };

            if let Some(id) = stream_id_opt {
                if let Some(meta) = context.stream_meta_map.get(id) {
                    check_meta(id, meta)?;
                } else if let Some(meta) = context.table_meta_map.get(id) {
                    check_meta(id, meta)?;
                } else if let Some(meta) = context.window_meta_map.get(id) {
                    check_meta(id, meta)?;
                } else if let Some(meta) = context.aggregation_meta_map.get(id) {
                    check_meta(id, meta)?;
                } else if let Some(state_meta) = context.state_meta_map.get(id) {
                    for (pos, opt_meta) in state_meta.meta_stream_events.iter().enumerate() {
                        if let Some(m) = opt_meta {
                            if let Some((idx, t)) = m.find_attribute_info(attribute_name) {
                                found = Some((
                                    [
                                        pos as i32,
                                        api_var.stream_index.unwrap_or(0),
                                        crate::core::util::eventflux_constants::BEFORE_WINDOW_DATA_INDEX as i32,
                                        *idx as i32,
                                    ],
                                    *t,
                                ));
                                _found_id = Some(id.to_string());
                                break;
                            }
                        }
                    }
                }
            } else {
                // Unqualified column. Try default_source first, but still check the name
                // is not in more than one source.
                let default_id = &context.default_source;
                let mut found_in_default = false;
                let mut also_found_elsewhere = false;

                // First pass. Is the column in default_source?
                if !default_id.is_empty() {
                    if let Some(meta) = context.stream_meta_map.get(default_id) {
                        if meta.find_attribute_info(attribute_name).is_some() {
                            found_in_default = true;
                        }
                    }
                    if !found_in_default {
                        if let Some(meta) = context.table_meta_map.get(default_id) {
                            if meta.find_attribute_info(attribute_name).is_some() {
                                found_in_default = true;
                            }
                        }
                    }
                }

                // Second pass. Is it also in another stream or table?
                if found_in_default {
                    for (id, meta) in &context.stream_meta_map {
                        if id != default_id && meta.find_attribute_info(attribute_name).is_some() {
                            also_found_elsewhere = true;
                            break;
                        }
                    }
                    if !also_found_elsewhere {
                        for (id, meta) in &context.table_meta_map {
                            if id != default_id
                                && meta.find_attribute_info(attribute_name).is_some()
                            {
                                also_found_elsewhere = true;
                                break;
                            }
                        }
                    }
                }

                // Found in default_source AND elsewhere is ambiguous, except in a mutation
                if found_in_default && also_found_elsewhere {
                    // In a mutation (UPDATE/DELETE/UPSERT), default_source is the target table
                    // on purpose, so the column resolves to the table.
                    if !context.is_mutation_context {
                        return Err(ExpressionParseError::new(
                            format!("Attribute '{attribute_name}' found in multiple sources"),
                            &api_var.eventflux_element,
                            context.query_name,
                        ));
                    }
                    // Debug log for the silent choice, so users can spot when they meant the
                    // stream column
                    log::debug!(
                        "Mutation context: ambiguous column '{}' resolved to table '{}'. \
                         Use explicit qualifier (e.g., '{}.{}') if stream column was intended.",
                        attribute_name,
                        default_id,
                        default_id,
                        attribute_name
                    );
                }

                // Now resolve the column
                if found_in_default {
                    if let Some(meta) = context.stream_meta_map.get(default_id) {
                        check_meta(default_id, meta)?;
                    } else if let Some(meta) = context.table_meta_map.get(default_id) {
                        check_meta(default_id, meta)?;
                    }
                } else {
                    // Not in default_source, so search every stream and table. check_meta
                    // errors if more than one has it.
                    for (id, meta) in &context.stream_meta_map {
                        check_meta(id, meta)?;
                    }
                    for (id, meta) in &context.table_meta_map {
                        check_meta(id, meta)?;
                    }
                }
                for (id, meta) in &context.window_meta_map {
                    check_meta(id, meta)?;
                }
                for (id, meta) in &context.aggregation_meta_map {
                    check_meta(id, meta)?;
                }
            }

            if let Some((position, attr_type)) = found {
                return Ok(Box::new(VariableExpressionExecutor::new(
                    position,
                    attr_type,
                    attribute_name.to_string(),
                )));
            }

            let stream_name = stream_id_opt.unwrap_or(&context.default_source);
            Err(ExpressionParseError::new(
                format!("Variable '{stream_name}.{attribute_name}' not found"),
                &api_var.eventflux_element,
                context.query_name,
            ))
        }
        ApiExpression::Add(api_op) => {
            let left_exec = parse_expression(&api_op.left_value, context)?;
            let right_exec = parse_expression(&api_op.right_value, context)?;
            Ok(Box::new(
                AddExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Subtract(api_op) => {
            let left_exec = parse_expression(&api_op.left_value, context)?;
            let right_exec = parse_expression(&api_op.right_value, context)?;
            Ok(Box::new(
                SubtractExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Multiply(api_op) => {
            let left_exec = parse_expression(&api_op.left_value, context)?;
            let right_exec = parse_expression(&api_op.right_value, context)?;
            Ok(Box::new(
                MultiplyExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Divide(api_op) => {
            let left_exec = parse_expression(&api_op.left_value, context)?;
            let right_exec = parse_expression(&api_op.right_value, context)?;
            Ok(Box::new(
                DivideExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Mod(api_op) => {
            let left_exec = parse_expression(&api_op.left_value, context)?;
            let right_exec = parse_expression(&api_op.right_value, context)?;
            Ok(Box::new(
                ModExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::And(api_op) => {
            let left_exec = parse_expression(&api_op.left_expression, context)?;
            let right_exec = parse_expression(&api_op.right_expression, context)?;
            Ok(Box::new(
                AndExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Or(api_op) => {
            let left_exec = parse_expression(&api_op.left_expression, context)?;
            let right_exec = parse_expression(&api_op.right_expression, context)?;
            Ok(Box::new(
                OrExpressionExecutor::new(left_exec, right_exec).map_err(|e| {
                    ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
                })?,
            ))
        }
        ApiExpression::Not(api_op) => {
            let exec = parse_expression(&api_op.expression, context)?;
            Ok(Box::new(NotExpressionExecutor::new(exec).map_err(|e| {
                ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name)
            })?))
        }
        ApiExpression::Compare(api_op) => {
            let left_exec = parse_expression(&api_op.left_expression, context)?;
            let right_exec = parse_expression(&api_op.right_expression, context)?;
            Ok(Box::new(
                CompareExpressionExecutor::new(left_exec, right_exec, api_op.operator).map_err(
                    |e| ExpressionParseError::new(e, &api_op.eventflux_element, context.query_name),
                )?,
            ))
        }
        ApiExpression::IsNull(api_op) => {
            if let Some(inner_expr) = &api_op.expression {
                let exec = parse_expression(inner_expr, context)?;
                Ok(Box::new(IsNullExpressionExecutor::new(exec)))
            } else {
                Err(ExpressionParseError::new(
                    "IsNull without an inner expression (IsNullStream) not yet fully supported here.".to_string(),
                    &api_op.eventflux_element,
                    context.query_name,
                ))
            }
        }
        ApiExpression::In(api_op) => {
            let val_exec = parse_expression(&api_op.expression, context)?;
            Ok(Box::new(InExpressionExecutor::new(
                val_exec,
                api_op.source_id.clone(),
                Arc::clone(&context.eventflux_app_context),
            )))
        }
        ApiExpression::Case(api_case) => {
            // Only Simple CASE has an operand
            let operand_exec = if let Some(ref operand) = api_case.operand {
                Some(Arc::from(parse_expression(operand, context)?))
            } else {
                None
            };

            let mut when_executors = Vec::new();
            for when_clause in &api_case.when_clauses {
                let condition_exec = Arc::from(parse_expression(&when_clause.condition, context)?);
                let result_exec = Arc::from(parse_expression(&when_clause.result, context)?);
                when_executors.push((condition_exec, result_exec));
            }

            let else_exec = Arc::from(parse_expression(&api_case.else_result, context)?);

            // The executor checks the CASE types and can fail here
            Ok(Box::new(
                CaseExpressionExecutor::new(api_case, operand_exec, when_executors, else_exec)
                    .map_err(|e| {
                        ExpressionParseError::new(
                            e,
                            &api_case.eventflux_element,
                            context.query_name,
                        )
                    })?,
            ))
        }
        ApiExpression::Cast(api_cast) => {
            let inner_exec = parse_expression(&api_cast.expression, context)?;
            Ok(Box::new(CastExecutor::new(
                inner_exec,
                api_cast.target_type,
            )))
        }
        ApiExpression::AttributeFunction(api_func) => {
            let mut arg_execs: Vec<Box<dyn ExpressionExecutor>> = Vec::new();
            for arg_expr in &api_func.parameters {
                arg_execs.push(parse_expression(arg_expr, context)?);
            }

            let function_lookup_name = if let Some(ns) = &api_func.extension_namespace {
                if ns.is_empty() {
                    api_func.function_name.clone()
                } else {
                    format!("{}:{}", ns, api_func.function_name)
                }
            } else {
                api_func.function_name.clone()
            };

            // Functions built here directly instead of through a registered factory. Note
            // that `event` and `allEvents` ignore their argument and always read position 0.
            match (
                api_func.extension_namespace.as_deref(),
                api_func.function_name.as_str(),
            ) {
                (None | Some(""), "event") => {
                    if arg_execs.len() == 1 {
                        Ok(Box::new(EventVariableFunctionExecutor::new(0, 0)))
                    } else {
                        Err(ExpressionParseError::new(
                            format!("event expects 1 argument, found {}", arg_execs.len()),
                            &api_func.eventflux_element,
                            context.query_name,
                        ))
                    }
                }
                (None | Some(""), "allEvents") => {
                    if arg_execs.len() == 1 {
                        Ok(Box::new(MultiValueVariableFunctionExecutor::new(0, [0, 0])))
                    } else {
                        Err(ExpressionParseError::new(
                            format!("allEvents expects 1 argument, found {}", arg_execs.len()),
                            &api_func.eventflux_element,
                            context.query_name,
                        ))
                    }
                }
                (None | Some(""), name) if name == "instanceOfBoolean" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfBooleanExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                (None | Some(""), name) if name == "instanceOfString" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfStringExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                (None | Some(""), name) if name == "instanceOfInteger" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfIntegerExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                (None | Some(""), name) if name == "instanceOfLong" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfLongExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                (None | Some(""), name) if name == "instanceOfFloat" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfFloatExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                (None | Some(""), name) if name == "instanceOfDouble" && arg_execs.len() == 1 => {
                    Ok(Box::new(
                        InstanceOfDoubleExpressionExecutor::new(arg_execs.remove(0)).map_err(
                            |e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            },
                        )?,
                    ))
                }
                // Every other function is looked up by name, in this order: attribute
                // aggregators, scalar functions, then script functions
                _ => {
                    if let Some(factory) = context
                        .eventflux_app_context
                        .get_eventflux_context()
                        .get_attribute_aggregator_factory(&function_lookup_name)
                    {
                        let mut exec = factory.create();
                        exec.init(
                            arg_execs,
                            ProcessingMode::BATCH,
                            false,
                            &context.eventflux_query_context,
                        )
                        .map_err(|e| {
                            ExpressionParseError::new(
                                e,
                                &api_func.eventflux_element,
                                context.query_name,
                            )
                        })?;
                        Ok(Box::new(AttributeAggregatorExpressionExecutor::new(exec)))
                    } else if let Some(scalar_fn_factory) = context
                        .eventflux_app_context
                        .get_eventflux_context()
                        .get_scalar_function_factory(&function_lookup_name)
                    {
                        Ok(Box::new(
                            AttributeFunctionExpressionExecutor::new(
                                scalar_fn_factory.clone_scalar_function(),
                                arg_execs,
                                Arc::clone(&context.eventflux_app_context),
                            )
                            .map_err(|e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            })?,
                        ))
                    } else if let Some(script_fn) = context
                        .eventflux_app_context
                        .get_script_function(&function_lookup_name)
                    {
                        Ok(Box::new(
                            AttributeFunctionExpressionExecutor::new(
                                Box::new(ScriptFunctionExecutor::new(
                                    function_lookup_name.clone(),
                                    script_fn.return_type,
                                )),
                                arg_execs,
                                Arc::clone(&context.eventflux_app_context),
                            )
                            .map_err(|e| {
                                ExpressionParseError::new(
                                    e,
                                    &api_func.eventflux_element,
                                    context.query_name,
                                )
                            })?,
                        ))
                    } else {
                        let scalars = context
                            .eventflux_app_context
                            .list_scalar_function_names()
                            .join(", ");
                        let aggs = context
                            .eventflux_app_context
                            .list_attribute_aggregator_names()
                            .join(", ");
                        Err(ExpressionParseError::new(
                            format!(
                                "Unsupported or unknown function: {function_lookup_name}. Known scalar functions: [{scalars}]. Known aggregators: [{aggs}]. If custom, register via EventFluxContext before parsing."
                            ),
                            &api_func.eventflux_element,
                            context.query_name,
                        ))
                    }
                }
            }
        }
        ApiExpression::IndexedVariable(indexed_var) => {
            let attribute_name = &indexed_var.attribute_name;
            let stream_id_opt = indexed_var
                .stream_id
                .as_deref()
                .or(Some(&context.default_source));

            // The source defaults to default_source and must have a pattern position
            let stream_id = stream_id_opt.ok_or_else(|| {
                ExpressionParseError::new(
                    "Indexed variable requires a stream identifier".to_string(),
                    &indexed_var.eventflux_element,
                    context.query_name,
                )
            })?;
            let state_pos_i32 = *context.stream_positions.get(stream_id).ok_or_else(|| {
                ExpressionParseError::new(
                    format!(
                        "Stream '{}' not found in pattern context. \
                         Indexed variable access (e.g., e1[0].price) is only valid in PATTERN/SEQUENCE queries.",
                        stream_id
                    ),
                    &indexed_var.eventflux_element,
                    context.query_name,
                )
            })?;
            let state_pos_usize = usize::try_from(state_pos_i32).map_err(|_| {
                ExpressionParseError::new(
                    format!("Invalid stream position for '{stream_id}'"),
                    &indexed_var.eventflux_element,
                    context.query_name,
                )
            })?;

            // Look up the attribute index and type. The first match wins.
            let mut found: Option<([i32; 2], ApiAttributeType)> = None;

            let mut check_meta = |meta: &MetaStreamEvent| -> Result<bool, ExpressionParseError> {
                if let Some((idx, t)) = meta.find_attribute_info(attribute_name) {
                    if found.is_none() {
                        let idx_i32 = i32::try_from(*idx).map_err(|_| {
                            ExpressionParseError::new(
                                format!("Attribute index {} exceeds supported range", idx),
                                &indexed_var.eventflux_element,
                                context.query_name,
                            )
                        })?;
                        found = Some((
                            [
                                crate::core::util::eventflux_constants::BEFORE_WINDOW_DATA_INDEX
                                    as i32,
                                idx_i32,
                            ],
                            *t,
                        ));
                    }
                    return Ok(true);
                }
                Ok(false)
            };

            if let Some(meta) = context.stream_meta_map.get(stream_id) {
                check_meta(meta)?;
            } else if let Some(meta) = context.table_meta_map.get(stream_id) {
                check_meta(meta)?;
            } else if let Some(meta) = context.window_meta_map.get(stream_id) {
                check_meta(meta)?;
            } else if let Some(meta) = context.aggregation_meta_map.get(stream_id) {
                check_meta(meta)?;
            } else if let Some(state_meta) = context.state_meta_map.get(stream_id) {
                for opt_meta in state_meta.meta_stream_events.iter().flatten() {
                    let set = check_meta(opt_meta)?;
                    if set {
                        break;
                    }
                }
            }

            if let Some((attr_position, attr_type)) = found {
                return Ok(Box::new(
                    crate::core::executor::IndexedVariableExecutor::new(
                        state_pos_usize,
                        indexed_var.index.clone(),
                        attr_position,
                        attr_type,
                        attribute_name.to_string(),
                    ),
                ));
            }

            Err(ExpressionParseError::new(
                format!("Indexed variable '{stream_id}.{attribute_name}' not found"),
                &indexed_var.eventflux_element,
                context.query_name,
            ))
        }
    }
}

fn convert_api_constant_to_core_attribute_value(
    api_val: &ApiConstantValue,
) -> (CoreAttributeValue, ApiAttributeType) {
    // A Time constant becomes a LONG and NULL is typed as OBJECT
    match api_val {
        ApiConstantValue::String(s) => (
            CoreAttributeValue::String(s.clone()),
            ApiAttributeType::STRING,
        ),
        ApiConstantValue::Int(i) => (CoreAttributeValue::Int(*i), ApiAttributeType::INT),
        ApiConstantValue::Long(l) => (CoreAttributeValue::Long(*l), ApiAttributeType::LONG),
        ApiConstantValue::Float(f) => (CoreAttributeValue::Float(*f), ApiAttributeType::FLOAT),
        ApiConstantValue::Double(d) => (CoreAttributeValue::Double(*d), ApiAttributeType::DOUBLE),
        ApiConstantValue::Bool(b) => (CoreAttributeValue::Bool(*b), ApiAttributeType::BOOL),
        ApiConstantValue::Time(t) => (CoreAttributeValue::Long(*t), ApiAttributeType::LONG),
        ApiConstantValue::Null => (CoreAttributeValue::Null, ApiAttributeType::OBJECT),
    }
}
