// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/parser/expression_parser.rs

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::config::eventflux_query_context::EventFluxQueryContext;
use crate::core::event::complex_event::ComplexEvent; // Added this import
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
    definition::attribute::Type as ApiAttributeType, // Import Type enum
    expression::{
        constant::ConstantValueWithFloat as ApiConstantValue, Expression as ApiExpression,
    },
};

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

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

// Wrapper executor for calling ScalarFunctionExecutor (UDFs and complex built-ins)
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
        // ComplexEvent should now be in scope
        // This simplified model assumes that the ScalarFunctionExecutor's `execute` method,
        // inherited from ExpressionExecutor, will correctly use its initialized state
        // (which might include information about its arguments derived from their executors during `init`)
        // to compute its value based on the incoming event.
        // A more complex model might involve this AttributeFunctionExpressionExecutor first
        // executing its `argument_executors` to get `AttributeValue`s and then passing those
        // values to a different method on `ScalarFunctionExecutor`, e.g., `eval(args: Vec<AttributeValue>)`.
        // For now, we stick to the ExpressionExecutor::execute signature for the UDF.
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
        // Re-initialize the cloned scalar function
        match AttributeFunctionExpressionExecutor::new(
            cloned_scalar_fn,
            cloned_args,
            Arc::clone(eventflux_app_context),
        ) {
            Ok(exec) => Box::new(exec),
            Err(e) => {
                // If cloning fails log the error and fall back to a constant NULL executor
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

// Simplified context for initial ExpressionParser focusing on single input stream scenarios.
/// Context for `ExpressionParser`, providing necessary metadata for variable
/// resolution.
///
/// The `stream_meta_map` contains `MetaStreamEvent` instances keyed by the
/// stream/table/window/aggregation identifier.  `default_source` indicates which
/// entry should be used when a variable does not explicitly specify a source.
pub struct ExpressionParserContext<'a> {
    pub eventflux_app_context: Arc<EventFluxAppContext>,
    pub eventflux_query_context: Arc<EventFluxQueryContext>,
    pub stream_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub table_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub window_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub aggregation_meta_map: HashMap<String, Arc<MetaStreamEvent>>,
    pub state_meta_map: HashMap<String, Arc<MetaStateEvent>>,
    /// Map of source id (stream/table) to its position within a StateEvent chain.
    /// Single stream queries map their input id to position 0 while joins and
    /// patterns map each participating id to the appropriate index.
    pub stream_positions: HashMap<String, i32>,
    pub default_source: String,
    pub query_name: &'a str,
    /// When true, indicates this is a mutation context (UPDATE/DELETE/UPSERT)
    /// where unqualified columns should resolve to the target table without
    /// raising ambiguity errors even if the column exists in the source stream.
    pub is_mutation_context: bool,
}

/// Parses query_api::Expression into core::ExpressionExecutor instances.
/// Current limitations: Variable resolution is simplified for single input streams.
/// Does not handle full complexity of all expression types or contexts (e.g., aggregations within HAVING).
pub fn parse_expression<'a>(
    api_expr: &ApiExpression,
    context: &ExpressionParserContext<'a>,
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

            // Helper closure to search a meta and record result
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
                // For unqualified columns, check default_source first, but still verify
                // the column isn't ambiguous (exists in multiple sources)
                let default_id = &context.default_source;
                let mut found_in_default = false;
                let mut also_found_elsewhere = false;

                // First pass: check if column exists in default_source
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

                // Second pass: check if column also exists in OTHER sources (ambiguity check)
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

                // If found in default_source AND elsewhere, raise ambiguity error
                // (unless this is a mutation context where table is intentionally the default)
                if found_in_default && also_found_elsewhere {
                    // In mutation contexts (UPDATE/DELETE/UPSERT), default_source is the target table.
                    // Unqualified columns should resolve to the table without ambiguity error.
                    if !context.is_mutation_context {
                        return Err(ExpressionParseError::new(
                            format!("Attribute '{attribute_name}' found in multiple sources"),
                            &api_var.eventflux_element,
                            context.query_name,
                        ));
                    }
                    // Log warning for debugging: ambiguity silently resolved to table column
                    // This helps users identify when they might have intended the stream column
                    log::debug!(
                        "Mutation context: ambiguous column '{}' resolved to table '{}'. \
                         Use explicit qualifier (e.g., '{}.{}') if stream column was intended.",
                        attribute_name,
                        default_id,
                        default_id,
                        attribute_name
                    );
                }

                // Now actually resolve the column
                if found_in_default {
                    // Bind to default_source
                    if let Some(meta) = context.stream_meta_map.get(default_id) {
                        check_meta(default_id, meta)?;
                    } else if let Some(meta) = context.table_meta_map.get(default_id) {
                        check_meta(default_id, meta)?;
                    }
                } else {
                    // Not in default_source, search all sources (will error if ambiguous)
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
            // Parse operand (for Simple CASE)
            let operand_exec = if let Some(ref operand) = api_case.operand {
                Some(Arc::from(parse_expression(operand, context)?))
            } else {
                None
            };

            // Parse all WHEN clauses
            let mut when_executors = Vec::new();
            for when_clause in &api_case.when_clauses {
                let condition_exec = Arc::from(parse_expression(&when_clause.condition, context)?);
                let result_exec = Arc::from(parse_expression(&when_clause.result, context)?);
                when_executors.push((condition_exec, result_exec));
            }

            // Parse ELSE expression
            let else_exec = Arc::from(parse_expression(&api_case.else_result, context)?);

            // Create CaseExpressionExecutor
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

            // Handle special variable functions not implemented via factories
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
                // All other functions (aggregators, scalar functions, scripts) are looked up from registry
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

            // Resolve stream position
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

            // Find attribute meta to get position and type
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
    // Changed to ApiAttributeType
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
