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

//! SQL to Query Converter
//!
//! Converts SQL statements to EventFlux query_api::Query structures.

use sqlparser::ast::{
    AccessExpr, BinaryOperator, Expr as SqlExpr, JoinConstraint, JoinOperator, OutputRateLimit,
    OutputRateLimitMode, OutputRateLimitUnit, PartitionKey, PatternExpression, PatternLogicalOp,
    PatternMode, Select as SqlSelect, SetExpr, Statement, Subscript, TableFactor, UnaryOperator,
    WithinConstraint,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::core::query::processor::stream::window::types::{
    WINDOW_TYPE_EXTERNAL_TIME, WINDOW_TYPE_EXTERNAL_TIME_BATCH, WINDOW_TYPE_LENGTH,
    WINDOW_TYPE_LENGTH_BATCH, WINDOW_TYPE_SESSION, WINDOW_TYPE_SORT, WINDOW_TYPE_TIME,
    WINDOW_TYPE_TIME_BATCH,
};
use crate::query_api::execution::partition::Partition;
use crate::query_api::execution::query::input::state::{
    AbsentStreamStateElement, CountStateElement, EveryStateElement, LogicalStateElement,
    LogicalStateElementType, NextStateElement, StateElement, StreamStateElement,
};
use crate::query_api::execution::query::input::stream::input_stream::InputStream;
use crate::query_api::execution::query::input::stream::single_input_stream::SingleInputStream;
use crate::query_api::execution::query::input::stream::state_input_stream::StateInputStream;
use crate::query_api::execution::query::output::output_stream::{
    InsertIntoStreamAction, OutputStream, OutputStreamAction,
};
use crate::query_api::execution::query::output::ratelimit::{
    EventsOutputRate, OutputRate, OutputRateBehavior, OutputRateVariant, SnapshotOutputRate,
    TimeOutputRate,
};
use crate::query_api::execution::query::Query;
use crate::query_api::expression::indexed_variable::{EventIndex, IndexedVariable};
use crate::query_api::expression::variable::Variable;
use crate::query_api::expression::CompareOperator;
use crate::query_api::expression::{Expression, WhenClause};

use super::catalog::SqlCatalog;
use super::error::ConverterError;
use super::expansion::SelectExpander;
use super::pattern_validation::PatternValidator;
use super::type_inference::TypeInferenceEngine;

/// SQL to Query Converter
pub struct SqlConverter;

impl SqlConverter {
    /// Convert SQL string to Query (legacy API - parses then converts)
    pub fn convert(sql: &str, catalog: &SqlCatalog) -> Result<Query, ConverterError> {
        // Parse SQL directly (WINDOW clause now handled natively by parser)
        let statements = Parser::parse_sql(&GenericDialect, sql)
            .map_err(|e| ConverterError::ConversionFailed(format!("SQL parse error: {}", e)))?;

        if statements.is_empty() {
            return Err(ConverterError::ConversionFailed(
                "No SQL statements found".to_string(),
            ));
        }

        // Convert SELECT or INSERT INTO statement to Query
        match &statements[0] {
            Statement::Query(query) => Self::convert_query_ast(query, catalog, None),
            Statement::Insert(insert) => {
                let target_stream = match &insert.table {
                    sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                    sqlparser::ast::TableObject::TableFunction(_) => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Table functions not supported in INSERT".to_string(),
                        ))
                    }
                };

                let source = insert.source.as_ref().ok_or_else(|| {
                    ConverterError::UnsupportedFeature(
                        "INSERT without SELECT source not supported".to_string(),
                    )
                })?;

                Self::convert_query_ast(source, catalog, Some(target_stream))
            }
            _ => Err(ConverterError::UnsupportedFeature(
                "Only SELECT and INSERT INTO queries are supported".to_string(),
            )),
        }
    }

    /// Convert parsed Query AST directly to Query (no re-parsing!)
    ///
    /// This is the preferred method when you already have a parsed AST.
    /// It avoids the overhead of serializing and re-parsing the AST.
    pub fn convert_query_ast(
        query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_stream_name: Option<String>,
    ) -> Result<Query, ConverterError> {
        Self::convert_query_internal(query, catalog, output_stream_name)
    }

    /// Convert a mutation query (UPDATE or DELETE from stream)
    ///
    /// Creates a simple passthrough query from the source stream with the given output action.
    /// The source stream events trigger the mutation operation on the target table.
    pub fn convert_mutation_query(
        source_stream_name: &str,
        catalog: &SqlCatalog,
        output_action: OutputStreamAction,
    ) -> Result<Query, ConverterError> {
        // Validate source stream exists
        catalog
            .get_relation(source_stream_name)
            .map_err(|_| ConverterError::SchemaNotFound(source_stream_name.to_string()))?;

        // Create input stream from source
        let single_stream = SingleInputStream::new_basic(
            source_stream_name.to_string(),
            false, // is_inner_stream
            false, // is_fault_stream
            None,  // stream_handler_id
            Vec::new(),
        );

        // Get all attributes from source stream for SELECT *
        let relation = catalog.get_relation(source_stream_name).unwrap();
        let attributes = relation.abstract_definition().get_attribute_list();

        // Build selector with all attributes from source stream
        let mut selector = crate::query_api::execution::query::selection::Selector::new();
        for attr in attributes {
            selector = selector.select_variable(Variable::new(attr.get_name().clone()));
        }

        // Create output stream with the mutation action
        let output_stream = OutputStream::new(output_action, None);

        // Build Query
        Ok(Query::query()
            .from(InputStream::Single(single_stream))
            .select(selector)
            .out_stream(output_stream))
    }

    /// Convert an UPSERT query (SELECT ... FROM stream with UpdateOrInsert output)
    ///
    /// Converts a full SELECT query and replaces its output action with the upsert action.
    /// Note: UPSERT currently only supports simple stream sources (no JOINs or WINDOWs)
    /// because the runtime processor requires StreamEvent input.
    pub fn convert_upsert_query(
        source_query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_action: OutputStreamAction,
    ) -> Result<Query, ConverterError> {
        // Validate: UPSERT only supports simple stream sources (no JOINs or WINDOWs)
        // because UpsertTableProcessor requires StreamEvent input
        if let SetExpr::Select(select) = source_query.body.as_ref() {
            // Check for JOINs (multiple FROM tables or explicit JOIN clauses)
            if select.from.len() > 1 {
                return Err(ConverterError::UnsupportedFeature(
                    "UPSERT with multiple source streams not supported".to_string(),
                ));
            }
            if let Some(from) = select.from.first() {
                if !from.joins.is_empty() {
                    return Err(ConverterError::UnsupportedFeature(
                        "UPSERT with JOIN source not supported".to_string(),
                    ));
                }
                // Check for streaming WINDOW on the table factor
                if let TableFactor::Table {
                    window: Some(_), ..
                } = &from.relation
                {
                    return Err(ConverterError::UnsupportedFeature(
                        "UPSERT with WINDOW source not supported".to_string(),
                    ));
                }
            }

            // Schema validation: Check SELECT column count matches target table
            // This is only validated when there's no SET clause (full row replacement mode)
            if let OutputStreamAction::UpdateOrInsert(ref upsert_action) = output_action {
                // Only validate when no SET clause - with SET, specific columns are updated
                if upsert_action.update_set_clause.is_none() {
                    let target_table = &upsert_action.target_id;
                    if let Ok(relation) = catalog.get_relation(target_table) {
                        let table_col_count = relation.abstract_definition().attribute_list.len();
                        let select_col_count = select.projection.len();

                        // Count only non-wildcard columns for accurate comparison
                        // Wildcards (SELECT *) are expanded later, so we can't validate them here
                        let has_wildcard = select.projection.iter().any(|p| {
                            matches!(
                                p,
                                sqlparser::ast::SelectItem::Wildcard(_)
                                    | sqlparser::ast::SelectItem::QualifiedWildcard(_, _)
                            )
                        });

                        if !has_wildcard && select_col_count != table_col_count {
                            return Err(ConverterError::SchemaMismatch(format!(
                                "UPSERT column count mismatch: SELECT has {} columns, \
                                 but table '{}' has {} columns. \
                                 Use explicit column list or SET clause for partial updates.",
                                select_col_count, target_table, table_col_count
                            )));
                        }
                    }
                }
            }
        }

        // First convert the source SELECT query normally
        let mut query = Self::convert_query_internal(source_query, catalog, None)?;

        // Replace the output action with the upsert action
        let output_stream = OutputStream::new(output_action, None);
        query = query.out_stream(output_stream);

        Ok(query)
    }

    /// Convert PARTITION statement to Partition execution element
    pub fn convert_partition(
        partition_keys: &[PartitionKey],
        body: &[Statement],
        catalog: &SqlCatalog,
    ) -> Result<Partition, ConverterError> {
        let mut partition = Partition::new();

        // Validate and convert partition keys
        for key in partition_keys {
            let stream_name = key.stream_name.to_string();
            let attribute_name = key.attribute.value.clone();

            // Validate relation (stream or table) exists
            catalog
                .get_relation(&stream_name)
                .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

            // Validate attribute exists in the relation
            if !catalog.has_column(&stream_name, &attribute_name) {
                return Err(ConverterError::InvalidExpression(format!(
                    "Attribute '{}' not found in relation '{}'",
                    attribute_name, stream_name
                )));
            }

            // Create partition expression
            let partition_expr = Expression::Variable(Variable::new(attribute_name.clone()));
            partition = partition.with_value_partition(stream_name, partition_expr);
        }

        // Convert body statements to queries
        for stmt in body {
            match stmt {
                Statement::Query(query) => {
                    let q = Self::convert_query_ast(query, catalog, None)?;
                    partition = partition.add_query(q);
                }
                Statement::Insert(insert) => {
                    let target_stream = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                        sqlparser::ast::TableObject::TableFunction(_) => {
                            return Err(ConverterError::UnsupportedFeature(
                                "Table functions not supported in INSERT".to_string(),
                            ))
                        }
                    };

                    let source = insert.source.as_ref().ok_or_else(|| {
                        ConverterError::UnsupportedFeature(
                            "INSERT without SELECT source not supported".to_string(),
                        )
                    })?;

                    let q = Self::convert_query_ast(source, catalog, Some(target_stream))?;
                    partition = partition.add_query(q);
                }
                _ => {
                    return Err(ConverterError::UnsupportedFeature(
                        "Only SELECT and INSERT INTO statements supported inside PARTITION"
                            .to_string(),
                    ))
                }
            }
        }

        Ok(partition)
    }

    /// Internal method to convert sqlparser Query AST to EventFlux Query
    fn convert_query_internal(
        sql_query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_stream_name: Option<String>,
    ) -> Result<Query, ConverterError> {
        // Extract limit and offset from limit_clause
        let (limit, offset) = match &sql_query.limit_clause {
            Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                (limit.as_ref(), offset.as_ref())
            }
            Some(sqlparser::ast::LimitClause::OffsetCommaLimit { .. }) => {
                return Err(ConverterError::UnsupportedFeature(
                    "MySQL-style LIMIT offset,limit syntax not supported".to_string(),
                ))
            }
            None => (None, None),
        };

        // Convert OUTPUT rate limiting clause if present
        let output_rate = sql_query
            .output_rate_limit
            .as_ref()
            .map(Self::convert_output_rate_limit)
            .transpose()?;

        match sql_query.body.as_ref() {
            SetExpr::Select(select) => Self::convert_select(
                select,
                catalog,
                sql_query.order_by.as_ref(),
                limit,
                offset,
                output_stream_name,
                output_rate,
            ),
            _ => Err(ConverterError::UnsupportedFeature(
                "Only simple SELECT supported".to_string(),
            )),
        }
    }

    /// Convert sqlparser's OutputRateLimit to query_api's OutputRate
    fn convert_output_rate_limit(
        rate_limit: &OutputRateLimit,
    ) -> Result<OutputRate, ConverterError> {
        // Validate rate limit value is positive
        if rate_limit.value == 0 {
            return Err(ConverterError::ConversionFailed(
                "Rate limit value must be greater than 0".to_string(),
            ));
        }

        let variant = match rate_limit.unit {
            OutputRateLimitUnit::Events => {
                // SNAPSHOT mode is not valid with EVENTS - parser should catch this,
                // but add defensive check for safety
                if matches!(rate_limit.mode, OutputRateLimitMode::Snapshot) {
                    return Err(ConverterError::UnsupportedFeature(
                        "SNAPSHOT mode is not supported with EVENTS - use time units instead"
                            .to_string(),
                    ));
                }

                // Convert mode to behavior for event-based rate limiting
                let behavior = match rate_limit.mode {
                    OutputRateLimitMode::All => OutputRateBehavior::All,
                    OutputRateLimitMode::First => OutputRateBehavior::First,
                    OutputRateLimitMode::Last => OutputRateBehavior::Last,
                    OutputRateLimitMode::Snapshot => unreachable!("Handled above"),
                };

                // Event-based rate limiting - validate value fits in i32.
                // Note: The API uses i32 for historical reasons; runtime converts to usize.
                // Future improvement: change API type to usize for more idiomatic Rust.
                let event_count: i32 = rate_limit.value.try_into().map_err(|_| {
                    ConverterError::ConversionFailed(format!(
                        "Event count {} exceeds maximum value {}",
                        rate_limit.value,
                        i32::MAX
                    ))
                })?;
                OutputRateVariant::Events(EventsOutputRate::new(event_count), behavior)
            }
            OutputRateLimitUnit::Milliseconds
            | OutputRateLimitUnit::Seconds
            | OutputRateLimitUnit::Minutes
            | OutputRateLimitUnit::Hours => {
                // Convert to milliseconds - may fail on overflow
                let millis_u64 = rate_limit.unit.to_millis(rate_limit.value).ok_or_else(|| {
                    ConverterError::ConversionFailed(
                        "Time value overflowed during conversion to milliseconds".to_string(),
                    )
                })?;

                // Safely convert to i64 to prevent overflow.
                // Note: The API uses i64 for historical reasons; u64 would be more idiomatic
                // for time durations. Future improvement: change API type to u64.
                let millis: i64 = millis_u64.try_into().map_err(|_| {
                    ConverterError::ConversionFailed(format!(
                        "Time value {} milliseconds exceeds maximum value {}",
                        millis_u64,
                        i64::MAX
                    ))
                })?;

                // Convert mode to appropriate variant
                match rate_limit.mode {
                    OutputRateLimitMode::Snapshot => {
                        OutputRateVariant::Snapshot(SnapshotOutputRate::new(millis))
                    }
                    OutputRateLimitMode::All => OutputRateVariant::Time(
                        TimeOutputRate::new(millis),
                        OutputRateBehavior::All,
                    ),
                    OutputRateLimitMode::First => OutputRateVariant::Time(
                        TimeOutputRate::new(millis),
                        OutputRateBehavior::First,
                    ),
                    OutputRateLimitMode::Last => OutputRateVariant::Time(
                        TimeOutputRate::new(millis),
                        OutputRateBehavior::Last,
                    ),
                }
            }
        };

        Ok(OutputRate::new(variant))
    }

    /// Convert SELECT statement to Query
    fn convert_select(
        select: &SqlSelect,
        catalog: &SqlCatalog,
        order_by: Option<&sqlparser::ast::OrderBy>,
        limit: Option<&SqlExpr>,
        offset: Option<&sqlparser::ast::Offset>,
        output_stream_name: Option<String>,
        output_rate: Option<OutputRate>,
    ) -> Result<Query, ConverterError> {
        // Check if this is a JOIN query
        let has_join = !select.from.is_empty() && !select.from[0].joins.is_empty();

        // Track pattern aliases for catalog augmentation
        let mut pattern_aliases: Vec<(String, String)> = Vec::new();

        let (input_stream, stream_name_for_selector) = if has_join {
            // Handle JOIN
            let join_input =
                Self::convert_join_from_clause(&select.from, &select.selection, catalog)?;
            (join_input, String::new())
        } else {
            // Handle single relation in FROM
            let first = select.from.first().ok_or_else(|| {
                ConverterError::ConversionFailed("No FROM clause found".to_string())
            })?;

            match &first.relation {
                TableFactor::Table { name, window, .. } => {
                    let stream_name = name
                        .0
                        .last()
                        .and_then(|part| part.as_ident())
                        .map(|ident| ident.value.clone())
                        .ok_or_else(|| {
                            ConverterError::ConversionFailed("No table name in FROM".to_string())
                        })?;

                    // Validate relation (stream or table) exists
                    let relation = catalog
                        .get_relation(&stream_name)
                        .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

                    // Validation 1.7: Tables cannot be queried directly without JOIN
                    // Tables are lookup structures that must be joined with a stream
                    if relation.is_table() {
                        return Err(ConverterError::DirectTableQuery(stream_name));
                    }

                    // Create InputStream (works for both streams and tables - runtime will differentiate)
                    let mut single_stream = SingleInputStream::new_basic(
                        stream_name.clone(),
                        false,      // is_inner_stream
                        false,      // is_fault_stream
                        None,       // stream_handler_id
                        Vec::new(), // pre_window_handlers
                    );

                    // Add WINDOW if present from AST
                    if let Some(window_ast) = window.as_ref() {
                        single_stream =
                            Self::add_window_from_ast(single_stream, window_ast, catalog)?;
                    }

                    // Add WHERE filter (BEFORE aggregation)
                    if let Some(where_expr) = &select.selection {
                        let filter_expr = Self::convert_expression(where_expr, catalog)?;
                        single_stream = single_stream.filter(filter_expr);
                    }

                    (InputStream::Single(single_stream), stream_name)
                }
                TableFactor::Pattern {
                    mode,
                    pattern,
                    within,
                    ..
                } => {
                    // Extract pattern aliases for catalog augmentation
                    pattern_aliases = Self::extract_pattern_aliases(pattern);

                    // Convert pattern/sequence to StateInputStream
                    let state_input_stream =
                        Self::convert_pattern_input(mode, pattern, within, catalog)?;
                    (state_input_stream, String::new())
                }
                _ => {
                    return Err(ConverterError::UnsupportedFeature(
                        "Complex FROM clauses not supported".to_string(),
                    ))
                }
            }
        };

        // For pattern queries, create a catalog with pattern aliases registered
        let effective_catalog: std::borrow::Cow<'_, SqlCatalog> = if !pattern_aliases.is_empty() {
            let mut catalog_with_aliases = catalog.clone();
            for (alias, stream_name) in &pattern_aliases {
                catalog_with_aliases.register_alias(alias.clone(), stream_name.clone());
            }
            std::borrow::Cow::Owned(catalog_with_aliases)
        } else {
            std::borrow::Cow::Borrowed(catalog)
        };

        let mut selector = SelectExpander::expand_select_items(
            &select.projection,
            &stream_name_for_selector,
            &effective_catalog,
            &pattern_aliases,
        )
        .map_err(|e| ConverterError::ConversionFailed(e.to_string()))?;

        // Add GROUP BY if present
        if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, modifiers) = &select.group_by {
            if !modifiers.is_empty() {
                return Err(ConverterError::UnsupportedFeature(
                    "GROUP BY modifiers (ROLLUP, CUBE, etc.) not supported".to_string(),
                ));
            }

            for expr in group_exprs {
                if let SqlExpr::Identifier(ident) = expr {
                    selector = selector.group_by(Variable::new(ident.value.clone()));
                } else {
                    return Err(ConverterError::UnsupportedFeature(
                        "Complex GROUP BY expressions not supported".to_string(),
                    ));
                }
            }
        }

        // Add HAVING (AFTER aggregation)
        if let Some(having) = &select.having {
            let having_expr = Self::convert_expression(having, catalog)?;
            selector = selector.having(having_expr);
        }

        // Add ORDER BY
        if let Some(order_by) = order_by {
            // Extract expressions from OrderBy
            let order_exprs = match &order_by.kind {
                sqlparser::ast::OrderByKind::Expressions(exprs) => exprs,
                sqlparser::ast::OrderByKind::All(_) => {
                    return Err(ConverterError::UnsupportedFeature(
                        "ORDER BY ALL not supported".to_string(),
                    ))
                }
            };

            for order_expr in order_exprs {
                // Extract variable from order_expr.expr
                let variable = match &order_expr.expr {
                    SqlExpr::Identifier(ident) => Variable::new(ident.value.clone()),
                    SqlExpr::CompoundIdentifier(idents) => {
                        if idents.len() == 1 {
                            Variable::new(idents[0].value.clone())
                        } else {
                            return Err(ConverterError::UnsupportedFeature(
                                "Qualified column names in ORDER BY not supported".to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Complex expressions in ORDER BY not supported".to_string(),
                        ))
                    }
                };

                // Determine order (ASC/DESC)
                let order = if let Some(asc) = order_expr.options.asc {
                    if asc {
                        crate::query_api::execution::query::selection::order_by_attribute::Order::Asc
                    } else {
                        crate::query_api::execution::query::selection::order_by_attribute::Order::Desc
                    }
                } else {
                    // Default to ASC if not specified
                    crate::query_api::execution::query::selection::order_by_attribute::Order::Asc
                };

                selector = selector.order_by_with_order(variable, order);
            }
        }

        // Add LIMIT
        if let Some(limit_expr) = limit {
            let limit_const = Self::convert_to_constant(limit_expr)?;
            selector = selector
                .limit(limit_const)
                .map_err(|e| ConverterError::ConversionFailed(format!("LIMIT error: {}", e)))?;
        }

        // Add OFFSET
        if let Some(offset_obj) = offset {
            let offset_const = Self::convert_to_constant(&offset_obj.value)?;
            selector = selector
                .offset(offset_const)
                .map_err(|e| ConverterError::ConversionFailed(format!("OFFSET error: {}", e)))?;
        }

        // Create output stream (use provided name or default to "OutputStream")
        let target_stream_name = output_stream_name.unwrap_or_else(|| "OutputStream".to_string());
        let output_action = InsertIntoStreamAction {
            target_id: target_stream_name,
            is_inner_stream: false,
            is_fault_stream: false,
        };
        let output_stream = OutputStream::new(OutputStreamAction::InsertInto(output_action), None);

        // Build Query
        let mut query = Query::query()
            .from(input_stream)
            .select(selector)
            .out_stream(output_stream);

        // Add OUTPUT rate limiting if present
        if let Some(rate) = output_rate {
            query = query.output(rate);
        }

        // Validate query for type correctness (no allocation, uses catalog reference)
        let type_engine = TypeInferenceEngine::new(catalog);
        type_engine.validate_query(&query).map_err(|e| {
            ConverterError::ConversionFailed(format!("Type validation failed: {}", e))
        })?;

        Ok(query)
    }

    /// Extract stream name from FROM clause
    #[allow(dead_code)]
    fn extract_from_stream(
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<String, ConverterError> {
        if from.is_empty() {
            return Err(ConverterError::ConversionFailed(
                "No FROM clause found".to_string(),
            ));
        }

        match &from[0].relation {
            TableFactor::Table { name, .. } => name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.clone())
                .ok_or_else(|| {
                    ConverterError::ConversionFailed("No table name in FROM".to_string())
                }),
            TableFactor::Pattern { mode, .. } => {
                // Return the alias or generated name; actual conversion happens in select parsing
                // We return a synthesized name to satisfy callers that expect a string.
                let name = format!("{:?}_pattern", mode);
                Ok(name)
            }
            _ => Err(ConverterError::UnsupportedFeature(
                "Complex FROM clauses not supported".to_string(),
            )),
        }
    }

    /// Extract window specification from TableFactor (native AST field)
    #[allow(dead_code)]
    fn extract_window_from_table_factor(
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Option<&sqlparser::ast::StreamingWindowSpec> {
        if from.is_empty() {
            return None;
        }

        match &from[0].relation {
            TableFactor::Table { window, .. } => window.as_ref(),
            _ => None,
        }
    }

    /// Convert JOIN from clause to JoinInputStream
    fn convert_join_from_clause(
        from: &[sqlparser::ast::TableWithJoins],
        _where_clause: &Option<SqlExpr>, // Reserved for future filter optimization
        catalog: &SqlCatalog,
    ) -> Result<InputStream, ConverterError> {
        use crate::query_api::execution::query::input::stream::join_input_stream::{
            EventTrigger, JoinInputStream, Type as JoinType,
        };

        if from.is_empty() || from[0].joins.is_empty() {
            return Err(ConverterError::ConversionFailed(
                "No JOIN found in FROM clause".to_string(),
            ));
        }

        // Extract left stream
        let left_stream_name = match &from[0].relation {
            TableFactor::Table { name, alias, .. } => {
                let stream_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        ConverterError::ConversionFailed("No left table name".to_string())
                    })?;

                // Validate relation (stream or table) exists for JOIN
                let _ = catalog
                    .get_relation(&stream_name)
                    .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

                let mut left_stream = SingleInputStream::new_basic(
                    stream_name.clone(),
                    false,
                    false,
                    None,
                    Vec::new(),
                );

                // Add alias if present
                if let Some(table_alias) = alias {
                    left_stream = left_stream.as_ref(table_alias.name.value.clone());
                }

                left_stream
            }
            TableFactor::Pattern { .. } => {
                return Err(ConverterError::UnsupportedFeature(
                    "JOIN against PATTERN/SEQUENCE inputs is not yet supported".to_string(),
                ))
            }
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Complex left table in JOIN".to_string(),
                ))
            }
        };

        // Get first JOIN (only support single JOIN currently)
        let join = &from[0].joins[0];

        // Extract right stream
        let right_stream_name = match &join.relation {
            TableFactor::Table { name, alias, .. } => {
                let stream_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        ConverterError::ConversionFailed("No right table name".to_string())
                    })?;

                // Validate relation (stream or table) exists for JOIN
                let _ = catalog
                    .get_relation(&stream_name)
                    .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

                let mut right_stream = SingleInputStream::new_basic(
                    stream_name.clone(),
                    false,
                    false,
                    None,
                    Vec::new(),
                );

                // Add alias if present
                if let Some(table_alias) = alias {
                    right_stream = right_stream.as_ref(table_alias.name.value.clone());
                }

                right_stream
            }
            TableFactor::Pattern { .. } => {
                return Err(ConverterError::UnsupportedFeature(
                    "JOIN against PATTERN/SEQUENCE inputs is not yet supported".to_string(),
                ))
            }
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Complex right table in JOIN".to_string(),
                ))
            }
        };

        // Register join aliases in catalog for ON condition expression parsing
        // This allows expressions like "a.symbol = b.company" where a and b are aliases
        let effective_catalog: std::borrow::Cow<'_, SqlCatalog> = {
            let left_alias = left_stream_name.get_stream_reference_id_str();
            let right_alias = right_stream_name.get_stream_reference_id_str();

            if left_alias.is_some() || right_alias.is_some() {
                let mut catalog_with_aliases = catalog.clone();
                if let Some(alias) = left_alias {
                    let stream_id = left_stream_name.get_stream_id_str();
                    catalog_with_aliases.register_alias(alias.to_string(), stream_id.to_string());
                }
                if let Some(alias) = right_alias {
                    let stream_id = right_stream_name.get_stream_id_str();
                    catalog_with_aliases.register_alias(alias.to_string(), stream_id.to_string());
                }
                std::borrow::Cow::Owned(catalog_with_aliases)
            } else {
                std::borrow::Cow::Borrowed(catalog)
            }
        };

        // Extract join type and ON condition together
        // Note: In SQL, plain JOIN and INNER JOIN are identical (ANSI standard),
        // so we normalize them to InnerJoin for consistency
        let (join_type, on_condition) = match &join.join_operator {
            // INNER JOIN variants (normalize plain JOIN to INNER JOIN)
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                let cond = Self::extract_on_condition(constraint, &effective_catalog)?;
                (JoinType::InnerJoin, cond)
            }
            // OUTER JOIN variants
            JoinOperator::LeftOuter(constraint) => {
                let cond = Self::extract_on_condition(constraint, &effective_catalog)?;
                (JoinType::LeftOuterJoin, cond)
            }
            JoinOperator::RightOuter(constraint) => {
                let cond = Self::extract_on_condition(constraint, &effective_catalog)?;
                (JoinType::RightOuterJoin, cond)
            }
            JoinOperator::FullOuter(constraint) => {
                let cond = Self::extract_on_condition(constraint, &effective_catalog)?;
                (JoinType::FullOuterJoin, cond)
            }
            _ => {
                return Err(ConverterError::UnsupportedFeature(format!(
                    "Unsupported join operator: {:?}",
                    join.join_operator
                )))
            }
        };

        // Create JoinInputStream
        let join_stream = JoinInputStream::new(
            left_stream_name,
            join_type,
            right_stream_name,
            on_condition,
            EventTrigger::All, // Default trigger
            None,              // No WITHIN clause currently
            None,              // No PER clause currently
        );

        Ok(InputStream::Join(Box::new(join_stream)))
    }

    /// Extract ON condition from JoinConstraint
    /// Currently only supports ON clause; USING and NATURAL joins are not yet implemented
    fn extract_on_condition(
        constraint: &JoinConstraint,
        catalog: &SqlCatalog,
    ) -> Result<Option<Expression>, ConverterError> {
        match constraint {
            JoinConstraint::On(expr) => Ok(Some(Self::convert_expression(expr, catalog)?)),
            JoinConstraint::Using(_) => Err(ConverterError::UnsupportedFeature(
                "JOIN USING clause not yet supported. Use ON clause instead.".to_string(),
            )),
            JoinConstraint::Natural => Err(ConverterError::UnsupportedFeature(
                "NATURAL JOIN not yet supported. Use explicit ON clause.".to_string(),
            )),
            JoinConstraint::None => Ok(None),
        }
    }

    /// Add window to SingleInputStream
    /// Add window from native AST StreamingWindowSpec
    fn add_window_from_ast(
        stream: SingleInputStream,
        window: &sqlparser::ast::StreamingWindowSpec,
        catalog: &SqlCatalog,
    ) -> Result<SingleInputStream, ConverterError> {
        use sqlparser::ast::StreamingWindowSpec;

        match window {
            StreamingWindowSpec::Tumbling { duration } => {
                // Tumbling windows are non-overlapping time-based batches
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(
                    None,
                    WINDOW_TYPE_TIME_BATCH.to_string(),
                    vec![duration_expr],
                ))
            }
            StreamingWindowSpec::Sliding { size, slide } => {
                // Sliding/hopping windows not yet implemented
                // TODO: Implement sliding window processor (requires size + slide parameters)
                let _size_expr = Self::convert_expression(size, catalog)?;
                let _slide_expr = Self::convert_expression(slide, catalog)?;
                Err(ConverterError::UnsupportedFeature(
                    "Sliding windows not yet implemented. Use 'time' for overlapping windows or 'timeBatch' for non-overlapping.".to_string()
                ))
            }
            StreamingWindowSpec::Length { size } => {
                let size_expr = Self::convert_expression(size, catalog)?;
                Ok(stream.window(None, WINDOW_TYPE_LENGTH.to_string(), vec![size_expr]))
            }
            StreamingWindowSpec::Session { gap } => {
                let gap_expr = Self::convert_expression(gap, catalog)?;
                Ok(stream.window(None, WINDOW_TYPE_SESSION.to_string(), vec![gap_expr]))
            }
            StreamingWindowSpec::Time { duration } => {
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(None, WINDOW_TYPE_TIME.to_string(), vec![duration_expr]))
            }
            StreamingWindowSpec::TimeBatch { duration } => {
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(
                    None,
                    WINDOW_TYPE_TIME_BATCH.to_string(),
                    vec![duration_expr],
                ))
            }
            StreamingWindowSpec::LengthBatch { size } => {
                let size_expr = Self::convert_expression(size, catalog)?;
                Ok(stream.window(None, WINDOW_TYPE_LENGTH_BATCH.to_string(), vec![size_expr]))
            }
            StreamingWindowSpec::ExternalTime {
                timestamp_field,
                duration,
            } => {
                let ts_expr = Self::convert_expression(timestamp_field, catalog)?;
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(
                    None,
                    WINDOW_TYPE_EXTERNAL_TIME.to_string(),
                    vec![ts_expr, duration_expr],
                ))
            }
            StreamingWindowSpec::ExternalTimeBatch {
                timestamp_field,
                duration,
            } => {
                let ts_expr = Self::convert_expression(timestamp_field, catalog)?;
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(
                    None,
                    WINDOW_TYPE_EXTERNAL_TIME_BATCH.to_string(),
                    vec![ts_expr, duration_expr],
                ))
            }
            StreamingWindowSpec::Sort { parameters } => {
                // Convert all parameters (size, attr1, 'asc', attr2, 'desc', ...)
                let mut converted_params = Vec::new();
                for param in parameters {
                    converted_params.push(Self::convert_expression(param, catalog)?);
                }
                Ok(stream.window(None, WINDOW_TYPE_SORT.to_string(), converted_params))
            }
            StreamingWindowSpec::Unique { key_attr } => {
                let key_expr = Self::convert_expression(key_attr, catalog)?;
                Ok(stream.window(None, "unique".to_string(), vec![key_expr]))
            }
            StreamingWindowSpec::FirstUnique { key_attr } => {
                let key_expr = Self::convert_expression(key_attr, catalog)?;
                Ok(stream.window(None, "firstUnique".to_string(), vec![key_expr]))
            }
            StreamingWindowSpec::Delay { duration } => {
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(None, "delay".to_string(), vec![duration_expr]))
            }
            StreamingWindowSpec::Expression { condition } => {
                // Expression window takes a string condition
                let condition_expr = Expression::value_string(condition.clone());
                Ok(stream.window(None, "expression".to_string(), vec![condition_expr]))
            }
            StreamingWindowSpec::Frequent { parameters } => {
                let mut converted_params = Vec::new();
                for param in parameters {
                    converted_params.push(Self::convert_expression(param, catalog)?);
                }
                Ok(stream.window(None, "frequent".to_string(), converted_params))
            }
            StreamingWindowSpec::LossyFrequent { parameters } => {
                let mut converted_params = Vec::new();
                for param in parameters {
                    converted_params.push(Self::convert_expression(param, catalog)?);
                }
                Ok(stream.window(None, "lossyFrequent".to_string(), converted_params))
            }
        }
    }

    /// Convert SQL expression to EventFlux Expression
    pub fn convert_expression(
        expr: &SqlExpr,
        catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        match expr {
            SqlExpr::Identifier(ident) => Ok(Expression::variable(ident.value.clone())),

            SqlExpr::CompoundIdentifier(parts) => {
                // Handle qualified identifiers like stream.column or alias.column
                if parts.len() == 2 {
                    let stream_ref = parts[0].value.clone(); // Stream name or alias (e.g., "t", "n")
                    let column_name = parts[1].value.clone(); // Column name (e.g., "symbol")

                    // Create variable with stream qualifier for JOIN queries
                    let var_with_stream = Variable::new(column_name).of_stream(stream_ref);
                    Ok(Expression::Variable(var_with_stream))
                } else {
                    Err(ConverterError::UnsupportedFeature(
                        "Multi-part identifiers not supported".to_string(),
                    ))
                }
            }

            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                // Handle indexed access for pattern event collections: e1[0].price, e1[last].symbol
                Self::convert_compound_field_access(root, access_chain, catalog)
            }

            SqlExpr::Value(value_with_span) => match &value_with_span.value {
                sqlparser::ast::Value::Number(n, _) => {
                    if n.contains('.') {
                        Ok(Expression::value_double(n.parse().map_err(|_| {
                            ConverterError::InvalidExpression(n.clone())
                        })?))
                    } else {
                        Ok(Expression::value_long(n.parse().map_err(|_| {
                            ConverterError::InvalidExpression(n.clone())
                        })?))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => {
                    Ok(Expression::value_string(s.clone()))
                }
                sqlparser::ast::Value::Boolean(b) => Ok(Expression::value_bool(*b)),
                _ => Err(ConverterError::UnsupportedFeature(format!(
                    "Value type {:?}",
                    value_with_span.value
                ))),
            },

            SqlExpr::Function(func) => {
                // Convert SQL function calls to EventFlux function calls
                Self::convert_function(func, catalog)
            }

            SqlExpr::BinaryOp { left, op, right } => {
                let left_expr = Self::convert_expression(left, catalog)?;
                let right_expr = Self::convert_expression(right, catalog)?;

                match op {
                    // Comparison operators
                    BinaryOperator::Gt => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::GreaterThan,
                        right_expr,
                    )),
                    BinaryOperator::GtEq => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::GreaterThanEqual,
                        right_expr,
                    )),
                    BinaryOperator::Lt => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::LessThan,
                        right_expr,
                    )),
                    BinaryOperator::LtEq => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::LessThanEqual,
                        right_expr,
                    )),
                    BinaryOperator::Eq => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::Equal,
                        right_expr,
                    )),
                    BinaryOperator::NotEq => Ok(Expression::compare(
                        left_expr,
                        CompareOperator::NotEqual,
                        right_expr,
                    )),

                    // Logical operators
                    BinaryOperator::And => Ok(Expression::and(left_expr, right_expr)),
                    BinaryOperator::Or => Ok(Expression::or(left_expr, right_expr)),

                    // Math operators
                    BinaryOperator::Plus => Ok(Expression::add(left_expr, right_expr)),
                    BinaryOperator::Minus => Ok(Expression::subtract(left_expr, right_expr)),
                    BinaryOperator::Multiply => Ok(Expression::multiply(left_expr, right_expr)),
                    BinaryOperator::Divide => Ok(Expression::divide(left_expr, right_expr)),
                    BinaryOperator::Modulo => Ok(Expression::modulus(left_expr, right_expr)),

                    _ => Err(ConverterError::UnsupportedFeature(format!(
                        "Binary operator {:?}",
                        op
                    ))),
                }
            }

            SqlExpr::UnaryOp { op, expr } => {
                let inner_expr = Self::convert_expression(expr, catalog)?;

                match op {
                    UnaryOperator::Not => Ok(Expression::not(inner_expr)),
                    // Convert -x to 0 - x
                    UnaryOperator::Minus => {
                        Ok(Expression::subtract(Expression::value_long(0), inner_expr))
                    }
                    // Convert +x to just x (unary plus is a no-op)
                    UnaryOperator::Plus => Ok(inner_expr),
                    _ => Err(ConverterError::UnsupportedFeature(format!(
                        "Unary operator {:?}",
                        op
                    ))),
                }
            }

            SqlExpr::Interval(interval) => {
                // Convert INTERVAL '5' SECOND to milliseconds
                Self::convert_interval_to_millis(interval)
            }

            SqlExpr::Case {
                case_token: _,
                end_token: _,
                operand,
                conditions,
                else_result,
            } => {
                // Validate at least one WHEN clause
                if conditions.is_empty() {
                    return Err(ConverterError::InvalidExpression(
                        "CASE expression must have at least one WHEN clause".to_string(),
                    ));
                }

                // Convert operand (for Simple CASE)
                let operand_expr = if let Some(ref op) = operand {
                    Some(Self::convert_expression(op, catalog)?)
                } else {
                    None
                };

                // Convert WHEN clauses (conditions is Vec<CaseWhen>)
                let when_clauses: Result<Vec<_>, _> = conditions
                    .iter()
                    .map(|case_when| {
                        let condition_expr =
                            Self::convert_expression(&case_when.condition, catalog)?;
                        let result_expr = Self::convert_expression(&case_when.result, catalog)?;
                        Ok(WhenClause::new(
                            Box::new(condition_expr),
                            Box::new(result_expr),
                        ))
                    })
                    .collect();
                let when_clauses = when_clauses?;

                // Convert ELSE expression (if None, inject NULL)
                let else_expr = if let Some(ref else_expr_sql) = else_result {
                    Self::convert_expression(else_expr_sql, catalog)?
                } else {
                    Expression::value_null()
                };

                Ok(Expression::case(operand_expr, when_clauses, else_expr))
            }

            SqlExpr::Cast {
                expr,
                data_type,
                format: _,
                kind: _,
            } => {
                // Convert CAST(expr AS type) to EventFlux Cast expression
                let inner_expr = Self::convert_expression(expr, catalog)?;
                let target_type =
                    crate::sql_compiler::type_mapping::sql_type_to_attribute_type(data_type)
                        .map_err(|e| ConverterError::UnsupportedFeature(e.to_string()))?;
                Ok(Expression::cast(inner_expr, target_type))
            }

            SqlExpr::Nested(inner_expr) => {
                // Parenthesized expression - simply unwrap and convert the inner expression
                Self::convert_expression(inner_expr, catalog)
            }

            // Handle FLOOR(x) and CEIL(x) expressions
            SqlExpr::Floor { expr, field } => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                match field {
                    sqlparser::ast::CeilFloorKind::DateTimeField(
                        sqlparser::ast::DateTimeField::NoDateTime,
                    ) => {
                        // Numeric floor function
                        Ok(Expression::function_no_ns(
                            "floor".to_string(),
                            vec![inner_expr],
                        ))
                    }
                    _ => Err(ConverterError::UnsupportedFeature(
                        "FLOOR TO datetime field not yet supported".to_string(),
                    )),
                }
            }

            SqlExpr::Ceil { expr, field } => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                match field {
                    sqlparser::ast::CeilFloorKind::DateTimeField(
                        sqlparser::ast::DateTimeField::NoDateTime,
                    ) => {
                        // Numeric ceil function
                        Ok(Expression::function_no_ns(
                            "ceil".to_string(),
                            vec![inner_expr],
                        ))
                    }
                    _ => Err(ConverterError::UnsupportedFeature(
                        "CEIL TO datetime field not yet supported".to_string(),
                    )),
                }
            }

            // Handle IS NULL and IS NOT NULL expressions
            SqlExpr::IsNull(expr) => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                Ok(Expression::is_null(inner_expr))
            }

            SqlExpr::IsNotNull(expr) => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                Ok(Expression::not(Expression::is_null(inner_expr)))
            }

            // Handle IN and NOT IN expressions
            // Convert x IN (a, b, c) to x = a OR x = b OR x = c
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => {
                if list.is_empty() {
                    // Empty list: IN () is always false, NOT IN () is always true
                    return Ok(Expression::value_bool(*negated));
                }

                let value_expr = Self::convert_expression(expr, catalog)?;

                // Build OR chain of equality comparisons
                let mut result: Option<Expression> = None;
                for item in list {
                    let item_expr = Self::convert_expression(item, catalog)?;
                    let eq_expr =
                        Expression::compare(value_expr.clone(), CompareOperator::Equal, item_expr);

                    result = Some(match result {
                        None => eq_expr,
                        Some(prev) => Expression::or(prev, eq_expr),
                    });
                }

                let in_expr = result.unwrap(); // Safe because we checked list is not empty

                if *negated {
                    Ok(Expression::not(in_expr))
                } else {
                    Ok(in_expr)
                }
            }

            // Handle BETWEEN expressions
            SqlExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value_expr = Self::convert_expression(expr, catalog)?;
                let low_expr = Self::convert_expression(low, catalog)?;
                let high_expr = Self::convert_expression(high, catalog)?;

                // x BETWEEN low AND high => x >= low AND x <= high
                let between_expr = Expression::and(
                    Expression::compare(
                        value_expr.clone(),
                        CompareOperator::GreaterThanEqual,
                        low_expr,
                    ),
                    Expression::compare(value_expr, CompareOperator::LessThanEqual, high_expr),
                );

                if *negated {
                    Ok(Expression::not(between_expr))
                } else {
                    Ok(between_expr)
                }
            }

            // Handle LIKE expressions
            // Convert LIKE to a str:matches function call
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char: _,
                any: _,
            } => {
                let value_expr = Self::convert_expression(expr, catalog)?;
                let pattern_expr = Self::convert_expression(pattern, catalog)?;
                // Use str:matches function for LIKE pattern matching
                let like_expr =
                    Expression::function_no_ns("like".to_string(), vec![value_expr, pattern_expr]);
                if *negated {
                    Ok(Expression::not(like_expr))
                } else {
                    Ok(like_expr)
                }
            }

            // Handle SUBSTRING expressions
            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let str_expr = Self::convert_expression(expr, catalog)?;
                let mut args = vec![str_expr];

                // SQL uses 1-based indexing, convert to 0-based for internal use
                if let Some(from_expr) = substring_from {
                    let from_expr_converted = Self::convert_expression(from_expr, catalog)?;
                    // Subtract 1 from the start position: SQL's 1 becomes internal 0
                    let zero_based_start =
                        Expression::subtract(from_expr_converted, Expression::value_int(1));
                    args.push(zero_based_start);
                }
                if let Some(for_expr) = substring_for {
                    args.push(Self::convert_expression(for_expr, catalog)?);
                }

                Ok(Expression::function_no_ns("substring".to_string(), args))
            }

            // Handle TRIM expressions
            SqlExpr::Trim {
                expr,
                trim_where: _,
                trim_what: _,
                trim_characters: _,
            } => {
                let str_expr = Self::convert_expression(expr, catalog)?;
                Ok(Expression::function_no_ns(
                    "trim".to_string(),
                    vec![str_expr],
                ))
            }

            _ => Err(ConverterError::UnsupportedFeature(format!(
                "Expression type {:?}",
                expr
            ))),
        }
    }

    /// Convert SQL INTERVAL to milliseconds as a Long expression
    ///
    /// # Approximations
    ///
    /// Note that YEAR and MONTH intervals use fixed approximations:
    /// - 1 YEAR = 365 days (does not account for leap years)
    /// - 1 MONTH = 30 days (months vary from 28-31 days)
    ///
    /// For precise time-based window operations, prefer SECOND, MINUTE, HOUR, or DAY units.
    ///
    /// # Example
    ///
    /// ```sql
    /// INTERVAL '5' SECOND  -- Exact: 5000 milliseconds
    /// INTERVAL '1' MONTH   -- Approximation: 2592000000 milliseconds (30 days)
    /// ```
    fn convert_interval_to_millis(
        interval: &sqlparser::ast::Interval,
    ) -> Result<Expression, ConverterError> {
        // Extract the numeric value
        let value = match interval.value.as_ref() {
            SqlExpr::Value(value_with_span) => match &value_with_span.value {
                sqlparser::ast::Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                    ConverterError::InvalidExpression(format!("Invalid interval value: {}", n))
                })?,
                sqlparser::ast::Value::SingleQuotedString(s) => s.parse::<i64>().map_err(|_| {
                    ConverterError::InvalidExpression(format!("Invalid interval value: {}", s))
                })?,
                _ => {
                    return Err(ConverterError::UnsupportedFeature(
                        "Complex interval values not supported".to_string(),
                    ))
                }
            },
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Complex interval expressions not supported".to_string(),
                ))
            }
        };

        // Convert based on time unit
        let millis = match &interval.leading_field {
            Some(sqlparser::ast::DateTimeField::Year) => {
                // Approximation: 365 days (ignores leap years)
                value * 365 * 24 * 60 * 60 * 1000
            }
            Some(sqlparser::ast::DateTimeField::Month) => {
                // Approximation: 30 days (months vary 28-31 days)
                value * 30 * 24 * 60 * 60 * 1000
            }
            Some(sqlparser::ast::DateTimeField::Day) => value * 24 * 60 * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Hour) => value * 60 * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Minute) => value * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Second) => value * 1000,
            None => {
                // Default to milliseconds if no unit specified
                value
            }
            _ => {
                return Err(ConverterError::UnsupportedFeature(format!(
                    "Interval unit {:?} not supported",
                    interval.leading_field
                )))
            }
        };

        Ok(Expression::value_long(millis))
    }

    /// Convert SQL function to EventFlux function call
    fn convert_function(
        func: &sqlparser::ast::Function,
        catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        // Get function name and strip backticks if present (for namespace-prefixed functions like `math:sin`)
        let raw_name = func.name.to_string();
        let func_name = raw_name
            .trim_start_matches('`')
            .trim_end_matches('`')
            .to_lowercase();

        // Extract function argument list
        let arg_list = match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => list,
            sqlparser::ast::FunctionArguments::None => {
                // Functions like CURRENT_TIMESTAMP with no args
                return Ok(Expression::function(None, func_name, Vec::new()));
            }
            sqlparser::ast::FunctionArguments::Subquery(_) => {
                return Err(ConverterError::UnsupportedFeature(
                    "Subquery as function argument not supported".to_string(),
                ));
            }
        };

        // Convert function arguments
        let mut args = Vec::new();
        for arg in &arg_list.args {
            match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => {
                    args.push(Self::convert_expression(expr, catalog)?);
                }
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    // Handle COUNT(*) - no arguments needed
                    // EventFlux count() takes no arguments
                }
                _ => {
                    return Err(ConverterError::UnsupportedFeature(
                        "Function argument type not supported".to_string(),
                    ));
                }
            }
        }

        // Map SQL function names to EventFlux function names
        let eventflux_func_name = match func_name.as_str() {
            // Aggregations
            "count" => "count",
            "sum" => "sum",
            "avg" => "avg",
            "min" => "min",
            "max" => "max",
            "minforever" => "minforever",
            "maxforever" => "maxforever",
            "distinctcount" => "distinctCount",
            "stddev" => "stddev",
            "first" => "first",
            "last" => "last",
            "and" => "and",
            "or" => "or",
            // Math functions
            "round" => "round",
            "abs" => "abs",
            "ceil" => "ceil",
            "floor" => "floor",
            "sqrt" => "sqrt",
            "sin" => "sin",
            "cos" => "cos",
            "tan" => "tan",
            "asin" => "asin",
            "acos" => "acos",
            "atan" => "atan",
            "exp" => "exp",
            "power" => "power",
            "pow" => "power",
            "ln" => "ln",
            "log" => "log",
            "log10" => "log10",
            "maximum" => "maximum",
            "minimum" => "minimum",
            "mod" => "mod",
            "sign" => "sign",
            "trunc" => "trunc",
            "truncate" => "trunc",
            // String functions
            "upper" => "upper",
            "lower" => "lower",
            "length" => "length",
            "concat" => "concat",
            "replace" => "replace",
            "trim" => "trim",
            "left" => "left",
            "right" => "right",
            "ltrim" => "ltrim",
            "rtrim" => "rtrim",
            "reverse" => "reverse",
            "repeat" => "repeat",
            "position" => "position",
            "locate" => "position", // MySQL compatibility
            "instr" => "position",  // MySQL/Oracle compatibility
            "ascii" => "ascii",
            "chr" => "chr",
            "char" => "chr", // MySQL compatibility
            // Note: substr/substring as function calls need 1-based to 0-based conversion
            // This is handled specially below before the match
            "substr" | "substring" => {
                // Apply 1-based to 0-based indexing conversion for start position
                if args.len() >= 2 {
                    let str_arg = args.remove(0);
                    let start_arg = args.remove(0);
                    let zero_based_start =
                        Expression::subtract(start_arg, Expression::value_int(1));
                    let mut new_args = vec![str_arg, zero_based_start];
                    new_args.extend(args);
                    return Ok(Expression::function_no_ns(
                        "substring".to_string(),
                        new_args,
                    ));
                }
                "substring"
            }
            "lpad" => "lpad",
            "rpad" => "rpad",
            // Utility functions
            "coalesce" => "coalesce",
            "default" => "default",
            "ifnull" => "default", // IFNULL maps to default (2 args only)
            "nullif" => "nullif",
            "uuid" => "uuid",
            "eventtimestamp" => "eventTimestamp",
            "now" => "now",
            _ => {
                return Err(ConverterError::UnsupportedFeature(format!(
                    "Function '{}' not supported",
                    func_name
                )))
            }
        };

        Ok(Expression::function_no_ns(
            eventflux_func_name.to_string(),
            args,
        ))
    }

    /// Convert SQL expression to Constant (for LIMIT/OFFSET)
    fn convert_to_constant(
        expr: &SqlExpr,
    ) -> Result<crate::query_api::expression::constant::Constant, ConverterError> {
        match expr {
            SqlExpr::Value(value_with_span) => {
                if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                    // Try to parse as i64 for LIMIT/OFFSET
                    let num = n.parse::<i64>().map_err(|_| {
                        ConverterError::ConversionFailed(format!(
                            "Invalid number for LIMIT/OFFSET: {}",
                            n
                        ))
                    })?;
                    Ok(crate::query_api::expression::constant::Constant::long(num))
                } else {
                    Err(ConverterError::UnsupportedFeature(
                        "LIMIT/OFFSET must be numeric constants".to_string(),
                    ))
                }
            }
            _ => Err(ConverterError::UnsupportedFeature(
                "LIMIT/OFFSET must be numeric constants".to_string(),
            )),
        }
    }

    // ============================================================================
    // Array Access Conversion Methods
    // ============================================================================

    /// Convert CompoundFieldAccess for pattern event collection access: e1[0].price, e1[last].symbol
    ///
    /// This handles indexed access to events in pattern event collections, where count
    /// quantifiers like A{3,5} produce multiple events that can be accessed by index.
    fn convert_compound_field_access(
        root: &SqlExpr,
        access_chain: &[AccessExpr],
        _catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        // Extract stream alias from root (e.g., "e1" from e1[0].price)
        let stream_id = match root {
            SqlExpr::Identifier(ident) => ident.value.clone(),
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Array access root must be an identifier (stream alias)".to_string(),
                ))
            }
        };

        // Validate access chain: must be [index].attribute format
        if access_chain.len() != 2 {
            return Err(ConverterError::UnsupportedFeature(format!(
                "Expected [index].attribute format, got {} access elements",
                access_chain.len()
            )));
        }

        // Extract index from first element (Subscript)
        let event_index = match &access_chain[0] {
            AccessExpr::Subscript(Subscript::Index { index }) => Self::extract_event_index(index)?,
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "First element must be array index [n] or [last]".to_string(),
                ))
            }
        };

        // Extract attribute name from second element (Dot)
        let attribute_name = match &access_chain[1] {
            AccessExpr::Dot(SqlExpr::Identifier(ident)) => ident.value.clone(),
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Second element must be .attribute".to_string(),
                ))
            }
        };

        // Create IndexedVariable with stream id (position resolved during expression parsing)
        let indexed_var = match event_index {
            EventIndex::Numeric(idx) => IndexedVariable::new_with_index(attribute_name, idx)
                .of_stream_with_index(stream_id, -1),
            EventIndex::Last => {
                IndexedVariable::new_with_last(attribute_name).of_stream_with_index(stream_id, -1)
            }
        };

        Ok(Expression::IndexedVariable(Box::new(indexed_var)))
    }

    /// Extract EventIndex from a subscript expression
    fn extract_event_index(index_expr: &SqlExpr) -> Result<EventIndex, ConverterError> {
        match index_expr {
            // Numeric index: e[0], e[1], e[2]
            SqlExpr::Value(value_with_span) => {
                if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                    let idx: usize = n.parse().map_err(|_| {
                        ConverterError::InvalidExpression(format!("Invalid array index: {}", n))
                    })?;
                    Ok(EventIndex::Numeric(idx))
                } else {
                    Err(ConverterError::InvalidExpression(
                        "Array index must be a number or 'last'".to_string(),
                    ))
                }
            }
            // 'last' keyword: e[last]
            SqlExpr::Identifier(ident) if ident.value.to_lowercase() == "last" => {
                Ok(EventIndex::Last)
            }
            _ => Err(ConverterError::UnsupportedFeature(
                "Array index must be numeric or 'last'".to_string(),
            )),
        }
    }

    // ============================================================================
    // Pattern Alias Extraction
    // ============================================================================

    /// Extract all pattern aliases from a PatternExpression
    ///
    /// Returns a Vec of (alias, stream_name) pairs.
    /// For example, `e1=A -> e2=B` returns `[("e1", "A"), ("e2", "B")]`
    pub fn extract_pattern_aliases(pattern: &PatternExpression) -> Vec<(String, String)> {
        let mut aliases = Vec::new();
        Self::collect_pattern_aliases(pattern, &mut aliases);
        aliases
    }

    /// Recursively collect pattern aliases from a PatternExpression
    fn collect_pattern_aliases(pattern: &PatternExpression, aliases: &mut Vec<(String, String)>) {
        match pattern {
            PatternExpression::Stream {
                alias, stream_name, ..
            } => {
                if let Some(alias_ident) = alias {
                    let stream_id = stream_name
                        .0
                        .last()
                        .and_then(|part| part.as_ident())
                        .map(|ident| ident.value.clone())
                        .unwrap_or_default();
                    aliases.push((alias_ident.value.clone(), stream_id));
                }
            }
            PatternExpression::Sequence { first, second } => {
                Self::collect_pattern_aliases(first, aliases);
                Self::collect_pattern_aliases(second, aliases);
            }
            PatternExpression::Logical { left, right, .. } => {
                Self::collect_pattern_aliases(left, aliases);
                Self::collect_pattern_aliases(right, aliases);
            }
            PatternExpression::Every { pattern } => {
                Self::collect_pattern_aliases(pattern, aliases);
            }
            PatternExpression::Count { pattern, .. } => {
                Self::collect_pattern_aliases(pattern, aliases);
            }
            PatternExpression::Grouped { pattern } => {
                Self::collect_pattern_aliases(pattern, aliases);
            }
            PatternExpression::Absent { .. } => {
                // Absent patterns don't have aliases (only stream_name)
            }
        }
    }

    // ============================================================================
    // Pattern Conversion Methods
    // ============================================================================

    /// Convert a pattern TableFactor to InputStream
    ///
    /// This handles FROM PATTERN (...) and FROM SEQUENCE (...) clauses,
    /// converting the pattern expression AST to Query API StateElements.
    ///
    /// Validation is performed before conversion to ensure the pattern
    /// adheres to grammar rules (EVERY restrictions, count bounds, etc.)
    pub fn convert_pattern_input(
        mode: &PatternMode,
        pattern: &PatternExpression,
        within: &Option<WithinConstraint>,
        catalog: &SqlCatalog,
    ) -> Result<InputStream, ConverterError> {
        // Validate pattern before conversion
        PatternValidator::validate(mode, pattern).map_err(|errors| {
            let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            ConverterError::ConversionFailed(format!(
                "Pattern validation failed:\n  - {}",
                error_messages.join("\n  - ")
            ))
        })?;

        // Convert pattern expression to StateElement
        let state_element = Self::convert_pattern_expression(pattern, catalog)?;

        // Convert WITHIN constraint to ExpressionConstant
        let within_time = match within {
            Some(WithinConstraint::Time(expr)) => {
                // Convert time expression (e.g., INTERVAL '10' SECOND) to milliseconds
                let time_expr = Self::convert_expression(expr, catalog)?;
                // Extract the constant value from the expression
                match time_expr {
                    Expression::Constant(c) => Some(c),
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "WITHIN time must be a constant expression".to_string(),
                        ))
                    }
                }
            }
            Some(WithinConstraint::EventCount(count)) => {
                return Err(ConverterError::UnsupportedFeature(format!(
                    "WITHIN {count} EVENTS is not yet supported; use time-based WITHIN"
                )));
            }
            None => None,
        };

        // Create StateInputStream based on mode
        let state_input_stream = match mode {
            PatternMode::Pattern => StateInputStream::pattern_stream(state_element, within_time),
            PatternMode::Sequence => StateInputStream::sequence_stream(state_element, within_time),
        };

        Ok(InputStream::State(Box::new(state_input_stream)))
    }

    /// Convert PatternExpression AST to StateElement
    fn convert_pattern_expression(
        expr: &PatternExpression,
        catalog: &SqlCatalog,
    ) -> Result<StateElement, ConverterError> {
        match expr {
            PatternExpression::Stream {
                alias,
                stream_name,
                filter,
            } => {
                // Get the stream name as string
                let stream_id = stream_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        ConverterError::ConversionFailed(
                            "Invalid stream name in pattern".to_string(),
                        )
                    })?;

                // Validation 1.9: Tables cannot be used in patterns/sequences
                // Only streams can be used as event sources in patterns
                if let Ok(relation) = catalog.get_relation(&stream_id) {
                    if relation.is_table() {
                        return Err(ConverterError::TableInPattern(stream_id));
                    }
                }

                // Create SingleInputStream with optional alias
                let mut single_stream = SingleInputStream::new_basic(
                    stream_id.clone(),
                    false, // is_inner
                    false, // is_fault
                    None,  // stream_ref_id (will set via as_ref if alias present)
                    Vec::new(),
                );

                // Add stream reference (alias) if present
                if let Some(alias_ident) = alias {
                    single_stream = single_stream.as_ref(alias_ident.value.clone());
                }

                // Add filter if present
                if let Some(filter_expr) = filter {
                    let converted_filter = Self::convert_expression(filter_expr, catalog)?;
                    single_stream = single_stream.filter(converted_filter);
                }

                // Create StreamStateElement
                let stream_state = StreamStateElement::new(single_stream);
                Ok(StateElement::Stream(stream_state))
            }

            PatternExpression::Count {
                pattern,
                min_count,
                max_count,
            } => {
                // First, convert the inner pattern to get the StreamStateElement
                let inner = Self::convert_pattern_expression(pattern, catalog)?;

                // Count quantifier requires a StreamStateElement, not arbitrary StateElement
                let stream_state = match inner {
                    StateElement::Stream(s) => s,
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Count quantifier can only be applied to stream patterns".to_string(),
                        ))
                    }
                };

                // Guard against counts that exceed i32 range to avoid overflow
                let min_i32 = i32::try_from(*min_count).map_err(|_| {
                    ConverterError::UnsupportedFeature(format!(
                        "Count quantifier lower bound {} exceeds supported range",
                        min_count
                    ))
                })?;
                let max_i32 = i32::try_from(*max_count).map_err(|_| {
                    ConverterError::UnsupportedFeature(format!(
                        "Count quantifier upper bound {} exceeds supported range",
                        max_count
                    ))
                })?;

                Ok(StateElement::Count(CountStateElement::new(
                    stream_state,
                    min_i32,
                    max_i32,
                )))
            }

            PatternExpression::Sequence { first, second } => {
                // Convert both sides of the sequence
                let first_state = Self::convert_pattern_expression(first, catalog)?;
                let second_state = Self::convert_pattern_expression(second, catalog)?;

                // Create NextStateElement (-> operator)
                Ok(StateElement::Next(Box::new(NextStateElement::new(
                    first_state,
                    second_state,
                ))))
            }

            PatternExpression::Logical { left, op, right } => {
                // Convert both sides
                let left_state = Self::convert_pattern_expression(left, catalog)?;
                let right_state = Self::convert_pattern_expression(right, catalog)?;

                // Map logical operator
                let logical_type = match op {
                    PatternLogicalOp::And => LogicalStateElementType::And,
                    PatternLogicalOp::Or => LogicalStateElementType::Or,
                };

                Ok(StateElement::Logical(LogicalStateElement::new(
                    left_state,
                    logical_type,
                    right_state,
                )))
            }

            PatternExpression::Every { pattern } => {
                // Convert the inner pattern
                let inner_state = Self::convert_pattern_expression(pattern, catalog)?;

                Ok(StateElement::Every(Box::new(EveryStateElement::new(
                    inner_state,
                ))))
            }

            PatternExpression::Absent {
                stream_name,
                duration,
            } => {
                // Get the stream name
                let stream_id = stream_name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        ConverterError::ConversionFailed(
                            "Invalid stream name in absent pattern".to_string(),
                        )
                    })?;

                // Create SingleInputStream for the absent stream
                let single_stream = SingleInputStream::new_basic(
                    stream_id,
                    false, // is_inner
                    false, // is_fault
                    None,  // stream_ref_id
                    Vec::new(),
                );

                // Convert duration expression to constant
                let duration_expr = Self::convert_expression(duration, catalog)?;
                let duration_const = match duration_expr {
                    Expression::Constant(c) => Some(c),
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Absent pattern duration must be a constant".to_string(),
                        ))
                    }
                };

                // Create AbsentStreamStateElement
                Ok(StateElement::AbsentStream(AbsentStreamStateElement::new(
                    single_stream,
                    duration_const,
                )))
            }

            PatternExpression::Grouped { pattern } => {
                // Grouped patterns just recurse into the inner pattern
                Self::convert_pattern_expression(pattern, catalog)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::definition::attribute::Type as AttributeType;
    use crate::query_api::definition::StreamDefinition;

    fn setup_catalog() -> SqlCatalog {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("StockStream".to_string())
            .attribute("symbol".to_string(), AttributeType::STRING)
            .attribute("price".to_string(), AttributeType::DOUBLE)
            .attribute("volume".to_string(), AttributeType::INT);

        catalog
            .register_stream("StockStream".to_string(), stream)
            .unwrap();
        catalog
    }

    #[test]
    fn test_simple_select() {
        let catalog = setup_catalog();
        let sql = "SELECT symbol, price FROM StockStream";
        let query = SqlConverter::convert(sql, &catalog).unwrap();

        // Verify query structure
        assert!(query.get_input_stream().is_some());
    }

    #[test]
    fn test_select_with_where() {
        let catalog = setup_catalog();
        let sql = "SELECT symbol, price FROM StockStream WHERE price > 100";
        let query = SqlConverter::convert(sql, &catalog).unwrap();

        assert!(query.get_input_stream().is_some());
    }

    #[test]
    fn test_select_with_window() {
        let catalog = setup_catalog();
        let sql = "SELECT symbol, price FROM StockStream WINDOW('length', 5)";
        let query = SqlConverter::convert(sql, &catalog).unwrap();

        assert!(query.get_input_stream().is_some());
    }

    #[test]
    fn test_unknown_stream_error() {
        let catalog = setup_catalog();
        let sql = "SELECT * FROM UnknownStream";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(result.is_err());
    }

    // ============================================================================
    // Type Validation Error Tests
    // ============================================================================

    #[test]
    fn test_where_clause_non_boolean_variable() {
        let catalog = setup_catalog();
        // WHERE price - returns DOUBLE, not BOOL
        let sql = "SELECT symbol, price FROM StockStream WHERE price";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type validation failed"),
            "Error message should mention type validation: {}",
            err_msg
        );
        assert!(
            err_msg.contains("WHERE"),
            "Error message should mention WHERE clause: {}",
            err_msg
        );
    }

    #[test]
    fn test_where_clause_arithmetic_expression() {
        let catalog = setup_catalog();
        // WHERE price * 2 - returns DOUBLE, not BOOL
        let sql = "SELECT symbol, price FROM StockStream WHERE price * 2";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type validation failed"),
            "Error message should mention type validation: {}",
            err_msg
        );
        assert!(
            err_msg.contains("WHERE"),
            "Error message should mention WHERE clause: {}",
            err_msg
        );
    }

    #[test]
    fn test_where_clause_valid_boolean() {
        let catalog = setup_catalog();
        // WHERE price > 100 - valid BOOL expression
        let sql = "SELECT symbol, price FROM StockStream WHERE price > 100";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(
            result.is_ok(),
            "Valid WHERE clause should succeed: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_having_clause_non_boolean() {
        let catalog = setup_catalog();
        // HAVING SUM(volume) - returns LONG, not BOOL
        let sql = "SELECT symbol, SUM(volume) as total FROM StockStream GROUP BY symbol HAVING SUM(volume)";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type validation failed"),
            "Error message should mention type validation: {}",
            err_msg
        );
        assert!(
            err_msg.contains("HAVING"),
            "Error message should mention HAVING clause: {}",
            err_msg
        );
    }

    #[test]
    fn test_having_clause_valid_boolean() {
        let catalog = setup_catalog();
        // HAVING SUM(volume) > 1000 - valid BOOL expression
        let sql = "SELECT symbol, SUM(volume) as total FROM StockStream GROUP BY symbol HAVING SUM(volume) > 1000";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(
            result.is_ok(),
            "Valid HAVING clause should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_where_with_function_non_boolean() {
        let catalog = setup_catalog();
        // WHERE ROUND(price, 2) - returns DOUBLE, not BOOL
        let sql = "SELECT symbol, price FROM StockStream WHERE ROUND(price, 2)";
        let result = SqlConverter::convert(sql, &catalog);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Type validation failed"),
            "Error message should mention type validation: {}",
            err_msg
        );
    }

    // ============================================================================
    // Pattern Conversion Tests
    // ============================================================================

    #[test]
    fn test_convert_pattern_basic_stream() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test basic stream pattern: e1=StockStream
        let pattern = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };

        let result = SqlConverter::convert_pattern_expression(&pattern, &catalog);
        assert!(result.is_ok(), "Should convert basic stream pattern");

        let state_element = result.unwrap();
        match state_element {
            StateElement::Stream(stream_state) => {
                assert_eq!(stream_state.get_stream_id(), "StockStream");
            }
            _ => panic!("Expected Stream state element"),
        }
    }

    #[test]
    fn test_convert_pattern_sequence() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test sequence: A -> B
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let stream_b = PatternExpression::Stream {
            alias: Some(Ident::new("e2")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let pattern = PatternExpression::Sequence {
            first: Box::new(stream_a),
            second: Box::new(stream_b),
        };

        let result = SqlConverter::convert_pattern_expression(&pattern, &catalog);
        assert!(result.is_ok(), "Should convert sequence pattern");

        let state_element = result.unwrap();
        match state_element {
            StateElement::Next(next) => {
                // Verify first and second are Stream elements
                assert!(matches!(
                    next.state_element.as_ref(),
                    StateElement::Stream(_)
                ));
                assert!(matches!(
                    next.next_state_element.as_ref(),
                    StateElement::Stream(_)
                ));
            }
            _ => panic!("Expected Next state element for sequence"),
        }
    }

    #[test]
    fn test_convert_pattern_count_quantifier() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test count: A{3,5}
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let pattern = PatternExpression::Count {
            pattern: Box::new(stream_a),
            min_count: 3,
            max_count: 5,
        };

        let result = SqlConverter::convert_pattern_expression(&pattern, &catalog);
        assert!(result.is_ok(), "Should convert count quantifier pattern");

        let state_element = result.unwrap();
        match state_element {
            StateElement::Count(count) => {
                assert_eq!(count.min_count, 3);
                assert_eq!(count.max_count, 5);
            }
            _ => panic!("Expected Count state element"),
        }
    }

    #[test]
    fn test_convert_pattern_logical_and() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test logical AND: A AND B
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let stream_b = PatternExpression::Stream {
            alias: Some(Ident::new("e2")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let pattern = PatternExpression::Logical {
            left: Box::new(stream_a),
            op: PatternLogicalOp::And,
            right: Box::new(stream_b),
        };

        let result = SqlConverter::convert_pattern_expression(&pattern, &catalog);
        assert!(result.is_ok(), "Should convert logical AND pattern");

        let state_element = result.unwrap();
        match state_element {
            StateElement::Logical(logical) => {
                assert_eq!(logical.logical_type, LogicalStateElementType::And);
            }
            _ => panic!("Expected Logical state element"),
        }
    }

    #[test]
    fn test_convert_pattern_every() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test EVERY: EVERY(A -> B)
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let stream_b = PatternExpression::Stream {
            alias: Some(Ident::new("e2")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let sequence = PatternExpression::Sequence {
            first: Box::new(stream_a),
            second: Box::new(stream_b),
        };
        let pattern = PatternExpression::Every {
            pattern: Box::new(sequence),
        };

        let result = SqlConverter::convert_pattern_expression(&pattern, &catalog);
        assert!(result.is_ok(), "Should convert EVERY pattern");

        let state_element = result.unwrap();
        match state_element {
            StateElement::Every(every) => {
                // Inner should be a sequence
                assert!(matches!(
                    every.state_element.as_ref(),
                    StateElement::Next(_)
                ));
            }
            _ => panic!("Expected Every state element"),
        }
    }

    #[test]
    fn test_convert_pattern_input_pattern_mode() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test convert_pattern_input with PATTERN mode
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };

        let result =
            SqlConverter::convert_pattern_input(&PatternMode::Pattern, &stream_a, &None, &catalog);

        assert!(result.is_ok(), "Should convert pattern input");
        let input_stream = result.unwrap();
        assert!(matches!(input_stream, InputStream::State(_)));
    }

    #[test]
    fn test_convert_pattern_input_sequence_mode() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test convert_pattern_input with SEQUENCE mode
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let stream_b = PatternExpression::Stream {
            alias: Some(Ident::new("e2")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let sequence = PatternExpression::Sequence {
            first: Box::new(stream_a),
            second: Box::new(stream_b),
        };

        let result =
            SqlConverter::convert_pattern_input(&PatternMode::Sequence, &sequence, &None, &catalog);

        assert!(result.is_ok(), "Should convert sequence input");
        let input_stream = result.unwrap();
        assert!(matches!(input_stream, InputStream::State(_)));
    }

    #[test]
    fn test_convert_pattern_with_within_time() {
        use sqlparser::ast::{DateTimeField, Ident, Interval, ObjectName, ObjectNamePart, Value};

        let catalog = setup_catalog();

        // Test WITHIN time constraint
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };

        // Create Interval expression using Value::Number which doesn't need span
        let within = WithinConstraint::Time(Box::new(SqlExpr::Interval(Interval {
            value: Box::new(SqlExpr::Value(
                Value::SingleQuotedString("10".to_string()).into(),
            )),
            leading_field: Some(DateTimeField::Second),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        })));

        let result = SqlConverter::convert_pattern_input(
            &PatternMode::Pattern,
            &stream_a,
            &Some(within),
            &catalog,
        );

        assert!(result.is_ok(), "Should convert pattern with WITHIN");
        if let InputStream::State(state) = result.unwrap() {
            assert!(state.within_time.is_some());
        } else {
            panic!("Expected State input stream");
        }
    }

    #[test]
    fn test_convert_pattern_with_within_events_unsupported() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // Test WITHIN event count constraint - currently not supported
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };

        let within = WithinConstraint::EventCount(100);

        let result = SqlConverter::convert_pattern_input(
            &PatternMode::Pattern,
            &stream_a,
            &Some(within),
            &catalog,
        );

        // WITHIN EVENTS is not yet supported - should return error
        assert!(result.is_err(), "WITHIN EVENTS should be unsupported");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("WITHIN") && err.contains("EVENTS") && err.contains("not yet supported"),
            "Error should mention unsupported: {}",
            err
        );
    }

    // ============================================================================
    // Pattern Validation Integration Tests
    // ============================================================================

    #[test]
    fn test_convert_pattern_validates_every_in_sequence() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // EVERY in SEQUENCE mode should be rejected
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let pattern = PatternExpression::Every {
            pattern: Box::new(stream_a),
        };

        let result =
            SqlConverter::convert_pattern_input(&PatternMode::Sequence, &pattern, &None, &catalog);

        assert!(result.is_err(), "EVERY in SEQUENCE should be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("EVERY not allowed in SEQUENCE mode"),
            "Error should mention EVERY not allowed: {}",
            err
        );
    }

    #[test]
    fn test_convert_pattern_validates_zero_count() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // A{0,5} should be rejected (min_count must be >= 1)
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let pattern = PatternExpression::Count {
            pattern: Box::new(stream_a),
            min_count: 0,
            max_count: 5,
        };

        let result =
            SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

        assert!(result.is_err(), "Zero min_count should be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("min_count must be >= 1"),
            "Error should mention min_count: {}",
            err
        );
    }

    #[test]
    fn test_convert_pattern_validates_nested_every() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let catalog = setup_catalog();

        // (EVERY A) -> B should be rejected (EVERY must be at top level)
        let stream_a = PatternExpression::Stream {
            alias: Some(Ident::new("e1")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let stream_b = PatternExpression::Stream {
            alias: Some(Ident::new("e2")),
            stream_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("StockStream"))]),
            filter: None,
        };
        let every_a = PatternExpression::Every {
            pattern: Box::new(stream_a),
        };
        let pattern = PatternExpression::Sequence {
            first: Box::new(every_a),
            second: Box::new(stream_b),
        };

        let result =
            SqlConverter::convert_pattern_input(&PatternMode::Pattern, &pattern, &None, &catalog);

        assert!(result.is_err(), "Nested EVERY should be rejected");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("EVERY must be at top level"),
            "Error should mention EVERY at top level: {}",
            err
        );
    }
}
