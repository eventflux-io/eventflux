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

//! SQL to Query converter.
//!
//! Turns parsed SQL statements into EventFlux query_api::Query and Partition structures.

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

/// Converts parsed SQL into Query API structures. Holds no data.
pub struct SqlConverter;

impl SqlConverter {
    /// Parse a SQL string and convert its first statement, which must be a SELECT or an
    /// INSERT INTO ... SELECT. Any later statements are ignored.
    ///
    /// The older entry point. Prefer `convert_query_ast` when you already have the AST.
    /// Does not run CREATE STREAM normalization.
    ///
    /// # Errors
    ///
    /// Returns `ConverterError` for a parse failure, no statements, an unsupported
    /// statement or any conversion error.
    pub fn convert(sql: &str, catalog: &SqlCatalog) -> Result<Query, ConverterError> {
        // The parser handles the WINDOW clause itself.
        let statements = Parser::parse_sql(&GenericDialect, sql)
            .map_err(|e| ConverterError::ConversionFailed(format!("SQL parse error: {}", e)))?;

        if statements.is_empty() {
            return Err(ConverterError::ConversionFailed(
                "No SQL statements found".to_string(),
            ));
        }

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

    /// Convert a parsed SELECT AST into a Query.
    ///
    /// Prefer this when you already have the AST. It avoids turning the AST back into text
    /// and parsing it again. `output_stream_name` is the INSERT INTO target. `None` means
    /// "OutputStream".
    pub fn convert_query_ast(
        query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_stream_name: Option<String>,
    ) -> Result<Query, ConverterError> {
        Self::convert_query_internal(query, catalog, output_stream_name)
    }

    /// Build the query behind a stream triggered UPDATE or DELETE.
    ///
    /// The query selects every column of the source stream and sends each event to
    /// `output_action`, which applies the change to the target table.
    ///
    /// # Errors
    ///
    /// Returns `ConverterError::SchemaNotFound` if the source stream or table is unknown.
    pub fn convert_mutation_query(
        source_stream_name: &str,
        catalog: &SqlCatalog,
        output_action: OutputStreamAction,
    ) -> Result<Query, ConverterError> {
        catalog
            .get_relation(source_stream_name)
            .map_err(|_| ConverterError::SchemaNotFound(source_stream_name.to_string()))?;

        let single_stream = SingleInputStream::new_basic(
            source_stream_name.to_string(),
            false, // is_inner_stream
            false, // is_fault_stream
            None,  // stream_ref_id (no alias)
            Vec::new(),
        );

        // Same as SELECT * on the source. The unwrap is safe, the lookup passed above.
        let relation = catalog.get_relation(source_stream_name).unwrap();
        let attributes = relation.abstract_definition().get_attribute_list();

        let mut selector = crate::query_api::execution::query::selection::Selector::new();
        for attr in attributes {
            selector = selector.select_variable(Variable::new(attr.get_name().clone()));
        }

        let output_stream = OutputStream::new(output_action, None);

        Ok(Query::query()
            .from(InputStream::Single(single_stream))
            .select(selector)
            .out_stream(output_stream))
    }

    /// Build the query behind an UPSERT. Converts the SELECT as usual, then swaps its
    /// output for the upsert action.
    ///
    /// UPSERT only reads one plain stream, with no JOIN and no WINDOW, because
    /// UpsertTableProcessor needs StreamEvent input.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedFeature` for more than one source, a JOIN or a WINDOW.
    /// Returns `SchemaMismatch` when there is no SET clause, no wildcard, and the SELECT
    /// column count differs from the table's. Also returns any error from converting the
    /// SELECT.
    pub fn convert_upsert_query(
        source_query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_action: OutputStreamAction,
    ) -> Result<Query, ConverterError> {
        if let SetExpr::Select(select) = source_query.body.as_ref() {
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
                if let TableFactor::Table {
                    window: Some(_), ..
                } = &from.relation
                {
                    return Err(ConverterError::UnsupportedFeature(
                        "UPSERT with WINDOW source not supported".to_string(),
                    ));
                }
            }

            // Without a SET clause the whole row is replaced, so the SELECT must give
            // exactly one value per table column. With SET only the listed columns change.
            if let OutputStreamAction::UpdateOrInsert(ref upsert_action) = output_action {
                if upsert_action.update_set_clause.is_none() {
                    let target_table = &upsert_action.target_id;
                    if let Ok(relation) = catalog.get_relation(target_table) {
                        let table_col_count = relation.abstract_definition().attribute_list.len();
                        let select_col_count = select.projection.len();

                        // Wildcards are only expanded later, so the count is not checked
                        // when the SELECT has one. An unknown table is not checked either.
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

        let mut query = Self::convert_query_internal(source_query, catalog, None)?;

        let output_stream = OutputStream::new(output_action, None);
        query = query.out_stream(output_stream);

        Ok(query)
    }

    /// Convert a PARTITION statement into a Partition.
    ///
    /// Each key becomes a value partition on that stream's attribute. The body may only
    /// hold SELECT and INSERT INTO statements.
    ///
    /// # Errors
    ///
    /// Returns `SchemaNotFound` for an unknown stream, `InvalidExpression` for an unknown
    /// attribute, `UnsupportedFeature` for any other body statement, and any error from
    /// converting the body queries.
    pub fn convert_partition(
        partition_keys: &[PartitionKey],
        body: &[Statement],
        catalog: &SqlCatalog,
    ) -> Result<Partition, ConverterError> {
        let mut partition = Partition::new();

        for key in partition_keys {
            let stream_name = key.stream_name.to_string();
            let attribute_name = key.attribute.value.clone();

            catalog
                .get_relation(&stream_name)
                .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

            if !catalog.has_column(&stream_name, &attribute_name) {
                return Err(ConverterError::InvalidExpression(format!(
                    "Attribute '{}' not found in relation '{}'",
                    attribute_name, stream_name
                )));
            }

            let partition_expr = Expression::Variable(Variable::new(attribute_name.clone()));
            partition = partition.with_value_partition(stream_name, partition_expr);
        }

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

    /// Convert a sqlparser Query AST into a Query. Only a plain SELECT body is supported.
    fn convert_query_internal(
        sql_query: &sqlparser::ast::Query,
        catalog: &SqlCatalog,
        output_stream_name: Option<String>,
    ) -> Result<Query, ConverterError> {
        // The MySQL form LIMIT offset, limit is rejected.
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

    /// Convert the OUTPUT clause into an OutputRate.
    ///
    /// EVENTS gives an event count rate. Time units give a time rate in milliseconds, or a
    /// snapshot rate for SNAPSHOT mode.
    ///
    /// # Errors
    ///
    /// Returns an error for a value of 0, SNAPSHOT with EVENTS, an event count above
    /// `i32::MAX`, or a time that overflows when turned into milliseconds or i64.
    fn convert_output_rate_limit(
        rate_limit: &OutputRateLimit,
    ) -> Result<OutputRate, ConverterError> {
        if rate_limit.value == 0 {
            return Err(ConverterError::ConversionFailed(
                "Rate limit value must be greater than 0".to_string(),
            ));
        }

        let variant = match rate_limit.unit {
            OutputRateLimitUnit::Events => {
                // The parser should already reject SNAPSHOT with EVENTS. Checked again
                // here to be safe.
                if matches!(rate_limit.mode, OutputRateLimitMode::Snapshot) {
                    return Err(ConverterError::UnsupportedFeature(
                        "SNAPSHOT mode is not supported with EVENTS - use time units instead"
                            .to_string(),
                    ));
                }

                let behavior = match rate_limit.mode {
                    OutputRateLimitMode::All => OutputRateBehavior::All,
                    OutputRateLimitMode::First => OutputRateBehavior::First,
                    OutputRateLimitMode::Last => OutputRateBehavior::Last,
                    OutputRateLimitMode::Snapshot => unreachable!("Handled above"),
                };

                // The count must fit in i32. The API uses i32 for historical reasons and
                // the runtime turns it into usize. Moving the API to usize would be cleaner.
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
                // Fails if the value overflows when turned into milliseconds.
                let millis_u64 = rate_limit.unit.to_millis(rate_limit.value).ok_or_else(|| {
                    ConverterError::ConversionFailed(
                        "Time value overflowed during conversion to milliseconds".to_string(),
                    )
                })?;

                // Must also fit in i64. The API uses i64 for historical reasons. u64 would
                // suit a duration better and is a possible later change.
                let millis: i64 = millis_u64.try_into().map_err(|_| {
                    ConverterError::ConversionFailed(format!(
                        "Time value {} milliseconds exceeds maximum value {}",
                        millis_u64,
                        i64::MAX
                    ))
                })?;

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
        let has_join = !select.from.is_empty() && !select.from[0].joins.is_empty();

        // Pattern aliases such as e1, added to a copy of the catalog further down.
        let mut pattern_aliases: Vec<(String, String)> = Vec::new();

        // The selector stream name is empty for JOIN and pattern queries.
        let (input_stream, stream_name_for_selector) = if has_join {
            // The WHERE clause is passed in but not used for a JOIN.
            let join_input =
                Self::convert_join_from_clause(&select.from, &select.selection, catalog)?;
            (join_input, String::new())
        } else {
            // One relation in FROM. Only the first FROM item is used.
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

                    let relation = catalog
                        .get_relation(&stream_name)
                        .map_err(|_| ConverterError::SchemaNotFound(stream_name.clone()))?;

                    // A table is a lookup structure. It can only be read through a JOIN
                    // with a stream.
                    if relation.is_table() {
                        return Err(ConverterError::DirectTableQuery(stream_name));
                    }

                    let mut single_stream = SingleInputStream::new_basic(
                        stream_name.clone(),
                        false,      // is_inner_stream
                        false,      // is_fault_stream
                        None,       // stream_ref_id (no alias)
                        Vec::new(), // stream handlers
                    );

                    // The window handler goes first, so it sits before the WHERE filter.
                    if let Some(window_ast) = window.as_ref() {
                        single_stream =
                            Self::add_window_from_ast(single_stream, window_ast, catalog)?;
                    }

                    // WHERE runs before aggregation, as a filter on the input stream.
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
                    pattern_aliases = Self::extract_pattern_aliases(pattern);

                    // A WHERE clause is not applied to a pattern query.
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

        // For a pattern query, expand the SELECT against a copy of the catalog that knows
        // the pattern aliases. Other queries use the catalog as is.
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

        // GROUP BY takes plain column names only.
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

        // HAVING runs after aggregation. It uses the plain catalog, without pattern aliases.
        if let Some(having) = &select.having {
            let having_expr = Self::convert_expression(having, catalog)?;
            selector = selector.having(having_expr);
        }

        // ORDER BY takes plain column names only.
        if let Some(order_by) = order_by {
            let order_exprs = match &order_by.kind {
                sqlparser::ast::OrderByKind::Expressions(exprs) => exprs,
                sqlparser::ast::OrderByKind::All(_) => {
                    return Err(ConverterError::UnsupportedFeature(
                        "ORDER BY ALL not supported".to_string(),
                    ))
                }
            };

            for order_expr in order_exprs {
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

                let order = if let Some(asc) = order_expr.options.asc {
                    if asc {
                        crate::query_api::execution::query::selection::order_by_attribute::Order::Asc
                    } else {
                        crate::query_api::execution::query::selection::order_by_attribute::Order::Desc
                    }
                } else {
                    // ASC when no direction is given
                    crate::query_api::execution::query::selection::order_by_attribute::Order::Asc
                };

                selector = selector.order_by_with_order(variable, order);
            }
        }

        if let Some(limit_expr) = limit {
            let limit_const = Self::convert_to_constant(limit_expr)?;
            selector = selector
                .limit(limit_const)
                .map_err(|e| ConverterError::ConversionFailed(format!("LIMIT error: {}", e)))?;
        }

        if let Some(offset_obj) = offset {
            let offset_const = Self::convert_to_constant(&offset_obj.value)?;
            selector = selector
                .offset(offset_const)
                .map_err(|e| ConverterError::ConversionFailed(format!("OFFSET error: {}", e)))?;
        }

        // INSERT INTO the given target, or "OutputStream" when there is none.
        let target_stream_name = output_stream_name.unwrap_or_else(|| "OutputStream".to_string());
        let output_action = InsertIntoStreamAction {
            target_id: target_stream_name,
            is_inner_stream: false,
            is_fault_stream: false,
        };
        let output_stream = OutputStream::new(OutputStreamAction::InsertInto(output_action), None);

        let mut query = Query::query()
            .from(input_stream)
            .select(selector)
            .out_stream(output_stream);

        if let Some(rate) = output_rate {
            query = query.output(rate);
        }

        // Check WHERE and HAVING types. parse_sql_application runs this check again.
        let type_engine = TypeInferenceEngine::new(catalog);
        type_engine.validate_query(&query).map_err(|e| {
            ConverterError::ConversionFailed(format!("Type validation failed: {}", e))
        })?;

        Ok(query)
    }

    /// The stream name of the first FROM item. Unused.
    ///
    /// A pattern gives a made up name such as "Pattern_pattern", built from the mode.
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
                // A made up name for callers that need some string. The real pattern
                // conversion happens in convert_select.
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

    /// Convert a FROM clause with a JOIN into a JoinInputStream.
    ///
    /// Only the first JOIN is read. Any further JOINs are silently ignored. Plain JOIN
    /// becomes an inner join. The trigger is always `All`, with no WITHIN or PER.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown stream or table, a pattern on either side, a join
    /// kind other than inner or outer, or USING and NATURAL joins.
    fn convert_join_from_clause(
        from: &[sqlparser::ast::TableWithJoins],
        _where_clause: &Option<SqlExpr>, // not used yet, kept for a later filter change
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

        // Holds the whole left SingleInputStream, not just its name.
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

        // Only one JOIN is supported. Later ones are ignored.
        let join = &from[0].joins[0];

        // Holds the whole right SingleInputStream, not just its name.
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

        // Add the join aliases to a copy of the catalog so the ON condition can use them,
        // as in "a.symbol = b.company".
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

        // In standard SQL plain JOIN means INNER JOIN, so both map to InnerJoin.
        let (join_type, on_condition) = match &join.join_operator {
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                let cond = Self::extract_on_condition(constraint, &effective_catalog)?;
                (JoinType::InnerJoin, cond)
            }
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

        let join_stream = JoinInputStream::new(
            left_stream_name,
            join_type,
            right_stream_name,
            on_condition,
            EventTrigger::All, // events from both sides trigger the join
            None,              // WITHIN is not supported in SQL joins yet
            None,              // PER is not supported in SQL joins yet
        );

        Ok(InputStream::Join(Box::new(join_stream)))
    }

    /// Convert the join constraint. ON gives its condition and no constraint gives `None`.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedFeature` for USING and NATURAL, which are not supported yet.
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

    /// Add the window from the parsed WINDOW clause to the stream as a window handler.
    ///
    /// TUMBLING maps to the timeBatch window. Every parameter is converted as an
    /// expression, except the EXPRESSION window, which takes its condition as a string.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedFeature` for SLIDING windows, which are not built yet, and any
    /// error from converting a parameter.
    fn add_window_from_ast(
        stream: SingleInputStream,
        window: &sqlparser::ast::StreamingWindowSpec,
        catalog: &SqlCatalog,
    ) -> Result<SingleInputStream, ConverterError> {
        use sqlparser::ast::StreamingWindowSpec;

        match window {
            StreamingWindowSpec::Tumbling { duration } => {
                // Tumbling means time batches that do not overlap, the same as timeBatch.
                let duration_expr = Self::convert_expression(duration, catalog)?;
                Ok(stream.window(
                    None,
                    WINDOW_TYPE_TIME_BATCH.to_string(),
                    vec![duration_expr],
                ))
            }
            StreamingWindowSpec::Sliding { size, slide } => {
                // TODO: Build a sliding (hopping) window processor that takes a size and a
                // slide. The parameters are still converted so bad ones report first.
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
                // Parameters in order, as in (size, attr1, 'asc', attr2, 'desc', ...)
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
                // The condition is passed as a string constant, not parsed here.
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

    /// Convert a SQL expression into an EventFlux Expression.
    ///
    /// Columns are not checked against the catalog here. Some SQL forms are rewritten.
    /// IN becomes a chain of = joined by OR, BETWEEN becomes >= AND <=, IS NOT NULL
    /// becomes NOT IS NULL, unary minus becomes 0 - x, and LIKE, FLOOR, CEIL, SUBSTRING
    /// and TRIM become function calls. Number literals with a dot become DOUBLE, others
    /// LONG.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedFeature` or `InvalidExpression` for any form not listed in the
    /// match, including the NULL literal and identifiers with more than two parts.
    pub fn convert_expression(
        expr: &SqlExpr,
        catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        match expr {
            SqlExpr::Identifier(ident) => Ok(Expression::variable(ident.value.clone())),

            SqlExpr::CompoundIdentifier(parts) => {
                // stream.column or alias.column
                if parts.len() == 2 {
                    let stream_ref = parts[0].value.clone(); // stream name or alias, such as "t"
                    let column_name = parts[1].value.clone(); // column name, such as "symbol"

                    let var_with_stream = Variable::new(column_name).of_stream(stream_ref);
                    Ok(Expression::Variable(var_with_stream))
                } else {
                    Err(ConverterError::UnsupportedFeature(
                        "Multi-part identifiers not supported".to_string(),
                    ))
                }
            }

            SqlExpr::CompoundFieldAccess { root, access_chain } => {
                // Indexed access into a pattern event list, as in e1[0].price or
                // e1[last].symbol
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
                // Turn a SQL function call into an EventFlux function call
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
                    // -x becomes 0 - x
                    UnaryOperator::Minus => {
                        Ok(Expression::subtract(Expression::value_long(0), inner_expr))
                    }
                    // +x is just x
                    UnaryOperator::Plus => Ok(inner_expr),
                    _ => Err(ConverterError::UnsupportedFeature(format!(
                        "Unary operator {:?}",
                        op
                    ))),
                }
            }

            SqlExpr::Interval(interval) => {
                // INTERVAL '5' SECOND becomes a number of milliseconds
                Self::convert_interval_to_millis(interval)
            }

            SqlExpr::Case {
                case_token: _,
                end_token: _,
                operand,
                conditions,
                else_result,
            } => {
                if conditions.is_empty() {
                    return Err(ConverterError::InvalidExpression(
                        "CASE expression must have at least one WHEN clause".to_string(),
                    ));
                }

                // Only a simple CASE has an operand.
                let operand_expr = if let Some(ref op) = operand {
                    Some(Self::convert_expression(op, catalog)?)
                } else {
                    None
                };

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

                // A missing ELSE becomes ELSE NULL.
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
                // The target type goes through the same mapping as column types.
                let inner_expr = Self::convert_expression(expr, catalog)?;
                let target_type =
                    crate::sql_compiler::type_mapping::sql_type_to_attribute_type(data_type)
                        .map_err(|e| ConverterError::UnsupportedFeature(e.to_string()))?;
                Ok(Expression::cast(inner_expr, target_type))
            }

            SqlExpr::Nested(inner_expr) => {
                // Parentheses only group. Convert what is inside.
                Self::convert_expression(inner_expr, catalog)
            }

            // FLOOR(x) and CEIL(x). The forms that round to a date or time field are not
            // supported.
            SqlExpr::Floor { expr, field } => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                match field {
                    sqlparser::ast::CeilFloorKind::DateTimeField(
                        sqlparser::ast::DateTimeField::NoDateTime,
                    ) => Ok(Expression::function_no_ns(
                        "floor".to_string(),
                        vec![inner_expr],
                    )),
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
                    ) => Ok(Expression::function_no_ns(
                        "ceil".to_string(),
                        vec![inner_expr],
                    )),
                    _ => Err(ConverterError::UnsupportedFeature(
                        "CEIL TO datetime field not yet supported".to_string(),
                    )),
                }
            }

            // IS NOT NULL becomes NOT (IS NULL).
            SqlExpr::IsNull(expr) => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                Ok(Expression::is_null(inner_expr))
            }

            SqlExpr::IsNotNull(expr) => {
                let inner_expr = Self::convert_expression(expr, catalog)?;
                Ok(Expression::not(Expression::is_null(inner_expr)))
            }

            // x IN (a, b, c) becomes x = a OR x = b OR x = c. NOT IN wraps that in NOT.
            SqlExpr::InList {
                expr,
                list,
                negated,
            } => {
                if list.is_empty() {
                    // IN () is always false and NOT IN () is always true.
                    return Ok(Expression::value_bool(*negated));
                }

                let value_expr = Self::convert_expression(expr, catalog)?;

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

                let in_expr = result.unwrap(); // safe, the list is not empty

                if *negated {
                    Ok(Expression::not(in_expr))
                } else {
                    Ok(in_expr)
                }
            }

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

            // LIKE becomes a call to the built in like function. Any ESCAPE character is
            // ignored.
            SqlExpr::Like {
                negated,
                expr,
                pattern,
                escape_char: _,
                any: _,
            } => {
                let value_expr = Self::convert_expression(expr, catalog)?;
                let pattern_expr = Self::convert_expression(pattern, catalog)?;
                let like_expr =
                    Expression::function_no_ns("like".to_string(), vec![value_expr, pattern_expr]);
                if *negated {
                    Ok(Expression::not(like_expr))
                } else {
                    Ok(like_expr)
                }
            }

            SqlExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let str_expr = Self::convert_expression(expr, catalog)?;
                let mut args = vec![str_expr];

                // SQL counts from 1 but the substring function counts from 0, so subtract
                // 1 from the start.
                if let Some(from_expr) = substring_from {
                    let from_expr_converted = Self::convert_expression(from_expr, catalog)?;
                    let zero_based_start =
                        Expression::subtract(from_expr_converted, Expression::value_int(1));
                    args.push(zero_based_start);
                }
                if let Some(for_expr) = substring_for {
                    args.push(Self::convert_expression(for_expr, catalog)?);
                }

                Ok(Expression::function_no_ns("substring".to_string(), args))
            }

            // TRIM becomes trim(x). BOTH, LEADING, TRAILING and custom trim characters are
            // ignored.
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

    /// Convert a SQL INTERVAL into a LONG constant in milliseconds.
    ///
    /// The value must be a whole number, given bare or as a quoted string. No unit means
    /// milliseconds. The multiplication is not checked for overflow.
    ///
    /// # Approximations
    ///
    /// YEAR and MONTH use fixed lengths:
    /// - 1 YEAR = 365 days, ignoring leap years
    /// - 1 MONTH = 30 days, though real months run 28 to 31 days
    ///
    /// For exact time windows, use SECOND, MINUTE, HOUR or DAY.
    ///
    /// # Errors
    ///
    /// Returns an error for a value that is not a whole number, a value that is not a
    /// plain literal, or a unit other than YEAR, MONTH, DAY, HOUR, MINUTE or SECOND.
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

        let millis = match &interval.leading_field {
            Some(sqlparser::ast::DateTimeField::Year) => {
                // Fixed 365 days, no leap years
                value * 365 * 24 * 60 * 60 * 1000
            }
            Some(sqlparser::ast::DateTimeField::Month) => {
                // Fixed 30 days
                value * 30 * 24 * 60 * 60 * 1000
            }
            Some(sqlparser::ast::DateTimeField::Day) => value * 24 * 60 * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Hour) => value * 60 * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Minute) => value * 60 * 1000,
            Some(sqlparser::ast::DateTimeField::Second) => value * 1000,
            None => {
                // No unit means milliseconds
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

    /// Convert a SQL function call into an EventFlux function call.
    ///
    /// Names are matched in lower case and mapped to EventFlux names, such as pow to
    /// power and ifnull to default. SUBSTR and SUBSTRING with a start argument get 1
    /// taken off the start, because SQL counts from 1 and the function counts from 0.
    /// COUNT(*) becomes count() with no arguments.
    ///
    /// A call written with no argument list, such as CURRENT_TIMESTAMP, skips the name
    /// mapping. It becomes a call to the lower case name as is.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedFeature` for a name not in the mapping, a subquery argument, or
    /// a named or qualified wildcard argument.
    fn convert_function(
        func: &sqlparser::ast::Function,
        catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        // Strip backticks, as in `math:sin`. A namespaced name like that is still not in
        // the mapping below, so it is rejected.
        let raw_name = func.name.to_string();
        let func_name = raw_name
            .trim_start_matches('`')
            .trim_end_matches('`')
            .to_lowercase();

        let arg_list = match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => list,
            sqlparser::ast::FunctionArguments::None => {
                // Written with no parentheses, such as CURRENT_TIMESTAMP
                return Ok(Expression::function(None, func_name, Vec::new()));
            }
            sqlparser::ast::FunctionArguments::Subquery(_) => {
                return Err(ConverterError::UnsupportedFeature(
                    "Subquery as function argument not supported".to_string(),
                ));
            }
        };

        let mut args = Vec::new();
        for arg in &arg_list.args {
            match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    expr,
                )) => {
                    args.push(Self::convert_expression(expr, catalog)?);
                }
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    // A * argument, as in COUNT(*), is dropped. EventFlux count() takes no
                    // arguments.
                }
                _ => {
                    return Err(ConverterError::UnsupportedFeature(
                        "Function argument type not supported".to_string(),
                    ));
                }
            }
        }

        // SQL name to EventFlux name. Anything not listed is rejected.
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
            "locate" => "position", // MySQL name
            "instr" => "position",  // MySQL and Oracle name
            "ascii" => "ascii",
            "chr" => "chr",
            "char" => "chr", // MySQL name
            // SQL counts the start from 1 and the substring function counts from 0. With a
            // start argument, return early with 1 taken off it. With no start argument,
            // fall through to a plain substring call.
            "substr" | "substring" => {
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
            "ifnull" => "default", // IFNULL takes 2 arguments, like default
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

    /// Turn a LIMIT or OFFSET value into a LONG constant.
    ///
    /// # Errors
    ///
    /// Returns an error unless the value is a number literal that fits in i64.
    fn convert_to_constant(
        expr: &SqlExpr,
    ) -> Result<crate::query_api::expression::constant::Constant, ConverterError> {
        match expr {
            SqlExpr::Value(value_with_span) => {
                if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
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

    /// Convert indexed access into a pattern event list, as in e1[0].price or
    /// e1[last].symbol, into an IndexedVariable.
    ///
    /// A count step such as A{3,5} can match many events, and this picks one by index.
    ///
    /// # Errors
    ///
    /// Returns an error unless the root is a plain alias and the access is exactly one
    /// `[n]` or `[last]` followed by one `.attribute`.
    fn convert_compound_field_access(
        root: &SqlExpr,
        access_chain: &[AccessExpr],
        _catalog: &SqlCatalog,
    ) -> Result<Expression, ConverterError> {
        // The root is the pattern alias, such as "e1" in e1[0].price.
        let stream_id = match root {
            SqlExpr::Identifier(ident) => ident.value.clone(),
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Array access root must be an identifier (stream alias)".to_string(),
                ))
            }
        };

        // Exactly [index] then .attribute.
        if access_chain.len() != 2 {
            return Err(ConverterError::UnsupportedFeature(format!(
                "Expected [index].attribute format, got {} access elements",
                access_chain.len()
            )));
        }

        let event_index = match &access_chain[0] {
            AccessExpr::Subscript(Subscript::Index { index }) => Self::extract_event_index(index)?,
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "First element must be array index [n] or [last]".to_string(),
                ))
            }
        };

        let attribute_name = match &access_chain[1] {
            AccessExpr::Dot(SqlExpr::Identifier(ident)) => ident.value.clone(),
            _ => {
                return Err(ConverterError::UnsupportedFeature(
                    "Second element must be .attribute".to_string(),
                ))
            }
        };

        // The stream position is a -1 placeholder. The runtime expression parser looks up
        // the real position from the stream id.
        let indexed_var = match event_index {
            EventIndex::Numeric(idx) => IndexedVariable::new_with_index(attribute_name, idx)
                .of_stream_with_index(stream_id, -1),
            EventIndex::Last => {
                IndexedVariable::new_with_last(attribute_name).of_stream_with_index(stream_id, -1)
            }
        };

        Ok(Expression::IndexedVariable(Box::new(indexed_var)))
    }

    /// Read the index inside the brackets. A whole number gives a numeric index and
    /// `last`, in any letter case, gives `EventIndex::Last`. Anything else is an error.
    fn extract_event_index(index_expr: &SqlExpr) -> Result<EventIndex, ConverterError> {
        match index_expr {
            // e[0], e[1], e[2]
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
            // e[last]
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

    /// Every (alias, stream_name) pair in a pattern, left to right.
    ///
    /// For example `e1=A -> e2=B` gives `[("e1", "A"), ("e2", "B")]`. Absent steps have no
    /// alias and add nothing.
    pub fn extract_pattern_aliases(pattern: &PatternExpression) -> Vec<(String, String)> {
        let mut aliases = Vec::new();
        Self::collect_pattern_aliases(pattern, &mut aliases);
        aliases
    }

    /// Walk the pattern and append each (alias, stream_name) pair.
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
                // An absent step has a stream name but no alias.
            }
        }
    }

    // ============================================================================
    // Pattern Conversion Methods
    // ============================================================================

    /// Convert FROM PATTERN (...) or FROM SEQUENCE (...) into a StateInputStream.
    ///
    /// The pattern is checked against the grammar rules first, such as the EVERY limits
    /// and count bounds.
    ///
    /// # Errors
    ///
    /// Returns `ConversionFailed` listing every broken pattern rule. Returns
    /// `UnsupportedFeature` for WITHIN n EVENTS or a WITHIN time that is not a constant.
    /// Also returns any error from converting the pattern steps.
    pub fn convert_pattern_input(
        mode: &PatternMode,
        pattern: &PatternExpression,
        within: &Option<WithinConstraint>,
        catalog: &SqlCatalog,
    ) -> Result<InputStream, ConverterError> {
        PatternValidator::validate(mode, pattern).map_err(|errors| {
            let error_messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
            ConverterError::ConversionFailed(format!(
                "Pattern validation failed:\n  - {}",
                error_messages.join("\n  - ")
            ))
        })?;

        let state_element = Self::convert_pattern_expression(pattern, catalog)?;

        let within_time = match within {
            Some(WithinConstraint::Time(expr)) => {
                // Such as INTERVAL '10' SECOND, which becomes a millisecond constant.
                let time_expr = Self::convert_expression(expr, catalog)?;
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

        let state_input_stream = match mode {
            PatternMode::Pattern => StateInputStream::pattern_stream(state_element, within_time),
            PatternMode::Sequence => StateInputStream::sequence_stream(state_element, within_time),
        };

        Ok(InputStream::State(Box::new(state_input_stream)))
    }

    /// Convert a pattern AST node into a StateElement.
    ///
    /// `A -> B` becomes Next, AND and OR become Logical, a count becomes Count, NOT A FOR t
    /// becomes AbsentStream and parentheses are dropped. A stream that is not in the
    /// catalog is not rejected here.
    ///
    /// # Errors
    ///
    /// Returns `TableInPattern` for a table, `UnsupportedFeature` for a count on anything
    /// but a single stream, a count above `i32::MAX`, or an absent duration that is not a
    /// constant. Also returns any error from converting a filter or duration.
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

                // Only streams can feed a pattern or sequence. Tables are rejected.
                if let Ok(relation) = catalog.get_relation(&stream_id) {
                    if relation.is_table() {
                        return Err(ConverterError::TableInPattern(stream_id));
                    }
                }

                let mut single_stream = SingleInputStream::new_basic(
                    stream_id.clone(),
                    false, // is_inner
                    false, // is_fault
                    None,  // stream_ref_id, set below with as_ref when there is an alias
                    Vec::new(),
                );

                if let Some(alias_ident) = alias {
                    single_stream = single_stream.as_ref(alias_ident.value.clone());
                }

                if let Some(filter_expr) = filter {
                    let converted_filter = Self::convert_expression(filter_expr, catalog)?;
                    single_stream = single_stream.filter(converted_filter);
                }

                let stream_state = StreamStateElement::new(single_stream);
                Ok(StateElement::Stream(stream_state))
            }

            PatternExpression::Count {
                pattern,
                min_count,
                max_count,
            } => {
                // A count can only wrap a single stream step.
                let inner = Self::convert_pattern_expression(pattern, catalog)?;

                let stream_state = match inner {
                    StateElement::Stream(s) => s,
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Count quantifier can only be applied to stream patterns".to_string(),
                        ))
                    }
                };

                // CountStateElement stores i32, so larger counts are rejected.
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
                // The -> operator
                let first_state = Self::convert_pattern_expression(first, catalog)?;
                let second_state = Self::convert_pattern_expression(second, catalog)?;

                Ok(StateElement::Next(Box::new(NextStateElement::new(
                    first_state,
                    second_state,
                ))))
            }

            PatternExpression::Logical { left, op, right } => {
                // LogicalStateElement::new panics unless both sides are stream or absent
                // steps.
                let left_state = Self::convert_pattern_expression(left, catalog)?;
                let right_state = Self::convert_pattern_expression(right, catalog)?;

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
                let inner_state = Self::convert_pattern_expression(pattern, catalog)?;

                Ok(StateElement::Every(Box::new(EveryStateElement::new(
                    inner_state,
                ))))
            }

            PatternExpression::Absent {
                stream_name,
                duration,
            } => {
                // Unlike a stream step, an absent step is not checked for being a table.
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

                let single_stream = SingleInputStream::new_basic(
                    stream_id,
                    false, // is_inner
                    false, // is_fault
                    None,  // stream_ref_id
                    Vec::new(),
                );

                // The FOR duration must turn into a constant, such as an INTERVAL.
                let duration_expr = Self::convert_expression(duration, catalog)?;
                let duration_const = match duration_expr {
                    Expression::Constant(c) => Some(c),
                    _ => {
                        return Err(ConverterError::UnsupportedFeature(
                            "Absent pattern duration must be a constant".to_string(),
                        ))
                    }
                };

                Ok(StateElement::AbsentStream(AbsentStreamStateElement::new(
                    single_stream,
                    duration_const,
                )))
            }

            PatternExpression::Grouped { pattern } => {
                // Parentheses only group. Convert what is inside.
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
        // WHERE price gives a DOUBLE, not a BOOL
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
        // WHERE price * 2 gives a DOUBLE, not a BOOL
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
        // WHERE price > 100 gives a BOOL, so it is valid
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
        // HAVING SUM(volume) gives a LONG, not a BOOL
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
        // HAVING SUM(volume) > 1000 gives a BOOL, so it is valid
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
        // WHERE ROUND(price, 2) gives a DOUBLE, not a BOOL
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

        // A single stream step, e1=StockStream
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

        // A sequence, A -> B
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

        // A count, A{3,5}
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

        // A logical AND, A AND B
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

        // EVERY(A -> B)
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

        // INTERVAL '10' SECOND
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

        // WITHIN n EVENTS, which is not supported yet
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

        // Must fail with an unsupported feature error
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
