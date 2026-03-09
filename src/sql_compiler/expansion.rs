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

//! SELECT Expansion - Expand SELECT * using Schema
//!
//! Expands wildcard SELECT items using schema information from the catalog.

use std::collections::HashSet;

use sqlparser::ast::{Expr as SqlExpr, SelectItem};

use crate::query_api::execution::query::selection::selector::Selector;
use crate::query_api::expression::variable::Variable;
use crate::query_api::expression::Expression;

use super::catalog::SqlCatalog;
use super::converter::SqlConverter;
use super::error::ExpansionError;

/// SELECT Item Expander
pub struct SelectExpander;

impl SelectExpander {
    /// Expand SELECT items using catalog schema
    ///
    /// For pattern queries (when pattern_aliases is non-empty):
    /// - SELECT * is not supported
    /// - Unqualified column references require explicit alias
    /// - Duplicate output column names are rejected (require AS alias)
    pub fn expand_select_items(
        items: &[SelectItem],
        from_stream: &str,
        catalog: &SqlCatalog,
        pattern_aliases: &[(String, String)],
    ) -> Result<Selector, ExpansionError> {
        let mut selector = Selector::new();
        let is_pattern_query = !pattern_aliases.is_empty();

        // Track output column names to detect duplicates
        let mut output_column_names: Vec<String> = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    // SELECT * not supported in pattern queries
                    if is_pattern_query {
                        return Err(ExpansionError::UnsupportedFeature(
                            "SELECT * is not supported in pattern queries. \
                             Specify columns with pattern aliases (e.g., e1.col, e2.col)"
                                .to_string(),
                        ));
                    }

                    // Expand SELECT * using schema
                    let columns = catalog
                        .get_all_columns(from_stream)
                        .map_err(|_| ExpansionError::UnknownStream(from_stream.to_string()))?;

                    for column in columns {
                        let col_name = column.get_name().to_string();
                        output_column_names.push(col_name.clone());
                        selector = selector.select_variable(Variable::new(col_name));
                    }
                }

                SelectItem::QualifiedWildcard(kind, _) => {
                    // Handle qualified wildcard: stream.* or alias.*
                    let object_name = match kind {
                        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => name,
                        sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(_) => {
                            return Err(ExpansionError::UnsupportedFeature(
                                "Expression wildcards not supported".to_string(),
                            ));
                        }
                    };

                    let qualifier = object_name
                        .0
                        .last()
                        .and_then(|part| part.as_ident())
                        .map(|ident| ident.value.as_str())
                        .ok_or_else(|| {
                            ExpansionError::InvalidSelectItem(
                                "Invalid qualified wildcard".to_string(),
                            )
                        })?;

                    // For pattern queries, resolve alias to stream name
                    let stream_name = if is_pattern_query {
                        pattern_aliases
                            .iter()
                            .find(|(alias, _)| alias == qualifier)
                            .map(|(_, stream)| stream.as_str())
                            .unwrap_or(qualifier)
                    } else {
                        qualifier
                    };

                    let columns = catalog
                        .get_all_columns(stream_name)
                        .map_err(|_| ExpansionError::UnknownStream(stream_name.to_string()))?;

                    for column in columns {
                        let col_name = column.get_name().to_string();
                        output_column_names.push(col_name.clone());
                        selector = selector.select_variable(Variable::new(col_name));
                    }
                }

                SelectItem::UnnamedExpr(expr) => {
                    // Single expression without alias
                    if let SqlExpr::Identifier(ident) = expr {
                        // Simple unqualified column reference
                        let column_name = ident.value.clone();

                        // In pattern queries, unqualified columns are ambiguous
                        if is_pattern_query {
                            // Check if this column exists in any pattern stream
                            let matching_streams: Vec<&str> = pattern_aliases
                                .iter()
                                .filter(|(_, stream)| catalog.has_column(stream, &column_name))
                                .map(|(alias, _)| alias.as_str())
                                .collect();

                            if matching_streams.is_empty() {
                                return Err(ExpansionError::UnknownColumn(
                                    "pattern".to_string(),
                                    column_name,
                                ));
                            } else {
                                // Column exists but needs qualification
                                return Err(ExpansionError::AmbiguousColumn(format!(
                                    "Column '{}' must be qualified with pattern alias. \
                                     Use {} instead",
                                    column_name,
                                    matching_streams
                                        .iter()
                                        .map(|a| format!("{}.{}", a, column_name))
                                        .collect::<Vec<_>>()
                                        .join(" or ")
                                )));
                            }
                        }

                        // Validate column exists (non-pattern query)
                        if !catalog.has_column(from_stream, &column_name) {
                            return Err(ExpansionError::UnknownColumn(
                                from_stream.to_string(),
                                column_name,
                            ));
                        }

                        output_column_names.push(column_name.clone());
                        selector = selector.select_variable(Variable::new(column_name));
                    } else if let SqlExpr::CompoundIdentifier(parts) = expr {
                        // Qualified column reference (e.g., e1.price)
                        if parts.len() == 2 {
                            let column_name = parts[1].value.clone();

                            // Track output column name for duplicate detection
                            output_column_names.push(column_name.clone());

                            // Convert using SqlConverter (handles alias resolution)
                            let converted_expr = SqlConverter::convert_expression(expr, catalog)
                                .map_err(|e| ExpansionError::InvalidSelectItem(e.to_string()))?;

                            // Use column name as output name (without qualifier)
                            selector = selector.select(column_name, converted_expr);
                        } else {
                            // More complex qualified identifier
                            let converted_expr = SqlConverter::convert_expression(expr, catalog)
                                .map_err(|e| ExpansionError::InvalidSelectItem(e.to_string()))?;
                            let auto_alias =
                                format!("expr_{}", selector.get_selection_list().len());
                            output_column_names.push(auto_alias.clone());
                            selector = selector.select(auto_alias, converted_expr);
                        }
                    } else {
                        // Complex expression - convert using SqlConverter
                        let converted_expr = SqlConverter::convert_expression(expr, catalog)
                            .map_err(|e| ExpansionError::InvalidSelectItem(e.to_string()))?;

                        // Generate automatic alias for complex expression
                        let auto_alias = format!("expr_{}", selector.get_selection_list().len());
                        output_column_names.push(auto_alias.clone());
                        selector = selector.select(auto_alias, converted_expr);
                    }
                }

                SelectItem::ExprWithAlias { expr, alias } => {
                    // Expression with explicit alias - always allowed
                    let alias_name = alias.value.clone();
                    output_column_names.push(alias_name.clone());

                    if let SqlExpr::Identifier(ident) = expr {
                        // Simple column with alias
                        let column_name = ident.value.clone();

                        // In pattern queries, unqualified columns need qualification
                        if is_pattern_query {
                            let matching_streams: Vec<&str> = pattern_aliases
                                .iter()
                                .filter(|(_, stream)| catalog.has_column(stream, &column_name))
                                .map(|(alias, _)| alias.as_str())
                                .collect();

                            if matching_streams.is_empty() {
                                return Err(ExpansionError::UnknownColumn(
                                    "pattern".to_string(),
                                    column_name,
                                ));
                            } else {
                                return Err(ExpansionError::AmbiguousColumn(format!(
                                    "Column '{}' must be qualified with pattern alias. \
                                     Use {} AS {} instead",
                                    column_name,
                                    matching_streams
                                        .iter()
                                        .map(|a| format!("{}.{}", a, column_name))
                                        .collect::<Vec<_>>()
                                        .join(" or "),
                                    alias_name
                                )));
                            }
                        }

                        // Validate column exists (non-pattern query)
                        if !catalog.has_column(from_stream, &column_name) {
                            return Err(ExpansionError::UnknownColumn(
                                from_stream.to_string(),
                                column_name,
                            ));
                        }

                        selector = selector.select(alias_name, Expression::variable(column_name));
                    } else {
                        // Complex expression with alias - convert using SqlConverter
                        let converted_expr = SqlConverter::convert_expression(expr, catalog)
                            .map_err(|e| ExpansionError::InvalidSelectItem(e.to_string()))?;

                        selector = selector.select(alias_name, converted_expr);
                    }
                }
            }
        }

        // Check for duplicate output column names (especially important for pattern queries)
        if is_pattern_query {
            let mut seen: HashSet<&str> = HashSet::new();
            for name in &output_column_names {
                if !seen.insert(name.as_str()) {
                    return Err(ExpansionError::AmbiguousColumn(format!(
                        "Duplicate output column '{}'. \
                         For same-stream patterns, use explicit aliases: \
                         e1.{} AS {}_1, e2.{} AS {}_2",
                        name, name, name, name, name
                    )));
                }
            }
        }

        Ok(selector)
    }

    /// Get explicit column list from SELECT items (for schema validation)
    pub fn get_explicit_columns(items: &[SelectItem]) -> Vec<String> {
        let mut columns = Vec::new();

        for item in items {
            match item {
                SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                    columns.push(ident.value.clone());
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let SqlExpr::Identifier(ident) = expr {
                        columns.push(ident.value.clone());
                    } else {
                        columns.push(alias.value.clone());
                    }
                }
                _ => {}
            }
        }

        columns
    }

    /// Check if SELECT contains wildcard
    pub fn has_wildcard(items: &[SelectItem]) -> bool {
        items.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_api::definition::attribute::Type as AttributeType;
    use crate::query_api::definition::StreamDefinition;

    fn setup_catalog() -> SqlCatalog {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("col1".to_string(), AttributeType::STRING)
            .attribute("col2".to_string(), AttributeType::INT)
            .attribute("col3".to_string(), AttributeType::DOUBLE);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();
        catalog
    }

    #[test]
    fn test_expand_wildcard() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let catalog = setup_catalog();
        let sql = "SELECT * FROM TestStream";
        let statements = Parser::parse_sql(&GenericDialect, sql).unwrap();

        if let sqlparser::ast::Statement::Query(query) = &statements[0] {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                let selector = SelectExpander::expand_select_items(
                    &select.projection,
                    "TestStream",
                    &catalog,
                    &[], // No pattern aliases
                )
                .unwrap();

                // Should expand to 3 columns
                let output_attrs = selector.get_selection_list();
                assert_eq!(output_attrs.len(), 3);
            }
        }
    }

    #[test]
    fn test_explicit_columns() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let catalog = setup_catalog();
        let sql = "SELECT col1, col2 FROM TestStream";
        let statements = Parser::parse_sql(&GenericDialect, sql).unwrap();

        if let sqlparser::ast::Statement::Query(query) = &statements[0] {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                let selector = SelectExpander::expand_select_items(
                    &select.projection,
                    "TestStream",
                    &catalog,
                    &[], // No pattern aliases
                )
                .unwrap();

                let output_attrs = selector.get_selection_list();
                assert_eq!(output_attrs.len(), 2);
            }
        }
    }

    #[test]
    fn test_has_wildcard() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql1 = "SELECT * FROM TestStream";
        let statements1 = Parser::parse_sql(&GenericDialect, sql1).unwrap();
        if let sqlparser::ast::Statement::Query(query) = &statements1[0] {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                assert!(SelectExpander::has_wildcard(&select.projection));
            }
        }

        let sql2 = "SELECT col1, col2 FROM TestStream";
        let statements2 = Parser::parse_sql(&GenericDialect, sql2).unwrap();
        if let sqlparser::ast::Statement::Query(query) = &statements2[0] {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                assert!(!SelectExpander::has_wildcard(&select.projection));
            }
        }
    }

    #[test]
    fn test_unknown_column_error() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let catalog = setup_catalog();
        let sql = "SELECT unknown_col FROM TestStream";
        let statements = Parser::parse_sql(&GenericDialect, sql).unwrap();

        if let sqlparser::ast::Statement::Query(query) = &statements[0] {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                let result = SelectExpander::expand_select_items(
                    &select.projection,
                    "TestStream",
                    &catalog,
                    &[], // No pattern aliases
                );
                assert!(result.is_err());
            }
        }
    }
}
