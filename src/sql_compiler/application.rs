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

//! Application Parser - Parse Complete SQL Applications
//!
//! Parses multi-statement SQL applications with DDL and queries.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::query_api::definition::TriggerDefinition;
use crate::query_api::execution::query::output::output_stream::{
    DeleteStreamAction, OutputStreamAction, UpdateOrInsertStreamAction, UpdateStreamAction,
};
use crate::query_api::execution::query::output::stream::UpdateSet;
use crate::query_api::expression::Variable;
use sqlparser::ast::{
    CreateStreamTrigger, FromTable, StreamTriggerTiming, TableFactor, UpdateTableFromKind,
};

use super::catalog::{SqlApplication, SqlCatalog};
use super::converter::SqlConverter;
use super::error::ApplicationError;
use super::normalization::normalize_stream_syntax;
use super::type_inference::TypeInferenceEngine;
use super::type_mapping::sql_type_to_attribute_type;
use super::with_clause::{extract_with_options, validate_with_clause};

/// Convert a parsed EventFlux streaming trigger to a TriggerDefinition
fn convert_stream_trigger(
    trigger: &CreateStreamTrigger,
) -> Result<TriggerDefinition, ApplicationError> {
    let name = trigger.name.to_string();

    match &trigger.timing {
        StreamTriggerTiming::Start => Ok(TriggerDefinition::new(name).at("start".to_string())),
        StreamTriggerTiming::Every { interval_ms } => {
            // interval_ms is pre-computed at parse time using parse_streaming_time_duration_ms()
            Ok(TriggerDefinition::new(name).at_every(*interval_ms as i64))
        }
        StreamTriggerTiming::Cron(expr) => Ok(TriggerDefinition::new(name).at(expr.clone())),
    }
}

/// Validate expression types in a query using the type inference engine
fn validate_query_types(
    query: &crate::query_api::execution::Query,
    catalog: &SqlCatalog,
) -> Result<(), ApplicationError> {
    let type_engine = TypeInferenceEngine::new(catalog);
    type_engine
        .validate_query(query)
        .map_err(ApplicationError::Type)
}

/// Extract the table/stream name from a TableFactor, ignoring any alias.
///
/// When SQL uses aliases like `UPDATE stockTable AS s ... FROM updateStream AS u`,
/// calling `.to_string()` on the TableFactor returns "stockTable AS s" which fails
/// catalog lookups. This function extracts just the base name ("stockTable").
fn extract_table_name(table_factor: &TableFactor) -> Result<String, ApplicationError> {
    match table_factor {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        other => Err(ApplicationError::Converter(
            super::error::ConverterError::UnsupportedFeature(format!(
                "Expected table reference, got: {:?}",
                other
            )),
        )),
    }
}

/// Extract the alias from a TableFactor, if present.
///
/// Returns the alias name (e.g., "s" from "stockTable AS s") or None if no alias.
fn extract_table_alias(table_factor: &TableFactor) -> Option<String> {
    match table_factor {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    }
}

/// Extract the source stream alias from a SELECT query's FROM clause.
///
/// For queries like `SELECT ... FROM streamName AS alias`, returns the alias ("alias").
/// Returns None if no alias is present or if the FROM clause is not a simple table reference.
fn extract_query_source_alias(query: &sqlparser::ast::Query) -> Option<String> {
    // Navigate: Query -> body -> Select -> from -> first table -> relation -> alias
    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
        if let Some(first_from) = select.from.first() {
            return extract_table_alias(&first_from.relation);
        }
    }
    None
}

/// Parse a complete SQL application with multiple statements
pub fn parse_sql_application(sql: &str) -> Result<SqlApplication, ApplicationError> {
    let mut catalog = SqlCatalog::new();
    let mut execution_elements = Vec::new();

    // Normalize EventFlux-specific syntax for standard SQL parsing
    let normalized_sql = normalize_stream_syntax(sql);

    // Parse all statements at once using sqlparser
    let parsed_statements = Parser::parse_sql(&GenericDialect, &normalized_sql).map_err(|e| {
        ApplicationError::Converter(super::error::ConverterError::ConversionFailed(format!(
            "SQL parse error: {}",
            e
        )))
    })?;

    if parsed_statements.is_empty() {
        return Err(ApplicationError::EmptyApplication);
    }

    // Process each parsed statement
    for stmt in parsed_statements {
        match stmt {
            sqlparser::ast::Statement::CreateTable(create) => {
                let name = create.name.to_string();

                // Extract and validate WITH clause options
                let with_config = extract_with_options(&create.table_options)?;

                // Distinguish between TABLE and STREAM:
                // - STREAM: has 'type' property (source/sink/internal)
                // - TABLE: no 'type' but has 'extension' (e.g., cache/jdbc)
                // - STREAM: no 'type' and no 'extension' (pure internal stream)
                let is_table =
                    with_config.get("type").is_none() && with_config.get("extension").is_some();

                if is_table {
                    // This is a TABLE (e.g., CREATE TABLE T (...) WITH ('extension' = 'cache'))
                    let mut table_def =
                        crate::query_api::definition::TableDefinition::new(name.clone());

                    // Extract column definitions
                    for col in &create.columns {
                        let attr_type = sql_type_to_attribute_type(&col.data_type)?;
                        table_def = table_def.attribute(col.name.value.clone(), attr_type);
                    }

                    if !with_config.is_empty() {
                        validate_with_clause(&with_config)?;
                        table_def = table_def.with_config(with_config);
                    }

                    catalog.register_table(name, table_def);
                } else {
                    // This is a STREAM (e.g., CREATE STREAM S (...) or CREATE STREAM S (...) WITH ('type' = 'source'))
                    let mut stream_def =
                        crate::query_api::definition::StreamDefinition::new(name.clone());

                    // Extract column definitions
                    for col in &create.columns {
                        let attr_type = sql_type_to_attribute_type(&col.data_type)?;
                        stream_def = stream_def.attribute(col.name.value.clone(), attr_type);
                    }

                    if !with_config.is_empty() {
                        validate_with_clause(&with_config)?;
                        stream_def = stream_def.with_config(with_config);
                    }

                    catalog.register_stream(name, stream_def)?;
                }
            }
            sqlparser::ast::Statement::Query(query) => {
                // Convert query AST directly (no re-parsing!)
                let q = SqlConverter::convert_query_ast(&query, &catalog, None)?;

                // Type validation: validate expression types in the query
                validate_query_types(&q, &catalog)?;

                execution_elements.push(crate::query_api::execution::ExecutionElement::Query(q));
            }
            sqlparser::ast::Statement::Insert(insert) => {
                // Convert INSERT AST directly (no re-parsing!)
                let target_stream = match &insert.table {
                    sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                    sqlparser::ast::TableObject::TableFunction(_) => {
                        return Err(ApplicationError::Converter(
                            super::error::ConverterError::UnsupportedFeature(
                                "Table functions not supported in INSERT".to_string(),
                            ),
                        ))
                    }
                };

                let source = insert.source.as_ref().ok_or_else(|| {
                    ApplicationError::Converter(super::error::ConverterError::UnsupportedFeature(
                        "INSERT without SELECT source not supported".to_string(),
                    ))
                })?;

                let q = SqlConverter::convert_query_ast(source, &catalog, Some(target_stream))?;

                // Type validation: validate expression types in the query
                validate_query_types(&q, &catalog)?;

                execution_elements.push(crate::query_api::execution::ExecutionElement::Query(q));
            }
            sqlparser::ast::Statement::Partition {
                partition_keys,
                body,
            } => {
                // Handle partition directly without re-parsing
                let partition = SqlConverter::convert_partition(&partition_keys, &body, &catalog)?;
                execution_elements.push(crate::query_api::execution::ExecutionElement::Partition(
                    partition,
                ));
            }
            sqlparser::ast::Statement::CreateStreamTrigger(stream_trigger) => {
                // Convert EventFlux streaming trigger to TriggerDefinition
                let trigger_def = convert_stream_trigger(&stream_trigger)?;
                catalog.register_trigger(trigger_def);
            }
            sqlparser::ast::Statement::Update {
                table,
                assignments,
                from,
                selection,
                ..
            } => {
                // Stream-triggered UPDATE: UPDATE table SET ... FROM stream WHERE ...
                // Extract target table name and alias
                let target_table = extract_table_name(&table.relation)?;
                let target_alias = extract_table_alias(&table.relation);

                // Extract source stream from FROM clause (name and alias)
                let (source_stream, source_alias) = match from {
                    Some(UpdateTableFromKind::AfterSet(tables))
                    | Some(UpdateTableFromKind::BeforeSet(tables)) => {
                        if tables.is_empty() {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "UPDATE requires FROM clause with source stream".to_string(),
                                ),
                            ));
                        }
                        if tables.len() > 1 {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "UPDATE with multiple source streams not supported".to_string(),
                                ),
                            ));
                        }
                        if !tables[0].joins.is_empty() {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "UPDATE with JOIN source not supported".to_string(),
                                ),
                            ));
                        }
                        // Check for streaming WINDOW on the source
                        if let TableFactor::Table {
                            window: Some(_), ..
                        } = &tables[0].relation
                        {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "UPDATE with WINDOW source not supported".to_string(),
                                ),
                            ));
                        }
                        (
                            extract_table_name(&tables[0].relation)?,
                            extract_table_alias(&tables[0].relation),
                        )
                    }
                    None => {
                        return Err(ApplicationError::Converter(
                            super::error::ConverterError::UnsupportedFeature(
                                "UPDATE requires FROM clause with source stream".to_string(),
                            ),
                        ))
                    }
                };

                // Convert WHERE condition to Expression
                let on_update_expression = match selection {
                    Some(expr) => SqlConverter::convert_expression(&expr, &catalog)?,
                    None => {
                        return Err(ApplicationError::Converter(
                            super::error::ConverterError::UnsupportedFeature(
                                "UPDATE requires WHERE clause".to_string(),
                            ),
                        ))
                    }
                };

                // Convert SET assignments to UpdateSet
                let mut update_set = UpdateSet::new();
                for assignment in &assignments {
                    // Extract column name and validate any qualifier matches target table
                    let column_name = match &assignment.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(obj_name) => {
                            let parts: Vec<_> = obj_name
                                .0
                                .iter()
                                .filter_map(|part| part.as_ident())
                                .map(|ident| ident.value.clone())
                                .collect();

                            if parts.len() > 1 {
                                // Has qualifier - validate it matches target table or alias
                                let qualifier = &parts[0];
                                let is_valid_qualifier = qualifier == &target_table
                                    || target_alias.as_ref().map_or(false, |a| a == qualifier);

                                if !is_valid_qualifier {
                                    return Err(ApplicationError::Converter(
                                        super::error::ConverterError::InvalidExpression(
                                            format!(
                                                "SET target '{}' has invalid qualifier '{}' - must be target table '{}'{}",
                                                assignment.target,
                                                qualifier,
                                                target_table,
                                                target_alias.as_ref().map_or(String::new(), |a| format!(" or alias '{}'", a))
                                            ),
                                        ),
                                    ));
                                }
                            }

                            // Use the last part as the column name
                            parts
                                .last()
                                .cloned()
                                .unwrap_or_else(|| assignment.target.to_string())
                        }
                        sqlparser::ast::AssignmentTarget::Tuple(_) => {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(format!(
                                    "Tuple assignments like 'SET {} = ...' are not supported. \
                                         Use individual column assignments instead.",
                                    assignment.target
                                )),
                            ));
                        }
                    };
                    let value_expr = SqlConverter::convert_expression(&assignment.value, &catalog)?;
                    update_set =
                        update_set.add_set_attribute(Variable::new(column_name), value_expr);
                }

                // Build query with UPDATE output action
                let q = SqlConverter::convert_mutation_query(
                    &source_stream,
                    &catalog,
                    OutputStreamAction::Update(UpdateStreamAction {
                        target_id: target_table,
                        on_update_expression,
                        update_set_clause: Some(update_set),
                        target_alias,
                        source_alias,
                    }),
                )?;

                validate_query_types(&q, &catalog)?;
                execution_elements.push(crate::query_api::execution::ExecutionElement::Query(q));
            }
            sqlparser::ast::Statement::Delete(delete) => {
                // Stream-triggered DELETE: DELETE FROM table USING stream WHERE ...
                // Extract target table from first FROM clause (name and alias)
                let (target_table, target_alias) = match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        if tables.is_empty() {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE requires FROM clause with target table".to_string(),
                                ),
                            ));
                        }
                        if tables.len() > 1 {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE with multiple target tables not supported".to_string(),
                                ),
                            ));
                        }
                        (
                            extract_table_name(&tables[0].relation)?,
                            extract_table_alias(&tables[0].relation),
                        )
                    }
                };

                // Extract source stream from USING clause (name and alias)
                let (source_stream, source_alias) = match &delete.using {
                    Some(tables) => {
                        if tables.is_empty() {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE requires USING clause with source stream".to_string(),
                                ),
                            ));
                        }
                        if tables.len() > 1 {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE with multiple source streams not supported".to_string(),
                                ),
                            ));
                        }
                        if !tables[0].joins.is_empty() {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE with JOIN source not supported".to_string(),
                                ),
                            ));
                        }
                        // Check for streaming WINDOW on the source
                        if let TableFactor::Table {
                            window: Some(_), ..
                        } = &tables[0].relation
                        {
                            return Err(ApplicationError::Converter(
                                super::error::ConverterError::UnsupportedFeature(
                                    "DELETE with WINDOW source not supported".to_string(),
                                ),
                            ));
                        }
                        (
                            extract_table_name(&tables[0].relation)?,
                            extract_table_alias(&tables[0].relation),
                        )
                    }
                    None => {
                        return Err(ApplicationError::Converter(
                            super::error::ConverterError::UnsupportedFeature(
                                "DELETE requires USING clause with source stream".to_string(),
                            ),
                        ))
                    }
                };

                // Convert WHERE condition to Expression
                let on_delete_expression = match &delete.selection {
                    Some(expr) => SqlConverter::convert_expression(expr, &catalog)?,
                    None => {
                        return Err(ApplicationError::Converter(
                            super::error::ConverterError::UnsupportedFeature(
                                "DELETE requires WHERE clause".to_string(),
                            ),
                        ))
                    }
                };

                // Build query with DELETE output action
                let q = SqlConverter::convert_mutation_query(
                    &source_stream,
                    &catalog,
                    OutputStreamAction::Delete(DeleteStreamAction {
                        target_id: target_table,
                        on_delete_expression,
                        target_alias,
                        source_alias,
                    }),
                )?;

                validate_query_types(&q, &catalog)?;
                execution_elements.push(crate::query_api::execution::ExecutionElement::Query(q));
            }
            sqlparser::ast::Statement::Upsert {
                table,
                source,
                on_condition,
            } => {
                // UPSERT: UPSERT INTO table SELECT ... FROM stream AS alias ON condition
                // Note: Current UPSERT syntax doesn't support table alias (UPSERT INTO table AS t)
                let target_table = table.to_string();

                // Extract source stream alias from SELECT ... FROM stream AS alias
                let source_alias = extract_query_source_alias(&source);

                // Convert ON condition to Expression
                let on_update_expression =
                    SqlConverter::convert_expression(&on_condition, &catalog)?;

                // Build query from source (SELECT ... FROM stream)
                // The source query provides the input stream and values
                let q = SqlConverter::convert_upsert_query(
                    &source,
                    &catalog,
                    OutputStreamAction::UpdateOrInsert(UpdateOrInsertStreamAction {
                        target_id: target_table,
                        on_update_expression,
                        update_set_clause: None, // All columns from SELECT are used
                        target_alias: None,      // UPSERT syntax doesn't support table alias
                        source_alias,            // Alias from SELECT ... FROM stream AS alias
                    }),
                )?;

                validate_query_types(&q, &catalog)?;
                execution_elements.push(crate::query_api::execution::ExecutionElement::Query(q));
            }
            _ => {
                return Err(ApplicationError::Converter(
                    super::error::ConverterError::UnsupportedFeature(format!(
                        "Unsupported statement type: {}",
                        stmt
                    )),
                ))
            }
        }
    }

    Ok(SqlApplication::new(catalog, execution_elements))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_application() {
        let sql = r#"
            CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

            SELECT symbol, price
            FROM StockStream
            WHERE price > 100;
        "#;

        let app = parse_sql_application(sql).unwrap();
        assert!(!app.catalog.is_empty());
        assert_eq!(app.execution_elements.len(), 1);
    }

    #[test]
    fn test_parse_multiple_queries() {
        let sql = r#"
            CREATE STREAM Input1 (x INT);
            CREATE STREAM Input2 (y INT);

            SELECT x FROM Input1;
            SELECT y FROM Input2;
        "#;

        let app = parse_sql_application(sql).unwrap();
        assert_eq!(app.catalog.get_stream_names().len(), 2);
        assert_eq!(app.execution_elements.len(), 2);
    }

    #[test]
    fn test_parse_with_window() {
        let sql = r#"
            CREATE STREAM SensorStream (temp DOUBLE);

            SELECT temp
            FROM SensorStream
            WINDOW('length', 10);
        "#;

        let app = parse_sql_application(sql).unwrap();
        assert_eq!(app.execution_elements.len(), 1);
    }

    #[test]
    fn test_empty_application_error() {
        let sql = "";
        let result = parse_sql_application(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_stream_in_query() {
        let sql = r#"
            CREATE STREAM Known (x INT);
            SELECT y FROM Unknown;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_wildcard() {
        let sql = r#"
            CREATE STREAM AllColumns (a INT, b DOUBLE, c VARCHAR);
            SELECT * FROM AllColumns;
        "#;

        let app = parse_sql_application(sql).unwrap();
        assert_eq!(app.execution_elements.len(), 1);
    }

    // ========================================================================
    // Integration Tests for WITH Clause (M1 Milestone)
    // ========================================================================

    #[test]
    fn test_create_stream_with_source_config() {
        let sql = r#"
            CREATE STREAM DataStream (id INT, value DOUBLE)
            WITH (
                type = 'source',
                extension = 'kafka',
                format = 'json'
            );
        "#;

        let app = parse_sql_application(sql);
        assert!(
            app.is_ok(),
            "Failed to parse source stream with WITH clause: {:?}",
            app.err()
        );

        let app = app.unwrap();
        assert!(!app.catalog.is_empty());
        assert!(app.catalog.get_stream("DataStream").is_ok());
    }

    #[test]
    fn test_create_stream_with_sink_config() {
        let sql = r#"
            CREATE STREAM OutputStream (result VARCHAR, count INT)
            WITH (
                type = 'sink',
                extension = 'log',
                format = 'text'
            );
        "#;

        let app = parse_sql_application(sql);
        assert!(
            app.is_ok(),
            "Failed to parse sink stream with WITH clause: {:?}",
            app.err()
        );

        let app = app.unwrap();
        assert!(app.catalog.get_stream("OutputStream").is_ok());
    }

    #[test]
    fn test_create_stream_with_internal_config() {
        let sql = r#"
            CREATE STREAM TempStream (id INT, value DOUBLE)
            WITH (
                type = 'internal'
            );
        "#;

        let app = parse_sql_application(sql);
        assert!(
            app.is_ok(),
            "Failed to parse internal stream with WITH clause: {:?}",
            app.err()
        );
    }

    #[test]
    fn test_create_stream_without_with_clause() {
        let sql = r#"
            CREATE STREAM SimpleStream (x INT, y DOUBLE);
        "#;

        let app = parse_sql_application(sql);
        assert!(app.is_ok(), "Failed to parse stream without WITH clause");
    }

    #[test]
    fn test_create_stream_with_validation_error_missing_extension() {
        let sql = r#"
            CREATE STREAM BadStream (id INT)
            WITH (
                type = 'source',
                format = 'json'
            );
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "Should fail validation: missing extension");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("extension"),
            "Error should mention missing extension: {}",
            err
        );
    }

    #[test]
    fn test_create_stream_without_format_allowed() {
        // Format is now optional at SQL level (some sources like timer use binary passthrough)
        let sql = r#"
            CREATE STREAM TimerStream (tick STRING)
            WITH (
                type = 'source',
                extension = 'timer'
            );
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "Should allow missing format for sources like timer"
        );

        // Note: Sources that require format (like Kafka) will fail at initialization time,
        // not at SQL parsing time. This allows binary passthrough sources (timer) to work.
    }

    #[test]
    fn test_create_stream_with_validation_error_internal_with_extension() {
        let sql = r#"
            CREATE STREAM BadStream (id INT)
            WITH (
                type = 'internal',
                extension = 'kafka'
            );
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "Should fail validation: internal stream with extension"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("internal") && err.contains("extension"),
            "Error should mention internal + extension conflict: {}",
            err
        );
    }

    #[test]
    fn test_create_stream_with_validation_error_internal_with_format() {
        let sql = r#"
            CREATE STREAM BadStream (id INT)
            WITH (
                type = 'internal',
                format = 'json'
            );
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "Should fail validation: internal stream with format"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("internal") && err.contains("format"),
            "Error should mention internal + format conflict: {}",
            err
        );
    }

    #[test]
    fn test_create_stream_with_validation_error_invalid_type() {
        let sql = r#"
            CREATE STREAM BadStream (id INT)
            WITH (
                type = 'invalid_type'
            );
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "Should fail validation: invalid stream type"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid stream type"),
            "Error should mention invalid type: {}",
            err
        );
    }

    #[test]
    fn test_integration_create_stream_with_query() {
        let sql = r#"
            CREATE STREAM DataStream (id INT, value DOUBLE)
            WITH (
                type = 'source',
                extension = 'kafka',
                format = 'json'
            );

            SELECT id, value
            FROM DataStream
            WHERE value > 100;
        "#;

        let app = parse_sql_application(sql);
        assert!(
            app.is_ok(),
            "Failed to parse application with stream + query: {:?}",
            app.err()
        );

        let app = app.unwrap();
        assert_eq!(app.catalog.get_stream_names().len(), 1);
        assert_eq!(app.execution_elements.len(), 1);
    }

    #[test]
    fn test_integration_merge_rust_defaults_with_sql_with() {
        // This test demonstrates the multi-layer configuration concept
        // In a real application, Rust defaults and TOML configs would be merged
        // with SQL WITH clause options (highest priority)

        let sql = r#"
            CREATE STREAM DataStream (id INT, value DOUBLE)
            WITH (
                type = 'source',
                extension = 'http',
                format = 'json'
            );
        "#;

        let app = parse_sql_application(sql);
        assert!(app.is_ok(), "Multi-layer config test should succeed");

        // Note: In production, the FlatConfig would be stored and merged with
        // application-level and stream-level TOML configurations before being
        // used to initialize the actual source/sink extensions.
    }

    // ========================================================================
    // WITH Config Storage Tests - Verify end-to-end flow
    // ========================================================================

    #[test]
    fn test_with_config_stored_in_stream_definition() {
        let sql = r#"
            CREATE STREAM TimerInput (tick BIGINT)
            WITH (
                type = 'source',
                extension = 'timer',
                "timer.interval" = '5000',
                format = 'json'
            );
        "#;

        let app = parse_sql_application(sql).expect("Failed to parse SQL with WITH clause");

        // Verify stream was registered
        let stream_def = app
            .catalog
            .get_stream("TimerInput")
            .expect("Stream not found in catalog");

        // Verify WITH config was stored
        assert!(
            stream_def.with_config.is_some(),
            "WITH config should be stored in StreamDefinition"
        );

        let config = stream_def.with_config.as_ref().unwrap();

        // Verify all properties were captured
        assert_eq!(
            config.get("type"),
            Some(&"source".to_string()),
            "type property should be stored"
        );
        assert_eq!(
            config.get("extension"),
            Some(&"timer".to_string()),
            "extension property should be stored"
        );
        assert_eq!(
            config.get("timer.interval"),
            Some(&"5000".to_string()),
            "timer.interval property should be stored"
        );
        assert_eq!(
            config.get("format"),
            Some(&"json".to_string()),
            "format property should be stored"
        );
    }

    #[test]
    fn test_stream_without_with_clause_has_no_config() {
        let sql = r#"
            CREATE STREAM SimpleStream (id INT, value DOUBLE);
        "#;

        let app = parse_sql_application(sql).expect("Failed to parse stream without WITH");

        let stream_def = app
            .catalog
            .get_stream("SimpleStream")
            .expect("Stream not found");

        // Streams without WITH clause should have None for with_config
        assert!(
            stream_def.with_config.is_none(),
            "Stream without WITH clause should have no stored config"
        );
    }

    #[test]
    fn test_with_config_custom_properties_stored() {
        let sql = r#"
            CREATE STREAM KafkaInput (message VARCHAR)
            WITH (
                type = 'source',
                extension = 'kafka',
                format = 'json',
                "kafka.bootstrap.servers" = 'localhost:9092',
                "kafka.topic" = 'events',
                "kafka.consumer.group" = 'my-group',
                "json.date-format" = 'yyyy-MM-dd HH:mm:ss'
            );
        "#;

        let app = parse_sql_application(sql).expect("Failed to parse Kafka source with properties");

        let stream_def = app
            .catalog
            .get_stream("KafkaInput")
            .expect("Stream not found");

        assert!(stream_def.with_config.is_some());
        let config = stream_def.with_config.as_ref().unwrap();

        // Verify all custom properties stored
        assert_eq!(
            config.get("kafka.bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(config.get("kafka.topic"), Some(&"events".to_string()));
        assert_eq!(
            config.get("kafka.consumer.group"),
            Some(&"my-group".to_string())
        );
        assert_eq!(
            config.get("json.date-format"),
            Some(&"yyyy-MM-dd HH:mm:ss".to_string())
        );
    }

    #[test]
    fn test_with_config_multiple_streams_independent() {
        let sql = r#"
            CREATE STREAM TimerSource (tick BIGINT)
            WITH (
                type = 'source',
                extension = 'timer',
                "timer.interval" = '1000',
                format = 'json'
            );

            CREATE STREAM LogSink (tick BIGINT)
            WITH (
                type = 'sink',
                extension = 'log',
                format = 'text',
                "log.prefix" = '[EVENT]'
            );
        "#;

        let app = parse_sql_application(sql).expect("Failed to parse multiple streams");

        let timer_def = app.catalog.get_stream("TimerSource").unwrap();
        let log_def = app.catalog.get_stream("LogSink").unwrap();

        // Both should have configs
        assert!(timer_def.with_config.is_some());
        assert!(log_def.with_config.is_some());

        let timer_config = timer_def.with_config.as_ref().unwrap();
        let log_config = log_def.with_config.as_ref().unwrap();

        // Verify independence - timer config shouldn't have log properties
        assert_eq!(timer_config.get("extension"), Some(&"timer".to_string()));
        assert_eq!(
            timer_config.get("timer.interval"),
            Some(&"1000".to_string())
        );
        assert!(timer_config.get("log.prefix").is_none());

        // Log config shouldn't have timer properties
        assert_eq!(log_config.get("extension"), Some(&"log".to_string()));
        assert_eq!(log_config.get("log.prefix"), Some(&"[EVENT]".to_string()));
        assert!(log_config.get("timer.interval").is_none());
    }

    #[test]
    fn test_with_config_empty_clause_treated_as_none() {
        let sql = r#"
            CREATE STREAM EmptyWith (x INT) WITH ();
        "#;

        let app = parse_sql_application(sql).expect("Failed to parse stream with empty WITH");

        let stream_def = app.catalog.get_stream("EmptyWith").unwrap();

        // Empty WITH clause should not store config
        assert!(
            stream_def.with_config.is_none(),
            "Empty WITH clause should not store configuration"
        );
    }

    // ========================================================================
    // Parser Tests for UPDATE, DELETE, UPSERT syntax
    // ========================================================================

    #[test]
    fn test_sqlparser_update_syntax() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Test UPDATE with FROM
        let update_sql = "UPDATE stockTable SET price = 100.0 FROM updateStream WHERE stockTable.symbol = updateStream.symbol";
        let result = Parser::parse_sql(&GenericDialect, update_sql);
        assert!(
            result.is_ok(),
            "UPDATE with FROM should parse: {:?}",
            result
        );
        if let Ok(stmts) = &result {
            println!("UPDATE statement: {:?}", stmts[0]);
            if let sqlparser::ast::Statement::Update {
                table,
                from,
                selection,
                ..
            } = &stmts[0]
            {
                println!("  table: {}", table);
                println!("  from: {:?}", from);
                println!("  selection: {:?}", selection);
            }
        }
    }

    #[test]
    fn test_sqlparser_delete_syntax() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Test DELETE with USING (PostgreSQL style)
        let delete_sql = "DELETE FROM stockTable USING deleteStream WHERE stockTable.symbol = deleteStream.symbol";
        let result = Parser::parse_sql(&GenericDialect, delete_sql);
        assert!(result.is_ok(), "DELETE USING should parse: {:?}", result);
        if let Ok(stmts) = &result {
            if let sqlparser::ast::Statement::Delete(delete) = &stmts[0] {
                println!("DELETE statement:");
                println!("  from: {:?}", delete.from);
                println!("  using: {:?}", delete.using);
                println!("  selection: {:?}", delete.selection);
            }
        }
    }

    #[test]
    fn test_sqlparser_upsert_syntax() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Test UPSERT (our custom syntax)
        let upsert_sql = "UPSERT INTO stockTable SELECT symbol, price, volume FROM stockStream ON stockTable.symbol = stockStream.symbol";
        let result = Parser::parse_sql(&GenericDialect, upsert_sql);
        assert!(result.is_ok(), "UPSERT should parse: {:?}", result);
        if let Ok(stmts) = &result {
            if let sqlparser::ast::Statement::Upsert {
                table,
                source,
                on_condition,
            } = &stmts[0]
            {
                println!("UPSERT statement:");
                println!("  table: {}", table);
                println!("  source: {:?}", source);
                println!("  on_condition: {:?}", on_condition);
            }
        }
    }

    // ========================================================================
    // Table Mutation Integration Tests
    // ========================================================================

    #[test]
    fn test_parse_update_statement() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPDATE stockTable SET price = updateStream.newPrice
            FROM updateStream
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE statement should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_parse_delete_statement() {
        let sql = r#"
            CREATE STREAM deleteStream (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            DELETE FROM stockTable
            USING deleteStream
            WHERE stockTable.symbol = deleteStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "DELETE statement should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_parse_upsert_statement() {
        let sql = r#"
            CREATE STREAM stockStream (symbol VARCHAR, price DOUBLE, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPSERT INTO stockTable
            SELECT symbol, price, volume FROM stockStream
            ON stockTable.symbol = stockStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPSERT statement should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_parse_upsert_with_source_alias() {
        // Test that UPSERT with source stream alias parses correctly
        // The alias should be preserved for ON condition parsing
        let sql = r#"
            CREATE STREAM stockStream (symbol VARCHAR, price DOUBLE, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPSERT INTO stockTable
            SELECT symbol, price, volume FROM stockStream AS ss
            ON stockTable.symbol = ss.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPSERT with source alias should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_parse_update_with_aliases() {
        // Test that UPDATE with aliases parses correctly
        // The alias should be ignored for catalog lookups
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPDATE stockTable AS s SET price = u.newPrice
            FROM updateStream AS u
            WHERE s.symbol = u.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE with aliases should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_parse_delete_with_aliases() {
        // Test that DELETE with aliases parses correctly
        // The alias should be ignored for catalog lookups
        let sql = r#"
            CREATE STREAM deleteStream (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            DELETE FROM stockTable AS s
            USING deleteStream AS d
            WHERE s.symbol = d.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "DELETE with aliases should parse: {:?}",
            result.err()
        );
        let app = result.unwrap();
        assert_eq!(
            app.execution_elements.len(),
            1,
            "Should have one execution element"
        );
    }

    #[test]
    fn test_update_missing_from_clause_error() {
        let sql = r#"
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPDATE stockTable SET price = 100 WHERE symbol = 'ABC';
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPDATE without FROM clause should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("FROM") || err.contains("source stream"),
            "Error should mention missing FROM clause: {}",
            err
        );
    }

    #[test]
    fn test_delete_missing_using_clause_error() {
        let sql = r#"
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            DELETE FROM stockTable WHERE symbol = 'ABC';
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "DELETE without USING clause should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("USING") || err.contains("source stream"),
            "Error should mention missing USING clause: {}",
            err
        );
    }

    #[test]
    fn test_update_missing_where_clause_error() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            UPDATE stockTable SET price = updateStream.newPrice
            FROM updateStream;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPDATE without WHERE clause should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("WHERE"),
            "Error should mention missing WHERE clause: {}",
            err
        );
    }

    #[test]
    fn test_update_nonexistent_source_stream_error() {
        let sql = r#"
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            UPDATE stockTable SET price = nonExistentStream.newPrice
            FROM nonExistentStream
            WHERE stockTable.symbol = nonExistentStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "UPDATE with non-existent source stream should fail"
        );
    }

    #[test]
    fn test_delete_nonexistent_source_stream_error() {
        let sql = r#"
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            DELETE FROM stockTable
            USING nonExistentStream
            WHERE stockTable.symbol = nonExistentStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "DELETE with non-existent source stream should fail"
        );
    }

    #[test]
    fn test_upsert_nonexistent_source_stream_error() {
        let sql = r#"
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            UPSERT INTO stockTable
            SELECT symbol, price FROM nonExistentStream
            ON stockTable.symbol = nonExistentStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "UPSERT with non-existent source stream should fail"
        );
    }

    #[test]
    fn test_delete_missing_where_clause_error() {
        let sql = r#"
            CREATE STREAM deleteStream (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            DELETE FROM stockTable
            USING deleteStream;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "DELETE without WHERE clause should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("WHERE"),
            "Error should mention missing WHERE clause: {}",
            err
        );
    }

    #[test]
    fn test_update_with_multiple_set_columns_parses() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE, newVolume INT);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE, volume INT);

            UPDATE stockTable SET price = updateStream.newPrice, volume = updateStream.newVolume
            FROM updateStream
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE with multiple SET columns should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_upsert_with_complex_on_condition_parses() {
        let sql = r#"
            CREATE STREAM stockStream (symbol VARCHAR, price DOUBLE, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE, volume INT);

            UPSERT INTO stockTable
            SELECT symbol, price, volume FROM stockStream
            ON stockTable.symbol = stockStream.symbol AND stockTable.price < stockStream.price;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPSERT with complex ON condition should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_update_with_expression_in_set_parses() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, multiplier DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            UPDATE stockTable SET price = updateStream.multiplier * 100.0
            FROM updateStream
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE with expression in SET clause should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_update_multiple_sources_rejected() {
        let sql = r#"
            CREATE STREAM stream1 (symbol VARCHAR, price DOUBLE);
            CREATE STREAM stream2 (symbol VARCHAR, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE, volume INT);

            UPDATE stockTable SET price = stream1.price
            FROM stream1, stream2
            WHERE stockTable.symbol = stream1.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "UPDATE with multiple source streams should fail"
        );
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("multiple source streams"),
            "Error should mention multiple sources: {}",
            err
        );
    }

    #[test]
    fn test_delete_multiple_sources_rejected() {
        let sql = r#"
            CREATE STREAM stream1 (symbol VARCHAR);
            CREATE STREAM stream2 (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR, price DOUBLE);

            DELETE FROM stockTable
            USING stream1, stream2
            WHERE stockTable.symbol = stream1.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "DELETE with multiple source streams should fail"
        );
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("multiple source streams"),
            "Error should mention multiple sources: {}",
            err
        );
    }

    #[test]
    fn test_update_with_join_rejected() {
        let sql = r#"
            CREATE STREAM stream1 (symbol VARCHAR, price DOUBLE);
            CREATE STREAM stream2 (symbol VARCHAR, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPDATE stockTable SET price = s1.price
            FROM stream1 s1 JOIN stream2 s2 ON s1.symbol = s2.symbol
            WHERE stockTable.symbol = s1.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPDATE with JOIN source should fail");
        let err = result.err().unwrap().to_string();
        assert!(err.contains("JOIN"), "Error should mention JOIN: {}", err);
    }

    #[test]
    fn test_delete_with_join_rejected() {
        let sql = r#"
            CREATE STREAM stream1 (symbol VARCHAR);
            CREATE STREAM stream2 (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            DELETE FROM stockTable
            USING stream1 s1 JOIN stream2 s2 ON s1.symbol = s2.symbol
            WHERE stockTable.symbol = s1.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "DELETE with JOIN source should fail");
        let err = result.err().unwrap().to_string();
        assert!(err.contains("JOIN"), "Error should mention JOIN: {}", err);
    }

    #[test]
    fn test_upsert_with_join_rejected() {
        let sql = r#"
            CREATE STREAM stream1 (symbol VARCHAR, price DOUBLE);
            CREATE STREAM stream2 (symbol VARCHAR, volume INT);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE, volume INT);

            UPSERT INTO stockTable
            SELECT s1.symbol, s1.price, s2.volume
            FROM stream1 s1 JOIN stream2 s2 ON s1.symbol = s2.symbol
            ON stockTable.symbol = s1.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPSERT with JOIN source should fail");
        let err = result.err().unwrap().to_string();
        assert!(err.contains("JOIN"), "Error should mention JOIN: {}", err);
    }

    #[test]
    fn test_upsert_with_window_rejected() {
        let sql = r#"
            CREATE STREAM stockStream (symbol VARCHAR, price DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPSERT INTO stockTable
            SELECT symbol, price
            FROM stockStream WINDOW('length', 10)
            ON stockTable.symbol = stockStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPSERT with WINDOW source should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("WINDOW"),
            "Error should mention WINDOW: {}",
            err
        );
    }

    #[test]
    fn test_update_with_window_rejected() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPDATE stockTable SET price = updateStream.newPrice
            FROM updateStream WINDOW('length', 10)
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "UPDATE with WINDOW source should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("WINDOW"),
            "Error should mention WINDOW: {}",
            err
        );
    }

    #[test]
    fn test_delete_with_window_rejected() {
        let sql = r#"
            CREATE STREAM deleteStream (symbol VARCHAR);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            DELETE FROM stockTable
            USING deleteStream WINDOW('length', 10)
            WHERE stockTable.symbol = deleteStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(result.is_err(), "DELETE with WINDOW source should fail");
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("WINDOW"),
            "Error should mention WINDOW: {}",
            err
        );
    }

    #[test]
    fn test_update_set_invalid_qualifier_rejected() {
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPDATE stockTable SET updateStream.price = updateStream.newPrice
            FROM updateStream
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_err(),
            "UPDATE SET with wrong qualifier should fail"
        );
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("invalid qualifier") || err.contains("updateStream"),
            "Error should mention invalid qualifier: {}",
            err
        );
    }

    #[test]
    fn test_update_set_valid_qualifier_accepted() {
        // SET with correct table name qualifier should work
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPDATE stockTable SET stockTable.price = updateStream.newPrice
            FROM updateStream
            WHERE stockTable.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE SET with correct qualifier should parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_update_set_valid_alias_qualifier_accepted() {
        // SET with correct alias qualifier should work
        let sql = r#"
            CREATE STREAM updateStream (symbol VARCHAR, newPrice DOUBLE);
            CREATE TABLE stockTable (symbol VARCHAR PRIMARY KEY, price DOUBLE);

            UPDATE stockTable AS s SET s.price = updateStream.newPrice
            FROM updateStream
            WHERE s.symbol = updateStream.symbol;
        "#;

        let result = parse_sql_application(sql);
        assert!(
            result.is_ok(),
            "UPDATE SET with alias qualifier should parse: {:?}",
            result.err()
        );
    }
}
