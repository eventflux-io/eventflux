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

//! SQL compiler.
//!
//! Parses EventFlux SQL and turns it into query_api structures the runtime can run.
//!
//! # Architecture
//!
//! An application goes through these stages:
//! 1. **Normalization** - Rewrite `CREATE STREAM` as `CREATE TABLE`
//! 2. **SQL Parsing** - A vendored fork of sqlparser-rs with native WINDOW() support
//! 3. **DDL** - Register each CREATE STREAM or CREATE TABLE in the catalog
//! 4. **Type Mapping** - Turn SQL column types into AttributeType
//! 5. **SELECT Expansion** - Expand SELECT * from the catalog schema
//! 6. **Conversion** - Build query_api::Query and Partition structures
//! 7. **Type Checking** - Check expression types in each query
//!
//! # Example
//!
//! ```rust,ignore
//! use eventflux::sql_compiler::parse_sql_application;
//!
//! let sql = r#"
//!     CREATE STREAM StockStream (symbol STRING, price DOUBLE);
//!
//!     SELECT symbol, price
//!     FROM StockStream
//!     WHERE price > 100;
//! "#;
//!
//! let app = parse_sql_application(sql)?;
//! ```

pub mod application;
pub mod catalog;
pub mod converter;
pub mod error;
pub mod expansion;
pub mod normalization;
pub mod pattern_validation;
pub mod type_inference;
pub mod type_mapping;
pub mod with_clause;

pub use application::parse_sql_application;
pub use catalog::{CreateStreamInfo, SqlApplication, SqlCatalog};
pub use converter::SqlConverter;
pub use error::{
    ApplicationError, CatalogError, ConverterError, ExpansionError, SqlCompilerError, TypeError,
};
pub use expansion::SelectExpander;
pub use normalization::{is_create_stream, normalize_stream_syntax};
pub use pattern_validation::{PatternValidationError, PatternValidator};
pub use type_inference::{TypeContext, TypeInferenceEngine};
pub use type_mapping::{attribute_type_to_sql_type, sql_type_to_attribute_type};
pub use with_clause::{extract_with_options, validate_with_clause};

/// Parse a full SQL application of one or more statements.
///
/// Same as `parse_sql_application`, with the error wrapped in `SqlCompilerError`.
pub fn parse(sql: &str) -> Result<SqlApplication, SqlCompilerError> {
    parse_sql_application(sql).map_err(SqlCompilerError::from)
}

/// Compile one SQL query against a catalog you built yourself.
///
/// The lower level entry point. Use it when you manage the catalog by hand. It does not
/// run the type checks that `parse_sql_application` runs.
///
/// # Example
/// ```rust,ignore
/// let mut catalog = SqlCatalog::new();
/// catalog.register_stream("StockStream".to_string(), stream_def)?;
///
/// let query = compile_sql_query(
///     "SELECT symbol, price FROM StockStream WHERE price > 100",
///     &catalog
/// )?;
/// ```
pub fn compile_sql_query(
    sql: &str,
    catalog: &SqlCatalog,
) -> Result<crate::query_api::execution::query::Query, SqlCompilerError> {
    SqlConverter::convert(sql, catalog).map_err(SqlCompilerError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Compiles only if the re-exports resolve.
        let _catalog = SqlCatalog::new();
    }
}
