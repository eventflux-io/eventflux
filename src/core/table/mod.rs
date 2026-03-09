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

//! Table storage implementations for EventFlux CEP engine.
//!
//! This module provides table storage backends for JOIN enrichment and stateful operations.
//!
//! # Architecture Decisions & Known Limitations
//!
//! ## Lock Poisoning Strategy (P3)
//!
//! All lock operations use `.unwrap()` which will panic if a lock is poisoned (i.e., if
//! another thread panicked while holding the lock). This is intentional:
//!
//! - **Data Integrity**: A poisoned lock indicates a thread panicked mid-operation, leaving
//!   data in an inconsistent state. Continuing could cause data corruption or silent data loss.
//! - **Fail-Fast**: For a stream processing engine, it's better to crash and restart cleanly
//!   than to process events against corrupted state.
//! - **Recovery**: The persistence layer (checkpoints, WAL) ensures state can be recovered
//!   after a restart.
//!
//! If graceful degradation is required for specific deployments, consider wrapping table
//! operations in `std::panic::catch_unwind` at the application boundary.
//!
//! ## Index Key Generation Performance (P2)
//!
//! The `row_values_to_key` function generates string keys for HashMap indexing. This involves
//! multiple allocations per row (one String per column, plus the joined result). For
//! high-throughput scenarios, consider:
//!
//! - Using a `Hasher` (e.g., `FxHasher`) to compute `u64` keys with collision buckets
//! - Pre-allocated buffer for serialization
//! - Custom hash implementation for `Vec<AttributeValue>`
//!
//! Current approach prioritizes correctness and debuggability over raw performance.
//!
//! ## Code Duplication in Mutation Methods (P2)
//!
//! The optimistic locking pattern (read → evaluate → verify-all → write) is repeated in:
//! - `update_with_set_executors`
//! - `upsert_with_set_executors`
//! - `delete_with_expression`
//! - `update_with_expression`
//!
//! Future refactoring could extract this into a `mutate_with_retry<F, R>` helper. Current
//! duplication is accepted because:
//! - Each method has different return types and semantics
//! - The upsert has unique INSERT fallback logic
//! - Explicit code is easier to debug and audit for correctness
//!
//! ## Allocation Rate in Mutation Loops (P2)
//!
//! `StateEvent.stream_events[1] = Some(table_row_event.clone())` allocates per-row during
//! condition evaluation. This is an architectural constraint: `StateEvent` owns its
//! `StreamEvent` references. Future optimization: change to `Arc<StreamEvent>` or references.

use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::state::state_event::StateEvent;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::query_api::execution::query::output::stream::UpdateSet;
use crate::query_api::expression::Expression;
use std::sync::RwLock;

mod cache_table;
mod jdbc_table;
use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::extension::TableFactory;
pub use cache_table::{CacheTable, CacheTableFactory};
pub use jdbc_table::{JdbcTable, JdbcTableFactory};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use std::any::Any;

use crate::query_api::expression::constant::{Constant, ConstantValueWithFloat};

pub(crate) fn constant_to_av(c: &Constant) -> AttributeValue {
    match c.get_value() {
        ConstantValueWithFloat::String(s) => AttributeValue::String(s.clone()),
        ConstantValueWithFloat::Int(i) => AttributeValue::Int(*i),
        ConstantValueWithFloat::Long(l) => AttributeValue::Long(*l),
        ConstantValueWithFloat::Float(f) => AttributeValue::Float(*f),
        ConstantValueWithFloat::Double(d) => AttributeValue::Double(*d),
        ConstantValueWithFloat::Bool(b) => AttributeValue::Bool(*b),
        ConstantValueWithFloat::Time(t) => AttributeValue::Long(*t),
        ConstantValueWithFloat::Null => AttributeValue::Null,
    }
}

/// Convert row values to a string key for index/grouping purposes.
///
/// This allows using rows as HashMap keys without requiring Hash trait on AttributeValue.
/// Used by `InMemoryTable` for maintaining the internal index structure.
///
/// # ⚠️ LIMITATION: Object Type Columns
///
/// **All OBJECT-typed columns hash to the same value "O"**, causing index collisions.
///
/// **Impact**:
/// - Tables where rows differ ONLY by OBJECT columns will have index collisions
/// - Index lookups won't distinguish between rows that differ only in OBJECT columns
/// - This is a fundamental limitation: `Box<dyn Any>` cannot be compared or hashed reliably,
///   and `Object(Some(_))` clones to `Object(None)`, breaking pointer-based keys
///
/// **Recommendation**:
/// - Ensure tables with OBJECT columns have at least one other column (String, Int, etc.)
///   that provides uniqueness for index lookups
/// - For serializable data, consider using `Bytes` type instead of `Object`
/// - OBJECT columns are best suited for read-heavy or reference data scenarios
fn row_values_to_key(row: &[AttributeValue]) -> String {
    row.iter()
        .map(|v| match v {
            AttributeValue::String(s) => format!("S:{}", s),
            AttributeValue::Int(i) => format!("I:{}", i),
            AttributeValue::Long(l) => format!("L:{}", l),
            AttributeValue::Float(f) => format!("F:{}", f),
            AttributeValue::Double(d) => format!("D:{}", d),
            AttributeValue::Bool(b) => format!("B:{}", b),
            AttributeValue::Bytes(bytes) => format!("Y:{:02x?}", bytes),
            AttributeValue::Null => "N".to_string(),
            // OBJECT columns all hash to "O" - see function docstring for limitations
            AttributeValue::Object(_) => "O".to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Find table rows matching a mutation condition expression.
///
/// Creates a StateEvent with stream event at position 0 and table row at position 1
/// for evaluating WHERE/ON conditions that reference both stream and table columns.
///
/// Returns ALL matching rows including duplicates. The caller is responsible for
/// handling duplicates correctly (e.g., by grouping updates by original value and
/// reinserting the correct count after delete).
///
/// # ⚠️ Memory Warning for External Table Implementers
///
/// This function takes `rows: Vec<Vec<AttributeValue>>` which is typically obtained
/// from `Table::all_rows()`. For external backends (JDBC, Redis, etc.), calling
/// `all_rows()` loads the **entire table into memory**, which can cause OOM crashes
/// for large datasets.
///
/// **External table implementations MUST override mutation methods** (`update_with_expression`,
/// `delete_with_expression`, `update_with_set_executors`, `upsert_with_set_executors`)
/// to push WHERE clauses to the database instead of using this function.
///
/// # Performance Note
///
/// This function clones a `StreamEvent` for each row to wrap it in a `StateEvent` for
/// expression evaluation. This is an architectural constraint of the current executor
/// interface. For very large tables, this creates allocation pressure. The `table_row_event`
/// structure is reused (values updated in-place), but the clone for `StateEvent` ownership
/// is unavoidable without changing the executor interface to accept borrowed data.
pub(crate) fn find_matching_rows_for_mutation(
    rows: Vec<Vec<AttributeValue>>,
    stream_event: &StreamEvent,
    condition_executor: &dyn ExpressionExecutor,
) -> Vec<Vec<AttributeValue>> {
    if rows.is_empty() {
        return Vec::new();
    }

    let mut matched_rows: Vec<Vec<AttributeValue>> = Vec::new();

    // Pre-allocate StateEvent and stream slot outside loop to reduce allocation churn.
    // Position 0 = stream event (constant), Position 1 = table row (updated per iteration)
    let mut state_event = StateEvent::new(2, 0);
    state_event.timestamp = stream_event.timestamp;
    state_event.stream_events[0] = Some(stream_event.clone());

    // Pre-allocate table row event with max column count across all rows
    let max_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

    for row in rows {
        // Reuse table_row_event by updating values in-place
        // First, reset values beyond this row's length to Null
        for j in row.len()..table_row_event.before_window_data.len() {
            table_row_event.before_window_data[j] = AttributeValue::Null;
        }
        // Copy current row values
        for (j, val) in row.iter().enumerate() {
            if j < table_row_event.before_window_data.len() {
                table_row_event.before_window_data[j] = val.clone();
            }
        }
        // NOTE: Clone is architecturally required here because StateEvent.stream_events owns its data.
        // Known allocation hotspot for large tables. Future: use Arc<StreamEvent> or references.
        state_event.stream_events[1] = Some(table_row_event.clone());

        // Evaluate condition - include ALL matching rows (duplicates preserved)
        if let Some(AttributeValue::Bool(true)) = condition_executor.execute(Some(&state_event)) {
            matched_rows.push(row);
        }
    }
    matched_rows
}

/// Marker trait for compiled conditions used by tables.
pub trait CompiledCondition: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Marker trait for compiled update sets used by tables.
pub trait CompiledUpdateSet: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

/// Simple wrapper implementing `CompiledCondition` for tables that do not
/// perform any special compilation.
#[derive(Debug, Clone)]
pub struct SimpleCompiledCondition(pub Expression);
impl CompiledCondition for SimpleCompiledCondition {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Simple wrapper implementing `CompiledUpdateSet` for tables that do not
/// perform any special compilation.
#[derive(Debug, Clone)]
pub struct SimpleCompiledUpdateSet(pub UpdateSet);
impl CompiledUpdateSet for SimpleCompiledUpdateSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Compiled condition representation used by [`InMemoryTable`] and [`CacheTable`].
#[derive(Debug, Clone)]
pub struct InMemoryCompiledCondition {
    /// Row of values that must match exactly.
    pub values: Vec<AttributeValue>,
}
impl CompiledCondition for InMemoryCompiledCondition {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Compiled update set representation used by [`InMemoryTable`] and [`CacheTable`].
#[derive(Debug, Clone)]
pub struct InMemoryCompiledUpdateSet {
    /// New values that should replace a matching row.
    pub values: Vec<AttributeValue>,
}
impl CompiledUpdateSet for InMemoryCompiledUpdateSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Trait representing a table that can store rows of `AttributeValue`s.
pub trait Table: Debug + Send + Sync {
    /// Inserts a row into the table.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    fn insert(
        &self,
        values: &[AttributeValue],
    ) -> Result<(), crate::core::exception::EventFluxError>;

    /// Updates rows matching `condition` using the values from `update_set`.
    /// Returns `true` if any row was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    fn update(
        &self,
        condition: &dyn CompiledCondition,
        update_set: &dyn CompiledUpdateSet,
    ) -> Result<bool, crate::core::exception::EventFluxError>;

    /// Deletes rows matching `condition` from the table.
    /// Returns `true` if any row was removed.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    fn delete(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError>;

    /// Finds the first row matching `condition` and returns a clone of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    fn find(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<Option<Vec<AttributeValue>>, crate::core::exception::EventFluxError>;

    /// Returns `true` if the table contains any row matching `condition`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    fn contains(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError>;

    /// Retrieve all rows currently stored in the table.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    ///
    /// # Performance Warning
    ///
    /// This method loads all rows into memory and should be used sparingly for external
    /// backends. For JDBC tables with large datasets, this can cause:
    /// - Memory exhaustion
    /// - Network saturation
    /// - Long query times
    ///
    /// **JDBC implementers**: Override `update_with_expression()` and `delete_with_expression()`
    /// to avoid calling this method during mutations. Push WHERE clauses to the database instead.
    fn all_rows(&self) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        Ok(Vec::new())
    }

    /// Find all rows that satisfy either the `compiled_condition` or
    /// `condition_executor` when evaluated against a joined event composed from
    /// `stream_event` and each row.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage operation fails (e.g., database error).
    ///
    /// # Performance Note for JDBC Implementers
    ///
    /// The default implementation calls `all_rows()` which is inefficient for external
    /// databases. JDBC tables should override this to push JOIN conditions to SQL:
    ///
    /// ```rust,ignore
    /// fn find_rows_for_join(...) -> Result<Vec<Vec<AttributeValue>>, EventFluxError> {
    ///     // Build: SELECT * FROM table WHERE symbol = ?
    ///     let params = self.extract_join_params(stream_event);
    ///     self.execute_query(&self.join_query_template, &params)
    /// }
    /// ```
    fn find_rows_for_join(
        &self,
        stream_event: &StreamEvent,
        _compiled_condition: Option<&dyn CompiledCondition>,
        condition_executor: Option<&dyn ExpressionExecutor>,
    ) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        let rows = self.all_rows()?;
        let mut matched = Vec::new();
        let stream_attr_count = stream_event.before_window_data.len();
        for row in rows.into_iter() {
            if let Some(exec) = condition_executor {
                let mut joined =
                    StreamEvent::new(stream_event.timestamp, stream_attr_count + row.len(), 0, 0);
                for i in 0..stream_attr_count {
                    joined.before_window_data[i] = stream_event.before_window_data[i].clone();
                }
                for j in 0..row.len() {
                    joined.before_window_data[stream_attr_count + j] = row[j].clone();
                }
                if let Some(AttributeValue::Bool(true)) = exec.execute(Some(&joined)) {
                    matched.push(row);
                }
            } else {
                matched.push(row);
            }
        }
        Ok(matched)
    }

    /// Compile a join condition referencing both stream and table attributes.
    /// Default implementation does not support join-specific compilation and
    /// returns `None`.
    fn compile_join_condition(
        &self,
        _cond: Expression,
        _stream_id: &str,
        _stream_def: &crate::query_api::definition::stream_definition::StreamDefinition,
    ) -> Option<Box<dyn CompiledCondition>> {
        None
    }

    /// Compile a conditional expression into a table-specific representation.
    ///
    /// By default this wraps the expression in [`SimpleCompiledCondition`].
    fn compile_condition(&self, cond: Expression) -> Box<dyn CompiledCondition> {
        Box::new(SimpleCompiledCondition(cond))
    }

    /// Compile an update set into a table-specific representation.
    ///
    /// By default this wraps the update set in [`SimpleCompiledUpdateSet`].
    fn compile_update_set(&self, us: UpdateSet) -> Box<dyn CompiledUpdateSet> {
        Box::new(SimpleCompiledUpdateSet(us))
    }

    /// Update rows matching a condition expression when evaluated against stream event + table row.
    ///
    /// This method is used for stream-triggered UPDATE mutations where the condition
    /// references both stream columns (e.g., `updateStream.symbol`) and table columns
    /// (e.g., `stockTable.symbol`).
    ///
    /// # Arguments
    /// * `stream_event` - The triggering event from the source stream
    /// * `condition_executor` - Expression executor for the WHERE condition
    /// * `set_column_indices` - Column indices in the table to update (for partial updates)
    /// * `set_values` - New values for each column in `set_column_indices`
    ///
    /// # Returns
    /// Number of rows updated, or error if operation fails
    ///
    /// # Performance Note for JDBC Implementers
    ///
    /// The default implementation calls `all_rows()` and filters in-memory, which is
    /// **inefficient for external databases**. JDBC/SQL-backed tables MUST override
    /// this method to push the WHERE clause to the database.
    ///
    /// ## Example Override for MySQL/PostgreSQL
    ///
    /// ```rust,ignore
    /// fn update_with_expression(
    ///     &self,
    ///     stream_event: &StreamEvent,
    ///     _condition_executor: &dyn ExpressionExecutor,
    ///     set_column_indices: &[usize],
    ///     set_values: &[AttributeValue],
    /// ) -> Result<usize, EventFluxError> {
    ///     // Build parameterized SQL: UPDATE table SET col1 = ?, col2 = ? WHERE symbol = ?
    ///     let set_clause = set_column_indices.iter()
    ///         .map(|&idx| format!("{} = ?", self.column_names[idx]))
    ///         .collect::<Vec<_>>()
    ///         .join(", ");
    ///
    ///     // Extract stream values for WHERE clause parameters
    ///     let where_params = self.extract_condition_params(stream_event);
    ///
    ///     let sql = format!(
    ///         "UPDATE {} SET {} WHERE {}",
    ///         self.table_name, set_clause, self.where_clause_template
    ///     );
    ///
    ///     // Execute with combined parameters: SET values + WHERE values
    ///     let mut params = set_values.to_vec();
    ///     params.extend(where_params);
    ///     self.execute_update(&sql, &params)
    /// }
    /// ```
    fn update_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_values: &[AttributeValue],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        let matched_rows =
            find_matching_rows_for_mutation(self.all_rows()?, stream_event, condition_executor);

        let mut updated_count = 0;
        for row in &matched_rows {
            // Determine new row values
            let new_row = if set_column_indices.is_empty() {
                // No column indices - full row replacement mode
                // Use set_values as the complete new row
                set_values.to_vec()
            } else {
                // Partial update - merge SET values into existing row
                let mut row_copy = row.clone();
                for (idx, &col_idx) in set_column_indices.iter().enumerate() {
                    if col_idx < row_copy.len() && idx < set_values.len() {
                        row_copy[col_idx] = set_values[idx].clone();
                    }
                }
                row_copy
            };

            // Use exact row values as condition, new row as update
            let cond = InMemoryCompiledCondition {
                values: row.clone(),
            };
            let us = InMemoryCompiledUpdateSet { values: new_row };
            if self.update(&cond, &us)? {
                updated_count += 1;
            }
        }
        Ok(updated_count)
    }

    /// Update rows with SET expressions that may reference table columns, evaluated per-row.
    ///
    /// This method is used for stream-triggered UPDATE mutations where SET expressions
    /// reference both stream columns and table columns (e.g., `SET price = table.price + stream.delta`).
    ///
    /// # Arguments
    /// * `stream_event` - The triggering event from the source stream
    /// * `condition_executor` - Expression executor for the WHERE condition
    /// * `set_column_indices` - Column indices for each SET clause attribute
    /// * `set_value_executors` - Expression executors for each SET clause value
    ///
    /// # Returns
    /// Number of rows updated, or error if operation fails
    ///
    /// # ⚠️ IMPORTANT: No Default Implementation
    ///
    /// This method has no safe default implementation. Table implementations MUST
    /// override this method with an atomic version that guarantees data integrity:
    /// - InMemoryTable: Uses in-place updates with held locks
    /// - JDBC/SQL backends: Should use database transactions or native UPDATE
    /// - Other backends: Must implement appropriate atomic guarantees
    ///
    /// A non-atomic delete-then-insert pattern would risk data loss if the insert
    /// fails after the delete succeeds.
    fn update_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        // Fallback: If no SET executors (simple case), delegate to update_with_expression
        // This maintains backward compatibility with existing Table implementations
        if set_value_executors.is_empty() {
            // No dynamic SET expressions - can use the simpler update_with_expression
            // which most Table implementations already support
            return self.update_with_expression(stream_event, condition_executor, &[], &[]);
        }

        // Dynamic SET expressions require table-specific atomic implementation
        // to prevent data loss from non-atomic delete-then-insert patterns
        Err(crate::core::exception::EventFluxError::OperationNotSupported {
            message: "UPDATE with SET expressions referencing table columns requires \
                     table-specific implementation. Override update_with_set_executors() \
                     in your Table implementation. See InMemoryTable for reference."
                .to_string(),
            operation: Some("update_with_set_executors".to_string()),
        })
    }

    /// Delete rows matching a condition expression when evaluated against stream event + table row.
    ///
    /// This method is used for stream-triggered DELETE mutations where the condition
    /// references both stream columns and table columns.
    ///
    /// # Arguments
    /// * `stream_event` - The triggering event from the source stream
    /// * `condition_executor` - Expression executor for the WHERE condition
    ///
    /// # Returns
    /// Number of rows deleted, or error if operation fails.
    ///
    /// # ⚠️ Count Accuracy Note
    ///
    /// For tables with duplicate rows (identical values), the returned count reflects
    /// the number of **distinct row values** removed, not the total rows removed.
    /// This is because `Table::delete()` removes ALL copies of a matching row in one
    /// call and returns `bool`. If accurate row counts are required for metrics,
    /// the Table implementation should track this internally.
    ///
    /// # Performance Note for JDBC Implementers
    ///
    /// The default implementation calls `all_rows()` and filters in-memory, which is
    /// **inefficient for external databases**. JDBC/SQL-backed tables MUST override
    /// this method to push the WHERE clause to the database.
    ///
    /// ## Example Override for MySQL/PostgreSQL
    ///
    /// ```rust,ignore
    /// fn delete_with_expression(
    ///     &self,
    ///     stream_event: &StreamEvent,
    ///     _condition_executor: &dyn ExpressionExecutor,
    /// ) -> Result<usize, EventFluxError> {
    ///     // Extract stream values for WHERE clause parameters
    ///     let where_params = self.extract_condition_params(stream_event);
    ///
    ///     // Build parameterized SQL: DELETE FROM table WHERE symbol = ?
    ///     let sql = format!(
    ///         "DELETE FROM {} WHERE {}",
    ///         self.table_name, self.where_clause_template
    ///     );
    ///
    ///     self.execute_delete(&sql, &where_params)
    /// }
    /// ```
    fn delete_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        let matched_rows =
            find_matching_rows_for_mutation(self.all_rows()?, stream_event, condition_executor);

        let mut deleted_count = 0;
        for row in &matched_rows {
            // Use exact row values as condition for deletion
            let cond = InMemoryCompiledCondition {
                values: row.clone(),
            };
            if self.delete(&cond)? {
                deleted_count += 1;
            }
        }
        Ok(deleted_count)
    }

    /// Upsert (update or insert) with SET expressions that may reference table columns.
    ///
    /// This method is used for stream-triggered UPSERT mutations where:
    /// 1. If a row matches the ON condition: UPDATE that row (optionally using SET expressions)
    /// 2. If no row matches: INSERT a new row from stream data
    ///
    /// # Arguments
    /// * `stream_event` - The triggering event from the source stream
    /// * `condition_executor` - Expression executor for the ON condition
    /// * `set_column_indices` - Column indices for each SET clause attribute (empty for full replacement)
    /// * `set_value_executors` - Expression executors for each SET clause value (empty for full replacement)
    ///
    /// # Returns
    /// Tuple of (rows_updated, rows_inserted), or error if operation fails
    ///
    /// # ⚠️ IMPORTANT: No Default Implementation
    ///
    /// This method has no safe default implementation. Table implementations MUST
    /// override this method with an atomic version that guarantees data integrity:
    /// - InMemoryTable: Uses in-place updates with held locks
    /// - JDBC/SQL backends: Should use database transactions or native UPSERT/MERGE
    /// - Other backends: Must implement appropriate atomic guarantees
    ///
    /// A non-atomic delete-then-insert pattern would risk data loss if the insert
    /// fails after the delete succeeds.
    fn upsert_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<(usize, usize), crate::core::exception::EventFluxError> {
        // Fallback: If no SET executors (simple UPSERT), try find-then-update/insert pattern
        // This maintains backward compatibility with existing Table implementations
        if set_value_executors.is_empty() {
            // Simple UPSERT without SET expressions: check if row exists, then update or insert
            let new_values = if let Some(output) = stream_event.get_output_data() {
                output.to_vec()
            } else {
                stream_event.before_window_data.clone()
            };

            if new_values.is_empty() {
                return Ok((0, 0));
            }

            // Find matching rows using the condition
            let matched = find_matching_rows_for_mutation(
                self.all_rows()?,
                stream_event,
                condition_executor,
            );

            if matched.is_empty() {
                // No match - insert
                self.insert(&new_values)?;
                return Ok((0, 1));
            } else {
                // Match found - update first matching row
                let cond = InMemoryCompiledCondition {
                    values: matched[0].clone(),
                };
                let us = InMemoryCompiledUpdateSet {
                    values: new_values,
                };
                if self.update(&cond, &us)? {
                    return Ok((1, 0));
                }
                return Ok((0, 0));
            }
        }

        // Dynamic SET expressions require table-specific atomic implementation
        Err(crate::core::exception::EventFluxError::OperationNotSupported {
            message: "UPSERT with SET expressions referencing table columns requires \
                     table-specific implementation. Override upsert_with_set_executors() \
                     in your Table implementation. See InMemoryTable for reference."
                .to_string(),
            operation: Some("upsert_with_set_executors".to_string()),
        })
    }

    /// Clone helper for boxed trait objects.
    ///
    /// # Errors
    ///
    /// Returns an error if the cloning operation fails (e.g., cannot reconnect to database).
    fn clone_table(&self) -> Result<Box<dyn Table>, crate::core::exception::EventFluxError>;

    /// Phase 2 validation: Verify connectivity and external resource availability
    ///
    /// This method is called during application initialization (Phase 2) to validate
    /// that external backing stores (databases, caches) are reachable and properly configured.
    ///
    /// **Fail-Fast Principle**: Application should NOT start if table backing stores are not ready.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default - in-memory tables don't need external validation.
    /// Tables with external backing stores (MySQL, PostgreSQL, Redis) MUST override this method.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - External backing store is reachable and properly configured
    /// * `Err(EventFluxError)` - Validation failed, application should not start
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // MySQL table validates database connectivity
    /// fn validate_connectivity(&self) -> Result<(), EventFluxError> {
    ///     // 1. Validate database connection
    ///     let conn = self.pool.get_conn()?;
    ///
    ///     // 2. Validate table exists
    ///     let exists: bool = conn.query_first(
    ///         format!("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='{}')", self.table_name)
    ///     )?.unwrap_or(false);
    ///
    ///     if !exists {
    ///         return Err(EventFluxError::configuration(
    ///             format!("Table '{}' does not exist in database", self.table_name)
    ///         ));
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn validate_connectivity(&self) -> Result<(), crate::core::exception::EventFluxError> {
        Ok(()) // Default: no validation needed (in-memory tables)
    }
}

impl Clone for Box<dyn Table> {
    fn clone(&self) -> Self {
        self.clone_table()
            .expect("Failed to clone table - this should not happen for in-memory tables")
    }
}

/// Maximum retry attempts for concurrent modification conflicts
const MAX_CONCURRENT_MODIFICATION_RETRIES: u32 = 3;

/// Simple in-memory table storing rows in a vector with HashMap index for O(1) lookups.
///
/// # Concurrency Model
///
/// Uses optimistic locking with retry for UPDATE/UPSERT operations:
/// 1. Read phase: Acquire read lock, find matching rows, capture current values
/// 2. Evaluate phase: Compute new values (can reference both stream and table data)
/// 3. Write phase: Acquire write lock, verify rows unchanged, apply updates
///
/// If a row was modified between phases, the operation is retried up to
/// [`MAX_CONCURRENT_MODIFICATION_RETRIES`] times before being skipped.
///
/// # Performance Notes
///
/// Row identity verification uses column-by-column comparison, skipping `Object` columns
/// (since `Box<dyn Any>` cannot be cloned or compared reliably). This is O(data_columns)
/// per row and could become expensive with very wide tables.
///
/// ## Future Optimization: Row Version Numbers
///
/// A more efficient approach would store a version/generation counter per row:
/// - Read: Capture (row_values, version_number)
/// - Write: Verify version_number unchanged (O(1) integer comparison)
/// - On match: Increment version and apply update
///
/// This would change row storage from `Vec<Vec<AttributeValue>>` to something like:
/// ```ignore
/// struct VersionedRow {
///     version: u64,
///     values: Vec<AttributeValue>,
/// }
/// ```
///
/// Benefits:
/// - O(1) identity check instead of O(columns)
/// - Enables ABA problem detection (row changed and changed back)
/// - Foundation for MVCC (Multi-Version Concurrency Control) if needed
#[derive(Debug)]
pub struct InMemoryTable {
    rows: RwLock<Vec<Vec<AttributeValue>>>,
    // Index: maps serialized row key → Vec of indices in rows Vec (supports duplicates)
    index: RwLock<HashMap<String, Vec<usize>>>,
    /// Counter for updates skipped due to concurrent modifications after all retries exhausted.
    /// When a row is modified/deleted between read and write phases and retries are exhausted,
    /// the update is skipped. This counter provides observability for such race conditions.
    concurrent_modification_skips: std::sync::atomic::AtomicU64,
    /// Counter for retry attempts due to concurrent modifications.
    /// Tracks how often the optimistic locking retry mechanism is triggered.
    concurrent_modification_retries: std::sync::atomic::AtomicU64,
}

impl Default for InMemoryTable {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTable {
    pub fn new() -> Self {
        Self {
            rows: RwLock::new(Vec::new()),
            index: RwLock::new(HashMap::new()),
            concurrent_modification_skips: std::sync::atomic::AtomicU64::new(0),
            concurrent_modification_retries: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns the count of updates skipped due to concurrent modifications after retries exhausted.
    /// Useful for monitoring optimistic locking failures in high-concurrency scenarios.
    pub fn concurrent_modification_skips(&self) -> u64 {
        self.concurrent_modification_skips
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the count of retry attempts due to concurrent modifications.
    /// High values indicate significant contention on this table.
    pub fn concurrent_modification_retries(&self) -> u64 {
        self.concurrent_modification_retries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Helper function to create a hash key from row values
    /// This enables O(1) lookups instead of O(n) linear scans
    fn row_to_key(row: &[AttributeValue]) -> String {
        // Delegate to the crate-level function to avoid code duplication
        row_values_to_key(row)
    }

    /// Compare two rows for identity, skipping Object columns.
    ///
    /// This is necessary because `AttributeValue::Object(Some(_))` clones to `Object(None)`
    /// (due to `Box<dyn Any>` not being cloneable), and `Object(_) == Object(_)` always
    /// returns `false` (no way to compare opaque types).
    ///
    /// For optimistic locking identity checks, we compare only the data columns that can
    /// be reliably cloned and compared. Object columns are skipped - if only Object columns
    /// changed, we treat it as "unchanged" for concurrency control purposes.
    ///
    /// # ⚠️ Concurrency Warning for Object Columns
    ///
    /// **Lost Update Risk**: If a row is modified (only in its Object fields) by another
    /// thread between the read and write phases, this function will NOT detect the change.
    /// The update will proceed, potentially overwriting the concurrent modification.
    ///
    /// **Mitigations**:
    /// 1. Ensure tables with Object columns have at least one non-Object column that
    ///    changes when the row is logically modified (e.g., `last_modified` timestamp)
    /// 2. For critical data, consider adding a `version` column that increments on each update
    /// 3. Avoid using Object columns in tables with high concurrent write rates
    /// 4. For truly opaque data, consider using `Bytes` type instead (can be compared)
    ///
    /// # Arguments
    /// * `a` - First row (typically the cloned snapshot from read phase)
    /// * `b` - Second row (typically the current row in the table)
    ///
    /// # Returns
    /// `true` if all non-Object columns are equal, `false` otherwise
    fn rows_equal_ignoring_objects(a: &[AttributeValue], b: &[AttributeValue]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (av, bv) in a.iter().zip(b.iter()) {
            match (av, bv) {
                // Skip Object columns - they can't be reliably compared after cloning
                (AttributeValue::Object(_), AttributeValue::Object(_)) => continue,
                // For all other types, require exact equality
                _ => {
                    if av != bv {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub fn all_rows(&self) -> Vec<Vec<AttributeValue>> {
        self.rows.read().unwrap().clone()
    }
}

impl Table for InMemoryTable {
    fn insert(
        &self,
        values: &[AttributeValue],
    ) -> Result<(), crate::core::exception::EventFluxError> {
        let key = Self::row_to_key(values);
        let mut rows = self.rows.write().unwrap();
        let new_index = rows.len();
        rows.push(values.to_vec());

        // Update index: add this row's index to the key's index list
        let mut index = self.index.write().unwrap();
        index.entry(key).or_insert_with(Vec::new).push(new_index);
        Ok(())
    }

    fn all_rows(&self) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        Ok(self.rows.read().unwrap().clone())
    }

    fn update(
        &self,
        condition: &dyn CompiledCondition,
        update_set: &dyn CompiledUpdateSet,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        let cond = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => c,
            None => return Ok(false),
        };
        let us = match update_set
            .as_any()
            .downcast_ref::<InMemoryCompiledUpdateSet>()
        {
            Some(u) => u,
            None => return Ok(false),
        };

        let old_key = Self::row_to_key(&cond.values);
        let new_key = Self::row_to_key(&us.values);

        // Use index to find matching rows (O(1) instead of O(n))
        let mut index = self.index.write().unwrap();
        let mut rows = self.rows.write().unwrap();

        let indices_to_update = if let Some(indices) = index.get(&old_key) {
            indices.clone()
        } else {
            return Ok(false);
        };

        if indices_to_update.is_empty() {
            return Ok(false);
        }

        // Update rows and maintain index
        for &idx in &indices_to_update {
            if let Some(row) = rows.get_mut(idx) {
                *row = us.values.clone();
            }
        }

        // Update index: remove old key entries, add new key entries
        index.remove(&old_key);
        index
            .entry(new_key)
            .or_insert_with(Vec::new)
            .extend(indices_to_update);

        Ok(true)
    }

    fn delete(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        let cond = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => c,
            None => return Ok(false),
        };

        let key = Self::row_to_key(&cond.values);
        let mut index = self.index.write().unwrap();
        let mut rows = self.rows.write().unwrap();

        // Check if any rows match (O(1) index lookup)
        if !index.contains_key(&key) {
            return Ok(false);
        }

        let orig_len = rows.len();
        rows.retain(|row| row.as_slice() != cond.values.as_slice());

        if orig_len == rows.len() {
            return Ok(false);
        }

        // Rebuild index since row indices have shifted after deletion
        // This is O(n) but delete is less frequent than reads/finds
        index.clear();
        for (idx, row) in rows.iter().enumerate() {
            let row_key = Self::row_to_key(row);
            index.entry(row_key).or_insert_with(Vec::new).push(idx);
        }

        Ok(true)
    }

    fn find(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<Option<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        let cond = condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
            .ok_or_else(|| {
                crate::core::exception::EventFluxError::Other("Invalid condition type".to_string())
            })?;

        // O(1) index lookup instead of O(n) linear scan
        let key = Self::row_to_key(&cond.values);
        let index = self.index.read().unwrap();

        if let Some(indices) = index.get(&key) {
            if let Some(&first_idx) = indices.first() {
                let rows = self.rows.read().unwrap();
                return Ok(rows.get(first_idx).cloned());
            }
        }
        Ok(None)
    }

    fn contains(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        let cond = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => c,
            None => return Ok(false),
        };

        // O(1) index lookup instead of O(n) linear scan
        let key = Self::row_to_key(&cond.values);
        let index = self.index.read().unwrap();
        Ok(index.contains_key(&key))
    }

    fn find_rows_for_join(
        &self,
        stream_event: &StreamEvent,
        _compiled_condition: Option<&dyn CompiledCondition>,
        condition_executor: Option<&dyn ExpressionExecutor>,
    ) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        let rows = self.rows.read().unwrap();
        let mut matched = Vec::new();
        let stream_attr_count = stream_event.before_window_data.len();
        for row in rows.iter() {
            if let Some(exec) = condition_executor {
                let mut joined =
                    StreamEvent::new(stream_event.timestamp, stream_attr_count + row.len(), 0, 0);
                for i in 0..stream_attr_count {
                    joined.before_window_data[i] = stream_event.before_window_data[i].clone();
                }
                for j in 0..row.len() {
                    joined.before_window_data[stream_attr_count + j] = row[j].clone();
                }
                if let Some(AttributeValue::Bool(true)) = exec.execute(Some(&joined)) {
                    matched.push(row.clone());
                }
            } else {
                matched.push(row.clone());
            }
        }
        Ok(matched)
    }

    fn compile_condition(&self, cond: Expression) -> Box<dyn CompiledCondition> {
        if let Expression::Constant(c) = cond {
            Box::new(InMemoryCompiledCondition {
                values: vec![constant_to_av(&c)],
            })
        } else {
            Box::new(InMemoryCompiledCondition { values: Vec::new() })
        }
    }

    fn compile_update_set(&self, us: UpdateSet) -> Box<dyn CompiledUpdateSet> {
        let mut vals = Vec::new();
        for sa in us.set_attributes.iter() {
            if let Expression::Constant(c) = &sa.value_to_set {
                vals.push(constant_to_av(c));
            }
        }
        Box::new(InMemoryCompiledUpdateSet { values: vals })
    }

    fn clone_table(&self) -> Result<Box<dyn Table>, crate::core::exception::EventFluxError> {
        let rows = self.rows.read().unwrap().clone();

        // Rebuild index for cloned table
        let mut index = HashMap::new();
        for (idx, row) in rows.iter().enumerate() {
            let key = Self::row_to_key(row);
            index.entry(key).or_insert_with(Vec::new).push(idx);
        }

        Ok(Box::new(InMemoryTable {
            rows: RwLock::new(rows),
            index: RwLock::new(index),
            concurrent_modification_skips: std::sync::atomic::AtomicU64::new(0),
            concurrent_modification_retries: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// Atomic implementation of update_with_set_executors for InMemoryTable.
    ///
    /// This implementation separates read and write phases to prevent potential deadlocks
    /// that could occur if expression executors (e.g., UDFs) attempt to access this table.
    /// Updates are applied atomically with incremental index maintenance.
    ///
    /// # Concurrent Modification Handling
    ///
    /// Uses optimistic locking with retry. If a row is modified between read and write phases,
    /// the operation retries from the beginning to re-evaluate expressions against current data.
    /// This prevents "lost updates" for expressions like `SET count = count + 1`.
    ///
    /// # ⚠️ Performance Warning
    ///
    /// Currently performs O(N) full table scan during the read phase, cloning a StreamEvent
    /// for each row to wrap in StateEvent for expression evaluation. For tables with 100k+ rows,
    /// this creates significant allocation pressure (100k+ vector clones per incoming event).
    ///
    /// **TODO: Indexed Fast-Path Optimization**
    /// Implement a fast-path that checks if the condition is a simple equality on indexed
    /// columns (e.g., `table.id == stream.id`) and use `self.index` for O(1) lookup before
    /// falling back to full scan. This would dramatically improve performance for the common
    /// case of keyed updates.
    fn update_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for handling concurrent modifications
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Phase 1: Read - collect row data and indices (read lock)
            // This allows expression evaluation without holding locks
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                // Pre-allocate StateEvent and stream slot outside loop to reduce allocations
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Pre-allocate table row event with max column count (assume consistent schema)
                let max_cols = rows.first().map_or(0, |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                for (idx, row) in rows.iter().enumerate() {
                    // Reuse table_row_event by updating values in-place (avoids full struct allocation)
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    // NOTE: Clone is required here because StateEvent.stream_events owns its StreamEvents.
                    // This is a known allocation hotspot. Future optimization: change StateEvent to use
                    // Arc<StreamEvent> or references, allowing zero-copy row iteration.
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matches.push((idx, row.clone()));
                    }
                }
                matches
            }; // Read lock released here

            if matched_rows.is_empty() {
                return Ok(total_updated);
            }

            // Phase 2: Evaluate SET expressions (no locks held - prevents deadlock)
            // If executors access this table, they can acquire locks safely
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            // Pre-allocate StateEvent outside loop to reduce allocations
            let mut state_event = StateEvent::new(2, 0);
            state_event.timestamp = stream_event.timestamp;
            state_event.stream_events[0] = Some(stream_event.clone());

            // Pre-allocate table row event with max column count
            let max_cols = matched_rows.first().map_or(0, |(_, r)| r.len());
            let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

            for (_idx, row) in &matched_rows {
                // Reuse table_row_event by updating values in-place
                for (j, val) in row.iter().enumerate() {
                    if j < table_row_event.before_window_data.len() {
                        table_row_event.before_window_data[j] = val.clone();
                    }
                }
                state_event.stream_events[1] = Some(table_row_event.clone());

                // Evaluate SET expressions
                let mut set_values = Vec::with_capacity(set_value_executors.len());
                let mut evaluation_failed = false;
                for executor in set_value_executors {
                    if let Some(val) = executor.execute(Some(&state_event)) {
                        set_values.push(val);
                    } else {
                        evaluation_failed = true;
                        break;
                    }
                }

                if evaluation_failed {
                    continue;
                }

                // Merge SET values into existing row
                let mut new_row = row.clone();
                for (i, &col_idx) in set_column_indices.iter().enumerate() {
                    if col_idx < new_row.len() && i < set_values.len() {
                        new_row[col_idx] = set_values[i].clone();
                    }
                }

                updates.push((row.clone(), new_row));
            }

            // Phase 3: Write - verify all rows first, then apply updates atomically
            // This prevents the double-update bug where partial application followed by
            // retry causes already-updated rows to be updated again.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Step 1: Verify ALL rows exist before applying ANY changes
            // This ensures atomicity - either all updates succeed or none are applied
            let mut verified_updates: Vec<(usize, String, Vec<AttributeValue>)> = Vec::new();
            let mut verification_failed = false;
            let mut verified_indices = std::collections::HashSet::new();

            for (original_row, new_row) in &updates {
                let key = Self::row_to_key(original_row);
                let found_idx = if let Some(candidate_indices) = index.get(&key) {
                    candidate_indices.iter().find_map(|&idx| {
                        if !verified_indices.contains(&idx)
                            && idx < rows.len()
                            && Self::rows_equal_ignoring_objects(&rows[idx], original_row)
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

                if let Some(idx) = found_idx {
                    verified_indices.insert(idx);
                    verified_updates.push((idx, key, new_row.clone()));
                } else {
                    // Any verification failure means we must retry the entire batch
                    verification_failed = true;
                    break;
                }
            }

            if verification_failed {
                // Don't apply any changes - retry if attempts remaining
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // All retries exhausted - return error instead of silently dropping writes
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(crate::core::exception::EventFluxError::QueryableRecordTable {
                    message: format!(
                        "Concurrent modification limit exceeded after {} retries. \
                         {} updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                        MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                    ),
                    table_name: None,
                });
            }

            // Step 2: Apply all updates (all rows verified)
            for (idx, old_key, new_row) in verified_updates {
                let new_key = Self::row_to_key(&new_row);
                rows[idx] = new_row;
                total_updated += 1;

                // Incremental index update (only if key changed)
                if old_key != new_key {
                    if let Some(indices) = index.get_mut(&old_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    index.entry(new_key).or_insert_with(Vec::new).push(idx);
                }
            }

            return Ok(total_updated);
        }

        // Should not reach here - loop always returns
        Ok(total_updated)
    }

    /// Atomic implementation of upsert_with_set_executors for InMemoryTable.
    ///
    /// This implementation separates read and write phases to prevent potential deadlocks
    /// that could occur if expression executors (e.g., UDFs) attempt to access this table.
    /// Updates are applied atomically with incremental index maintenance.
    ///
    /// # ⚠️ Performance Warning
    ///
    /// Currently performs O(N) full table scan during the read phase, cloning a StreamEvent
    /// for each row to wrap in StateEvent for expression evaluation. For tables with 100k+ rows,
    /// this creates significant allocation pressure (100k+ vector clones per incoming event).
    ///
    /// **TODO: Indexed Fast-Path Optimization**
    /// Implement a fast-path that checks if the ON condition is a simple equality on indexed
    /// columns (e.g., `table.id == stream.id`) and use `self.index` for O(1) lookup before
    /// falling back to full scan.
    fn upsert_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<(usize, usize), crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for handling concurrent modifications in UPDATE path
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Phase 1: Read - collect matching row data and indices (read lock)
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                // Pre-allocate StateEvent and stream slot outside loop to reduce allocations
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Pre-allocate table row event with max column count (assume consistent schema)
                let max_cols = rows.first().map_or(0, |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                for (idx, row) in rows.iter().enumerate() {
                    // Reuse table_row_event by updating values in-place
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matches.push((idx, row.clone()));
                    }
                }
                matches
            }; // Read lock released here

            if matched_rows.is_empty() {
                // No matches found - exit retry loop and proceed to INSERT path
                break;
            }

            // UPDATE path: Found matching row(s)
            // Phase 2: Evaluate SET expressions (no locks held - prevents deadlock)
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            if !set_value_executors.is_empty() {
                // Pre-allocate StateEvent outside loop to reduce allocations
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Pre-allocate table row event with max column count
                let max_cols = matched_rows.first().map_or(0, |(_, r)| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                // SET executors present - evaluate per row with table row context
                for (_idx, row) in &matched_rows {
                    // Reuse table_row_event by updating values in-place
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    // Evaluate SET expressions
                    let mut set_values = Vec::with_capacity(set_value_executors.len());
                    let mut evaluation_failed = false;
                    for executor in set_value_executors {
                        if let Some(val) = executor.execute(Some(&state_event)) {
                            set_values.push(val);
                        } else {
                            evaluation_failed = true;
                            break;
                        }
                    }

                    if evaluation_failed {
                        continue;
                    }

                    // Merge SET values into existing row
                    let updated_row = if set_column_indices.is_empty() {
                        set_values
                    } else {
                        let mut new_row = row.clone();
                        for (i, &col_idx) in set_column_indices.iter().enumerate() {
                            if col_idx < new_row.len() && i < set_values.len() {
                                new_row[col_idx] = set_values[i].clone();
                            }
                        }
                        new_row
                    };

                    updates.push((row.clone(), updated_row));
                }
            } else {
                // No SET executors - use stream values for full row replacement
                let new_values = if let Some(output) = stream_event.get_output_data() {
                    output.to_vec()
                } else {
                    stream_event.before_window_data.clone()
                };

                if !new_values.is_empty() {
                    for (_idx, row) in &matched_rows {
                        updates.push((row.clone(), new_values.clone()));
                    }
                }
            }

            // Phase 3: Write - verify all rows first, then apply updates atomically
            // This prevents the double-update bug where partial application followed by
            // retry causes already-updated rows to be updated again.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Step 1: Verify ALL rows exist before applying ANY changes
            let mut verified_updates: Vec<(usize, String, Vec<AttributeValue>)> = Vec::new();
            let mut verification_failed = false;
            let mut verified_indices = std::collections::HashSet::new();

            for (original_row, new_row) in &updates {
                let key = Self::row_to_key(original_row);
                let found_idx = if let Some(candidate_indices) = index.get(&key) {
                    candidate_indices.iter().find_map(|&idx| {
                        if !verified_indices.contains(&idx)
                            && idx < rows.len()
                            && Self::rows_equal_ignoring_objects(&rows[idx], original_row)
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

                if let Some(idx) = found_idx {
                    verified_indices.insert(idx);
                    verified_updates.push((idx, key, new_row.clone()));
                } else {
                    verification_failed = true;
                    break;
                }
            }

            if verification_failed {
                // Don't apply any changes - retry if attempts remaining
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // All retries exhausted - return error instead of silently dropping writes
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(crate::core::exception::EventFluxError::QueryableRecordTable {
                    message: format!(
                        "Concurrent modification limit exceeded after {} retries. \
                         {} upsert updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                        MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                    ),
                    table_name: None,
                });
            }

            // Step 2: Apply all updates (all rows verified)
            for (idx, old_key, new_row) in verified_updates {
                let new_key = Self::row_to_key(&new_row);
                rows[idx] = new_row;
                total_updated += 1;

                if old_key != new_key {
                    if let Some(indices) = index.get_mut(&old_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    index.entry(new_key).or_insert_with(Vec::new).push(idx);
                }
            }

            return Ok((total_updated, 0));
        }

        // INSERT path: No matching rows found (or retry loop exited without matches)
        // Fall through to INSERT logic below
        {
            // INSERT path: No matching rows found in Phase 1
            // CRITICAL: Must re-verify under write lock to prevent race condition where
            // another thread inserts a matching row between our read and write phases.
            let new_values = if let Some(output) = stream_event.get_output_data() {
                output.to_vec()
            } else {
                stream_event.before_window_data.clone()
            };

            if !new_values.is_empty() {
                let mut rows = self.rows.write().unwrap();
                let mut index = self.index.write().unwrap();

                // Re-verify: Check if a matching row was inserted by another thread
                // between Phase 1 (read lock released) and now (write lock acquired).
                // This prevents duplicate inserts for what should be an upsert.
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                let max_cols = rows.first().map_or(new_values.len(), |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                let mut matching_idx: Option<usize> = None;
                for (idx, row) in rows.iter().enumerate() {
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matching_idx = Some(idx);
                        break; // Found a match - will update instead of insert
                    }
                }

                if let Some(idx) = matching_idx {
                    // A matching row appeared between read and write phases.
                    // Update it instead of inserting a duplicate.
                    let old_key = Self::row_to_key(&rows[idx]);
                    let new_key = Self::row_to_key(&new_values);

                    rows[idx] = new_values;

                    // Update index if key changed
                    if old_key != new_key {
                        if let Some(indices) = index.get_mut(&old_key) {
                            indices.retain(|&i| i != idx);
                            if indices.is_empty() {
                                index.remove(&old_key);
                            }
                        }
                        index.entry(new_key).or_insert_with(Vec::new).push(idx);
                    }
                    Ok((1, 0)) // Updated, not inserted
                } else {
                    // No match found even under write lock - safe to insert
                    let key = Self::row_to_key(&new_values);
                    let new_index = rows.len();
                    rows.push(new_values);
                    index.entry(key).or_insert_with(Vec::new).push(new_index);
                    Ok((0, 1))
                }
            } else {
                Ok((0, 0))
            }
        }
    }

    /// Optimized delete_with_expression for InMemoryTable.
    ///
    /// This override avoids cloning all rows via `all_rows()` and uses proper
    /// concurrent modification handling with retry logic.
    ///
    /// # Concurrent Modification Handling
    ///
    /// Uses optimistic locking with retry. If rows are modified between read and write phases,
    /// the operation retries from the beginning to ensure consistency.
    fn delete_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        // Retry loop for handling concurrent modifications
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Phase 1: Read - find matching row indices and values (read lock)
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                // Pre-allocate StateEvent and stream slot outside loop
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Pre-allocate table row event with max column count
                let max_cols = rows.first().map_or(0, |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                for (idx, row) in rows.iter().enumerate() {
                    // Reuse table_row_event by updating values in-place
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matches.push((idx, row.clone()));
                    }
                }
                matches
            }; // Read lock released here

            if matched_rows.is_empty() {
                return Ok(0);
            }

            // Phase 2: Write - verify all rows exist, then delete atomically
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Step 1: Verify ALL rows exist before applying ANY changes
            let mut to_delete: Vec<usize> = Vec::new();
            let mut verification_failed = false;
            let mut verified_indices = std::collections::HashSet::new();

            for (_original_idx, original_row) in &matched_rows {
                let key = Self::row_to_key(original_row);
                let found_idx = if let Some(candidate_indices) = index.get(&key) {
                    candidate_indices.iter().find_map(|&idx| {
                        if !verified_indices.contains(&idx)
                            && idx < rows.len()
                            && Self::rows_equal_ignoring_objects(&rows[idx], original_row)
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

                if let Some(idx) = found_idx {
                    verified_indices.insert(idx);
                    to_delete.push(idx);
                } else {
                    // Row was modified or deleted - retry
                    verification_failed = true;
                    break;
                }
            }

            if verification_failed {
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // All retries exhausted - return error instead of silently dropping writes
                let skipped = matched_rows.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(crate::core::exception::EventFluxError::QueryableRecordTable {
                    message: format!(
                        "Concurrent modification limit exceeded after {} retries. \
                         {} deletes could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                        MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                    ),
                    table_name: None,
                });
            }

            // Step 2: Delete rows using swap_remove for O(1) per deletion
            // Sort descending so we process higher indices first (swap_remove won't affect them)
            to_delete.sort_by(|a, b| b.cmp(a));
            let deleted_count = to_delete.len();

            for idx in to_delete {
                let last_idx = rows.len() - 1;

                if idx == last_idx {
                    // Deleting last element - just pop, no index update needed for moved element
                    let removed = rows.pop().unwrap();
                    let key = Self::row_to_key(&removed);
                    if let Some(indices) = index.get_mut(&key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&key);
                        }
                    }
                } else {
                    // swap_remove: swap with last element, then pop
                    // Only need to update index for the element that moved (was at last_idx, now at idx)
                    let removed = rows.swap_remove(idx);
                    let removed_key = Self::row_to_key(&removed);

                    // Remove deleted row from index
                    if let Some(indices) = index.get_mut(&removed_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&removed_key);
                        }
                    }

                    // Update moved row's index (was at last_idx, now at idx)
                    let moved_key = Self::row_to_key(&rows[idx]);
                    if let Some(indices) = index.get_mut(&moved_key) {
                        for i in indices.iter_mut() {
                            if *i == last_idx {
                                *i = idx;
                                break;
                            }
                        }
                    }
                }
            }

            return Ok(deleted_count);
        }

        Ok(0)
    }

    /// Optimized update_with_expression for InMemoryTable.
    ///
    /// This override avoids cloning all rows via `all_rows()` and uses proper
    /// concurrent modification handling with retry logic.
    ///
    /// # Note
    ///
    /// This method is for simple updates where SET values are pre-computed.
    /// For SET expressions that reference table columns (e.g., `SET count = count + 1`),
    /// use `update_with_set_executors` instead.
    fn update_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_values: &[AttributeValue],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for handling concurrent modifications
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Phase 1: Read - find matching rows (read lock)
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                let max_cols = rows.first().map_or(0, |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                for (idx, row) in rows.iter().enumerate() {
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matches.push((idx, row.clone()));
                    }
                }
                matches
            }; // Read lock released here

            if matched_rows.is_empty() {
                return Ok(total_updated);
            }

            // Phase 2: Compute new row values (no locks held)
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            for (_idx, row) in &matched_rows {
                let new_row = if set_column_indices.is_empty() {
                    // Full row replacement mode
                    set_values.to_vec()
                } else {
                    // Partial update - merge SET values into existing row
                    let mut row_copy = row.clone();
                    for (i, &col_idx) in set_column_indices.iter().enumerate() {
                        if col_idx < row_copy.len() && i < set_values.len() {
                            row_copy[col_idx] = set_values[i].clone();
                        }
                    }
                    row_copy
                };
                updates.push((row.clone(), new_row));
            }

            // Phase 3: Write - verify all rows first, then apply updates atomically
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Step 1: Verify ALL rows exist before applying ANY changes
            let mut verified_updates: Vec<(usize, String, Vec<AttributeValue>)> = Vec::new();
            let mut verification_failed = false;
            let mut verified_indices = std::collections::HashSet::new();

            for (original_row, new_row) in &updates {
                let key = Self::row_to_key(original_row);
                let found_idx = if let Some(candidate_indices) = index.get(&key) {
                    candidate_indices.iter().find_map(|&idx| {
                        if !verified_indices.contains(&idx)
                            && idx < rows.len()
                            && Self::rows_equal_ignoring_objects(&rows[idx], original_row)
                        {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

                if let Some(idx) = found_idx {
                    verified_indices.insert(idx);
                    verified_updates.push((idx, key, new_row.clone()));
                } else {
                    verification_failed = true;
                    break;
                }
            }

            if verification_failed {
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // All retries exhausted - return error instead of silently dropping writes
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(crate::core::exception::EventFluxError::QueryableRecordTable {
                    message: format!(
                        "Concurrent modification limit exceeded after {} retries. \
                         {} updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                        MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                    ),
                    table_name: None,
                });
            }

            // Step 2: Apply all updates (all rows verified)
            for (idx, old_key, new_row) in verified_updates {
                let new_key = Self::row_to_key(&new_row);
                rows[idx] = new_row;
                total_updated += 1;

                // Incremental index update (only if key changed)
                if old_key != new_key {
                    if let Some(indices) = index.get_mut(&old_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    index.entry(new_key).or_insert_with(Vec::new).push(idx);
                }
            }

            return Ok(total_updated);
        }

        Ok(total_updated)
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryTableFactory;

impl TableFactory for InMemoryTableFactory {
    fn name(&self) -> &'static str {
        "inMemory"
    }
    fn create(
        &self,
        _name: String,
        _properties: HashMap<String, String>,
        _ctx: Arc<EventFluxContext>,
    ) -> Result<Arc<dyn Table>, String> {
        Ok(Arc::new(InMemoryTable::new()))
    }

    fn clone_box(&self) -> Box<dyn TableFactory> {
        Box::new(self.clone())
    }
}
