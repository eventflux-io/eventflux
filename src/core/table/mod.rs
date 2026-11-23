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

//! Table storage for the EventFlux CEP engine.
//!
//! Tables back JOIN enrichment and other stateful operations.
//!
//! # Design choices and known limits
//!
//! ## Poisoned locks (P3)
//!
//! Every lock call uses `.unwrap()`, so it panics if the lock is poisoned, meaning
//! another thread panicked while holding it. This is on purpose:
//!
//! - **Data integrity**: A poisoned lock means a thread panicked in the middle of a
//!   change, so the data may be half updated. Going on could corrupt or silently lose data.
//! - **Fail fast**: For a stream engine it is better to crash and restart cleanly than
//!   to process events against broken state.
//! - **Recovery**: The persistence layer (checkpoints, WAL) lets state be recovered
//!   after a restart.
//!
//! If a deployment needs to keep running instead, wrap table calls in
//! `std::panic::catch_unwind` at the application boundary.
//!
//! ## Cost of index keys (P2)
//!
//! `row_values_to_key` builds string keys for the HashMap index. That allocates several
//! times per row, one String per column plus the joined result. For very high
//! throughput, options are:
//!
//! - A `Hasher` such as `FxHasher` that computes `u64` keys, with collision buckets
//! - A reused buffer for building keys
//! - A custom hash for `Vec<AttributeValue>`
//!
//! For now correctness and easy debugging come before raw speed.
//!
//! ## Repeated code in the change methods (P2)
//!
//! The optimistic locking steps (read, evaluate, check all, write) repeat in:
//! - `update_with_set_executors`
//! - `upsert_with_set_executors`
//! - `delete_with_expression`
//! - `update_with_expression`
//!
//! A `mutate_with_retry<F, R>` helper could hold them later. The repetition is kept
//! for now because:
//! - Each method returns something different and behaves differently
//! - The upsert has its own INSERT fallback
//! - Plain code is easier to debug and check for correctness
//!
//! ## Allocations while matching rows (P2)
//!
//! `StateEvent.stream_events[1] = Some(table_row_event.clone())` allocates once per row
//! while a condition is checked. `StateEvent` owns its `StreamEvent`s, so this cannot
//! be avoided today. A later change to `Arc<StreamEvent>` or borrowed events would fix it.

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

// Converts a query constant to an AttributeValue. A Time constant becomes a Long.
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

/// Turns a row into a string key for indexing and grouping.
///
/// Lets rows act as HashMap keys even though AttributeValue has no Hash. Each value
/// gets a type prefix (such as `S:` or `I:`) and values are joined with `|`.
/// `InMemoryTable` uses it for its index.
///
/// # Limitation: OBJECT columns
///
/// **Every OBJECT column gives the same key part, "O"**, so keys can collide.
///
/// **Effect**:
/// - Rows that differ ONLY in OBJECT columns get the same key
/// - Index lookups cannot tell such rows apart
/// - This cannot be fixed here. A `Box<dyn Any>` cannot be compared or hashed, and
///   `Object(Some(_))` clones to `Object(None)`, so pointer based keys do not work either
///
/// **Advice**:
/// - Give tables with OBJECT columns at least one other column (String, Int and so
///   on) that makes rows unique
/// - For data that can be serialized, use `Bytes` instead of `Object`
/// - OBJECT columns suit data that is mostly read, such as reference data
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
            // Every OBJECT column gives "O". See the doc comment above for the limits.
            AttributeValue::Object(_) => "O".to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

/// Returns the table rows that match a change condition.
///
/// Builds a StateEvent with the stream event at position 0 and the table row at
/// position 1, so WHERE and ON conditions can use both stream and table columns.
/// A row matches only when the condition returns `Bool(true)`.
///
/// Returns EVERY matching row, duplicates included. The caller must handle
/// duplicates, for example by grouping updates by original value and inserting the
/// right count back after a delete.
///
/// # Memory warning for external table authors
///
/// `rows` usually comes from `Table::all_rows()`. For external backends (JDBC, Redis
/// and so on), `all_rows()` loads the **whole table into memory**, which can run out of
/// memory on large tables.
///
/// **External tables MUST override the change methods** (`update_with_expression`,
/// `delete_with_expression`, `update_with_set_executors`, `upsert_with_set_executors`)
/// and push the WHERE clause to the database instead of using this function.
///
/// # Performance
///
/// Clones a `StreamEvent` per row to put it in the `StateEvent` for evaluation. The
/// executor interface needs owned events, so this cannot be avoided without changing
/// it to accept borrowed data. `table_row_event` itself is reused and its values are
/// overwritten each time, but the clone still allocates, which adds up on big tables.
pub(crate) fn find_matching_rows_for_mutation(
    rows: Vec<Vec<AttributeValue>>,
    stream_event: &StreamEvent,
    condition_executor: &dyn ExpressionExecutor,
) -> Vec<Vec<AttributeValue>> {
    if rows.is_empty() {
        return Vec::new();
    }

    let mut matched_rows: Vec<Vec<AttributeValue>> = Vec::new();

    // Build the StateEvent once, outside the loop, to cut allocations.
    // Position 0 is the stream event and never changes. Position 1 is the table row,
    // replaced each time round.
    let mut state_event = StateEvent::new(2, 0);
    state_event.timestamp = stream_event.timestamp;
    state_event.stream_events[0] = Some(stream_event.clone());

    // Size the reused row event for the widest row.
    let max_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

    for row in rows {
        // Reuse table_row_event. Set slots past this row's length to Null, then copy
        // in this row's values.
        for j in row.len()..table_row_event.before_window_data.len() {
            table_row_event.before_window_data[j] = AttributeValue::Null;
        }
        for (j, val) in row.iter().enumerate() {
            if j < table_row_event.before_window_data.len() {
                table_row_event.before_window_data[j] = val.clone();
            }
        }
        // The clone is needed because StateEvent.stream_events owns its events. A
        // known hot spot on large tables. Arc<StreamEvent> or borrowing would fix it.
        state_event.stream_events[1] = Some(table_row_event.clone());

        // Keep EVERY matching row, duplicates included.
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

/// Wraps the raw expression as a `CompiledCondition`, for tables that do no
/// compilation of their own.
#[derive(Debug, Clone)]
pub struct SimpleCompiledCondition(pub Expression);
impl CompiledCondition for SimpleCompiledCondition {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Wraps the raw update set as a `CompiledUpdateSet`, for tables that do no
/// compilation of their own.
#[derive(Debug, Clone)]
pub struct SimpleCompiledUpdateSet(pub UpdateSet);
impl CompiledUpdateSet for SimpleCompiledUpdateSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Compiled condition used by [`InMemoryTable`] and [`CacheTable`].
#[derive(Debug, Clone)]
pub struct InMemoryCompiledCondition {
    /// A row matches only if it equals these values exactly.
    pub values: Vec<AttributeValue>,
}
impl CompiledCondition for InMemoryCompiledCondition {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Compiled update set used by [`InMemoryTable`] and [`CacheTable`].
#[derive(Debug, Clone)]
pub struct InMemoryCompiledUpdateSet {
    /// Values that replace a matching row.
    pub values: Vec<AttributeValue>,
}
impl CompiledUpdateSet for InMemoryCompiledUpdateSet {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A table that stores rows of `AttributeValue`s.
pub trait Table: Debug + Send + Sync {
    /// Inserts a row into the table.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
    fn insert(
        &self,
        values: &[AttributeValue],
    ) -> Result<(), crate::core::exception::EventFluxError>;

    /// Updates rows matching `condition` with the values from `update_set`.
    /// Returns `true` if any row was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
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
    /// Returns an error if the storage call fails, such as a database error.
    fn delete(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError>;

    /// Finds the first row matching `condition` and returns a clone of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
    fn find(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<Option<Vec<AttributeValue>>, crate::core::exception::EventFluxError>;

    /// Returns `true` if the table contains any row matching `condition`.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
    fn contains(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError>;

    /// Returns every row in the table.
    ///
    /// The default returns an empty list. Tables that hold rows must override it.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
    ///
    /// # Performance warning
    ///
    /// Loads every row into memory, so use it sparingly on external backends. On large
    /// JDBC tables it can cause:
    /// - Running out of memory
    /// - A flooded network
    /// - Slow queries
    ///
    /// **JDBC authors**: Override `update_with_expression()` and `delete_with_expression()`
    /// so changes do not call this. Push the WHERE clause to the database instead.
    fn all_rows(&self) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        Ok(Vec::new())
    }

    /// Returns the rows that join with `stream_event`.
    ///
    /// The default ignores `compiled_condition`. For each row it builds a joined event
    /// whose before window data is the stream's values followed by the row's values,
    /// and keeps the row if `condition_executor` returns `Bool(true)`. With no executor
    /// it returns every row.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage call fails, such as a database error.
    ///
    /// # Performance note for JDBC authors
    ///
    /// The default calls `all_rows()`, which is slow on external databases. JDBC
    /// tables should override this and push the JOIN condition into SQL:
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
                joined.before_window_data[stream_attr_count..(row.len() + stream_attr_count)]
                    .clone_from_slice(&row[..]);
                if let Some(AttributeValue::Bool(true)) = exec.execute(Some(&joined)) {
                    matched.push(row);
                }
            } else {
                matched.push(row);
            }
        }
        Ok(matched)
    }

    /// Compiles a join condition that uses both stream and table attributes.
    /// The default has no join compilation and returns `None`.
    fn compile_join_condition(
        &self,
        _cond: Expression,
        _stream_id: &str,
        _stream_def: &crate::query_api::definition::stream_definition::StreamDefinition,
    ) -> Option<Box<dyn CompiledCondition>> {
        None
    }

    /// Compiles a condition into this table's own form.
    ///
    /// The default wraps the expression in [`SimpleCompiledCondition`].
    fn compile_condition(&self, cond: Expression) -> Box<dyn CompiledCondition> {
        Box::new(SimpleCompiledCondition(cond))
    }

    /// Compiles an update set into this table's own form.
    ///
    /// The default wraps the update set in [`SimpleCompiledUpdateSet`].
    fn compile_update_set(&self, us: UpdateSet) -> Box<dyn CompiledUpdateSet> {
        Box::new(SimpleCompiledUpdateSet(us))
    }

    /// Updates the rows for which the condition, run on the stream event plus the
    /// table row, is true.
    ///
    /// Used for UPDATE triggered by a stream event, where the condition uses both
    /// stream columns (such as `updateStream.symbol`) and table columns (such as
    /// `stockTable.symbol`).
    ///
    /// # Arguments
    /// * `stream_event` - The event from the source stream that triggered the update
    /// * `condition_executor` - Executor for the WHERE condition
    /// * `set_column_indices` - Table columns to update. Empty means replace the whole
    ///   row with `set_values`.
    /// * `set_values` - New value for each column in `set_column_indices`
    ///
    /// # Returns
    /// Number of rows updated, or an error if the update fails. In the default, each
    /// distinct matching row value counts once even if it appears several times.
    ///
    /// # Performance note for JDBC authors
    ///
    /// The default calls `all_rows()` and filters in memory, which is **slow on external
    /// databases**. JDBC and other SQL backed tables MUST override this and push the
    /// WHERE clause to the database.
    ///
    /// ## Example override for MySQL or PostgreSQL
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
            let new_row = if set_column_indices.is_empty() {
                // No column indices, so set_values is the whole new row.
                set_values.to_vec()
            } else {
                // Partial update. Copy the SET values into the existing row and skip
                // indexes that are out of range.
                let mut row_copy = row.clone();
                for (idx, &col_idx) in set_column_indices.iter().enumerate() {
                    if col_idx < row_copy.len() && idx < set_values.len() {
                        row_copy[col_idx] = set_values[idx].clone();
                    }
                }
                row_copy
            };

            // Match on the exact old row and replace it with the new one. Every copy
            // of that row changes in one call, so later duplicates return false.
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

    /// Updates rows with SET expressions that can use table columns, run once per row.
    ///
    /// Used for UPDATE triggered by a stream event, where SET expressions use both
    /// stream and table columns, such as `SET price = table.price + stream.delta`.
    ///
    /// # Arguments
    /// * `stream_event` - The event from the source stream that triggered the update
    /// * `condition_executor` - Executor for the WHERE condition
    /// * `set_column_indices` - Table column for each SET clause attribute
    /// * `set_value_executors` - Executor for each SET clause value
    ///
    /// # Returns
    /// Number of rows updated, or an error if the update fails
    ///
    /// # IMPORTANT: no safe default
    ///
    /// The default only handles an empty `set_value_executors`, by calling
    /// `update_with_expression` with no columns and no values. Otherwise it returns
    /// `OperationNotSupported`. Tables MUST override this with a version that changes
    /// each row in one step:
    /// - InMemoryTable: updates rows in place while holding the lock
    /// - JDBC and SQL backends: should use a transaction or a native UPDATE
    /// - Other backends: must give the same all or nothing guarantee
    ///
    /// Deleting and then inserting in two steps would lose data if the insert fails
    /// after the delete succeeds.
    fn update_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        _set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        // With no SET executors, hand off to update_with_expression, which most tables
        // already support. With empty columns and values it replaces each matched row
        // with an empty row.
        if set_value_executors.is_empty() {
            return self.update_with_expression(stream_event, condition_executor, &[], &[]);
        }

        // SET expressions need a table specific version that changes each row in one
        // step, so a two step delete and insert cannot lose data.
        Err(
            crate::core::exception::EventFluxError::OperationNotSupported {
                message: "UPDATE with SET expressions referencing table columns requires \
                     table-specific implementation. Override update_with_set_executors() \
                     in your Table implementation. See InMemoryTable for reference."
                    .to_string(),
                operation: Some("update_with_set_executors".to_string()),
            },
        )
    }

    /// Deletes the rows for which the condition, run on the stream event plus the
    /// table row, is true.
    ///
    /// Used for DELETE triggered by a stream event, where the condition uses both
    /// stream and table columns.
    ///
    /// # Arguments
    /// * `stream_event` - The event from the source stream that triggered the delete
    /// * `condition_executor` - Executor for the WHERE condition
    ///
    /// # Returns
    /// Number of rows deleted, or an error if the delete fails.
    ///
    /// # Count accuracy
    ///
    /// For tables with duplicate rows (same values), the default counts **distinct row
    /// values** removed, not total rows removed. `Table::delete()` removes EVERY copy of
    /// a matching row in one call and only returns a `bool`. If metrics need exact row
    /// counts, the table must track them itself.
    ///
    /// # Performance note for JDBC authors
    ///
    /// The default calls `all_rows()` and filters in memory, which is **slow on external
    /// databases**. JDBC and other SQL backed tables MUST override this and push the
    /// WHERE clause to the database.
    ///
    /// ## Example override for MySQL or PostgreSQL
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
            // Delete by exact row value. Later duplicates of a deleted row return false.
            let cond = InMemoryCompiledCondition {
                values: row.clone(),
            };
            if self.delete(&cond)? {
                deleted_count += 1;
            }
        }
        Ok(deleted_count)
    }

    /// Updates a matching row or inserts a new one, with SET expressions that can use
    /// table columns.
    ///
    /// Used for UPSERT triggered by a stream event:
    /// 1. If a row matches the ON condition, UPDATE it, optionally with SET expressions
    /// 2. If no row matches, INSERT a new row from the stream data
    ///
    /// # Arguments
    /// * `stream_event` - The event from the source stream that triggered the upsert
    /// * `condition_executor` - Executor for the ON condition
    /// * `set_column_indices` - Table column for each SET clause attribute. Empty means
    ///   replace the whole row.
    /// * `set_value_executors` - Executor for each SET clause value. Empty means
    ///   replace the whole row.
    ///
    /// # Returns
    /// (rows_updated, rows_inserted), or an error if the upsert fails
    ///
    /// # IMPORTANT: no safe default
    ///
    /// The default only handles an empty `set_value_executors`. It takes the new row
    /// from the stream event's output data, or its before window data if there is none,
    /// and returns (0, 0) if that row is empty. It then inserts the row, or replaces
    /// the first match with it. Otherwise it returns `OperationNotSupported`. Tables
    /// MUST override this with a version that changes each row in one step:
    /// - InMemoryTable: updates rows in place while holding the lock
    /// - JDBC and SQL backends: should use a transaction or a native UPSERT or MERGE
    /// - Other backends: must give the same all or nothing guarantee
    ///
    /// Deleting and then inserting in two steps would lose data if the insert fails
    /// after the delete succeeds.
    fn upsert_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        _set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<(usize, usize), crate::core::exception::EventFluxError> {
        // With no SET executors, find a match and then update it or insert. This keeps
        // existing Table implementations working. It is not one atomic step.
        if set_value_executors.is_empty() {
            let new_values = if let Some(output) = stream_event.get_output_data() {
                output.to_vec()
            } else {
                stream_event.before_window_data.clone()
            };

            if new_values.is_empty() {
                return Ok((0, 0));
            }

            let matched =
                find_matching_rows_for_mutation(self.all_rows()?, stream_event, condition_executor);

            if matched.is_empty() {
                self.insert(&new_values)?;
                return Ok((0, 1));
            } else {
                // Replace the first match. Since update matches by value, any
                // duplicates of that row change too, but the count stays 1.
                let cond = InMemoryCompiledCondition {
                    values: matched[0].clone(),
                };
                let us = InMemoryCompiledUpdateSet { values: new_values };
                if self.update(&cond, &us)? {
                    return Ok((1, 0));
                }
                return Ok((0, 0));
            }
        }

        // SET expressions need a table specific version that changes each row in one step.
        Err(
            crate::core::exception::EventFluxError::OperationNotSupported {
                message: "UPSERT with SET expressions referencing table columns requires \
                     table-specific implementation. Override upsert_with_set_executors() \
                     in your Table implementation. See InMemoryTable for reference."
                    .to_string(),
                operation: Some("upsert_with_set_executors".to_string()),
            },
        )
    }

    /// Clones the table into a new box. Backs `Clone` for `Box<dyn Table>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the clone fails, for example when it cannot reconnect to
    /// the database.
    fn clone_table(&self) -> Result<Box<dyn Table>, crate::core::exception::EventFluxError>;

    /// Phase 2 validation. Checks that the table's external store is reachable.
    ///
    /// Called while the app starts up (Phase 2) to check that external stores, such as
    /// databases and caches, are reachable and set up correctly.
    ///
    /// **Fail fast**: The app should NOT start if a table's store is not ready.
    ///
    /// # Default
    ///
    /// Returns Ok, since in memory tables have nothing external to check. Tables with
    /// an external store (MySQL, PostgreSQL, Redis) MUST override this.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The external store is reachable and set up correctly
    /// * `Err(EventFluxError)` - The check failed and the app should not start
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
        Ok(()) // nothing external to check
    }
}

// Panics if clone_table fails. That cannot happen for in memory tables, but can for
// tables backed by an external store.
impl Clone for Box<dyn Table> {
    fn clone(&self) -> Self {
        self.clone_table()
            .expect("Failed to clone table - this should not happen for in-memory tables")
    }
}

/// How many times a change is retried when another thread changed the row first.
const MAX_CONCURRENT_MODIFICATION_RETRIES: u32 = 3;

/// In memory table. Rows live in a vector, with a HashMap index for O(1) lookups.
///
/// # Concurrency
///
/// UPDATE and UPSERT use optimistic locking with retry:
/// 1. Read: take the read lock, find matching rows, save their current values
/// 2. Evaluate: compute new values, which can use stream and table data
/// 3. Write: take the write lock, check the rows did not change, apply the updates
///
/// If a row changed between steps, the change is retried up to
/// [`MAX_CONCURRENT_MODIFICATION_RETRIES`] times and then skipped.
///
/// # Performance
///
/// The "did not change" check compares column by column and skips `Object` columns,
/// since `Box<dyn Any>` cannot be cloned or compared. That costs O(data_columns) per
/// row and can get slow on very wide tables.
///
/// ## Possible later speedup: row version numbers
///
/// Faster would be a version counter per row:
/// - Read: save (row_values, version_number)
/// - Write: check version_number is the same, an O(1) integer compare
/// - On match: bump the version and apply the update
///
/// That would change row storage from `Vec<Vec<AttributeValue>>` to something like:
/// ```ignore
/// struct VersionedRow {
///     version: u64,
///     values: Vec<AttributeValue>,
/// }
/// ```
///
/// Gains:
/// - O(1) change check instead of O(columns)
/// - Catches a row that changed and then changed back (the ABA problem)
/// - A base for MVCC (keeping several versions of each row) if ever needed
#[derive(Debug)]
pub struct InMemoryTable {
    rows: RwLock<Vec<Vec<AttributeValue>>>,
    // Maps a row key to every index in `rows` with that value, so duplicates work.
    index: RwLock<HashMap<String, Vec<usize>>>,
    /// Updates skipped because another thread kept changing or deleting the row
    /// between the read and write steps until all retries ran out. Lets you see how
    /// often such races happen.
    concurrent_modification_skips: std::sync::atomic::AtomicU64,
    /// Retries caused by another thread changing a row first. Shows how often the
    /// optimistic locking retry kicks in.
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

    /// Returns how many updates were skipped after all retries ran out because another
    /// thread changed the row. Useful to watch optimistic locking failures under load.
    pub fn concurrent_modification_skips(&self) -> u64 {
        self.concurrent_modification_skips
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns how many retries happened because another thread changed a row first.
    /// High values mean heavy contention on this table.
    pub fn concurrent_modification_retries(&self) -> u64 {
        self.concurrent_modification_retries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Builds the index key for a row, so lookups are O(1) instead of a linear scan.
    /// Same as [`row_values_to_key`].
    fn row_to_key(row: &[AttributeValue]) -> String {
        row_values_to_key(row)
    }

    /// Checks whether two rows are the same, skipping Object columns.
    ///
    /// Needed because `AttributeValue::Object(Some(_))` clones to `Object(None)`, since
    /// `Box<dyn Any>` cannot be cloned, and `Object(_) == Object(_)` is always `false`,
    /// since opaque values cannot be compared.
    ///
    /// The optimistic locking check compares only columns that can be cloned and
    /// compared. Object columns are skipped, so a row where only Object columns changed
    /// counts as unchanged.
    ///
    /// # Concurrency warning for Object columns
    ///
    /// **Lost update risk**: If another thread changes only the Object fields of a row
    /// between the read and write steps, this check will NOT see it. The update goes
    /// ahead and can overwrite that change.
    ///
    /// **Ways to reduce the risk**:
    /// 1. Give tables with Object columns at least one other column that changes on
    ///    every logical change, such as a `last_modified` timestamp
    /// 2. For important data, add a `version` column that goes up on each update
    /// 3. Avoid Object columns in tables with many concurrent writes
    /// 4. For opaque data, use `Bytes` instead, which can be compared
    ///
    /// # Arguments
    /// * `a` - First row, usually the copy saved in the read step
    /// * `b` - Second row, usually the current row in the table
    ///
    /// # Returns
    /// `true` if the rows have the same length and every non Object column is equal
    fn rows_equal_ignoring_objects(a: &[AttributeValue], b: &[AttributeValue]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (av, bv) in a.iter().zip(b.iter()) {
            match (av, bv) {
                // Object values cannot be compared after cloning, so skip them.
                (AttributeValue::Object(_), AttributeValue::Object(_)) => continue,
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

        let mut index = self.index.write().unwrap();
        index.entry(key).or_default().push(new_index);
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

        // Find matching rows through the index, O(1) instead of O(n). Rows that differ
        // only in Object columns share a key, so they all get replaced.
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

        for &idx in &indices_to_update {
            if let Some(row) = rows.get_mut(idx) {
                *row = us.values.clone();
            }
        }

        // Move the indices from the old key to the new key.
        index.remove(&old_key);
        index.entry(new_key).or_default().extend(indices_to_update);

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

        // Quick O(1) index check before the scan.
        if !index.contains_key(&key) {
            return Ok(false);
        }

        let orig_len = rows.len();
        rows.retain(|row| row.as_slice() != cond.values.as_slice());

        if orig_len == rows.len() {
            return Ok(false);
        }

        // Rebuild the index, since row positions shifted after the delete. O(n), but
        // deletes are rarer than reads and finds.
        index.clear();
        for (idx, row) in rows.iter().enumerate() {
            let row_key = Self::row_to_key(row);
            index.entry(row_key).or_default().push(idx);
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

        // O(1) index lookup instead of a linear scan. Errors above, instead of
        // returning None, if the condition is not an InMemoryCompiledCondition.
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

        // O(1) index lookup instead of a linear scan.
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
                joined.before_window_data[stream_attr_count..(row.len() + stream_attr_count)]
                    .clone_from_slice(&row[..]);
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

        // Rebuild the index for the copy. Retry counters start at zero.
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

    /// update_with_set_executors for InMemoryTable, applying the changes under one
    /// write lock.
    ///
    /// Reading and writing are separate steps, so expression executors (such as UDFs)
    /// that read this table cannot deadlock. The updates and index changes are then
    /// applied together under the write lock.
    ///
    /// # Concurrent changes
    ///
    /// Uses optimistic locking with retry. If a row changed between the read and write
    /// steps, the whole operation starts again and runs the expressions on current
    /// data. This avoids lost updates for expressions like `SET count = count + 1`.
    ///
    /// # Performance warning
    ///
    /// The read step scans the whole table, O(N), and clones a StreamEvent per row to
    /// put in the StateEvent for evaluation. With 100k+ rows that is 100k+ vector
    /// clones for every incoming event.
    ///
    /// **TODO: fast path through the index**
    /// When the condition is a simple equality on indexed columns, such as
    /// `table.id == stream.id`, look rows up through `self.index` in O(1) before falling
    /// back to the full scan. That would make the common keyed update much faster.
    fn update_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for rows changed by another thread.
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Step 1, read. Under the read lock, collect the matching rows and their
            // indexes, so the expressions can then run with no lock held.
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                // Build the StateEvent once, outside the loop, to cut allocations.
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Size the reused row event from the first row. Assumes every row has
                // the same width.
                let max_cols = rows.first().map_or(0, |r| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                for (idx, row) in rows.iter().enumerate() {
                    // Overwrite the reused event's values instead of building a new one.
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    // The clone is needed because StateEvent.stream_events owns its
                    // StreamEvents. A known allocation hot spot. Arc<StreamEvent> or
                    // borrowing would allow going through rows with no copies.
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    if let Some(AttributeValue::Bool(true)) =
                        condition_executor.execute(Some(&state_event))
                    {
                        matches.push((idx, row.clone()));
                    }
                }
                matches
            }; // read lock released here

            if matched_rows.is_empty() {
                return Ok(total_updated);
            }

            // Step 2, evaluate the SET expressions with no lock held, so executors that
            // read this table cannot deadlock.
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            let mut state_event = StateEvent::new(2, 0);
            state_event.timestamp = stream_event.timestamp;
            state_event.stream_events[0] = Some(stream_event.clone());

            let max_cols = matched_rows.first().map_or(0, |(_, r)| r.len());
            let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

            for (_idx, row) in &matched_rows {
                for (j, val) in row.iter().enumerate() {
                    if j < table_row_event.before_window_data.len() {
                        table_row_event.before_window_data[j] = val.clone();
                    }
                }
                state_event.stream_events[1] = Some(table_row_event.clone());

                // If any SET expression returns None, this row is skipped silently.
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

                // Copy the SET values into the existing row.
                let mut new_row = row.clone();
                for (i, &col_idx) in set_column_indices.iter().enumerate() {
                    if col_idx < new_row.len() && i < set_values.len() {
                        new_row[col_idx] = set_values[i].clone();
                    }
                }

                updates.push((row.clone(), new_row));
            }

            // Step 3, write. Check every row first, then apply all updates. Applying
            // some and then retrying would update those rows twice.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Check that EVERY row is still there before changing ANY of them, so
            // either all updates apply or none do.
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
                    // One failed check means the whole batch must be retried.
                    verification_failed = true;
                    break;
                }
            }

            if verification_failed {
                // Apply nothing. Retry if attempts are left.
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // Out of retries. Return an error instead of silently dropping writes.
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(
                    crate::core::exception::EventFluxError::QueryableRecordTable {
                        message: format!(
                            "Concurrent modification limit exceeded after {} retries. \
                         {} updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                            MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                        ),
                        table_name: None,
                    },
                );
            }

            // Every row checked out, so apply all updates.
            for (idx, old_key, new_row) in verified_updates {
                let new_key = Self::row_to_key(&new_row);
                rows[idx] = new_row;
                total_updated += 1;

                // Fix up the index only when the key changed.
                if old_key != new_key {
                    if let Some(indices) = index.get_mut(&old_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    index.entry(new_key).or_default().push(idx);
                }
            }

            return Ok(total_updated);
        }

        // Not reached. Every pass of the loop returns.
        Ok(total_updated)
    }

    /// upsert_with_set_executors for InMemoryTable, applying the changes under one
    /// write lock.
    ///
    /// Reading and writing are separate steps, so expression executors (such as UDFs)
    /// that read this table cannot deadlock. The updates and index changes are then
    /// applied together under the write lock.
    ///
    /// # Performance warning
    ///
    /// The read step scans the whole table, O(N), and clones a StreamEvent per row to
    /// put in the StateEvent for evaluation. With 100k+ rows that is 100k+ vector
    /// clones for every incoming event.
    ///
    /// **TODO: fast path through the index**
    /// When the ON condition is a simple equality on indexed columns, such as
    /// `table.id == stream.id`, look rows up through `self.index` in O(1) before falling
    /// back to the full scan.
    fn upsert_with_set_executors(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_value_executors: &[Box<dyn ExpressionExecutor>],
    ) -> Result<(usize, usize), crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for rows changed by another thread, on the UPDATE path.
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Step 1, read. Under the read lock, collect the matching rows and their indexes.
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                // Build the StateEvent once, outside the loop, to cut allocations.
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Size the reused row event from the first row. Assumes every row has
                // the same width.
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
            }; // read lock released here

            if matched_rows.is_empty() {
                // Nothing matched. Leave the loop and go to the INSERT path.
                break;
            }

            // UPDATE path, at least one row matched.
            // Step 2, evaluate the SET expressions with no lock held, so executors that
            // read this table cannot deadlock.
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            if !set_value_executors.is_empty() {
                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                let max_cols = matched_rows.first().map_or(0, |(_, r)| r.len());
                let mut table_row_event = StreamEvent::new(stream_event.timestamp, max_cols, 0, 0);

                // Run the SET executors for each row, with that row in the event.
                for (_idx, row) in &matched_rows {
                    for (j, val) in row.iter().enumerate() {
                        if j < table_row_event.before_window_data.len() {
                            table_row_event.before_window_data[j] = val.clone();
                        }
                    }
                    state_event.stream_events[1] = Some(table_row_event.clone());

                    // If any SET expression returns None, this row is skipped silently.
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

                    // With no column indices the SET values are the whole new row.
                    // Otherwise copy them into the existing row.
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
                // No SET executors. Replace each matched row with the stream values,
                // output data first, else before window data. An empty value list
                // updates nothing.
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

            // Step 3, write. Check every row first, then apply all updates. Applying
            // some and then retrying would update those rows twice.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Check that EVERY row is still there before changing ANY of them.
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
                // Apply nothing. Retry if attempts are left.
                if attempt < MAX_CONCURRENT_MODIFICATION_RETRIES {
                    self.concurrent_modification_retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // Out of retries. Return an error instead of silently dropping writes.
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(
                    crate::core::exception::EventFluxError::QueryableRecordTable {
                        message: format!(
                            "Concurrent modification limit exceeded after {} retries. \
                         {} upsert updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                            MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                        ),
                        table_name: None,
                    },
                );
            }

            // Every row checked out, so apply all updates.
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
                    index.entry(new_key).or_default().push(idx);
                }
            }

            return Ok((total_updated, 0));
        }

        // INSERT path. The read step found no matching row.
        {
            // CRITICAL: check again under the write lock. Another thread may have
            // inserted a matching row between our read and write steps.
            let new_values = if let Some(output) = stream_event.get_output_data() {
                output.to_vec()
            } else {
                stream_event.before_window_data.clone()
            };

            if !new_values.is_empty() {
                let mut rows = self.rows.write().unwrap();
                let mut index = self.index.write().unwrap();

                // Look again for a match added by another thread after the read lock
                // was released, so an upsert does not insert a duplicate. The condition
                // runs here while the write lock is held.
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
                        break; // update this row instead of inserting
                    }
                }

                if let Some(idx) = matching_idx {
                    // A matching row showed up between the read and write steps.
                    // Replace it with the stream values instead of inserting a duplicate.
                    // SET executors are not applied on this path.
                    let old_key = Self::row_to_key(&rows[idx]);
                    let new_key = Self::row_to_key(&new_values);

                    rows[idx] = new_values;

                    if old_key != new_key {
                        if let Some(indices) = index.get_mut(&old_key) {
                            indices.retain(|&i| i != idx);
                            if indices.is_empty() {
                                index.remove(&old_key);
                            }
                        }
                        index.entry(new_key).or_default().push(idx);
                    }
                    Ok((1, 0)) // updated, not inserted
                } else {
                    // Still no match under the write lock, so inserting is safe.
                    let key = Self::row_to_key(&new_values);
                    let new_index = rows.len();
                    rows.push(new_values);
                    index.entry(key).or_default().push(new_index);
                    Ok((0, 1))
                }
            } else {
                Ok((0, 0))
            }
        }
    }

    /// Faster delete_with_expression for InMemoryTable.
    ///
    /// Does not copy the whole table through `all_rows()`, and retries when rows change
    /// underneath it. Unlike the default, it counts every deleted row, duplicates
    /// included. Deleting with swap_remove does not keep row order.
    ///
    /// # Concurrent changes
    ///
    /// Uses optimistic locking with retry. If rows change between the read and write
    /// steps, the whole operation starts again. After all retries it returns an error.
    fn delete_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        // Retry loop for rows changed by another thread.
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Step 1, read. Under the read lock, find the matching rows and their indexes.
            let matched_rows: Vec<(usize, Vec<AttributeValue>)> = {
                let rows = self.rows.read().unwrap();
                let mut matches = Vec::new();

                let mut state_event = StateEvent::new(2, 0);
                state_event.timestamp = stream_event.timestamp;
                state_event.stream_events[0] = Some(stream_event.clone());

                // Size the reused row event from the first row.
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
            }; // read lock released here

            if matched_rows.is_empty() {
                return Ok(0);
            }

            // Step 2, write. Check every row, then delete them all.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Check that EVERY row is still there before changing ANY of them.
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
                    // The row changed or is gone, so retry.
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
                // Out of retries. Return an error instead of silently dropping writes.
                let skipped = matched_rows.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(
                    crate::core::exception::EventFluxError::QueryableRecordTable {
                        message: format!(
                            "Concurrent modification limit exceeded after {} retries. \
                         {} deletes could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                            MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                        ),
                        table_name: None,
                    },
                );
            }

            // Delete with swap_remove, O(1) each. Go from the highest index down, so a
            // swap never moves a row that is still waiting to be deleted.
            to_delete.sort_by(|a, b| b.cmp(a));
            let deleted_count = to_delete.len();

            for idx in to_delete {
                let last_idx = rows.len() - 1;

                if idx == last_idx {
                    // Last row, so just pop it. No other row moves.
                    let removed = rows.pop().unwrap();
                    let key = Self::row_to_key(&removed);
                    if let Some(indices) = index.get_mut(&key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&key);
                        }
                    }
                } else {
                    // swap_remove moves the last row into idx. Only that moved row's
                    // index entry needs fixing, from last_idx to idx.
                    let removed = rows.swap_remove(idx);
                    let removed_key = Self::row_to_key(&removed);

                    if let Some(indices) = index.get_mut(&removed_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&removed_key);
                        }
                    }

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

    /// Faster update_with_expression for InMemoryTable.
    ///
    /// Does not copy the whole table through `all_rows()`, and retries when rows change
    /// underneath it. After all retries it returns an error. Counts every updated
    /// row, duplicates included.
    ///
    /// # Note
    ///
    /// For simple updates where the SET values are already computed. For SET
    /// expressions that use table columns, such as `SET count = count + 1`, use
    /// `update_with_set_executors` instead.
    fn update_with_expression(
        &self,
        stream_event: &StreamEvent,
        condition_executor: &dyn ExpressionExecutor,
        set_column_indices: &[usize],
        set_values: &[AttributeValue],
    ) -> Result<usize, crate::core::exception::EventFluxError> {
        use crate::core::event::state::state_event::StateEvent;

        let mut total_updated = 0;

        // Retry loop for rows changed by another thread.
        for attempt in 0..=MAX_CONCURRENT_MODIFICATION_RETRIES {
            // Step 1, read. Under the read lock, find the matching rows.
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
            }; // read lock released here

            if matched_rows.is_empty() {
                return Ok(total_updated);
            }

            // Step 2, compute the new rows with no lock held.
            let mut updates: Vec<(Vec<AttributeValue>, Vec<AttributeValue>)> = Vec::new();

            for (_idx, row) in &matched_rows {
                let new_row = if set_column_indices.is_empty() {
                    // No column indices, so set_values is the whole new row.
                    set_values.to_vec()
                } else {
                    // Partial update. Copy the SET values into the existing row.
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

            // Step 3, write. Check every row first, then apply all updates.
            let mut rows = self.rows.write().unwrap();
            let mut index = self.index.write().unwrap();

            // Check that EVERY row is still there before changing ANY of them.
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
                // Out of retries. Return an error instead of silently dropping writes.
                let skipped = updates.len();
                self.concurrent_modification_skips
                    .fetch_add(skipped as u64, std::sync::atomic::Ordering::Relaxed);
                return Err(
                    crate::core::exception::EventFluxError::QueryableRecordTable {
                        message: format!(
                            "Concurrent modification limit exceeded after {} retries. \
                         {} updates could not be applied due to table contention. \
                         Consider routing to fault stream or implementing backoff retry.",
                            MAX_CONCURRENT_MODIFICATION_RETRIES, skipped
                        ),
                        table_name: None,
                    },
                );
            }

            // Every row checked out, so apply all updates.
            for (idx, old_key, new_row) in verified_updates {
                let new_key = Self::row_to_key(&new_row);
                rows[idx] = new_row;
                total_updated += 1;

                // Fix up the index only when the key changed.
                if old_key != new_key {
                    if let Some(indices) = index.get_mut(&old_key) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    index.entry(new_key).or_default().push(idx);
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
