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

use crate::core::config::eventflux_context::EventFluxContext;
use crate::core::event::stream::stream_event::StreamEvent;
use crate::core::event::value::AttributeValue;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::table::{
    constant_to_av, CompiledCondition, CompiledUpdateSet, InMemoryCompiledCondition,
    InMemoryCompiledUpdateSet, Table,
};
use crate::query_api::execution::query::output::stream::UpdateSet;
use crate::query_api::expression::Expression;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct JdbcTable {
    table_name: String,
    data_source_name: String,
    eventflux_context: Arc<EventFluxContext>,
    conn: Arc<Mutex<Connection>>,
}

impl JdbcTable {
    pub fn new(
        table_name: String,
        data_source_name: String,
        eventflux_context: Arc<EventFluxContext>,
    ) -> Result<Self, String> {
        let ds = eventflux_context
            .get_data_source(&data_source_name)
            .ok_or_else(|| format!("DataSource '{data_source_name}' not found"))?;
        let conn_any = ds.get_connection()?;
        let conn_arc = match conn_any.downcast::<Arc<Mutex<Connection>>>() {
            Ok(c) => *c,
            Err(conn_any) => {
                let conn_box = conn_any
                    .downcast::<Connection>()
                    .map_err(|_| "Invalid connection type".to_string())?;
                Arc::new(Mutex::new(*conn_box))
            }
        };
        Ok(Self {
            table_name,
            data_source_name,
            eventflux_context,
            conn: conn_arc,
        })
    }

    fn av_to_val(av: &AttributeValue) -> Value {
        match av {
            AttributeValue::String(s) => Value::Text(s.clone()),
            AttributeValue::Int(i) => Value::Integer(*i as i64),
            AttributeValue::Long(l) => Value::Integer(*l),
            AttributeValue::Float(f) => Value::Real(*f as f64),
            AttributeValue::Double(d) => Value::Real(*d),
            AttributeValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
            AttributeValue::Bytes(b) => Value::Blob(b.clone()),
            AttributeValue::Null => Value::Null,
            AttributeValue::Object(_) => Value::Null,
        }
    }

    fn row_to_attr(row: &rusqlite::Row) -> Vec<AttributeValue> {
        (0..row.as_ref().column_count())
            .map(|i| match row.get_ref_unwrap(i) {
                ValueRef::Null => AttributeValue::Null,
                ValueRef::Integer(v) => AttributeValue::Long(v),
                ValueRef::Real(v) => AttributeValue::Double(v),
                ValueRef::Text(v) => AttributeValue::String(String::from_utf8_lossy(v).to_string()),
                ValueRef::Blob(v) => AttributeValue::Bytes(v.to_vec()),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct JdbcCompiledCondition {
    pub where_clause: String,
    pub params: Vec<AttributeValue>,
}

impl CompiledCondition for JdbcCompiledCondition {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct JdbcCompiledUpdateSet {
    pub assignments: String,
    pub params: Vec<AttributeValue>,
}

impl CompiledUpdateSet for JdbcCompiledUpdateSet {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone)]
pub enum JoinParamSource {
    Constant(AttributeValue),
    StreamAttr(usize),
}

#[derive(Debug, Clone)]
pub struct JdbcJoinCompiledCondition {
    pub where_clause: String,
    pub params: Vec<JoinParamSource>,
}

impl CompiledCondition for JdbcJoinCompiledCondition {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn expression_to_sql(expr: &Expression, params: &mut Vec<AttributeValue>) -> String {
    use crate::query_api::expression::condition::compare::Operator as CmpOp;
    use Expression::*;
    match expr {
        Constant(c) => {
            params.push(constant_to_av(c));
            "?".to_string()
        }
        Variable(v) => v.attribute_name.clone(),
        Compare(c) => {
            let left = expression_to_sql(&c.left_expression, params);
            let right = expression_to_sql(&c.right_expression, params);
            let op = match c.operator {
                CmpOp::LessThan => "<",
                CmpOp::GreaterThan => ">",
                CmpOp::LessThanEqual => "<=",
                CmpOp::GreaterThanEqual => ">=",
                CmpOp::Equal => "=",
                CmpOp::NotEqual => "!=",
            };
            format!("{left} {op} {right}")
        }
        And(a) => {
            let left = expression_to_sql(&a.left_expression, params);
            let right = expression_to_sql(&a.right_expression, params);
            format!("({left} AND {right})")
        }
        Or(o) => {
            let left = expression_to_sql(&o.left_expression, params);
            let right = expression_to_sql(&o.right_expression, params);
            format!("({left} OR {right})")
        }
        Not(n) => {
            let inner = expression_to_sql(&n.expression, params);
            format!("NOT ({inner})")
        }
        _ => String::new(),
    }
}

fn expression_to_sql_join(
    expr: &Expression,
    stream_id: &str,
    stream_def: &crate::query_api::definition::stream_definition::StreamDefinition,
    params: &mut Vec<JoinParamSource>,
) -> String {
    use crate::query_api::expression::condition::compare::Operator as CmpOp;
    use Expression::*;
    match expr {
        Constant(c) => {
            params.push(JoinParamSource::Constant(constant_to_av(c)));
            "?".to_string()
        }
        Variable(v) => {
            if v.stream_id.as_deref() == Some(stream_id) {
                if let Some(idx) = stream_def
                    .abstract_definition
                    .attribute_list
                    .iter()
                    .position(|a| a.get_name() == &v.attribute_name)
                {
                    params.push(JoinParamSource::StreamAttr(idx));
                    "?".to_string()
                } else {
                    v.attribute_name.clone()
                }
            } else {
                v.attribute_name.clone()
            }
        }
        Compare(c) => {
            let left = expression_to_sql_join(&c.left_expression, stream_id, stream_def, params);
            let right = expression_to_sql_join(&c.right_expression, stream_id, stream_def, params);
            let op = match c.operator {
                CmpOp::LessThan => "<",
                CmpOp::GreaterThan => ">",
                CmpOp::LessThanEqual => "<=",
                CmpOp::GreaterThanEqual => ">=",
                CmpOp::Equal => "=",
                CmpOp::NotEqual => "!=",
            };
            format!("{left} {op} {right}")
        }
        And(a) => {
            let left = expression_to_sql_join(&a.left_expression, stream_id, stream_def, params);
            let right = expression_to_sql_join(&a.right_expression, stream_id, stream_def, params);
            format!("({left} AND {right})")
        }
        Or(o) => {
            let left = expression_to_sql_join(&o.left_expression, stream_id, stream_def, params);
            let right = expression_to_sql_join(&o.right_expression, stream_id, stream_def, params);
            format!("({left} OR {right})")
        }
        Not(n) => {
            let inner = expression_to_sql_join(&n.expression, stream_id, stream_def, params);
            format!("NOT ({inner})")
        }
        _ => String::new(),
    }
}

impl Table for JdbcTable {
    fn insert(
        &self,
        values: &[AttributeValue],
    ) -> Result<(), crate::core::exception::EventFluxError> {
        let placeholders = (0..values.len()).map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("INSERT INTO {} VALUES ({})", self.table_name, placeholders);
        let params: Vec<Value> = values.iter().map(Self::av_to_val).collect();

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        conn.execute(&sql, params_from_iter(params.iter()))
            .map_err(|e| {
                log::error!("JDBC insert failed on table '{}': {}", self.table_name, e);
                crate::core::exception::EventFluxError::from(e)
            })?;

        Ok(())
    }

    fn all_rows(&self) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        let sql = format!("SELECT * FROM {}", self.table_name);

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            log::error!(
                "JDBC prepare failed for all_rows on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        let mut rows = stmt.query([]).map_err(|e| {
            log::error!(
                "JDBC query failed for all_rows on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| {
            log::error!(
                "JDBC row iteration failed for all_rows on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })? {
            out.push(Self::row_to_attr(row));
        }
        Ok(out)
    }

    fn update(
        &self,
        condition: &dyn CompiledCondition,
        update_set: &dyn CompiledUpdateSet,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        if let (Some(cond), Some(us)) = (
            condition.as_any().downcast_ref::<JdbcCompiledCondition>(),
            update_set.as_any().downcast_ref::<JdbcCompiledUpdateSet>(),
        ) {
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                self.table_name, us.assignments, cond.where_clause
            );
            let mut params: Vec<Value> = us.params.iter().map(Self::av_to_val).collect();
            params.extend(cond.params.iter().map(Self::av_to_val));

            let conn = self.conn.lock().map_err(|e| {
                crate::core::exception::EventFluxError::DatabaseRuntime {
                    message: format!("Failed to acquire connection lock: {}", e),
                    source: None,
                }
            })?;

            let count = conn
                .execute(&sql, params_from_iter(params.iter()))
                .map_err(|e| {
                    log::error!("JDBC update failed on table '{}': {}", self.table_name, e);
                    crate::core::exception::EventFluxError::from(e)
                })?;
            return Ok(count > 0);
        }

        let cond_vals = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => &c.values,
            None => return Ok(false),
        };
        let us_vals = match update_set
            .as_any()
            .downcast_ref::<InMemoryCompiledUpdateSet>()
        {
            Some(u) => &u.values,
            None => return Ok(false),
        };
        let set_clause = (0..us_vals.len())
            .map(|i| format!("c{i}=?"))
            .collect::<Vec<_>>()
            .join(",");
        let where_clause = (0..cond_vals.len())
            .map(|i| format!("c{i}=?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            self.table_name, set_clause, where_clause
        );
        let mut params: Vec<Value> = us_vals.iter().map(Self::av_to_val).collect();
        params.extend(cond_vals.iter().map(Self::av_to_val));

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        let count = conn
            .execute(&sql, params_from_iter(params.iter()))
            .map_err(|e| {
                log::error!(
                    "JDBC update (fallback) failed on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;
        Ok(count > 0)
    }

    fn delete(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        if let Some(cond) = condition.as_any().downcast_ref::<JdbcCompiledCondition>() {
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                self.table_name, cond.where_clause
            );
            let params: Vec<Value> = cond.params.iter().map(Self::av_to_val).collect();

            let conn = self.conn.lock().map_err(|e| {
                crate::core::exception::EventFluxError::DatabaseRuntime {
                    message: format!("Failed to acquire connection lock: {}", e),
                    source: None,
                }
            })?;

            let count = conn
                .execute(&sql, params_from_iter(params.iter()))
                .map_err(|e| {
                    log::error!("JDBC delete failed on table '{}': {}", self.table_name, e);
                    crate::core::exception::EventFluxError::from(e)
                })?;
            return Ok(count > 0);
        }

        let values = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => &c.values,
            None => return Ok(false),
        };
        let where_clause = (0..values.len())
            .map(|i| format!("c{i}=?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {} WHERE {}", self.table_name, where_clause);
        let params: Vec<Value> = values.iter().map(Self::av_to_val).collect();

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        let count = conn
            .execute(&sql, params_from_iter(params.iter()))
            .map_err(|e| {
                log::error!(
                    "JDBC delete (fallback) failed on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;
        Ok(count > 0)
    }

    fn find(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<Option<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        if let Some(cond) = condition.as_any().downcast_ref::<JdbcCompiledCondition>() {
            let sql = format!(
                "SELECT * FROM {} WHERE {} LIMIT 1",
                self.table_name, cond.where_clause
            );
            let params: Vec<Value> = cond.params.iter().map(Self::av_to_val).collect();

            let conn = self.conn.lock().map_err(|e| {
                crate::core::exception::EventFluxError::DatabaseRuntime {
                    message: format!("Failed to acquire connection lock: {}", e),
                    source: None,
                }
            })?;

            let mut stmt = conn.prepare(&sql).map_err(|e| {
                log::error!(
                    "JDBC prepare failed for find on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;

            let mut rows = stmt.query(params_from_iter(params.iter())).map_err(|e| {
                log::error!(
                    "JDBC query failed for find on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;

            return rows
                .next()
                .map_err(|e| {
                    log::error!(
                        "JDBC row fetch failed for find on table '{}': {}",
                        self.table_name,
                        e
                    );
                    crate::core::exception::EventFluxError::from(e)
                })
                .map(|opt_row| opt_row.map(|row| Self::row_to_attr(row)));
        }

        let values = match condition
            .as_any()
            .downcast_ref::<InMemoryCompiledCondition>()
        {
            Some(c) => &c.values,
            None => return Ok(None),
        };
        let where_clause = (0..values.len())
            .map(|i| format!("c{i}=?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            self.table_name, where_clause
        );
        let params: Vec<Value> = values.iter().map(Self::av_to_val).collect();

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            log::error!(
                "JDBC prepare failed for find (fallback) on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        let mut rows = stmt.query(params_from_iter(params.iter())).map_err(|e| {
            log::error!(
                "JDBC query failed for find (fallback) on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        rows.next()
            .map_err(|e| {
                log::error!(
                    "JDBC row fetch failed for find (fallback) on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })
            .map(|opt_row| opt_row.map(|row| Self::row_to_attr(row)))
    }

    fn contains(
        &self,
        condition: &dyn CompiledCondition,
    ) -> Result<bool, crate::core::exception::EventFluxError> {
        Ok(self.find(condition)?.is_some())
    }

    fn find_rows_for_join(
        &self,
        stream_event: &StreamEvent,
        compiled_condition: Option<&dyn CompiledCondition>,
        condition_executor: Option<&dyn ExpressionExecutor>,
    ) -> Result<Vec<Vec<AttributeValue>>, crate::core::exception::EventFluxError> {
        if let Some(cond) =
            compiled_condition.and_then(|c| c.as_any().downcast_ref::<JdbcJoinCompiledCondition>())
        {
            let sql = if cond.where_clause.is_empty() {
                format!("SELECT * FROM {}", self.table_name)
            } else {
                format!(
                    "SELECT * FROM {} WHERE {}",
                    self.table_name, cond.where_clause
                )
            };
            let mut params: Vec<Value> = Vec::new();
            for p in &cond.params {
                match p {
                    JoinParamSource::Constant(av) => params.push(Self::av_to_val(av)),
                    JoinParamSource::StreamAttr(idx) => {
                        params.push(Self::av_to_val(&stream_event.before_window_data[*idx]))
                    }
                }
            }

            let conn = self.conn.lock().map_err(|e| {
                crate::core::exception::EventFluxError::DatabaseRuntime {
                    message: format!("Failed to acquire connection lock: {}", e),
                    source: None,
                }
            })?;

            let mut stmt = conn.prepare(&sql).map_err(|e| {
                log::error!(
                    "JDBC prepare failed for find_rows_for_join on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;

            let mut rows = stmt.query(params_from_iter(params.iter())).map_err(|e| {
                log::error!(
                    "JDBC query failed for find_rows_for_join on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })?;

            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(|e| {
                log::error!(
                    "JDBC row iteration failed for find_rows_for_join on table '{}': {}",
                    self.table_name,
                    e
                );
                crate::core::exception::EventFluxError::from(e)
            })? {
                out.push(Self::row_to_attr(row));
            }
            return Ok(out);
        }

        // Fallback to in-memory style evaluation
        let sql = format!("SELECT * FROM {}", self.table_name);

        let conn = self.conn.lock().map_err(|e| {
            crate::core::exception::EventFluxError::DatabaseRuntime {
                message: format!("Failed to acquire connection lock: {}", e),
                source: None,
            }
        })?;

        let mut stmt = conn.prepare(&sql).map_err(|e| {
            log::error!(
                "JDBC prepare failed for find_rows_for_join (fallback) on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        let mut rows_iter = stmt.query([]).map_err(|e| {
            log::error!(
                "JDBC query failed for find_rows_for_join (fallback) on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })?;

        let mut matched = Vec::new();
        let stream_attr_count = stream_event.before_window_data.len();
        while let Some(row) = rows_iter.next().map_err(|e| {
            log::error!(
                "JDBC row iteration failed for find_rows_for_join (fallback) on table '{}': {}",
                self.table_name,
                e
            );
            crate::core::exception::EventFluxError::from(e)
        })? {
            let vals = Self::row_to_attr(row);
            if let Some(exec) = condition_executor {
                let mut joined =
                    StreamEvent::new(stream_event.timestamp, stream_attr_count + vals.len(), 0, 0);
                for i in 0..stream_attr_count {
                    joined.before_window_data[i] = stream_event.before_window_data[i].clone();
                }
                joined.before_window_data[stream_attr_count..(vals.len() + stream_attr_count)]
                    .clone_from_slice(&vals[..]);
                if let Some(AttributeValue::Bool(true)) = exec.execute(Some(&joined)) {
                    matched.push(vals);
                }
            } else {
                matched.push(vals);
            }
        }
        Ok(matched)
    }

    fn compile_condition(&self, cond: Expression) -> Box<dyn CompiledCondition> {
        let mut params = Vec::new();
        let where_clause = expression_to_sql(&cond, &mut params);
        Box::new(JdbcCompiledCondition {
            where_clause,
            params,
        })
    }

    fn compile_update_set(&self, us: UpdateSet) -> Box<dyn CompiledUpdateSet> {
        let mut params = Vec::new();
        let mut assigns = Vec::new();
        for sa in us.set_attributes.iter() {
            let expr_sql = expression_to_sql(&sa.value_to_set, &mut params);
            assigns.push(format!("{} = {}", sa.table_column.attribute_name, expr_sql));
        }
        Box::new(JdbcCompiledUpdateSet {
            assignments: assigns.join(","),
            params,
        })
    }

    fn compile_join_condition(
        &self,
        cond: Expression,
        stream_id: &str,
        stream_def: &crate::query_api::definition::stream_definition::StreamDefinition,
    ) -> Option<Box<dyn CompiledCondition>> {
        let mut params = Vec::new();
        let where_clause = expression_to_sql_join(&cond, stream_id, stream_def, &mut params);
        Some(Box::new(JdbcJoinCompiledCondition {
            where_clause,
            params,
        }))
    }

    fn clone_table(&self) -> Result<Box<dyn Table>, crate::core::exception::EventFluxError> {
        Ok(Box::new(
            JdbcTable::new(
                self.table_name.clone(),
                self.data_source_name.clone(),
                Arc::clone(&self.eventflux_context),
            )
            .map_err(|e| {
                log::error!("Failed to clone JDBC table '{}': {}", self.table_name, e);
                crate::core::exception::EventFluxError::DatabaseRuntime {
                    message: format!("Failed to clone JDBC table '{}': {}", self.table_name, e),
                    source: None,
                }
            })?,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct JdbcTableFactory;

impl crate::core::extension::TableFactory for JdbcTableFactory {
    fn name(&self) -> &'static str {
        "jdbc"
    }
    fn create(
        &self,
        table_name: String,
        mut properties: std::collections::HashMap<String, String>,
        ctx: Arc<EventFluxContext>,
    ) -> Result<Arc<dyn Table>, String> {
        let ds = properties
            .remove("data_source")
            .ok_or_else(|| "data_source property required".to_string())?;
        Ok(Arc::new(JdbcTable::new(table_name, ds, ctx)?))
    }

    fn clone_box(&self) -> Box<dyn crate::core::extension::TableFactory> {
        Box::new(self.clone())
    }
}
