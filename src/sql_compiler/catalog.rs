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

//! SQL catalog.
//!
//! Keeps the stream, table and trigger definitions that SQL queries are checked against.

use crate::query_api::definition::abstract_definition::AbstractDefinition;
use crate::query_api::definition::attribute::{Attribute, Type as AttributeType};
use crate::query_api::definition::{StreamDefinition, TableDefinition, TriggerDefinition};
use crate::query_api::eventflux_app::EventFluxApp;
use crate::query_api::execution::ExecutionElement;
use crate::query_api::expression::Expression;
use sqlparser::ast::ColumnDef;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{ApplicationError, CatalogError};
use super::type_inference::TypeInferenceEngine;

/// A stream or a table, as named in a SQL query.
///
/// Gives one type for schema lookups and checks, whichever kind the name refers to.
///
/// # Semantics
///
/// - **Stream**: A source of events in time order
/// - **Table**: A stored lookup table, in memory or backed by a database
///
/// # Query API vs Runtime
///
/// The Query API names streams and tables the same way, through `SingleInputStream`.
/// The runtime works out which one it is by checking `EventFluxApp.table_definition_map`,
/// and picks the processor:
///
/// - Stream to stream JOIN → `JoinProcessor` (join over windows)
/// - Stream to table JOIN → `TableJoinProcessor` (lookup to enrich each event)
///
/// So the query structures do not need to know the schema.
///
/// # Examples
///
/// The last query below is rejected. The converter returns `DirectTableQuery` because
/// a table must be used through a JOIN.
///
/// ```sql
/// -- Stream-Stream JOIN (temporal)
/// SELECT * FROM stream1 JOIN stream2 ON stream1.id = stream2.id;
///
/// -- Stream-Table JOIN (enrichment)
/// SELECT * FROM events JOIN user_profiles ON events.userId = user_profiles.id;
///
/// -- FROM clause with table
/// SELECT * FROM customer_cache WHERE region = 'US';
/// ```
#[derive(Debug, Clone)]
pub enum Relation {
    Stream(Arc<StreamDefinition>),

    Table(Arc<TableDefinition>),
}

impl Relation {
    /// The schema (attribute names and types), for a stream or a table alike.
    pub fn abstract_definition(&self) -> &AbstractDefinition {
        match self {
            Relation::Stream(stream) => &stream.abstract_definition,
            Relation::Table(table) => &table.abstract_definition,
        }
    }

    pub fn is_stream(&self) -> bool {
        matches!(self, Relation::Stream(_))
    }

    pub fn is_table(&self) -> bool {
        matches!(self, Relation::Table(_))
    }
}

/// The name and columns of a CREATE STREAM statement.
#[derive(Debug, Clone)]
pub struct CreateStreamInfo {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Holds the stream, table and trigger definitions known to a SQL application, plus
/// stream aliases.
#[derive(Debug, Clone)]
pub struct SqlCatalog {
    streams: HashMap<String, Arc<StreamDefinition>>,
    tables: HashMap<String, Arc<TableDefinition>>,
    triggers: HashMap<String, Arc<TriggerDefinition>>,
    /// Alias to real stream name.
    aliases: HashMap<String, String>,
}

impl SqlCatalog {
    pub fn new() -> Self {
        SqlCatalog {
            streams: HashMap::new(),
            tables: HashMap::new(),
            triggers: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    /// Register a trigger, replacing any trigger with the same name.
    ///
    /// Also registers a stream of the same name so queries can read it in FROM. Like the
    /// runtime trigger stream, it has one `triggered_time` LONG column, so queries can
    /// use `Tick.triggered_time`. It silently replaces any stream with that name, with no
    /// duplicate check.
    pub fn register_trigger(&mut self, trigger: TriggerDefinition) {
        let trigger_name = trigger.id.clone();

        self.triggers
            .insert(trigger_name.clone(), Arc::new(trigger));

        let stream_def = StreamDefinition::new(trigger_name.clone()).attribute(
            crate::query_api::constants::TRIGGERED_TIME.to_string(),
            AttributeType::LONG,
        );
        self.streams.insert(trigger_name, Arc::new(stream_def));
    }

    pub fn get_trigger(&self, name: &str) -> Option<Arc<TriggerDefinition>> {
        self.triggers.get(name).map(Arc::clone)
    }

    /// All trigger names, in no set order.
    pub fn get_trigger_names(&self) -> Vec<String> {
        self.triggers.keys().cloned().collect()
    }

    /// Register a stream.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::DuplicateStream` if a stream with that name exists.
    pub fn register_stream(
        &mut self,
        name: String,
        definition: StreamDefinition,
    ) -> Result<(), CatalogError> {
        if self.streams.contains_key(&name) {
            return Err(CatalogError::DuplicateStream(name));
        }
        self.streams.insert(name, Arc::new(definition));
        Ok(())
    }

    /// Register a table. Unlike streams, a table with the same name is silently replaced.
    pub fn register_table(&mut self, name: String, definition: TableDefinition) {
        self.tables.insert(name, Arc::new(definition));
    }

    /// Map an alias to a stream name, replacing any earlier mapping for that alias.
    pub fn register_alias(&mut self, alias: String, stream_name: String) {
        self.aliases.insert(alias, stream_name);
    }

    /// This catalog plus the join and pattern aliases of `query`, so its columns resolve
    /// as `a.price` or `e1.price`. Borrows this catalog when the query has no aliases,
    /// and copies it only when it does.
    pub(crate) fn with_query_aliases(
        &self,
        query: &crate::query_api::execution::query::Query,
    ) -> Cow<'_, SqlCatalog> {
        let aliases = query_aliases(query);
        if aliases.is_empty() {
            return Cow::Borrowed(self);
        }
        let mut catalog = self.clone();
        for (alias, stream_id) in aliases {
            catalog.register_alias(alias, stream_id);
        }
        Cow::Owned(catalog)
    }

    /// Look up a stream by its name, or else by an alias.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::UnknownStream` if neither matches.
    pub fn get_stream(&self, name: &str) -> Result<Arc<StreamDefinition>, CatalogError> {
        if let Some(def) = self.streams.get(name) {
            return Ok(Arc::clone(def));
        }

        if let Some(actual_name) = self.aliases.get(name) {
            if let Some(def) = self.streams.get(actual_name) {
                return Ok(Arc::clone(def));
            }
        }

        Err(CatalogError::UnknownStream(name.to_string()))
    }

    /// Look up a table by name. Aliases are not checked.
    pub fn get_table(&self, name: &str) -> Option<Arc<TableDefinition>> {
        self.tables.get(name).map(Arc::clone)
    }

    /// Look up a stream or table by name or alias, for FROM and JOIN clauses.
    ///
    /// Use this instead of `get_stream()` or `get_table()` when either kind is fine.
    /// Streams are checked first, including aliases, so a stream wins over a table with
    /// the same name. A table alias, as in `JOIN stockTable AS t`, is checked last.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the stream or table to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Relation)` - The relation (either Stream or Table variant)
    /// * `Err(CatalogError::UnknownRelation)` - If neither a stream nor table exists with that name
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Validate a relation exists in a FROM clause
    /// let relation = catalog.get_relation("user_events")?;
    ///
    /// // Check if it's a stream or table
    /// match relation {
    ///     Relation::Stream(s) => println!("Found stream: {}", s.abstract_definition.id),
    ///     Relation::Table(t) => println!("Found table: {}", t.abstract_definition.id),
    /// }
    ///
    /// // Access schema regardless of type
    /// let schema = relation.abstract_definition();
    /// ```
    pub fn get_relation(&self, name: &str) -> Result<Relation, CatalogError> {
        if let Ok(stream) = self.get_stream(name) {
            return Ok(Relation::Stream(stream));
        }

        if let Some(table) = self.get_table(name) {
            return Ok(Relation::Table(table));
        }

        if let Some(table) = self.aliases.get(name).and_then(|t| self.get_table(t)) {
            return Ok(Relation::Table(table));
        }

        Err(CatalogError::UnknownRelation(name.to_string()))
    }

    /// Whether a stream (by name or alias) or a table with this name exists.
    pub fn has_relation(&self, name: &str) -> bool {
        self.get_stream(name).is_ok() || self.get_table(name).is_some()
    }

    /// Whether a stream or table has a column with this name. Used to check column
    /// references in WHERE and ON clauses.
    ///
    /// # Arguments
    ///
    /// * `relation_name` - The name of the stream or table
    /// * `column_name` - The name of the column to check
    ///
    /// # Returns
    ///
    /// * `true` if the column exists in the relation
    /// * `false` if the relation does not exist or has no such column
    pub fn has_column(&self, relation_name: &str, column_name: &str) -> bool {
        self.get_relation(relation_name)
            .map(|relation| {
                relation
                    .abstract_definition()
                    .get_attribute_list()
                    .iter()
                    .any(|attr| attr.get_name() == column_name)
            })
            .unwrap_or(false)
    }

    /// The type of a column in a stream or table.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::UnknownRelation` if there is no such stream or table, or
    /// `CatalogError::UnknownColumn` if it has no such column.
    pub fn get_column_type(
        &self,
        relation_name: &str,
        column_name: &str,
    ) -> Result<AttributeType, CatalogError> {
        let relation = self.get_relation(relation_name)?;
        let definition = relation.abstract_definition();

        definition
            .get_attribute_list()
            .iter()
            .find(|attr| attr.get_name() == column_name)
            .map(|attr| *attr.get_type())
            .ok_or_else(|| {
                CatalogError::UnknownColumn(relation_name.to_string(), column_name.to_string())
            })
    }

    /// A copy of every column of a stream or table, in schema order.
    ///
    /// # Errors
    ///
    /// Returns `CatalogError::UnknownRelation` if there is no such stream or table.
    pub fn get_all_columns(&self, relation_name: &str) -> Result<Vec<Attribute>, CatalogError> {
        let relation = self.get_relation(relation_name)?;
        Ok(relation.abstract_definition().get_attribute_list().to_vec())
    }

    /// All stream names, including trigger streams, in no set order.
    pub fn get_stream_names(&self) -> Vec<String> {
        self.streams.keys().cloned().collect()
    }

    /// Whether there are no streams, tables or triggers. Aliases are not counted.
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.tables.is_empty() && self.triggers.is_empty()
    }
}

impl Default for SqlCatalog {
    fn default() -> Self {
        Self::new()
    }
}

/// A parsed SQL application. Its catalog plus its queries and partitions in order.
#[derive(Debug, Clone)]
pub struct SqlApplication {
    pub catalog: SqlCatalog,
    pub execution_elements: Vec<ExecutionElement>,
}

impl SqlApplication {
    pub fn new(catalog: SqlCatalog, execution_elements: Vec<ExecutionElement>) -> Self {
        SqlApplication {
            catalog,
            execution_elements,
        }
    }

    pub fn get_catalog(&self) -> &SqlCatalog {
        &self.catalog
    }

    pub fn get_execution_elements(&self) -> &[ExecutionElement] {
        &self.execution_elements
    }

    /// Whether there are no queries or partitions. Definitions are not counted.
    pub fn is_empty(&self) -> bool {
        self.execution_elements.is_empty()
    }

    /// Add a stream definition to `app` for each query output that has none yet, for top
    /// level queries and queries inside partitions.
    ///
    /// Attribute types come from type inference on the SELECT list, with each query's own
    /// join and pattern aliases. An attribute with no AS name takes the variable name, or
    /// "output" for any other expression. A query with no target writes to
    /// "OutputStream". Must run before the catalog is moved into `app`.
    ///
    /// # Errors
    ///
    /// Returns `ApplicationError::Type` if the type of an output attribute cannot be
    /// inferred.
    fn process_output_streams(&self, app: &mut EventFluxApp) -> Result<(), ApplicationError> {
        for elem in &self.execution_elements {
            match elem {
                ExecutionElement::Query(query) => self.add_inferred_output_stream(query, app)?,
                ExecutionElement::Partition(partition) => {
                    for query in &partition.query_list {
                        self.add_inferred_output_stream(query, app)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Add the inferred output stream of one query to `app`, unless its target already
    /// has a definition. See `process_output_streams`.
    fn add_inferred_output_stream(
        &self,
        query: &crate::query_api::execution::query::Query,
        app: &mut EventFluxApp,
    ) -> Result<(), ApplicationError> {
        let target_stream_name = query
            .get_output_stream()
            .get_target_id()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "OutputStream".to_string());

        // Meant to skip table targets. The tables are only copied into `app` after this
        // runs, so the map is still empty here.
        if app.table_definition_map.contains_key(&target_stream_name)
            || app.stream_definition_map.contains_key(&target_stream_name)
        {
            return Ok(());
        }

        // Each query gets its own aliases, so two queries can both use alias `a` for
        // different streams.
        let catalog = self.catalog.with_query_aliases(query);
        let type_engine = TypeInferenceEngine::new(&catalog);
        let context = type_engine.build_context_from_query(query);

        let mut output_stream = StreamDefinition::new(target_stream_name.clone());
        for output_attr in query.get_selector().get_selection_list() {
            let attr_name = output_attr.get_rename().clone().unwrap_or_else(|| {
                if let Expression::Variable(var) = output_attr.get_expression() {
                    var.get_attribute_name().to_string()
                } else {
                    "output".to_string()
                }
            });
            let attr_type = type_engine
                .infer_type(output_attr.get_expression(), &context)
                .map_err(ApplicationError::Type)?;
            output_stream = output_stream.attribute(attr_name, attr_type);
        }

        app.stream_definition_map
            .insert(target_stream_name, Arc::new(output_stream));
        Ok(())
    }

    /// Build the EventFluxApp the runtime is created from.
    ///
    /// Inferred output streams are added first. Declared streams are copied in after, so a
    /// declared stream replaces an inferred one with the same name.
    ///
    /// # Errors
    ///
    /// Returns `ApplicationError::Type` if the type of an output attribute cannot be
    /// inferred. See `process_output_streams`.
    pub fn to_eventflux_app(self, app_name: String) -> Result<EventFluxApp, ApplicationError> {
        let mut app = EventFluxApp::new(app_name);

        // Must run while the catalog is still here, because type inference reads it.
        self.process_output_streams(&mut app)?;

        for (stream_name, stream_def) in self.catalog.streams {
            app.stream_definition_map.insert(stream_name, stream_def);
        }

        for (table_name, table_def) in self.catalog.tables {
            app.table_definition_map.insert(table_name, table_def);
        }

        for (trigger_name, trigger_def) in self.catalog.triggers {
            app.trigger_definition_map.insert(trigger_name, trigger_def);
        }

        for elem in self.execution_elements {
            app.add_execution_element(elem);
        }

        Ok(app)
    }
}

/// The (alias, stream_id) pairs of a join or pattern query, such as ("a", "Stream1") for
/// `FROM Stream1 AS a JOIN ...` or ("e1", "Stream1") for `PATTERN (e1=Stream1 -> ...)`.
/// Empty for a single stream query.
fn query_aliases(query: &crate::query_api::execution::query::Query) -> Vec<(String, String)> {
    use crate::query_api::execution::query::input::stream::input_stream::InputStream;

    let mut aliases = Vec::new();
    match query.get_input_stream() {
        Some(InputStream::Join(join)) => {
            push_alias(&join.left_input_stream, &mut aliases);
            push_alias(&join.right_input_stream, &mut aliases);
        }
        Some(InputStream::State(state)) => {
            collect_state_aliases(&state.state_element, &mut aliases)
        }
        Some(InputStream::Single(_)) | None => {}
    }
    aliases
}

/// Walk a pattern tree and append the (alias, stream_id) pair of every stream that has
/// an alias.
fn collect_state_aliases(
    element: &crate::query_api::execution::query::input::state::StateElement,
    aliases: &mut Vec<(String, String)>,
) {
    use crate::query_api::execution::query::input::state::StateElement;

    match element {
        StateElement::Stream(stream_state) => {
            push_alias(stream_state.get_single_input_stream(), aliases)
        }
        StateElement::Count(count) => push_alias(
            count.stream_state_element.get_single_input_stream(),
            aliases,
        ),
        StateElement::AbsentStream(absent) => push_alias(
            absent.stream_state_element.get_single_input_stream(),
            aliases,
        ),
        StateElement::Next(next) => {
            collect_state_aliases(&next.state_element, aliases);
            collect_state_aliases(&next.next_state_element, aliases);
        }
        StateElement::Logical(logical) => {
            collect_state_aliases(&logical.stream_state_element_1, aliases);
            collect_state_aliases(&logical.stream_state_element_2, aliases);
        }
        StateElement::Every(every) => collect_state_aliases(&every.state_element, aliases),
    }
}

/// Append the (alias, stream_id) pair of `stream`, if it has an alias.
fn push_alias(
    stream: &crate::query_api::execution::query::input::stream::single_input_stream::SingleInputStream,
    aliases: &mut Vec<(String, String)>,
) {
    if let Some(alias) = stream.get_stream_reference_id_str() {
        aliases.push((alias.to_string(), stream.get_stream_id_str().to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_creation() {
        let catalog = SqlCatalog::new();
        assert!(catalog.is_empty());
    }

    #[test]
    fn test_register_stream() {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("col1".to_string(), AttributeType::STRING);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();
        assert!(!catalog.is_empty());
        assert!(catalog.get_stream("TestStream").is_ok());
    }

    #[test]
    fn test_duplicate_stream_error() {
        let mut catalog = SqlCatalog::new();
        let stream1 = StreamDefinition::new("TestStream".to_string());
        let stream2 = StreamDefinition::new("TestStream".to_string());

        catalog
            .register_stream("TestStream".to_string(), stream1)
            .unwrap();
        let result = catalog.register_stream("TestStream".to_string(), stream2);
        assert!(result.is_err());
    }

    #[test]
    fn test_has_column() {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("col1".to_string(), AttributeType::STRING)
            .attribute("col2".to_string(), AttributeType::INT);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();
        assert!(catalog.has_column("TestStream", "col1"));
        assert!(catalog.has_column("TestStream", "col2"));
        assert!(!catalog.has_column("TestStream", "col3"));
    }

    #[test]
    fn test_get_column_type() {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("name".to_string(), AttributeType::STRING)
            .attribute("age".to_string(), AttributeType::INT);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();

        let name_type = catalog.get_column_type("TestStream", "name").unwrap();
        assert_eq!(name_type, AttributeType::STRING);

        let age_type = catalog.get_column_type("TestStream", "age").unwrap();
        assert_eq!(age_type, AttributeType::INT);
    }

    #[test]
    fn test_get_all_columns() {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("col1".to_string(), AttributeType::STRING)
            .attribute("col2".to_string(), AttributeType::INT);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();
        let columns = catalog.get_all_columns("TestStream").unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].get_name(), "col1");
        assert_eq!(columns[1].get_name(), "col2");
    }

    #[test]
    fn test_alias() {
        let mut catalog = SqlCatalog::new();
        let stream = StreamDefinition::new("TestStream".to_string())
            .attribute("col1".to_string(), AttributeType::STRING);

        catalog
            .register_stream("TestStream".to_string(), stream)
            .unwrap();
        catalog.register_alias("ts".to_string(), "TestStream".to_string());

        assert!(catalog.get_stream("ts").is_ok());
        assert!(catalog.has_column("ts", "col1"));
    }
}
