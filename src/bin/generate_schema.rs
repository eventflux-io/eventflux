// SPDX-License-Identifier: MIT OR Apache-2.0

//! Schema Generator Binary
//!
//! Generates a JSON schema file containing metadata about all registered
//! EventFlux extensions (sources, sinks, functions, windows, aggregators, etc.)
//!
//! This schema is used by EventFlux Studio for:
//! - Dynamic form generation for connector configuration
//! - Function autocomplete and documentation
//! - Window type selection and parameter validation
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin generate_schema
//! ```
//!
//! Output is written to: `studio/webview/src/schemas/eventflux-schema.json`

use eventflux::core::config::eventflux_context::EventFluxContext;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Root schema structure
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EventFluxSchema {
    /// Schema version (matches EventFlux version)
    version: String,
    /// Generation timestamp
    generated: String,
    /// Source connector definitions
    sources: BTreeMap<String, ConnectorSchema>,
    /// Sink connector definitions
    sinks: BTreeMap<String, ConnectorSchema>,
    /// Data format mappers (json, csv, bytes)
    mappers: BTreeMap<String, MapperSchema>,
    /// Window processor definitions
    windows: BTreeMap<String, WindowSchema>,
    /// Aggregation function definitions
    aggregators: BTreeMap<String, AggregatorSchema>,
    /// Scalar function definitions
    functions: BTreeMap<String, FunctionSchema>,
    /// Collection aggregation functions (for patterns)
    collection_aggregators: BTreeMap<String, CollectionAggregatorSchema>,
    /// Table extension definitions
    tables: BTreeMap<String, TableSchema>,
}

/// Schema for source/sink connectors
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectorSchema {
    name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    supported_formats: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    required_parameters: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    optional_parameters: Vec<String>,
}

/// Schema for mappers (json, csv, bytes)
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MapperSchema {
    name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    required_parameters: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    optional_parameters: Vec<String>,
}

/// Schema for window processors
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WindowSchema {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Schema for aggregation functions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AggregatorSchema {
    name: String,
    arity: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Schema for scalar functions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FunctionSchema {
    name: String,
}

/// Schema for collection aggregation functions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CollectionAggregatorSchema {
    name: String,
    supports_count_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Schema for table extensions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TableSchema {
    name: String,
}

fn main() {
    println!("EventFlux Schema Generator");
    println!("==========================\n");

    // Create context with all default extensions registered
    let ctx = EventFluxContext::new();

    // Build the schema
    let schema = generate_schema(&ctx);

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&schema).expect("Failed to serialize schema");

    // Determine output path
    let output_path = Path::new("studio/webview/src/schemas/eventflux-schema.json");

    // Create directory if it doesn't exist
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).expect("Failed to create schemas directory");
    }

    // Write the schema file
    fs::write(output_path, &json).expect("Failed to write schema file");

    println!("Schema generated successfully!");
    println!("Output: {}", output_path.display());
    println!("\nSummary:");
    println!("  Sources: {}", schema.sources.len());
    println!("  Sinks: {}", schema.sinks.len());
    println!("  Mappers: {}", schema.mappers.len());
    println!("  Windows: {}", schema.windows.len());
    println!("  Aggregators: {}", schema.aggregators.len());
    println!("  Functions: {}", schema.functions.len());
    println!(
        "  Collection Aggregators: {}",
        schema.collection_aggregators.len()
    );
    println!("  Tables: {}", schema.tables.len());
}

fn generate_schema(ctx: &EventFluxContext) -> EventFluxSchema {
    EventFluxSchema {
        version: env!("CARGO_PKG_VERSION").to_string(),
        generated: chrono::Utc::now().to_rfc3339(),
        sources: generate_sources(ctx),
        sinks: generate_sinks(ctx),
        mappers: generate_mappers(ctx),
        windows: generate_windows(ctx),
        aggregators: generate_aggregators(ctx),
        functions: generate_functions(ctx),
        collection_aggregators: generate_collection_aggregators(ctx),
        tables: generate_tables(ctx),
    }
}

fn generate_sources(ctx: &EventFluxContext) -> BTreeMap<String, ConnectorSchema> {
    let mut sources = BTreeMap::new();

    for name in ctx.list_source_factory_names() {
        if let Some(factory) = ctx.get_source_factory(&name) {
            sources.insert(
                name.clone(),
                ConnectorSchema {
                    name: factory.name().to_string(),
                    supported_formats: factory
                        .supported_formats()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    required_parameters: factory
                        .required_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    optional_parameters: factory
                        .optional_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                },
            );
        }
    }

    sources
}

fn generate_sinks(ctx: &EventFluxContext) -> BTreeMap<String, ConnectorSchema> {
    let mut sinks = BTreeMap::new();

    for name in ctx.list_sink_factory_names() {
        if let Some(factory) = ctx.get_sink_factory(&name) {
            sinks.insert(
                name.clone(),
                ConnectorSchema {
                    name: factory.name().to_string(),
                    supported_formats: factory
                        .supported_formats()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    required_parameters: factory
                        .required_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    optional_parameters: factory
                        .optional_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                },
            );
        }
    }

    sinks
}

fn generate_mappers(ctx: &EventFluxContext) -> BTreeMap<String, MapperSchema> {
    let mut mappers = BTreeMap::new();

    // Source mappers
    for name in ctx.list_source_mapper_factory_names() {
        if let Some(factory) = ctx.get_source_mapper_factory(&name) {
            mappers.insert(
                name.clone(),
                MapperSchema {
                    name: factory.name().to_string(),
                    required_parameters: factory
                        .required_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                    optional_parameters: factory
                        .optional_parameters()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                },
            );
        }
    }

    mappers
}

fn generate_windows(ctx: &EventFluxContext) -> BTreeMap<String, WindowSchema> {
    let mut windows = BTreeMap::new();

    for name in ctx.list_window_factory_names() {
        if let Some(factory) = ctx.get_window_factory(&name) {
            windows.insert(
                name.clone(),
                WindowSchema {
                    name: factory.name().to_string(),
                    description: None, // WindowProcessorFactory doesn't have description
                },
            );
        }
    }

    windows
}

fn generate_aggregators(ctx: &EventFluxContext) -> BTreeMap<String, AggregatorSchema> {
    let mut aggregators = BTreeMap::new();

    for name in ctx.list_attribute_aggregator_names() {
        if let Some(factory) = ctx.get_attribute_aggregator_factory(&name) {
            let desc = factory.description();
            aggregators.insert(
                name.clone(),
                AggregatorSchema {
                    name: factory.name().to_string(),
                    arity: factory.arity(),
                    description: if desc.is_empty() {
                        None
                    } else {
                        Some(desc.to_string())
                    },
                },
            );
        }
    }

    aggregators
}

fn generate_functions(ctx: &EventFluxContext) -> BTreeMap<String, FunctionSchema> {
    let mut functions = BTreeMap::new();

    for name in ctx.list_scalar_function_names() {
        functions.insert(name.clone(), FunctionSchema { name: name.clone() });
    }

    functions
}

fn generate_collection_aggregators(
    ctx: &EventFluxContext,
) -> BTreeMap<String, CollectionAggregatorSchema> {
    let mut aggregators = BTreeMap::new();

    for name in ctx.list_collection_aggregation_function_names() {
        if let Some(func) = ctx.get_collection_aggregation_function(&name) {
            let desc = func.description();
            aggregators.insert(
                name.clone(),
                CollectionAggregatorSchema {
                    name: func.name().to_string(),
                    supports_count_only: func.supports_count_only(),
                    description: if desc.is_empty() {
                        None
                    } else {
                        Some(desc.to_string())
                    },
                },
            );
        }
    }

    aggregators
}

fn generate_tables(ctx: &EventFluxContext) -> BTreeMap<String, TableSchema> {
    let mut tables = BTreeMap::new();

    for name in ctx.list_table_factory_names() {
        if let Some(factory) = ctx.get_table_factory(&name) {
            tables.insert(
                name.clone(),
                TableSchema {
                    name: factory.name().to_string(),
                },
            );
        }
    }

    tables
}
