// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/util/mod.rs

pub mod attribute_converter;
pub mod compression; // High-performance shared compression utility
pub mod event_serde;
pub mod event_serialization; // Advanced event serialization with proper AttributeValue handling
pub mod eventflux_constants; // Added eventflux_constants module
pub mod executor_service;
pub mod id_generator;
pub mod metrics;
pub mod parser; // Added parser module
pub mod pipeline; // Crossbeam-based high-performance event processing pipeline
pub mod scheduled_executor_service;
pub mod scheduler; // new scheduler module
pub mod serialization;
pub mod thread_barrier;
pub mod type_system; // New comprehensive type system
#[cfg(test)]
pub mod type_system_tests; // Comprehensive type system tests
                           // Potentially other existing util submodules:
                           // pub mod cache;
                           // pub mod collection;
                           // pub mod config; // This might conflict with core::config
                           // pub mod error;  // This might conflict with core::exception or query_api::error
                           // pub mod event;  // This might conflict with core::event
                           // pub mod extension;
pub mod lock;
// pub mod persistence;
pub mod snapshot;
pub mod statistics; // This might conflict with core::config::StatisticsConfiguration
                    // pub mod timestamp;
                    // pub mod transport;

pub use self::attribute_converter::{
    get_property_value, get_property_value_from_str, is_numeric_type,
};
pub use self::compression::{
    CompressibleStateHolder, CompressionEngine, CompressionHints, DataCharacteristics,
    DataSizeRange, GLOBAL_COMPRESSION_ENGINE,
}; // High-performance compression utilities
pub use self::event_serde::{event_from_bytes, event_to_bytes};
pub use self::event_serialization::{
    EventSerializationService, SerializableAttributeValue, SerializableEventData, StorageStrategy,
}; // Advanced event serialization
pub use self::eventflux_constants::EventFluxConstants; // Re-export EventFluxConstants
pub use self::executor_service::{ExecutorService, ExecutorServiceRegistry};
pub use self::id_generator::IdGenerator;
pub use self::lock::{LockSynchronizer, LockWrapper};
pub use self::metrics::*;
pub use self::parser::{parse_expression, ExpressionParserContext}; // Re-export key items from parser
pub use self::pipeline::{
    BackpressureStrategy, EventPipeline, EventPool, MetricsSnapshot, PipelineBuilder,
    PipelineConfig, PipelineMetrics, PipelineResult, PooledEvent,
}; // High-performance crossbeam pipeline
pub use self::scheduled_executor_service::ScheduledExecutorService;
pub use self::scheduler::{Schedulable, Scheduler};
pub use self::serialization::{from_bytes, to_bytes};
pub use self::snapshot::{IncrementalSnapshot, PersistenceReference};
pub use self::statistics::{DefaultStatisticsManager, StatisticsManager};
pub use self::thread_barrier::ThreadBarrier;
pub use self::type_system::{get_arithmetic_result_type, TypeConverter, TypePrecedence};
pub use crate::core::persistence::StateHolder;
