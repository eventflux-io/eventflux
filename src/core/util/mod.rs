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

pub mod attribute_converter;
pub mod compression; // Shared compression for StateHolders
pub mod event_serde;
pub mod event_serialization; // Event serialization that handles every AttributeValue type
pub mod eventflux_constants;
pub mod executor_service;
pub mod id_generator;
pub mod metrics;
pub mod parser;
pub mod pipeline; // Event pipeline built on crossbeam queues
pub mod scheduled_executor_service;
pub mod scheduler;
pub mod serialization;
pub mod thread_barrier;
pub mod type_system;
#[cfg(test)]
pub mod type_system_tests;
// Other util submodules from the Java version that are not ported:
// pub mod cache;
// pub mod collection;
// pub mod config; // This might conflict with core::config
// pub mod error;  // This might conflict with core::exception or query_api::error
// pub mod event;  // This might conflict with core::event
// pub mod extension;
pub mod lock;
// pub mod persistence;
pub mod snapshot;
pub mod statistics; // Separate from core::config::StatisticsConfiguration
                    // pub mod timestamp;
                    // pub mod transport;

pub use self::attribute_converter::{
    get_property_value, get_property_value_from_str, is_numeric_type,
};
pub use self::compression::{
    CompressibleStateHolder, CompressionEngine, CompressionHints, DataCharacteristics,
    DataSizeRange, GLOBAL_COMPRESSION_ENGINE,
};
pub use self::event_serde::{event_from_bytes, event_to_bytes};
pub use self::event_serialization::{
    EventSerializationService, SerializableAttributeValue, SerializableEventData, StorageStrategy,
};
pub use self::eventflux_constants::EventFluxConstants;
pub use self::executor_service::{ExecutorService, ExecutorServiceRegistry};
pub use self::id_generator::IdGenerator;
pub use self::lock::{LockSynchronizer, LockWrapper};
pub use self::metrics::*;
pub use self::parser::{parse_expression, ExpressionParserContext};
pub use self::pipeline::{
    BackpressureStrategy, EventPipeline, EventPool, MetricsSnapshot, PipelineBuilder,
    PipelineConfig, PipelineMetrics, PipelineResult, PooledEvent,
};
pub use self::scheduled_executor_service::ScheduledExecutorService;
pub use self::scheduler::{Schedulable, Scheduler};
pub use self::serialization::{from_bytes, to_bytes};
pub use self::snapshot::{IncrementalSnapshot, PersistenceReference};
pub use self::statistics::{DefaultStatisticsManager, StatisticsManager};
pub use self::thread_barrier::ThreadBarrier;
pub use self::type_system::{get_arithmetic_result_type, TypeConverter, TypePrecedence};
pub use crate::core::persistence::StateHolder;
