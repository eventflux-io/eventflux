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

pub mod handler;
pub mod input;
pub mod junction_factory;
pub mod mapper;
pub mod output;
pub mod stream_initializer;
pub mod stream_junction;

pub use self::handler::{SinkStreamHandler, SourceStreamHandler};
pub use self::input::source::{timer_source::TimerSource, Source};
pub use self::input::{InputHandler, InputManager};
pub use self::junction_factory::{
    BenchmarkResult, JunctionBenchmark, JunctionConfig, StreamJunctionFactory,
};
pub use self::output::{LogSink, Sink, StreamCallback};
pub use self::stream_initializer::{
    initialize_stream, initialize_streams, InitializedSink, InitializedSource, InitializedStream,
    InitializedStreamHandler, StreamHandlers,
};
pub use self::stream_junction::{
    JunctionPerformanceMetrics, OnErrorAction, Publisher, StreamJunction,
};

// Re-export BackpressureStrategy for custom junction configurations
pub use crate::core::util::pipeline::BackpressureStrategy;

// Re-export mapper types for convenience
pub use self::mapper::{
    csv_mapper::{CsvSinkMapper, CsvSourceMapper},
    factory::{
        CsvSinkMapperFactory, CsvSourceMapperFactory, JsonSinkMapperFactory,
        JsonSourceMapperFactory, MapperFactoryRegistry, SinkMapperFactory, SourceMapperFactory,
    },
    json_mapper::{JsonSinkMapper, JsonSourceMapper},
    validation::{
        validate_mapper_config, validate_sink_mapper_config, validate_source_mapper_config,
    },
    SinkMapper, SourceMapper,
};
