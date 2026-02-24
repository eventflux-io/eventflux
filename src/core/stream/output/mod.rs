// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/stream/output/mod.rs

pub mod error_store;
pub mod mapper;
pub mod sink;
pub mod stream_callback; // For sink implementations

pub use self::error_store::{ErrorStore, InMemoryErrorStore};
pub use self::mapper::SinkMapper;
pub use self::sink::{LogSink, Sink};
pub use self::stream_callback::{LogStreamCallback, StreamCallback};
