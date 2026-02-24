// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/input/stream/state/receiver/mod.rs
// Stream receivers for Pattern and Sequence processing

pub mod pattern_stream_receiver;
pub mod sequence_stream_receiver;

// Re-export receiver types
pub use pattern_stream_receiver::PatternStreamReceiver;
pub use sequence_stream_receiver::SequenceStreamReceiver;
