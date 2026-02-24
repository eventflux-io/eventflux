// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/mod.rs
pub mod attribute; // For OutputAttributeProcessor and future aggregators/processors
pub mod group_by_key_generator;
pub mod order_by_event_comparator; // For OrderByEventComparator.java
pub mod select_processor; // Corresponds to QuerySelector.java // For GroupByKeyGenerator.java

pub use self::attribute::OutputAttributeProcessor; // Re-export for convenience
pub use self::group_by_key_generator::GroupByKeyGenerator;
pub use self::order_by_event_comparator::OrderByEventComparator;
pub use self::select_processor::SelectProcessor;

// Other components like OrderByEventComparator, GroupByKeyGenerator would be exported here.
