// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/query/selector/attribute/mod.rs
pub mod aggregator;
pub mod output_attribute_processor;
pub use self::aggregator::*;
pub use self::output_attribute_processor::OutputAttributeProcessor;

// aggregator/ and processor/ sub-packages from Java would be modules here if their contents are ported.
// pub mod aggregator;
// pub mod processor; // This name might conflict with core::query::processor if not careful with paths.
// Java has ...selector.attribute.processor.AttributeProcessor
// and ...query.processor.Processor (interface)
// The prompt names this OutputAttributeProcessor, which is good for distinction.
