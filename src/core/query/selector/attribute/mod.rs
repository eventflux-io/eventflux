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
