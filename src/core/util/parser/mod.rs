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

// src/core/util/parser/mod.rs

pub mod eventflux_app_parser; // Added
pub mod expression_parser;
pub mod query_parser; // Added
pub mod trigger_parser;
// Other parsers will be added here later:
// pub mod aggregation_parser;
// ... etc.

pub use self::eventflux_app_parser::EventFluxAppParser; // Added
pub use self::expression_parser::{
    parse_expression, ExpressionParseError, ExpressionParserContext,
};
pub use self::query_parser::QueryParser; // Added
pub use self::trigger_parser::TriggerParser;
pub use crate::core::partition::parser::PartitionParser;
