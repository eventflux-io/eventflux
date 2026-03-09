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

pub mod abstract_definition;
pub mod aggregation_definition;
pub mod attribute;
pub mod function_definition;
pub mod stream_definition;
pub mod table_definition;
pub mod trigger_definition;
pub mod window_definition;

pub use self::abstract_definition::AbstractDefinition;
pub use self::aggregation_definition::AggregationDefinition; // Keep this
pub use self::attribute::{Attribute, Type as AttributeType};
pub use self::function_definition::FunctionDefinition;
pub use self::stream_definition::StreamDefinition;
pub use self::table_definition::TableDefinition;
pub use self::trigger_definition::TriggerDefinition;
pub use self::window_definition::WindowDefinition;
pub use crate::query_api::aggregation::TimePeriod as AggregationTimePeriod; // Import directly
