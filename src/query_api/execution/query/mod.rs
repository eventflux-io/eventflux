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

// Queries and their parts. Input, selection and output.

pub mod input;
pub mod on_demand_query;
#[allow(clippy::module_inception)]
pub mod query;
pub mod selection;
pub mod store_query;

pub mod output;

pub use self::on_demand_query::{OnDemandQuery, OnDemandQueryType};
pub use self::query::Query;
pub use self::store_query::{StoreQuery, StoreQueryType};

pub use self::input::InputStream;
pub use self::selection::Order as OrderByOrder;
pub use self::selection::OrderByAttribute;
pub use self::selection::OutputAttribute;
pub use self::selection::Selector;

pub use self::output::OutputEventType;
pub use self::output::OutputRate;
pub use self::output::OutputStream;
pub use self::output::SetAttribute;

// output/mod.rs used to export these under aliases, like this. It now exports the plain
// names, so the re-exports above use them directly.
// pub use self::output_stream::{OutputStream as ActualOutputStream, OutputEventType as ActualOutputEventType};
// pub use self.ratelimit::OutputRate;
