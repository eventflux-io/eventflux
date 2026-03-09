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

use std::sync::Arc;

use crate::core::query::query_runtime::{QueryRuntime, QueryRuntimeTrait};

/// Runtime representation of a Partition.
#[derive(Debug, Default)]
pub struct PartitionRuntime {
    pub query_runtimes: Vec<Arc<QueryRuntime>>,
}

impl PartitionRuntime {
    pub fn new() -> Self {
        Self {
            query_runtimes: Vec::new(),
        }
    }

    pub fn add_query_runtime(&mut self, qr: Arc<QueryRuntime>) {
        self.query_runtimes.push(qr);
    }

    pub fn start(&self) {
        for qr in &self.query_runtimes {
            println!("Starting query runtime {} in partition", qr.get_query_id());
        }
    }

    pub fn shutdown(&self) {
        for qr in &self.query_runtimes {
            println!("Stopping query runtime {} in partition", qr.get_query_id());
        }
    }
}

pub mod parser;
pub use parser::PartitionParser;
