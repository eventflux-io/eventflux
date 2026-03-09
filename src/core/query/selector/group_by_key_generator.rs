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

use crate::core::event::complex_event::ComplexEvent;
use crate::core::executor::expression_executor::ExpressionExecutor;
use crate::core::util::eventflux_constants::KEY_DELIMITER;

/// Generates a composite key for group-by operations using a list of expression executors.
#[derive(Debug)]
pub struct GroupByKeyGenerator {
    executors: Vec<Box<dyn ExpressionExecutor>>,
}

impl Clone for GroupByKeyGenerator {
    fn clone(&self) -> Self {
        Self {
            executors: Vec::new(),
        }
    }
}

impl GroupByKeyGenerator {
    pub fn new(executors: Vec<Box<dyn ExpressionExecutor>>) -> Self {
        Self { executors }
    }

    pub fn construct_event_key(&self, event: &dyn ComplexEvent) -> Option<String> {
        if self.executors.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(self.executors.len());
        for exec in &self.executors {
            match exec.execute(Some(event)) {
                Some(val) => parts.push(val.to_string()),
                None => parts.push("null".to_string()),
            }
        }
        Some(parts.join(KEY_DELIMITER))
    }
}
