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

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::event::complex_event::ComplexEvent;
use crate::core::event::value::AttributeValue;
use std::fmt::Debug;
use std::sync::Arc;

pub trait Script: Debug + Send + Sync {
    fn init(&mut self, ctx: &Arc<EventFluxAppContext>) -> Result<(), String>;
    fn eval(&self, event: Option<&dyn ComplexEvent>) -> Option<AttributeValue>;
    fn clone_box(&self) -> Box<dyn Script>;
}

impl Clone for Box<dyn Script> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
