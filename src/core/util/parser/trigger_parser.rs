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

// src/core/util/parser/trigger_parser.rs

use std::sync::Arc;

use crate::core::config::eventflux_app_context::EventFluxAppContext;
use crate::core::eventflux_app_runtime_builder::EventFluxAppRuntimeBuilder;
use crate::core::stream::junction_factory::{JunctionConfig, StreamJunctionFactory};
use crate::core::trigger::TriggerRuntime;
use crate::query_api::constants::TRIGGERED_TIME;
use crate::query_api::definition::{AttributeType, StreamDefinition, TriggerDefinition};

pub struct TriggerParser;

impl TriggerParser {
    pub fn parse(
        builder: &mut EventFluxAppRuntimeBuilder,
        definition: &TriggerDefinition,
        eventflux_app_context: &Arc<EventFluxAppContext>,
    ) -> Result<TriggerRuntime, String> {
        let stream_def = Arc::new(
            StreamDefinition::new(definition.id.clone())
                .attribute(TRIGGERED_TIME.to_string(), AttributeType::LONG),
        );
        builder.add_stream_definition(Arc::clone(&stream_def));

        let junction_config = JunctionConfig::new(definition.id.clone())
            .with_buffer_size(eventflux_app_context.buffer_size as usize)
            .with_async(false); // Triggers use synchronous mode

        let junction = StreamJunctionFactory::create(
            junction_config,
            Arc::clone(&stream_def),
            Arc::clone(eventflux_app_context),
            None,
        )
        .map_err(|e| format!("Failed to create trigger junction: {}", e))?;

        builder.add_stream_junction(definition.id.clone(), Arc::clone(&junction));

        let scheduler = eventflux_app_context.get_scheduler().unwrap_or_else(|| {
            Arc::new(crate::core::util::Scheduler::new(Arc::new(
                crate::core::util::ExecutorService::default(),
            )))
        });
        Ok(TriggerRuntime::new(
            Arc::new(definition.clone()),
            junction,
            scheduler,
        ))
    }
}
