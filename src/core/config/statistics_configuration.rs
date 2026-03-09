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

// Corresponds to io.eventflux.core.config.StatisticsConfiguration

// Placeholder for StatisticsTrackerFactory
// This would be a trait or struct related to metrics.
#[derive(Clone, Debug, Default)]
pub struct StatisticsTrackerFactoryPlaceholder {
    _dummy: String,
}

#[derive(Clone, Debug)] // Default can be custom
pub struct StatisticsConfiguration {
    // StatisticsConfiguration in Java is not a EventFluxElement, so no eventflux_element field.
    pub metric_prefix: String,
    pub factory: StatisticsTrackerFactoryPlaceholder, // Placeholder for actual factory type
}

impl StatisticsConfiguration {
    // Constructor takes the factory, metric_prefix has a default in Java.
    pub fn new(factory: StatisticsTrackerFactoryPlaceholder) -> Self {
        Self {
            metric_prefix: "io.eventflux".to_string(), // Default from Java
            factory,
        }
    }

    /// Returns the statistics tracker factory associated with this configuration.
    pub fn get_factory(&self) -> &StatisticsTrackerFactoryPlaceholder {
        &self.factory
    }

    /// Returns the configured metric prefix.
    pub fn get_metric_prefix(&self) -> &str {
        &self.metric_prefix
    }

    /// Sets the metric prefix. Mirrors the Java setter.
    pub fn set_metric_prefix(&mut self, metric_prefix: String) {
        self.metric_prefix = metric_prefix;
    }
}

impl Default for StatisticsConfiguration {
    fn default() -> Self {
        Self {
            metric_prefix: "io.eventflux".to_string(),
            factory: StatisticsTrackerFactoryPlaceholder::default(),
        }
    }
}
