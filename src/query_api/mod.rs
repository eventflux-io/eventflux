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

pub mod aggregation;
pub mod annotation;
pub mod constants;
pub mod definition;
pub mod error;
pub mod eventflux_app;
pub mod eventflux_element;
pub mod execution;
pub mod expression; // Added aggregation module

// Re-export key types if desired, e.g.
pub use self::aggregation::Within;
pub use self::constants::*;
pub use self::definition::StreamDefinition;
pub use self::eventflux_app::EventFluxApp;
pub use self::eventflux_element::EventFluxElement; // Re-exporting Within
