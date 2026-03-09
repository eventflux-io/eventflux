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

// Corresponds to package io.eventflux.query.api.execution.query.input.handler

pub mod filter;
pub mod stream_function;
pub mod stream_handler; // Interface, will be a trait or enum
pub mod window; // Renamed to window_handler internally if needed to avoid conflict

// Re-export key types
pub use self::filter::Filter;
pub use self::stream_function::StreamFunction;
pub use self::stream_handler::StreamHandler; // This will be the main enum/trait
pub use self::window::Window as WindowHandler; // Alias to avoid conflicts if necessary elsewhere

// The StreamHandler will likely be an enum in Rust:
// pub enum StreamHandler {
//     Filter(Filter),
//     Function(StreamFunction),
//     Window(WindowHandler),
// }
// Each struct (Filter, StreamFunction, WindowHandler) will implement EventFluxElement
// and potentially a common trait if methods beyond EventFluxElement are shared from Java's StreamHandler.
// Java's StreamHandler has `getParameters()` and EventFluxElement context.
// The Extension interface is implemented by StreamFunction and Window.
