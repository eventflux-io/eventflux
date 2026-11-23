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

// Handlers that run on an input stream before the query sees its events. Filters, stream
// functions and windows.

pub mod filter;
pub mod stream_function;
pub mod stream_handler;
pub mod window;

pub use self::filter::Filter;
pub use self::stream_function::StreamFunction;
pub use self::stream_handler::StreamHandler;
// Renamed so it does not clash with other Window types.
pub use self::window::Window as WindowHandler;

// Sketch of the StreamHandler enum. The real one in stream_handler.rs boxes the window.
// pub enum StreamHandler {
//     Filter(Filter),
//     Function(StreamFunction),
//     Window(WindowHandler),
// }
// Each handler struct holds an EventFluxElement field. StreamHandlerTrait gives access to
// their parameters.
