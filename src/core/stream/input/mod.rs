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

// src/core/stream/input/mod.rs

pub mod input_distributor;
pub mod input_entry_valve;
pub mod input_handler;
pub mod input_manager;
pub mod mapper;
pub mod source;
pub mod table_input_handler; // For sub-package source/

pub use self::input_distributor::InputDistributor;
pub use self::input_entry_valve::InputEntryValve;
pub use self::input_handler::InputHandler;
pub use self::input_manager::InputManager;
pub use self::mapper::SourceMapper;
pub use self::table_input_handler::TableInputHandler;
// pub use self::input_processor::InputProcessor; // If defined here
// pub use self::table_input_handler::TableInputHandler;
