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

pub mod order_by_attribute;
pub mod output_attribute;
pub mod selector;

pub use self::order_by_attribute::{Order, OrderByAttribute}; // Order enum from order_by_attribute
pub use self::output_attribute::OutputAttribute;
pub use self::selector::Selector;

// BasicSelector.java is not translated as a separate struct.
// Its functionality (builder methods returning Self) is part of Selector.java,
// and thus part of the Rust Selector struct. If specific dispatch or typing for
// BasicSelector is needed later, it could be added as a wrapper or distinct type.
