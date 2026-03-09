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

// src/core/event/state/mod.rs

pub mod meta_state_event;
pub mod meta_state_event_attribute; // MetaStateEventAttribute.java
pub mod populater;
pub mod state_event;
pub mod state_event_cloner;
pub mod state_event_factory; // for sub-package

pub use self::meta_state_event::MetaStateEvent;
pub use self::meta_state_event_attribute::MetaStateEventAttribute;
pub use self::populater::*;
pub use self::state_event::StateEvent;
pub use self::state_event_cloner::StateEventCloner;
pub use self::state_event_factory::StateEventFactory;
