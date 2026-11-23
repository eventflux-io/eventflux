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

//! M4: 3 Phase Validation System
//!
//! EventFlux validates an application in three phases:
//! - Phase 1: Parse time. Syntax only, while the SQL is parsed and TOML is loaded.
//! - Phase 2: Application start. Fails at once, before any event is processed.
//! - Phase 3: Runtime. Retries on failure while events are processed.
//!
//! This module holds the checks for:
//! - Circular dependencies between stream queries
//! - The schema of DLQ (dead letter queue) streams
//! - DLQ stream names, and DLQ streams that would feed back into themselves

pub mod circular_dependency;
pub mod dlq_validation;
pub mod query_helpers;

pub use circular_dependency::detect_circular_dependencies;
pub use dlq_validation::{
    source_dlq_stream, validate_dlq_reference, validate_dlq_schema, validate_dlq_stream_name,
    validate_no_recursive_dlq, validate_source_dlq_configs,
};
pub use query_helpers::QuerySourceExtractor;
