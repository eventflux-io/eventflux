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

//! M4: 3-Phase Validation System
//!
//! Provides comprehensive validation for EventFlux applications across three phases:
//! - Phase 1: Parse-Time (Syntax Only) - During SQL parsing and TOML loading
//! - Phase 2: Application Initialization (Fail-Fast) - Before processing events
//! - Phase 3: Runtime (Resilient Retry) - During event processing
//!
//! This module implements validation logic for:
//! - Circular dependency detection in stream queries
//! - DLQ (Dead Letter Queue) schema validation
//! - DLQ stream name and recursive restriction validation

pub mod circular_dependency;
pub mod dlq_validation;
pub mod query_helpers;

pub use circular_dependency::detect_circular_dependencies;
pub use dlq_validation::{
    validate_dlq_schema, validate_dlq_stream_name, validate_no_recursive_dlq,
};
pub use query_helpers::QuerySourceExtractor;
