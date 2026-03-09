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

// src/core/distributed/grpc/mod.rs

//! gRPC transport implementation using Tonic
//!
//! This module provides the gRPC transport layer for distributed communication.
//! It includes both client and server implementations with streaming support,
//! compression, and advanced features like load balancing and health checks.

pub mod simple_transport;
pub mod transport;

// Include generated protobuf code
include!("eventflux.transport.rs");
