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

//! gRPC transport built on Tonic.
//!
//! Carries messages between nodes over gRPC. `transport` adapts the gRPC client to
//! the shared [`Transport`](crate::core::distributed::transport::Transport) trait.
//! `simple_transport` is a smaller client that talks to the gRPC service directly.

pub mod simple_transport;
pub mod transport;

// Code generated from the protobuf definitions.
include!("eventflux.transport.rs");
