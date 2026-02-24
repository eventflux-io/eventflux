// SPDX-License-Identifier: MIT OR Apache-2.0

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
