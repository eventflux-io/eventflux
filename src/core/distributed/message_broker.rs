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

// src/core/distributed/message_broker.rs

//! Message Broker Abstraction
//!
//! This module provides the message broker abstraction for event distribution
//! across nodes. It supports multiple broker implementations (Kafka, Pulsar, NATS, etc.).

use super::DistributedResult;
use async_trait::async_trait;

/// Message broker trait for event distribution
#[async_trait]
pub trait MessageBroker: Send + Sync {
    /// Produce a message to a topic
    async fn produce(&self, topic: &str, message: BrokerMessage) -> DistributedResult<()>;

    /// Consume messages from a topic
    async fn consume(&self, topic: &str) -> DistributedResult<Vec<BrokerMessage>>;

    /// Subscribe to a topic
    async fn subscribe(&self, topic: &str) -> DistributedResult<()>;

    /// Unsubscribe from a topic
    async fn unsubscribe(&self, topic: &str) -> DistributedResult<()>;
}

/// Message for broker
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    pub id: String,
    pub payload: Vec<u8>,
    pub timestamp: std::time::SystemTime,
    pub headers: std::collections::HashMap<String, String>,
}

/// In-memory broker (for testing)
pub struct InMemoryBroker;

#[async_trait]
impl MessageBroker for InMemoryBroker {
    async fn produce(&self, _topic: &str, _message: BrokerMessage) -> DistributedResult<()> {
        Ok(())
    }

    async fn consume(&self, _topic: &str) -> DistributedResult<Vec<BrokerMessage>> {
        Ok(vec![])
    }

    async fn subscribe(&self, _topic: &str) -> DistributedResult<()> {
        Ok(())
    }

    async fn unsubscribe(&self, _topic: &str) -> DistributedResult<()> {
        Ok(())
    }
}
