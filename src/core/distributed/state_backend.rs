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

// src/core/distributed/state_backend.rs

//! State backends.
//!
//! Defines the trait for storing state shared between nodes, with an in memory
//! backend for a single node and a Redis backend.

use super::{DistributedError, DistributedResult};
use async_trait::async_trait;
use deadpool_redis::redis::{self, AsyncCommands, RedisResult};
use deadpool_redis::{Config, Pool, Runtime};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Key value store for state shared between nodes.
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Prepare the backend. Call before any other method.
    async fn initialize(&mut self) -> DistributedResult<()>;

    /// Returns the value for `key`, or `None` if it is not set.
    async fn get(&self, key: &str) -> DistributedResult<Option<Vec<u8>>>;

    async fn set(&self, key: &str, value: Vec<u8>) -> DistributedResult<()>;

    /// Set several values at once. Either all are written or none are.
    async fn set_multi(&self, kvs: Vec<(String, Vec<u8>)>) -> DistributedResult<()>;

    async fn delete(&self, key: &str) -> DistributedResult<()>;

    /// Save the current state under `checkpoint_id`.
    async fn checkpoint(&self, checkpoint_id: &str) -> DistributedResult<()>;

    /// Replace the current state with the state saved under `checkpoint_id`.
    async fn restore(&self, checkpoint_id: &str) -> DistributedResult<()>;

    async fn shutdown(&mut self) -> DistributedResult<()>;
}

/// In memory state backend for a single node.
///
/// `checkpoint` and `restore` do nothing. `shutdown` clears all state.
pub struct InMemoryBackend {
    state: Arc<RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl StateBackend for InMemoryBackend {
    async fn initialize(&mut self) -> DistributedResult<()> {
        Ok(())
    }

    async fn get(&self, key: &str) -> DistributedResult<Option<Vec<u8>>> {
        let state = self.state.read().await;
        Ok(state.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> DistributedResult<()> {
        let mut state = self.state.write().await;
        state.insert(key.to_string(), value);
        Ok(())
    }

    async fn set_multi(&self, kvs: Vec<(String, Vec<u8>)>) -> DistributedResult<()> {
        let mut state = self.state.write().await;
        for (key, value) in kvs {
            state.insert(key, value);
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> DistributedResult<()> {
        let mut state = self.state.write().await;
        state.remove(key);
        Ok(())
    }

    async fn checkpoint(&self, _checkpoint_id: &str) -> DistributedResult<()> {
        Ok(())
    }

    async fn restore(&self, _checkpoint_id: &str) -> DistributedResult<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        let mut state = self.state.write().await;
        state.clear();
        Ok(())
    }
}

/// Settings for [`RedisBackend`].
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis URL, such as `redis://localhost:6379`, the default.
    pub url: String,
    /// Most connections in the pool. Default 10.
    pub max_connections: usize,
    /// How long to wait for a free pool connection, in milliseconds. Default 5000.
    pub connection_timeout_ms: u64,
    /// Prefix added to every state key. Default `eventflux:state:`.
    pub key_prefix: String,
    /// How long entries live, in seconds. `None`, the default, means they never
    /// expire.
    pub ttl_seconds: Option<u64>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            max_connections: 10,
            connection_timeout_ms: 5000,
            key_prefix: "eventflux:state:".to_string(),
            ttl_seconds: None,
        }
    }
}

/// Redis state backend.
///
/// State lives in Redis under `key_prefix`. Checkpoints are copies held in this
/// process memory, not in Redis, so they are lost on shutdown or restart and are
/// not seen by other nodes.
#[allow(clippy::type_complexity)]
pub struct RedisBackend {
    /// `None` until [`initialize`](StateBackend::initialize) succeeds.
    pool: Option<Pool>,
    config: RedisConfig,
    /// Saved key value pairs for each checkpoint id. Keys are stored without the
    /// prefix.
    checkpoints: Arc<RwLock<HashMap<String, Vec<(String, Vec<u8>)>>>>,
}

impl std::fmt::Debug for RedisBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisBackend")
            .field("config", &self.config)
            .field("pool_initialized", &self.pool.is_some())
            .field(
                "checkpoints_count",
                &format!(
                    "{} checkpoints",
                    self.checkpoints.try_read().map_or(0, |c| c.len())
                ),
            )
            .finish()
    }
}

impl Default for RedisBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisBackend {
    /// Create a backend with the default settings. Not connected until
    /// [`initialize`](StateBackend::initialize) is called.
    pub fn new() -> Self {
        Self {
            pool: None,
            config: RedisConfig::default(),
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a backend with the given settings. Not connected until
    /// [`initialize`](StateBackend::initialize) is called.
    pub fn with_config(config: RedisConfig) -> Self {
        Self {
            pool: None,
            config,
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn make_key(&self, key: &str) -> String {
        format!("{}{}", self.config.key_prefix, key)
    }

    /// Take a connection from the pool.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the backend is not initialized or no connection is
    /// available in time.
    async fn get_connection(&self) -> DistributedResult<deadpool_redis::Connection> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| DistributedError::StateError {
                message: "Redis backend not initialized".to_string(),
            })?;

        pool.get().await.map_err(|e| DistributedError::StateError {
            message: format!("Failed to get Redis connection: {}", e),
        })
    }

    /// Run a Redis command on a pooled connection.
    ///
    /// `operation` gets the connection and returns a spawned task that runs the
    /// command.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if no connection is available, the task panics or is
    /// cancelled, or the command fails.
    async fn execute<F, R>(&self, operation: F) -> DistributedResult<R>
    where
        F: FnOnce(deadpool_redis::Connection) -> tokio::task::JoinHandle<RedisResult<R>>,
        R: Send + 'static,
    {
        let conn = self.get_connection().await?;

        let handle = operation(conn);
        let result = handle.await.map_err(|e| DistributedError::StateError {
            message: format!("Redis operation failed: {}", e),
        })?;

        result.map_err(|e| DistributedError::StateError {
            message: format!("Redis command failed: {}", e),
        })
    }
}

#[async_trait]
impl StateBackend for RedisBackend {
    /// Build the connection pool and check it with a `PING`.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the pool cannot be built, no connection can be made,
    /// or the ping fails.
    async fn initialize(&mut self) -> DistributedResult<()> {
        let mut cfg = Config::from_url(&self.config.url);

        if cfg.pool.is_none() {
            cfg.pool = Some(deadpool_redis::PoolConfig::default());
        }

        if let Some(ref mut pool_config) = cfg.pool {
            pool_config.max_size = self.config.max_connections;
            pool_config.timeouts.wait = Some(std::time::Duration::from_millis(
                self.config.connection_timeout_ms,
            ));
        }

        let pool =
            cfg.create_pool(Some(Runtime::Tokio1))
                .map_err(|e| DistributedError::StateError {
                    message: format!("Failed to create Redis pool: {}", e),
                })?;

        let conn = pool.get().await.map_err(|e| DistributedError::StateError {
            message: format!("Failed to connect to Redis: {}", e),
        })?;

        let mut conn = conn;
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| DistributedError::StateError {
                message: format!("Redis ping failed: {}", e),
            })?;

        self.pool = Some(pool);
        Ok(())
    }

    async fn get(&self, key: &str) -> DistributedResult<Option<Vec<u8>>> {
        let redis_key = self.make_key(key);

        self.execute(|mut conn| {
            tokio::spawn(async move { conn.get::<_, Option<Vec<u8>>>(&redis_key).await })
        })
        .await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> DistributedResult<()> {
        let redis_key = self.make_key(key);

        if let Some(ttl) = self.config.ttl_seconds {
            self.execute(|mut conn| {
                tokio::spawn(async move { conn.set_ex::<_, _, ()>(&redis_key, &value, ttl).await })
            })
            .await
        } else {
            self.execute(|mut conn| {
                tokio::spawn(async move { conn.set::<_, _, ()>(&redis_key, &value).await })
            })
            .await
        }
    }

    async fn set_multi(&self, kvs: Vec<(String, Vec<u8>)>) -> DistributedResult<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;

        // MULTI/EXEC makes the whole batch apply together.
        let mut pipe = redis::pipe();
        pipe.atomic();

        // .ignore() on each command keeps EXEC from returning an array of results.
        for (key, value) in kvs {
            let redis_key = self.make_key(&key);
            if let Some(ttl) = self.config.ttl_seconds {
                pipe.set_ex(&redis_key, &value, ttl).ignore();
            } else {
                pipe.set(&redis_key, &value).ignore();
            }
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| DistributedError::StateError {
                message: format!("Redis atomic set_multi failed: {}", e),
            })
    }

    async fn delete(&self, key: &str) -> DistributedResult<()> {
        let redis_key = self.make_key(key);

        self.execute(|mut conn| tokio::spawn(async move { conn.del::<_, ()>(&redis_key).await }))
            .await
    }

    /// Copy every prefixed key and value from Redis into memory under
    /// `checkpoint_id`. Replaces an earlier checkpoint with the same id.
    ///
    /// Uses `KEYS`, which blocks Redis while it scans every key. The read is not
    /// atomic, so writes made during the copy may be partly included.
    async fn checkpoint(&self, checkpoint_id: &str) -> DistributedResult<()> {
        let pattern = format!("{}*", self.config.key_prefix);

        let keys: Vec<String> = self
            .execute(|mut conn| tokio::spawn(async move { conn.keys(&pattern).await }))
            .await?;

        // An empty key list still stores an empty checkpoint.
        let values: Vec<Option<Vec<u8>>> = if keys.is_empty() {
            Vec::new()
        } else {
            let keys_clone = keys.clone();
            self.execute(|mut conn| tokio::spawn(async move { conn.mget(&keys_clone).await }))
                .await?
        };

        // Keys that expired or were deleted between KEYS and MGET come back as None
        // and are skipped.
        let mut checkpoint_data = Vec::new();
        for (key, value_opt) in keys.iter().zip(values.iter()) {
            if let Some(value) = value_opt {
                let original_key = key
                    .strip_prefix(&self.config.key_prefix)
                    .unwrap_or(key)
                    .to_string();
                checkpoint_data.push((original_key, value.clone()));
            }
        }

        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.insert(checkpoint_id.to_string(), checkpoint_data);

        Ok(())
    }

    /// Delete every prefixed key in Redis, then write back the saved pairs one by
    /// one.
    ///
    /// Not atomic. A failure part way leaves Redis with only some of the saved
    /// pairs. Restored keys get the configured TTL again.
    ///
    /// # Errors
    ///
    /// Returns `StateError` if the checkpoint id is unknown or a Redis command
    /// fails.
    async fn restore(&self, checkpoint_id: &str) -> DistributedResult<()> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint_data =
            checkpoints
                .get(checkpoint_id)
                .ok_or_else(|| DistributedError::StateError {
                    message: format!("Checkpoint {} not found", checkpoint_id),
                })?;

        let pattern = format!("{}*", self.config.key_prefix);
        let existing_keys: Vec<String> = self
            .execute(|mut conn| {
                let pattern_clone = pattern.clone();
                tokio::spawn(async move { conn.keys(&pattern_clone).await })
            })
            .await?;

        if !existing_keys.is_empty() {
            let keys_to_delete = existing_keys.clone();
            self.execute(|mut conn| {
                tokio::spawn(async move { conn.del::<_, ()>(&keys_to_delete).await })
            })
            .await?;
        }

        for (key, value) in checkpoint_data {
            self.set(key, value.clone()).await?;
        }

        Ok(())
    }

    /// Drop the pool and every saved checkpoint. Does nothing if never initialized.
    async fn shutdown(&mut self) -> DistributedResult<()> {
        if let Some(pool) = self.pool.take() {
            let mut checkpoints = self.checkpoints.write().await;
            checkpoints.clear();

            // Dropping the pool closes its connections.
            drop(pool);
        }
        Ok(())
    }
}
