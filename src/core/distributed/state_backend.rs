// SPDX-License-Identifier: MIT OR Apache-2.0

// src/core/distributed/state_backend.rs

//! State Backend Abstraction
//!
//! This module provides the state backend abstraction for distributed state management.
//! It supports multiple backends (Redis, Ignite, Hazelcast, etc.) for storing and
//! synchronizing state across nodes.

use super::{DistributedError, DistributedResult};
use async_trait::async_trait;
use deadpool_redis::{Config, Pool, Runtime};
use redis::{AsyncCommands, RedisResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// State backend trait for distributed state management
#[async_trait]
pub trait StateBackend: Send + Sync {
    /// Initialize the state backend
    async fn initialize(&mut self) -> DistributedResult<()>;

    /// Get state value
    async fn get(&self, key: &str) -> DistributedResult<Option<Vec<u8>>>;

    /// Set state value
    async fn set(&self, key: &str, value: Vec<u8>) -> DistributedResult<()>;

    /// Set multiple state values atomically
    async fn set_multi(&self, kvs: Vec<(String, Vec<u8>)>) -> DistributedResult<()>;

    /// Delete state value
    async fn delete(&self, key: &str) -> DistributedResult<()>;

    /// Create a checkpoint
    async fn checkpoint(&self, checkpoint_id: &str) -> DistributedResult<()>;

    /// Restore from checkpoint
    async fn restore(&self, checkpoint_id: &str) -> DistributedResult<()>;

    /// Shutdown the backend
    async fn shutdown(&mut self) -> DistributedResult<()>;
}

/// In-memory state backend (for single-node)
pub struct InMemoryBackend {
    state: Arc<RwLock<std::collections::HashMap<String, Vec<u8>>>>,
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

/// Redis configuration for state backend
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis URL (e.g., "redis://localhost:6379")
    pub url: String,
    /// Maximum number of connections in the pool
    pub max_connections: usize,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Key prefix for all EventFlux state keys
    pub key_prefix: String,
    /// TTL for state entries in seconds (None for no expiration)
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

/// Redis state backend for distributed state management
pub struct RedisBackend {
    pool: Option<Pool>,
    config: RedisConfig,
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

impl RedisBackend {
    /// Create a new Redis backend with default configuration
    pub fn new() -> Self {
        Self {
            pool: None,
            config: RedisConfig::default(),
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new Redis backend with custom configuration
    pub fn with_config(config: RedisConfig) -> Self {
        Self {
            pool: None,
            config,
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create the full Redis key with prefix
    fn make_key(&self, key: &str) -> String {
        format!("{}{}", self.config.key_prefix, key)
    }

    /// Get a connection from the pool
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

    /// Execute Redis command with error handling
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
    async fn initialize(&mut self) -> DistributedResult<()> {
        let mut cfg = Config::from_url(&self.config.url);

        // Initialize pool config if it doesn't exist
        if cfg.pool.is_none() {
            cfg.pool = Some(deadpool_redis::PoolConfig::default());
        }

        // Configure pool settings
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

        // Test connection
        let conn = pool.get().await.map_err(|e| DistributedError::StateError {
            message: format!("Failed to connect to Redis: {}", e),
        })?;

        // Test with a simple ping
        let mut conn = conn;
        redis::cmd("PING")
            .query_async::<_, String>(&mut conn)
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

        // Build pipeline with MULTI/EXEC for atomic execution
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Add all SET operations to the pipeline
        // Call .ignore() on each command so EXEC doesn't return an array of results
        for (key, value) in kvs {
            let redis_key = self.make_key(&key);
            if let Some(ttl) = self.config.ttl_seconds {
                pipe.set_ex(&redis_key, &value, ttl).ignore();
            } else {
                pipe.set(&redis_key, &value).ignore();
            }
        }

        // Execute pipeline atomically
        pipe.query_async::<_, ()>(&mut conn)
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

    async fn checkpoint(&self, checkpoint_id: &str) -> DistributedResult<()> {
        // Get all keys with our prefix
        let pattern = format!("{}*", self.config.key_prefix);

        let keys: Vec<String> = self
            .execute(|mut conn| tokio::spawn(async move { conn.keys(&pattern).await }))
            .await?;

        // Continue even if keys is empty - we still want to store an empty checkpoint

        // Get all values for these keys (if any)
        let values: Vec<Option<Vec<u8>>> = if keys.is_empty() {
            Vec::new()
        } else {
            let keys_clone = keys.clone();
            self.execute(|mut conn| tokio::spawn(async move { conn.mget(&keys_clone).await }))
                .await?
        };

        // Store checkpoint data
        let mut checkpoint_data = Vec::new();
        for (key, value_opt) in keys.iter().zip(values.iter()) {
            if let Some(value) = value_opt {
                // Remove prefix to get original key
                let original_key = key
                    .strip_prefix(&self.config.key_prefix)
                    .unwrap_or(key)
                    .to_string();
                checkpoint_data.push((original_key, value.clone()));
            }
        }

        // Store checkpoint
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.insert(checkpoint_id.to_string(), checkpoint_data);

        Ok(())
    }

    async fn restore(&self, checkpoint_id: &str) -> DistributedResult<()> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint_data =
            checkpoints
                .get(checkpoint_id)
                .ok_or_else(|| DistributedError::StateError {
                    message: format!("Checkpoint {} not found", checkpoint_id),
                })?;

        // Clear existing state
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

        // Restore checkpoint data
        for (key, value) in checkpoint_data {
            self.set(key, value.clone()).await?;
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> DistributedResult<()> {
        if let Some(pool) = self.pool.take() {
            // Clear checkpoints
            let mut checkpoints = self.checkpoints.write().await;
            checkpoints.clear();

            // Close pool (pool handles connection cleanup automatically)
            drop(pool);
        }
        Ok(())
    }
}
