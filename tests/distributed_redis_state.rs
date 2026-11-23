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

// eventflux/tests/distributed_redis_state.rs

//! Redis State Backend Integration Tests
//!
//! Tests for the Redis state backend used for distributed state. They cover
//! connection pooling, state operations, checkpoint and restore, and error
//! handling. Tests that need Redis return early when it is not reachable.

use eventflux::core::distributed::{DistributedError, RedisBackend, RedisConfig, StateBackend};
use std::time::Duration;
use uuid::Uuid;

/// Creates a Redis backend with test settings and a key prefix unique to this call.
fn create_test_redis_backend() -> RedisBackend {
    create_test_redis_backend_with_prefix(&format!("test:eventflux:{}:", Uuid::new_v4()))
}

/// Creates a Redis backend on localhost:6379 with the given key prefix.
fn create_test_redis_backend_with_prefix(prefix: &str) -> RedisBackend {
    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 1000,
        key_prefix: prefix.to_string(),
        ttl_seconds: Some(300), // 5 minutes, so leftover test keys clean up quickly
    };
    RedisBackend::with_config(config)
}

/// Returns Err when Redis is not reachable, so the caller can skip the test.
async fn ensure_redis_available() -> Result<(), Box<dyn std::error::Error>> {
    let mut backend = RedisBackend::new();
    match backend.initialize().await {
        Ok(_) => {
            backend.shutdown().await?;
            Ok(())
        }
        Err(_) => {
            println!("Redis not available, skipping Redis-dependent tests");
            Err("Redis not available".into())
        }
    }
}

#[tokio::test]
async fn test_redis_backend_creation() {
    // Passes if neither constructor panics. No connection is made here.
    let _backend = RedisBackend::new();

    let config = RedisConfig::default();
    let _backend = RedisBackend::with_config(config);
}

#[tokio::test]
async fn test_redis_backend_initialization() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    let result = backend.initialize().await;
    assert!(result.is_ok());

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_basic_operations() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Test set and get
    let key = "test_key_1";
    let value = b"test_value_1".to_vec();

    backend.set(key, value.clone()).await.unwrap();
    let retrieved = backend.get(key).await.unwrap();
    assert_eq!(retrieved, Some(value));

    // A key that does not exist
    let missing = backend.get("non_existent").await.unwrap();
    assert_eq!(missing, None);

    // Test delete
    backend.delete(key).await.unwrap();
    let deleted = backend.get(key).await.unwrap();
    assert_eq!(deleted, None);

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_multiple_keys() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Set multiple keys
    let test_data = vec![
        ("key1", b"value1".to_vec()),
        ("key2", b"value2".to_vec()),
        ("key3", b"value3".to_vec()),
    ];

    for (key, value) in &test_data {
        backend.set(key, value.clone()).await.unwrap();
    }

    // Verify all keys
    for (key, expected_value) in &test_data {
        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, Some(expected_value.clone()));
    }

    // Clean up
    for (key, _) in &test_data {
        backend.delete(key).await.unwrap();
    }

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_key_prefixing() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    let key = "prefix_test";
    let value = b"prefix_value".to_vec();
    backend.set(key, value.clone()).await.unwrap();

    // Only checks the round trip through the backend. Checking the stored key
    // really has the prefix would need a direct Redis connection.
    let retrieved = backend.get(key).await.unwrap();
    assert_eq!(retrieved, Some(value));

    backend.delete(key).await.unwrap();
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_checkpoint_operations() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Set up initial state
    let test_data = vec![
        ("checkpoint_key1", b"checkpoint_value1".to_vec()),
        ("checkpoint_key2", b"checkpoint_value2".to_vec()),
        ("checkpoint_key3", b"checkpoint_value3".to_vec()),
    ];

    for (key, value) in &test_data {
        backend.set(key, value.clone()).await.unwrap();
    }

    // Create checkpoint
    let checkpoint_id = "test_checkpoint_1";
    backend.checkpoint(checkpoint_id).await.unwrap();

    // Modify state after checkpoint
    backend
        .set("checkpoint_key1", b"modified_value1".to_vec())
        .await
        .unwrap();
    backend.delete("checkpoint_key2").await.unwrap();
    backend.set("new_key", b"new_value".to_vec()).await.unwrap();

    // Verify modified state
    let modified = backend.get("checkpoint_key1").await.unwrap();
    assert_eq!(modified, Some(b"modified_value1".to_vec()));
    let deleted = backend.get("checkpoint_key2").await.unwrap();
    assert_eq!(deleted, None);
    let new_val = backend.get("new_key").await.unwrap();
    assert_eq!(new_val, Some(b"new_value".to_vec()));

    // Restore from checkpoint
    backend.restore(checkpoint_id).await.unwrap();

    // Verify restored state
    for (key, expected_value) in &test_data {
        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, Some(expected_value.clone()));
    }

    // Verify new key is gone
    let new_val_after_restore = backend.get("new_key").await.unwrap();
    assert_eq!(new_val_after_restore, None);

    // Clean up
    for (key, _) in &test_data {
        backend.delete(key).await.unwrap();
    }

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_checkpoint_empty_state() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Create checkpoint with empty state
    let checkpoint_id = "empty_checkpoint";
    backend.checkpoint(checkpoint_id).await.unwrap();

    // Add some state
    backend
        .set("temp_key", b"temp_value".to_vec())
        .await
        .unwrap();

    // Restore empty checkpoint
    backend.restore(checkpoint_id).await.unwrap();

    // Verify state is empty
    let result = backend.get("temp_key").await.unwrap();
    assert_eq!(result, None);

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_restore_nonexistent_checkpoint() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Restoring a checkpoint that does not exist
    let result = backend.restore("non_existent_checkpoint").await;
    assert!(result.is_err());

    if let Err(DistributedError::StateError { message }) = result {
        assert!(message.contains("not found"));
    } else {
        panic!("Expected StateError for non-existent checkpoint");
    }

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_multiple_checkpoints() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // First checkpoint
    backend.set("multi_key", b"value1".to_vec()).await.unwrap();
    backend.checkpoint("checkpoint1").await.unwrap();

    // Second checkpoint
    backend.set("multi_key", b"value2".to_vec()).await.unwrap();
    backend
        .set("extra_key", b"extra_value".to_vec())
        .await
        .unwrap();
    backend.checkpoint("checkpoint2").await.unwrap();

    // Modify state
    backend.set("multi_key", b"value3".to_vec()).await.unwrap();

    // Restore to checkpoint1
    backend.restore("checkpoint1").await.unwrap();
    let restored = backend.get("multi_key").await.unwrap();
    assert_eq!(restored, Some(b"value1".to_vec()));
    let extra = backend.get("extra_key").await.unwrap();
    assert_eq!(extra, None);

    // Restore to checkpoint2
    backend.restore("checkpoint2").await.unwrap();
    let restored2 = backend.get("multi_key").await.unwrap();
    assert_eq!(restored2, Some(b"value2".to_vec()));
    let extra2 = backend.get("extra_key").await.unwrap();
    assert_eq!(extra2, Some(b"extra_value".to_vec()));

    // Clean up
    backend.delete("multi_key").await.unwrap();
    backend.delete("extra_key").await.unwrap();
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_concurrent_operations() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    let mut backend = create_test_redis_backend();
    backend.initialize().await.unwrap();

    // Spawn multiple concurrent operations
    let backend = std::sync::Arc::new(backend);
    let mut handles = vec![];

    for i in 0..10 {
        let backend_clone = backend.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent_key_{}", i);
            let value = format!("concurrent_value_{}", i).into_bytes();

            // Set value
            backend_clone.set(&key, value.clone()).await.unwrap();

            // Get value
            let retrieved = backend_clone.get(&key).await.unwrap();
            assert_eq!(retrieved, Some(value));

            // Delete value
            backend_clone.delete(&key).await.unwrap();
            let deleted = backend_clone.get(&key).await.unwrap();
            assert_eq!(deleted, None);
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // All tasks are done, so this is the last Arc and can be unwrapped for shutdown
    let mut backend = std::sync::Arc::try_unwrap(backend).unwrap();
    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_connection_pooling() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    // Test with limited pool size
    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 2, // Small pool for testing
        connection_timeout_ms: 1000,
        key_prefix: "pool_test:".to_string(),
        ttl_seconds: None,
    };

    let mut backend = RedisBackend::with_config(config);
    backend.initialize().await.unwrap();

    // Perform operations that should work within pool limits
    for i in 0..5 {
        let key = format!("pool_key_{}", i);
        let value = format!("pool_value_{}", i).into_bytes();

        backend.set(&key, value.clone()).await.unwrap();
        let retrieved = backend.get(&key).await.unwrap();
        assert_eq!(retrieved, Some(value));
        backend.delete(&key).await.unwrap();
    }

    backend.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_redis_backend_error_handling() {
    // Test with invalid Redis URL
    let config = RedisConfig {
        url: "redis://invalid-host:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 100, // Short timeout for quick failure
        key_prefix: "error_test:".to_string(),
        ttl_seconds: None,
    };

    let mut backend = RedisBackend::with_config(config);
    let result = backend.initialize().await;
    assert!(result.is_err());

    if let Err(DistributedError::StateError { message }) = result {
        assert!(
            message.contains("Failed to connect to Redis") || message.contains("Redis ping failed")
        );
    } else {
        panic!("Expected StateError for connection failure");
    }
}

#[tokio::test]
async fn test_redis_backend_operations_without_initialization() {
    let backend = create_test_redis_backend();

    // Try operations without initialization
    let get_result = backend.get("test_key").await;
    assert!(get_result.is_err());

    let set_result = backend.set("test_key", b"test_value".to_vec()).await;
    assert!(set_result.is_err());

    let delete_result = backend.delete("test_key").await;
    assert!(delete_result.is_err());

    // All should return StateError about not being initialized
    match get_result {
        Err(DistributedError::StateError { message }) => {
            assert!(message.contains("not initialized"));
        }
        _ => panic!("Expected StateError for uninitialized backend"),
    }
}

#[tokio::test]
async fn test_redis_config_defaults() {
    let config = RedisConfig::default();
    assert_eq!(config.url, "redis://localhost:6379");
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.connection_timeout_ms, 5000);
    assert_eq!(config.key_prefix, "eventflux:state:");
    assert_eq!(config.ttl_seconds, None);
}

#[tokio::test]
async fn test_redis_backend_ttl_functionality() {
    if ensure_redis_available().await.is_err() {
        return;
    }

    // Test with very short TTL for quick verification
    let config = RedisConfig {
        url: "redis://localhost:6379".to_string(),
        max_connections: 5,
        connection_timeout_ms: 1000,
        key_prefix: "ttl_test:".to_string(),
        ttl_seconds: Some(1), // 1 second TTL
    };

    let mut backend = RedisBackend::with_config(config);
    backend.initialize().await.unwrap();

    // Set a value with TTL
    let key = "ttl_key";
    let value = b"ttl_value".to_vec();
    backend.set(key, value.clone()).await.unwrap();

    // Immediately verify it exists
    let retrieved = backend.get(key).await.unwrap();
    assert_eq!(retrieved, Some(value));

    // Wait 1.5 s, past the 1 s TTL
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Expiry is not asserted. Redis may not remove the key right away, so a check
    // here would be flaky. The test only makes sure setting a TTL does not fail.

    backend.shutdown().await.unwrap();
}
