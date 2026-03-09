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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Simple wrapper around a `Mutex` identified by an id.
#[derive(Debug, Clone)]
pub struct LockWrapper {
    lock_id: String,
    lock: Arc<Mutex<()>>, // always initialized
}

impl LockWrapper {
    /// Create a new lock wrapper with the given id.
    pub fn new(lock_id: &str) -> Self {
        Self {
            lock_id: lock_id.to_string(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Acquire the underlying lock.
    pub fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap()
    }

    /// Replace the internal lock with another `Arc`.
    pub fn set_lock(&mut self, lock: Arc<Mutex<()>>) {
        self.lock = lock;
    }

    /// Get the current lock `Arc`.
    pub fn get_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.lock)
    }
}

impl PartialEq for LockWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.lock_id == other.lock_id
    }
}
impl Eq for LockWrapper {}

/// Synchronizes two or more `LockWrapper`s so that they share the same mutex.
#[derive(Debug, Default)]
pub struct LockSynchronizer {
    holder_map: HashMap<String, Vec<Arc<Mutex<LockWrapper>>>>,
}

impl LockSynchronizer {
    pub fn new() -> Self {
        Self {
            holder_map: HashMap::new(),
        }
    }

    /// Ensure both wrappers share the same lock and update any previously
    /// synchronized wrappers as well.
    pub fn sync(&mut self, one: Arc<Mutex<LockWrapper>>, two: Arc<Mutex<LockWrapper>>) {
        let id1 = one.lock().unwrap().lock_id.clone();
        let id2 = two.lock().unwrap().lock_id.clone();
        let one_exists = self.holder_map.contains_key(&id1);
        let two_exists = self.holder_map.contains_key(&id2);

        if !one_exists && !two_exists {
            let new_lock = Arc::new(Mutex::new(()));
            one.lock().unwrap().set_lock(new_lock.clone());
            two.lock().unwrap().set_lock(new_lock.clone());
            self.holder_map.insert(id1.clone(), vec![Arc::clone(&two)]);
            self.holder_map.insert(id2.clone(), vec![Arc::clone(&one)]);
        } else if one_exists && !two_exists {
            let lock = one.lock().unwrap().get_lock();
            two.lock().unwrap().set_lock(lock.clone());
            self.holder_map
                .entry(id1.clone())
                .or_default()
                .push(Arc::clone(&two));
            self.holder_map.insert(id2.clone(), vec![Arc::clone(&one)]);
        } else if !one_exists && two_exists {
            let lock = two.lock().unwrap().get_lock();
            one.lock().unwrap().set_lock(lock.clone());
            self.holder_map
                .entry(id2.clone())
                .or_default()
                .push(Arc::clone(&one));
            self.holder_map.insert(id1.clone(), vec![Arc::clone(&two)]);
        } else {
            let lock = one.lock().unwrap().get_lock();
            two.lock().unwrap().set_lock(lock.clone());
            if let Some(list) = self.holder_map.get(&id2) {
                for holder in list {
                    holder.lock().unwrap().set_lock(lock.clone());
                }
            }
            self.holder_map
                .entry(id1.clone())
                .or_default()
                .push(Arc::clone(&two));
        }
    }
}
