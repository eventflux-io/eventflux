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

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::core::persistence::StateHolder;
use crate::core::util::{from_bytes, to_bytes};

use super::persistence_store::PersistenceStore;

/// Report of a persistence operation
#[derive(Debug, Clone)]
pub struct PersistReport {
    /// Revision ID that was created
    pub revision: String,
    /// Number of components successfully persisted
    pub success_count: usize,
    /// Number of components that failed to persist
    pub failure_count: usize,
    /// Component IDs that succeeded
    pub succeeded_components: Vec<String>,
    /// Component IDs and error messages that failed
    pub failed_components: Vec<(String, String)>,
}

/// Basic snapshot service keeping arbitrary state bytes.
#[derive(Default)]
pub struct SnapshotService {
    state: Mutex<Vec<u8>>, // serialized runtime state
    pub persistence_store: Option<Arc<dyn PersistenceStore>>,
    pub eventflux_app_id: String,
    state_holders: Mutex<HashMap<String, Arc<Mutex<dyn StateHolder>>>>,
}

#[derive(Serialize, Deserialize, Default)]
struct SnapshotData {
    main: Vec<u8>,
    holders: HashMap<String, crate::core::persistence::StateSnapshot>,
}

impl std::fmt::Debug for SnapshotService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotService")
            .field("eventflux_app_id", &self.eventflux_app_id)
            .finish()
    }
}

impl SnapshotService {
    pub fn new(eventflux_app_id: String) -> Self {
        Self {
            state: Mutex::new(Vec::new()),
            persistence_store: None,
            eventflux_app_id,
            state_holders: Mutex::new(HashMap::new()),
        }
    }

    /// Replace the current internal state.
    pub fn set_state(&self, data: Vec<u8>) {
        *self.state.lock().unwrap() = data;
    }

    /// Register a state holder to be included in snapshots.
    pub fn register_state_holder(&self, id: String, holder: Arc<Mutex<dyn StateHolder>>) {
        log::info!("SnapshotService: Registering state holder: {}", id);
        self.state_holders.lock().unwrap().insert(id, holder);
        log::info!(
            "SnapshotService: Total registered holders: {}",
            self.state_holders.lock().unwrap().len()
        );
    }

    /// Retrieve a copy of the internal state.
    pub fn snapshot(&self) -> Vec<u8> {
        self.state.lock().unwrap().clone()
    }

    /// Persist the current state via the configured store.
    ///
    /// Attempts to serialize and persist all registered state holders. Errors are
    /// collected and reported rather than causing immediate failure, allowing
    /// callers to handle partial failures appropriately.
    ///
    /// # Returns
    ///
    /// * `Ok(PersistReport)` - Persistence completed (possibly with some failures)
    ///   - Contains revision ID, success/failure counts, and component details
    ///   - Caller should check `failure_count` to detect partial failures
    /// * `Err(String)` - Critical failure (no persistence store, serialization error)
    ///
    /// # Error Handling
    ///
    /// Individual component serialization failures are logged and accumulated in the
    /// report. The operation continues to attempt persisting remaining components.
    /// Only critical failures (e.g., no persistence store) cause immediate error return.
    pub fn persist(&self) -> Result<PersistReport, String> {
        let mut holders = HashMap::new();
        let mut succeeded_components = Vec::new();
        let mut failed_components = Vec::new();

        let hints = crate::core::persistence::SerializationHints::default();
        for (id, holder) in self.state_holders.lock().unwrap().iter() {
            log::info!("Persisting state for component: {}", id);
            match holder.lock().unwrap().serialize_state(&hints) {
                Ok(snapshot) => {
                    holders.insert(id.clone(), snapshot);
                    succeeded_components.push(id.clone());
                    log::info!("Successfully persisted state for: {}", id);
                }
                Err(e) => {
                    let error_msg = format!("{:?}", e);
                    log::error!("Failed to serialize state for {id}: {error_msg}");
                    failed_components.push((id.clone(), error_msg));
                    continue;
                }
            }
        }

        let snapshot = SnapshotData {
            main: self.snapshot(),
            holders,
        };
        let data = to_bytes(&snapshot).map_err(|e| e.to_string())?;
        let store = self
            .persistence_store
            .as_ref()
            .ok_or("No persistence store")?;
        let revision = Utc::now().timestamp_millis().to_string();
        store.save(&self.eventflux_app_id, &revision, &data);

        let report = PersistReport {
            revision: revision.clone(),
            success_count: succeeded_components.len(),
            failure_count: failed_components.len(),
            succeeded_components,
            failed_components,
        };

        // Return error on partial or complete failure
        // Persistence is a critical operation - partial success IS a failure
        if report.failure_count > 0 {
            let failed_ids: Vec<&str> = report
                .failed_components
                .iter()
                .map(|(id, _)| id.as_str())
                .collect();
            return Err(format!(
                "Partial persistence failure: {} component(s) failed to persist (revision: {}). Failed: {}",
                report.failure_count,
                report.revision,
                failed_ids.join(", ")
            ));
        }

        Ok(report)
    }

    /// Reset all registered state holders to their initial (empty) values.
    pub fn clear_state_holders(&self) {
        let holders: Vec<_> = self.state_holders.lock().unwrap()
            .iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (id, holder) in holders {
            holder.lock().unwrap().reset_state();
            log::debug!("Reset state for component: {}", id);
        }
        *self.state.lock().unwrap() = Vec::new();
    }

    /// Load the given revision from the store and set as current state.
    pub fn restore_revision(&self, revision: &str) -> Result<(), String> {
        let store = self
            .persistence_store
            .as_ref()
            .ok_or("No persistence store")?;
        if let Some(data) = store.load(&self.eventflux_app_id, revision) {
            let snap: SnapshotData = from_bytes(&data).map_err(|e| e.to_string())?;
            self.set_state(snap.main);
            for (id, snapshot) in snap.holders {
                log::info!("Restoring state for component: {}", id);
                if let Some(holder) = self.state_holders.lock().unwrap().get(&id) {
                    // Use the full snapshot with all metadata (compression, checksum, version, etc.)
                    match holder.lock().unwrap().deserialize_state(&snapshot) {
                        Ok(_) => {
                            log::info!("Successfully restored state for: {}", id);
                        }
                        Err(e) => {
                            log::error!("Failed to restore state for {id}: {e:?}");
                            log::error!("Component ID: {}, Error details: {}", id, e);
                        }
                    }
                } else {
                    log::info!("No state holder found for component: {}", id);
                }
            }
            Ok(())
        } else {
            Err("Revision not found".into())
        }
    }
}
