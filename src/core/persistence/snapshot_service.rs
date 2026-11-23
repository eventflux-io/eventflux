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

/// Result of a persist call.
#[derive(Debug, Clone)]
pub struct PersistReport {
    /// Revision id that was saved.
    pub revision: String,
    /// Components saved.
    pub success_count: usize,
    /// Components that failed to save.
    pub failure_count: usize,
    /// Ids of the components saved.
    pub succeeded_components: Vec<String>,
    /// Id and error message of each component that failed.
    pub failed_components: Vec<(String, String)>,
}

/// Takes and restores snapshots of an app's state.
///
/// A snapshot holds a raw state byte buffer plus the state of every registered
/// state holder.
#[derive(Default)]
pub struct SnapshotService {
    state: Mutex<Vec<u8>>, // serialized runtime state
    /// Where snapshots are saved. Persist and restore fail without one.
    pub persistence_store: Option<Arc<dyn PersistenceStore>>,
    pub eventflux_app_id: String,
    /// State holders by component id.
    state_holders: Mutex<HashMap<String, Arc<Mutex<dyn StateHolder>>>>,
}

/// What is saved for one revision.
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

    /// Replace the raw state buffer.
    pub fn set_state(&self, data: Vec<u8>) {
        *self.state.lock().unwrap() = data;
    }

    /// Register a state holder to include in snapshots. Replaces any holder
    /// already registered under `id`.
    pub fn register_state_holder(&self, id: String, holder: Arc<Mutex<dyn StateHolder>>) {
        log::info!("SnapshotService: Registering state holder: {}", id);
        self.state_holders.lock().unwrap().insert(id, holder);
        log::info!(
            "SnapshotService: Total registered holders: {}",
            self.state_holders.lock().unwrap().len()
        );
    }

    /// Returns a copy of the raw state buffer.
    pub fn snapshot(&self) -> Vec<u8> {
        self.state.lock().unwrap().clone()
    }

    /// Save the raw state and every registered state holder to the store.
    ///
    /// The revision id is the current time in milliseconds. A holder that fails
    /// to serialize is logged and left out, and the others are still saved.
    ///
    /// # Returns
    ///
    /// `Ok(PersistReport)` only when every holder was saved. `failure_count` is
    /// then always 0.
    ///
    /// # Errors
    ///
    /// Returns an error message if the snapshot cannot be serialized, there is no
    /// persistence store, or any holder failed. A partial save counts as a
    /// failure, but the revision with the holders that did succeed has already
    /// been written to the store by then. The message names that revision and the
    /// failed components.
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

        // Persistence is critical, so any failed component makes the whole call
        // fail, even though the partial revision is already saved.
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

    /// Reset every registered state holder to its initial empty state, and clear
    /// the raw state buffer.
    pub fn clear_state_holders(&self) {
        let holders: Vec<_> = self
            .state_holders
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (id, holder) in holders {
            holder.lock().unwrap().reset_state();
            log::debug!("Reset state for component: {}", id);
        }
        *self.state.lock().unwrap() = Vec::new();
    }

    /// Load a revision from the store and restore the raw state and each state
    /// holder from it.
    ///
    /// A holder that fails to restore, or a saved component with no registered
    /// holder, is only logged. The call still returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error message if there is no persistence store, the revision is
    /// not found, or the saved data cannot be deserialized.
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
                    // Pass the whole snapshot so the holder sees its metadata, such
                    // as compression, checksum and version.
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
