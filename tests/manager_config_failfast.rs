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

//! A configuration that fails to load must fail app creation (fail-fast),
//! not silently start the engine with defaults (wrong buffer sizes, sync
//! junctions where async was configured, fallback app names breaking
//! persistence keys).

use eventflux::core::config::ConfigManager;
use eventflux::core::eventflux_manager::EventFluxManager;

#[tokio::test]
async fn config_load_failure_fails_app_creation() {
    let path = std::env::temp_dir().join(format!(
        "eventflux-broken-config-{}.yaml",
        std::process::id()
    ));
    std::fs::write(&path, "eventflux: [this is: not valid yaml").unwrap();

    let manager =
        EventFluxManager::new_with_config_manager(ConfigManager::from_file(path.to_str().unwrap()));
    let app = "\
        CREATE STREAM In (v INT);\n\
        CREATE STREAM Out (v INT);\n\
        INSERT INTO Out SELECT v FROM In;\n";
    let result = manager.create_eventflux_app_runtime_from_string(app).await;
    std::fs::remove_file(&path).ok();

    let err = result.expect_err(
        "a broken config file must fail app creation, not silently start with defaults",
    );
    assert!(
        err.to_lowercase().contains("config"),
        "error should point at the configuration, got: {err}"
    );
}
