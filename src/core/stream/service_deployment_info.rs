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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceProtocol {
    TCP,
    UDP,
    SCTP,
}

#[derive(Debug, Clone)]
pub struct ServiceDeploymentInfo {
    pub service_protocol: ServiceProtocol,
    pub is_pulling: bool,
    pub secured: bool,
    pub port: u16,
    pub deployment_properties: std::collections::HashMap<String, String>,
}

impl ServiceDeploymentInfo {
    pub fn new(service_protocol: ServiceProtocol, port: u16, secured: bool) -> Self {
        Self {
            service_protocol,
            is_pulling: false,
            secured,
            port,
            deployment_properties: std::collections::HashMap::new(),
        }
    }

    pub fn new_default(port: u16, secured: bool) -> Self {
        Self::new(ServiceProtocol::TCP, port, secured)
    }

    pub fn default() -> Self {
        Self {
            service_protocol: ServiceProtocol::TCP,
            is_pulling: true,
            secured: false,
            port: 0,
            deployment_properties: std::collections::HashMap::new(),
        }
    }

    pub fn add_deployment_properties(&mut self, props: std::collections::HashMap<String, String>) {
        for (k, v) in props.into_iter() {
            self.deployment_properties.entry(k).or_insert(v);
        }
    }
}
