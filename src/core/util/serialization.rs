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

use serde::{de::DeserializeOwned, Serialize};

/// Serialize any serde serializable object to bytes using bincode.
pub fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(value)
}

/// Deserialize bytes back into an object using bincode.
pub fn from_bytes<T: DeserializeOwned>(data: &[u8]) -> Result<T, bincode::Error> {
    bincode::deserialize(data)
}
