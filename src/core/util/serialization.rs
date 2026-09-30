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

/// Unified error for bincode 2/3, which splits encode and decode errors
/// into separate types.
#[derive(Debug, thiserror::Error)]
pub enum SerdeError {
    #[error("encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

/// The one wire-format definition for every bincode use in the engine.
///
/// `legacy()` is byte-identical to bincode 1.x (fixed-width ints, little
/// endian), so snapshots/checkpoints persisted before the bincode 3
/// migration keep decoding. Switching to `standard()` (varint) is a
/// deliberate format break, not a drive-by upgrade.
fn config() -> impl bincode::config::Config {
    bincode::config::legacy()
}

/// Serialize any serde serializable object to bytes using bincode.
/// `?Sized` admits slices (`&[Event]`), which the binary mappers use.
pub fn to_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, SerdeError> {
    Ok(bincode::serde::encode_to_vec(value, config())?)
}

/// Deserialize bytes back into an object using bincode.
pub fn from_bytes<T: DeserializeOwned>(data: &[u8]) -> Result<T, SerdeError> {
    Ok(bincode::serde::decode_from_slice(data, config())?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_format_matches_bincode_1() {
        // Golden bytes produced by bincode 1.3's `serialize` for these
        // exact values. `config::legacy()` must keep emitting them so
        // snapshots/checkpoints persisted before the bincode-2 migration
        // still decode. A failure here means the wire format broke.
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Golden {
            id: u64,
            name: String,
            values: Vec<i32>,
            flag: bool,
        }

        let value = Golden {
            id: 7,
            name: "ev".to_string(),
            values: vec![1, -2],
            flag: true,
        };

        // bincode 1.x: u64 fixed LE, string as u64 len + bytes,
        // vec as u64 len + fixed LE elements, bool as one byte
        let bincode1_bytes: Vec<u8> = vec![
            7, 0, 0, 0, 0, 0, 0, 0, // id
            2, 0, 0, 0, 0, 0, 0, 0, b'e', b'v', // name
            2, 0, 0, 0, 0, 0, 0, 0, // values len
            1, 0, 0, 0, // 1i32
            254, 255, 255, 255, // -2i32
            1,   // flag
        ];

        assert_eq!(to_bytes(&value).unwrap(), bincode1_bytes);
        let decoded: Golden = from_bytes(&bincode1_bytes).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_roundtrip_via_unified_error() {
        let original = vec![("k".to_string(), 42u32)];
        let bytes = to_bytes(&original).unwrap();
        let back: Vec<(String, u32)> = from_bytes(&bytes).unwrap();
        assert_eq!(back, original);

        // Garbage decodes to the Decode arm of the unified error
        let err = from_bytes::<String>(&[0xFF; 3]).unwrap_err();
        assert!(matches!(err, SerdeError::Decode(_)));
    }
}
