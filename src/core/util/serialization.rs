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

/// One error type for bincode 2, which has separate encode and decode error types.
#[derive(Debug, thiserror::Error)]
pub enum SerdeError {
    #[error("encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

/// The single byte format setting for every bincode use in the engine.
///
/// `legacy()` writes exactly the same bytes as bincode 1.x (fixed width ints, little
/// endian), so snapshots and checkpoints saved before the move to bincode 2 still
/// decode. Switching to `standard()` (varint) breaks the format on purpose and must
/// not be done as a casual upgrade.
fn config() -> impl bincode::config::Config {
    bincode::config::legacy()
}

/// Serializes any serde value to bytes with bincode.
/// `?Sized` allows slices (`&[Event]`), which the binary mappers use.
///
/// # Errors
/// Returns `SerdeError::Encode` if encoding fails.
pub fn to_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, SerdeError> {
    Ok(bincode::serde::encode_to_vec(value, config())?)
}

/// Decodes bytes written by [`to_bytes`] back into a value. Extra trailing bytes are
/// ignored.
///
/// # Errors
/// Returns `SerdeError::Decode` if the bytes do not decode as `T`.
pub fn from_bytes<T: DeserializeOwned>(data: &[u8]) -> Result<T, SerdeError> {
    Ok(bincode::serde::decode_from_slice(data, config())?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_format_matches_bincode_1() {
        // Known good bytes from bincode 1.3 `serialize` for these exact values.
        // `config::legacy()` must keep writing them so snapshots and checkpoints saved
        // before the move to bincode 2 still decode. A failure here means the byte
        // format broke.
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

        // Bad bytes give the Decode variant of the shared error
        let err = from_bytes::<String>(&[0xFF; 3]).unwrap_err();
        assert!(matches!(err, SerdeError::Decode(_)));
    }
}
