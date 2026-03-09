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

use eventflux::core::event::value::AttributeValue;
use eventflux::core::table::{
    CacheTable, InMemoryCompiledCondition, InMemoryCompiledUpdateSet, Table,
};

#[test]
fn test_cache_insert_and_eviction() {
    let table = CacheTable::new(2);
    let r1 = vec![AttributeValue::Int(1)];
    let r2 = vec![AttributeValue::Int(2)];
    let r3 = vec![AttributeValue::Int(3)];
    table.insert(&r1).unwrap();
    table.insert(&r2).unwrap();
    table.insert(&r3).unwrap();
    // r1 should be evicted
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: r1 })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition { values: r2.clone() })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition { values: r3.clone() })
        .unwrap());
}

#[test]
fn test_cache_update_delete_find() {
    let table = CacheTable::new(3);
    let r1 = vec![AttributeValue::Int(1)];
    table.insert(&r1).unwrap();
    let r2 = vec![AttributeValue::Int(2)];
    let cond = InMemoryCompiledCondition { values: r1.clone() };
    let us = InMemoryCompiledUpdateSet { values: r2.clone() };
    assert!(table.update(&cond, &us).unwrap());
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: r1 })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition { values: r2.clone() })
        .unwrap());
    assert_eq!(
        table
            .find(&InMemoryCompiledCondition { values: r2.clone() })
            .unwrap(),
        Some(r2.clone())
    );
    assert!(table
        .delete(&InMemoryCompiledCondition { values: r2.clone() })
        .unwrap());
    assert!(table
        .find(&InMemoryCompiledCondition { values: r2 })
        .unwrap()
        .is_none());
}
