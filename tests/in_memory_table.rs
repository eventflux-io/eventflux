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
    InMemoryCompiledCondition, InMemoryCompiledUpdateSet, InMemoryTable, Table,
};

#[test]
fn test_insert_and_contains() {
    let table = InMemoryTable::new();
    let row = vec![
        AttributeValue::Int(1),
        AttributeValue::String("a".to_string()),
    ];
    table.insert(&row).unwrap();
    assert!(table
        .contains(&InMemoryCompiledCondition {
            values: row.clone()
        })
        .unwrap());
}

#[test]
fn test_contains_false() {
    let table = InMemoryTable::new();
    let row1 = vec![AttributeValue::Int(2)];
    let row2 = vec![AttributeValue::Int(3)];
    table.insert(&row1).unwrap();
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: row2 })
        .unwrap());
}

#[test]
fn test_update() {
    let table = InMemoryTable::new();
    let old = vec![AttributeValue::Int(1)];
    let new = vec![AttributeValue::Int(2)];
    table.insert(&old).unwrap();
    let cond = InMemoryCompiledCondition {
        values: old.clone(),
    };
    let us = InMemoryCompiledUpdateSet {
        values: new.clone(),
    };
    assert!(table.update(&cond, &us).unwrap());
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: old })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition { values: new })
        .unwrap());
}

#[test]
fn test_delete() {
    let table = InMemoryTable::new();
    let row1 = vec![AttributeValue::Int(1)];
    let row2 = vec![AttributeValue::Int(2)];
    table.insert(&row1).unwrap();
    table.insert(&row2).unwrap();
    assert!(table
        .delete(&InMemoryCompiledCondition {
            values: row1.clone()
        })
        .unwrap());
    assert!(!table
        .contains(&InMemoryCompiledCondition { values: row1 })
        .unwrap());
    assert!(table
        .contains(&InMemoryCompiledCondition {
            values: row2.clone()
        })
        .unwrap());
}

#[test]
fn test_find() {
    let table = InMemoryTable::new();
    let row = vec![AttributeValue::Int(42)];
    table.insert(&row).unwrap();
    let found = table
        .find(&InMemoryCompiledCondition {
            values: row.clone(),
        })
        .unwrap();
    assert_eq!(found, Some(row.clone()));
    assert!(table
        .find(&InMemoryCompiledCondition {
            values: vec![AttributeValue::Int(0)]
        })
        .unwrap()
        .is_none());
}
