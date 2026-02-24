// SPDX-License-Identifier: MIT OR Apache-2.0

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
