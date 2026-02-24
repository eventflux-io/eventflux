// SPDX-License-Identifier: MIT OR Apache-2.0

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
