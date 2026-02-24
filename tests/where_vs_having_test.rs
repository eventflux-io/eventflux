// SPDX-License-Identifier: MIT OR Apache-2.0

//! Critical test to verify WHERE (pre-aggregation) vs HAVING (post-aggregation) distinction
//!
//! This test ensures that:
//! 1. WHERE filters individual rows BEFORE aggregation
//! 2. HAVING filters aggregated groups AFTER aggregation
//! 3. The COUNT in HAVING reflects only rows that passed WHERE filter

#[path = "common/mod.rs"]
mod common;
use common::AppRunner;
use eventflux::core::event::value::AttributeValue;

#[tokio::test]
async fn test_where_before_having_after_aggregation() {
    // This query tests that WHERE and HAVING execute in the correct order
    let app = r#"
        CREATE STREAM Products (category STRING, price INT);
        CREATE STREAM Output (category STRING, count BIGINT);

        INSERT INTO Output
        SELECT category, COUNT(*) as count
        FROM Products
        WHERE price > 100
        GROUP BY category
        HAVING COUNT(*) > 2;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    // Send test data:
    // Category A: 4 products (3 with price > 100, 1 with price <= 100)
    //   - WHERE should filter out 1 product
    //   - COUNT(*) after WHERE = 3
    //   - HAVING COUNT(*) > 2 should PASS (3 > 2)
    //
    // Category B: 4 products (2 with price > 100, 2 with price <= 100)
    //   - WHERE should filter out 2 products
    //   - COUNT(*) after WHERE = 2
    //   - HAVING COUNT(*) > 2 should FAIL (2 not > 2)
    //
    // Category C: 5 products (ALL with price > 100)
    //   - WHERE filters out nothing
    //   - COUNT(*) after WHERE = 5
    //   - HAVING COUNT(*) > 2 should PASS (5 > 2)

    runner.send_batch(
        "Products",
        vec![
            // Category A
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(50),
            ], // Filtered by WHERE
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(150),
            ], // Passes WHERE
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(200),
            ], // Passes WHERE
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(120),
            ], // Passes WHERE
            // Category B
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(80),
            ], // Filtered by WHERE
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(90),
            ], // Filtered by WHERE
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(110),
            ], // Passes WHERE
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(130),
            ], // Passes WHERE
            // Category C
            vec![
                AttributeValue::String("C".to_string()),
                AttributeValue::Int(150),
            ], // Passes WHERE
            vec![
                AttributeValue::String("C".to_string()),
                AttributeValue::Int(160),
            ], // Passes WHERE
            vec![
                AttributeValue::String("C".to_string()),
                AttributeValue::Int(170),
            ], // Passes WHERE
            vec![
                AttributeValue::String("C".to_string()),
                AttributeValue::Int(180),
            ], // Passes WHERE
            vec![
                AttributeValue::String("C".to_string()),
                AttributeValue::Int(190),
            ], // Passes WHERE
        ],
    );

    let output = runner.shutdown();

    // Expected output:
    // - Category A: count=3 (passes HAVING)
    // - Category B: count=2 (filtered by HAVING)
    // - Category C: count=5 (passes HAVING)

    println!("Output events: {:?}", output);

    // Should have exactly 2 groups (A and C, B filtered by HAVING)
    assert_eq!(
        output.len(),
        2,
        "Expected 2 categories (A and C), got {}. Category B should be filtered by HAVING COUNT(*) > 2",
        output.len()
    );

    // Find Category A in output
    let category_a = output.iter().find(|row| {
        if let AttributeValue::String(cat) = &row[0] {
            cat == "A"
        } else {
            false
        }
    });
    assert!(category_a.is_some(), "Category A should be in output");

    if let Some(row) = category_a {
        if let AttributeValue::Long(count) = &row[1] {
            assert_eq!(
                *count, 3,
                "Category A count should be 3 (only counting rows that passed WHERE filter)"
            );
        } else {
            panic!("Invalid count type for Category A");
        }
    }

    // Find Category C in output
    let category_c = output.iter().find(|row| {
        if let AttributeValue::String(cat) = &row[0] {
            cat == "C"
        } else {
            false
        }
    });
    assert!(category_c.is_some(), "Category C should be in output");

    if let Some(row) = category_c {
        if let AttributeValue::Long(count) = &row[1] {
            assert_eq!(
                *count, 5,
                "Category C count should be 5 (all products passed WHERE filter)"
            );
        } else {
            panic!("Invalid count type for Category C");
        }
    }

    // Verify Category B is NOT in output (filtered by HAVING)
    let category_b = output.iter().find(|row| {
        if let AttributeValue::String(cat) = &row[0] {
            cat == "B"
        } else {
            false
        }
    });
    assert!(
        category_b.is_none(),
        "Category B should be filtered out by HAVING (count=2, but HAVING requires > 2)"
    );
}

#[tokio::test]
async fn test_having_without_where() {
    // Verify HAVING works correctly when there's no WHERE clause
    let app = r#"
        CREATE STREAM Sales (product STRING, amount INT);
        CREATE STREAM Output (product STRING, total BIGINT);

        INSERT INTO Output
        SELECT product, SUM(amount) as total
        FROM Sales
        GROUP BY product
        HAVING SUM(amount) > 500;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    runner.send_batch(
        "Sales",
        vec![
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(200),
            ],
            vec![
                AttributeValue::String("A".to_string()),
                AttributeValue::Int(400),
            ], // Total: 600 > 500
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(100),
            ],
            vec![
                AttributeValue::String("B".to_string()),
                AttributeValue::Int(200),
            ], // Total: 300 <= 500
        ],
    );

    let output = runner.shutdown();

    // Only product A should pass (600 > 500), B filtered by HAVING (300 <= 500)
    assert_eq!(output.len(), 1, "Expected only product A in output");

    if let (AttributeValue::String(product), AttributeValue::Long(total)) =
        (&output[0][0], &output[0][1])
    {
        assert_eq!(product, "A");
        assert_eq!(*total, 600);
    } else {
        panic!("Invalid output format");
    }
}

#[tokio::test]
async fn test_where_without_having() {
    // Verify WHERE works correctly when there's no HAVING clause
    let app = r#"
        CREATE STREAM Data (category STRING, value INT);
        CREATE STREAM Output (category STRING, count BIGINT);

        INSERT INTO Output
        SELECT category, COUNT(*) as count
        FROM Data
        WHERE value > 50
        GROUP BY category;
    "#;

    let runner = AppRunner::new(app, "Output").await;

    runner.send_batch(
        "Data",
        vec![
            vec![
                AttributeValue::String("X".to_string()),
                AttributeValue::Int(30),
            ], // Filtered by WHERE
            vec![
                AttributeValue::String("X".to_string()),
                AttributeValue::Int(60),
            ], // Counted
            vec![
                AttributeValue::String("X".to_string()),
                AttributeValue::Int(70),
            ], // Counted
            vec![
                AttributeValue::String("Y".to_string()),
                AttributeValue::Int(100),
            ], // Counted
        ],
    );

    let output = runner.shutdown();

    // Both X and Y should be in output (no HAVING filter)
    assert_eq!(output.len(), 2, "Expected both categories in output");

    // Find X
    let cat_x = output.iter().find(|row| {
        if let AttributeValue::String(cat) = &row[0] {
            cat == "X"
        } else {
            false
        }
    });
    assert!(cat_x.is_some(), "Category X should be in output");
    if let Some(row) = cat_x {
        if let AttributeValue::Long(count) = &row[1] {
            assert_eq!(
                *count, 2,
                "Category X should have count 2 (WHERE filtered out 1)"
            );
        }
    }

    // Find Y
    let cat_y = output.iter().find(|row| {
        if let AttributeValue::String(cat) = &row[0] {
            cat == "Y"
        } else {
            false
        }
    });
    assert!(cat_y.is_some(), "Category Y should be in output");
    if let Some(row) = cat_y {
        if let AttributeValue::Long(count) = &row[1] {
            assert_eq!(*count, 1, "Category Y should have count 1");
        }
    }
}
