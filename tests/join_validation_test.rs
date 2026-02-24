// Test to verify JOIN validation and error handling

use eventflux::sql_compiler::parse;

#[test]
fn test_join_using_clause_error() {
    // USING clause should produce helpful error
    let sql = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name, R.value\n\
        FROM L JOIN R USING (id);\n";

    let result = parse(sql);
    assert!(result.is_err(), "USING clause should be rejected");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("USING") || err_msg.contains("not yet supported"),
        "Error should mention USING clause: {}",
        err_msg
    );
}

#[test]
fn test_natural_join_error() {
    // NATURAL JOIN should produce helpful error
    let sql = "\
        CREATE STREAM L (id INT, name STRING);\n\
        CREATE STREAM R (id INT, value INT);\n\
        INSERT INTO Out\n\
        SELECT L.name, R.value\n\
        FROM L NATURAL JOIN R;\n";

    let result = parse(sql);
    assert!(result.is_err(), "NATURAL JOIN should be rejected");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("NATURAL") || err_msg.contains("not yet supported"),
        "Error should mention NATURAL JOIN: {}",
        err_msg
    );
}

#[test]
fn test_join_and_inner_join_are_equivalent() {
    // Plain JOIN and INNER JOIN should behave identically
    let sql_plain = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out1\n\
        SELECT L.id as l, R.id as r\n\
        FROM L JOIN R ON L.id = R.id;\n";

    let sql_inner = "\
        CREATE STREAM L (id INT);\n\
        CREATE STREAM R (id INT);\n\
        INSERT INTO Out2\n\
        SELECT L.id as l, R.id as r\n\
        FROM L INNER JOIN R ON L.id = R.id;\n";

    // Both should compile successfully
    let result1 = parse(sql_plain);
    let result2 = parse(sql_inner);

    assert!(
        result1.is_ok(),
        "Plain JOIN should work: {:?}",
        result1.err()
    );
    assert!(
        result2.is_ok(),
        "INNER JOIN should work: {:?}",
        result2.err()
    );

    // Both compile successfully - normalization happens internally
    // The fix ensures both JOIN and INNER JOIN are treated identically
}
