use super::*;

// ─── generate: repeated parameter binding ───────────────────────────────

#[test]
fn test_generate_repeated_param_emits_bind_per_occurrence() {
    // $1 appears 4 times, $2 once — must emit 5 bind calls in SQL order
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "FindItems",
        "SELECT * FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2",
        vec![Parameter::scalar(1, "accountId", SqlType::BigInt, false), Parameter::scalar(2, "inputData", SqlType::Text, false)],
        vec![],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ps.setLong(1, accountId)"));
    assert!(src.contains("ps.setLong(2, accountId)"));
    assert!(src.contains("ps.setLong(3, accountId)"));
    assert!(src.contains("ps.setLong(4, accountId)"));
    assert!(src.contains("ps.setString(5, inputData)"));
}

// ─── generate: parameter binding ────────────────────────────────────────

#[test]
fn test_generate_nullable_param_uses_set_object() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ps.setObject(1, bio)")); // nullable → setObject
    assert!(src.contains("ps.setLong(2, id)")); // non-nullable → typed setter
}
