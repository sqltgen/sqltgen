use super::*;

// ─── generate: repeated parameter binding ───────────────────────────────

#[test]
fn test_generate_repeated_param_expands_tuple() {
    // $1 appears 4 times, $2 once — tuple must have 5 entries
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "FindItems",
        "DELETE FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2",
        vec![Parameter::scalar(1, "accountId", SqlType::BigInt, false), Parameter::scalar(2, "inputData", SqlType::Text, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("(account_id, account_id, account_id, account_id, input_data)"));
}

// ─── generate: placeholder rewriting ────────────────────────────────────

#[test]
fn test_generate_postgres_rewrites_placeholders_to_percent_s() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("\"DELETE FROM user WHERE id = %s\""));
}

#[test]
fn test_generate_sqlite_rewrites_placeholders_to_question_mark() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("\"DELETE FROM user WHERE id = ?\""));
}

// ─── generate: nullable params ───────────────────────────────────────────

#[test]
fn test_generate_nullable_param_pg() {
    // Nullable param → `T | None` in function signature; Python passes None directly.
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("bio: str | None"), "nullable param should be str | None");
    assert!(src.contains("id: int"), "non-nullable param should be plain int");
    assert!(!src.contains("id: int | None"), "non-nullable param must not be Optional");
}

#[test]
fn test_generate_nullable_param_sqlite() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("bio: str | None"), "nullable param should be str | None");
    assert!(src.contains("id: int"), "non-nullable param should be plain int");
}

#[test]
fn test_generate_nullable_param_mysql() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("bio: str | None"), "nullable param should be str | None");
    assert!(src.contains("id: int"), "non-nullable param should be plain int");
}

#[test]
fn test_bug_b_sqlite_dynamic_scalar_after_in_binding_order() {
    // Bug B also affects the SQLite Dynamic branch which uses conn.execute.
    // This test fails until the root cause is fixed.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = sq().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // Bug: active precedes list elements in the execute args.
    assert!(!src.contains("(active,) + tuple(ids)"), "active must not precede list in args when it follows IN");
    // Fix: list elements come first, then active.
    assert!(src.contains("tuple(ids) + (active,)"), "list elements must precede the scalar-after in execute args");
}
