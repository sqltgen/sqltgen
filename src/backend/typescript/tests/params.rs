use super::*;

// ─── generate: nullable params ───────────────────────────────────────────

#[test]
fn test_nullable_param_pg_ts() {
    // Nullable param → `T | null` in function signature; pg passes null directly.
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
    assert!(!content.contains("id: number | null"), "non-nullable param must not be nullable");
}

#[test]
fn test_nullable_param_sqlite_ts() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
}

#[test]
fn test_nullable_param_mysql_ts() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("bio: string | null"), "nullable param should be string | null");
    assert!(content.contains("id: number"), "non-nullable param should be plain number");
}
