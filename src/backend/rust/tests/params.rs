use super::*;

// ─── generate: nullable params ───────────────────────────────────────────

#[test]
fn test_generate_nullable_param_pg() {
    // Nullable param → `Option<T>` in function signature; sqlx handles binding.
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("bio: Option<String>"), "nullable param should be Option<String>");
    assert!(src.contains("id: i64"), "non-nullable param should be plain i64");
    assert!(!src.contains("id: Option<i64>"), "non-nullable param must not be wrapped in Option");
}

#[test]
fn test_generate_nullable_param_mysql() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE users SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("bio: Option<String>"), "nullable param should be Option<String>");
    assert!(src.contains("id: i64"), "non-nullable param should be plain i64");
}

// ─── generate: SQLite placeholder rewriting ─────────────────────────────

#[test]
fn test_generate_sqlite_rewrites_placeholders() {
    let schema = Schema::default();
    let query = Query::exec("GetUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    // ?1 should be rewritten to ? for sqlx sqlite; SQL is in a raw string binding
    assert!(src.contains("DELETE FROM user WHERE id = ?"));
}

// ─── generate: repeated param ────────────────────────────────────────────

#[test]
fn test_generate_repeated_param_pg_binds_once_per_unique_param() {
    // Postgres uses $N reference-by-index, so sqlx only needs one .bind(genre)
    // even when $1 appears multiple times in the SQL.
    let schema = Schema::default();
    let query = Query::many(
        "ListByGenreOrAll",
        "SELECT id FROM t WHERE $1 = 'all' OR genre = $1",
        vec![Parameter::scalar(1, "genre", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    let bind_count = src.matches(".bind(genre)").count();
    assert_eq!(bind_count, 1, "Postgres $N → one .bind() per unique param, got: {}", bind_count);
}

#[test]
fn test_generate_repeated_param_mysql_binds_per_occurrence() {
    // MySQL uses ? (positional-sequential), so each occurrence of $1 needs its
    // own .bind(). The first gets .clone() so the value is not moved early.
    let schema = Schema::default();
    let query = Query::many(
        "ListByGenreOrAll",
        "SELECT id FROM t WHERE $1 = 'all' OR genre = $1",
        vec![Parameter::scalar(1, "genre", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    // Two occurrences of $1 → two ? → two .bind() calls
    let bind_count = src.matches(".bind(genre").count();
    assert_eq!(bind_count, 2, "MySQL positional → two .bind() calls, got: {}", bind_count);
    // First occurrence must clone to avoid a move before the second use
    assert!(src.contains(".bind(genre.clone())"), "first bind must clone to avoid move");
}
