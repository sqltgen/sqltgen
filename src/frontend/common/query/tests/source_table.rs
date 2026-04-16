use super::*;

/// `SELECT * FROM users` — single table, bare wildcard → source = users.
#[test]
fn test_source_table_bare_star_single_table() {
    let sql = "-- name: ListUsers :many\nSELECT * FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// `SELECT u.* FROM users u` — qualified wildcard → source = users.
#[test]
fn test_source_table_qualified_star() {
    let sql = "-- name: ListUsers :many\nSELECT u.* FROM users u;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// `SELECT u.* FROM users u INNER JOIN posts p ON u.id = p.user_id` —
/// qualified wildcard over non-nullable side → source = users.
#[test]
fn test_source_table_qualified_star_with_join() {
    let sql = "-- name: ListUsers :many\nSELECT u.* FROM users u INNER JOIN posts p ON u.id = p.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// `SELECT * FROM (SELECT * FROM users) AS sub` — derived table wrapping a
/// wildcard select → source must resolve to "users".
#[test]
fn test_source_table_derived_table() {
    let sql = "-- name: GetUser :one\nSELECT * FROM (SELECT * FROM users) AS sub;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// `WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp` — single CTE
/// whose body is a wildcard select → source must resolve to "users".
#[test]
fn test_source_table_cte() {
    let sql = "-- name: GetUser :one\nWITH tmp AS (SELECT * FROM users) SELECT * FROM tmp;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// `WITH a AS (SELECT * FROM users), b AS (SELECT * FROM a) SELECT * FROM b` —
/// chained CTEs where each wildcards the previous → source must resolve to "users".
#[test]
fn test_source_table_chained_ctes() {
    let sql = "-- name: GetUser :one\nWITH a AS (SELECT * FROM users), b AS (SELECT * FROM a) SELECT * FROM b;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// Three levels of nested derived tables all selecting `*` → source must
/// resolve to "users".
#[test]
fn test_source_table_triple_nested_derived() {
    let sql = "-- name: GetUser :one\nSELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users) AS a) AS b) AS c;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.source.as_ref().map(|s| s.name.as_str()), Some("users"));
}

/// CTE whose body selects explicit columns (not `*`) → the inner select has
/// no single source, so the outer wildcard cannot be resolved → None.
#[test]
fn test_source_table_cte_non_wildcard_inner() {
    let sql = "-- name: GetUser :one\nWITH tmp AS (SELECT id, name FROM users) SELECT * FROM tmp;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert!(q.source.is_none());
}

/// `SELECT u.* FROM posts p LEFT JOIN users u ON u.id = p.user_id` —
/// u is on the nullable side of the LEFT JOIN → source must be None.
#[test]
fn test_source_table_none_for_nullable_side_of_left_join() {
    let sql = "-- name: GetUser :one\nSELECT u.* FROM posts p LEFT JOIN users u ON u.id = p.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert!(q.source.is_none());
}

/// `SELECT id, name FROM users` — explicit column list → source must be None.
#[test]
fn test_source_table_none_for_explicit_column_list() {
    let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert!(q.source.is_none());
}

/// `SELECT * FROM users u INNER JOIN posts p ON …` — bare `*` with multiple
/// tables in scope → source must be None (ambiguous).
#[test]
fn test_source_table_none_for_bare_star_multiple_tables() {
    let sql = "-- name: GetAll :many\nSELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert!(q.source.is_none());
}

/// CTE as the final source → source must be None (CTE is not a schema table).
#[test]
fn test_source_table_none_for_cte_source() {
    let sql = "-- name: GetUser :one\nWITH cte AS (SELECT id, name FROM users WHERE id = $1)\nSELECT * FROM cte;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert!(q.source.is_none());
}
