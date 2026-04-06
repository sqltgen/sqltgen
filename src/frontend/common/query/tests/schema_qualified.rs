use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{Column, Query, Schema, SqlType, Table};
use sqlparser::dialect::PostgreSqlDialect;

fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
}

fn parse_queries_with_default_schema(sql: &str, schema: &Schema, default_schema: &str) -> anyhow::Result<Vec<Query>> {
    let config = ResolverConfig { default_schema: Some(default_schema.to_string()), ..Default::default() };
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &config)
}

fn schema_with_qualified_tables() -> Schema {
    Schema::with_tables(vec![
        Table::with_schema("public", "users", vec![Column::new_primary_key("id", SqlType::Integer), Column::new_not_nullable("name", SqlType::Text)]),
        Table::with_schema("internal", "users", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("email", SqlType::Text)]),
    ])
}

// ─── Exact schema match (both sides qualified) ─────────────────────────────

#[test]
fn test_select_from_schema_qualified_table() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: GetUser :one\nSELECT id, name FROM public.users WHERE id = $1;";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].result_columns.len(), 2);
    assert_eq!(queries[0].result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn test_select_different_schema_same_table_name() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: GetInternalUser :one\nSELECT id, email FROM internal.users WHERE id = $1;";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries[0].result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(queries[0].result_columns[1].name, "email");
}

// ─── DML with schema-qualified tables ───────────────────────────────────────

#[test]
fn test_insert_schema_qualified() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: CreateUser :exec\nINSERT INTO public.users (id, name) VALUES ($1, $2);";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries[0].params.len(), 2);
    assert_eq!(queries[0].params[0].sql_type, SqlType::Integer);
    assert_eq!(queries[0].params[1].sql_type, SqlType::Text);
}

#[test]
fn test_update_schema_qualified() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: UpdateUser :exec\nUPDATE public.users SET name = $1 WHERE id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries[0].params[0].sql_type, SqlType::Text);
    assert_eq!(queries[0].params[1].sql_type, SqlType::Integer);
}

#[test]
fn test_delete_schema_qualified() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: DeleteUser :exec\nDELETE FROM public.users WHERE id = $1;";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries[0].params[0].sql_type, SqlType::Integer);
}

// ─── Cross-schema joins ─────────────────────────────────────────────────────

#[test]
fn test_join_across_schemas() {
    let schema = schema_with_qualified_tables();
    let sql = "-- name: JoinSchemas :many\nSELECT p.id, p.name, i.email FROM public.users p JOIN internal.users i ON p.id = i.id;";
    let queries = parse_queries(sql, &schema).unwrap();
    assert_eq!(queries[0].result_columns.len(), 3);
    assert_eq!(queries[0].result_columns[0].sql_type, SqlType::Integer); // public.users.id
    assert_eq!(queries[0].result_columns[2].sql_type, SqlType::Text); // internal.users.email
}

// ─── Default schema matching ────────────────────────────────────────────────

#[test]
fn test_unqualified_query_matches_default_schema_table() {
    let schema = Schema::with_tables(vec![Table::with_schema(
        "public",
        "users",
        vec![Column::new_primary_key("id", SqlType::Integer), Column::new_not_nullable("name", SqlType::Text)],
    )]);
    let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
    let queries = parse_queries_with_default_schema(sql, &schema, "public").unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].result_columns.len(), 2);
    assert_eq!(queries[0].result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn test_unqualified_query_no_match_non_default_schema() {
    let schema = Schema::with_tables(vec![Table::with_schema("internal", "users", vec![Column::new_primary_key("id", SqlType::Integer)])]);
    let sql = "-- name: GetUser :one\nSELECT id FROM users WHERE id = $1;";
    // default_schema is "public" but table is in "internal" → should not resolve
    let queries = parse_queries_with_default_schema(sql, &schema, "public").unwrap();
    assert_eq!(queries[0].params[0].sql_type, SqlType::Text); // unresolved fallback
}

#[test]
fn test_qualified_query_matches_unqualified_table_via_default_schema() {
    // Table stored without schema, query uses schema that matches the default
    let schema =
        Schema::with_tables(vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer), Column::new_not_nullable("name", SqlType::Text)])]);
    let sql = "-- name: GetUser :one\nSELECT id, name FROM public.users WHERE id = $1;";
    let queries = parse_queries_with_default_schema(sql, &schema, "public").unwrap();
    assert_eq!(queries[0].result_columns.len(), 2);
    assert_eq!(queries[0].result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn test_qualified_query_non_default_no_match_unqualified_table() {
    let schema = Schema::with_tables(vec![Table::new("users", vec![Column::new_primary_key("id", SqlType::Integer)])]);
    let sql = "-- name: GetUser :one\nSELECT id FROM internal.users WHERE id = $1;";
    // default_schema is "public" but query asks for "internal" → should not resolve
    let queries = parse_queries_with_default_schema(sql, &schema, "public").unwrap();
    assert_eq!(queries[0].params[0].sql_type, SqlType::Text); // unresolved fallback
}
