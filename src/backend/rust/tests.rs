use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType};

fn pg() -> RustCodegen {
    RustCodegen { target: RustTarget::Postgres }
}
fn sqlite() -> RustCodegen {
    RustCodegen { target: RustTarget::Sqlite }
}
fn mysql() -> RustCodegen {
    RustCodegen { target: RustTarget::Mysql }
}

// ─── generate: struct file ──────────────────────────────────────────────

#[test]
fn test_generate_table_struct() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "user.rs");
    assert!(src.contains("#[derive(Debug, sqlx::FromRow)]"));
    assert!(src.contains("pub struct User {"));
    assert!(src.contains("pub id: i64,"));
    assert!(src.contains("pub name: String,"));
    assert!(src.contains("pub bio: Option<String>,"));
}

#[test]
fn test_generate_mod_file() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "mod.rs");
    assert!(src.contains("pub mod user;"));
    assert!(src.contains("pub mod queries;"));
}

// ─── generate: pool type ────────────────────────────────────────────────

#[test]
fn test_generate_postgres_uses_pg_pool() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use sqlx::PgPool;"));
    assert!(src.contains("pool: &PgPool"));
}

#[test]
fn test_generate_sqlite_uses_sqlite_pool() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use sqlx::SqlitePool;"));
    assert!(src.contains("pool: &SqlitePool"));
}

// ─── generate: query commands ───────────────────────────────────────────

#[test]
fn test_generate_exec_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn delete_user(pool: &PgPool, id: i64) -> Result<(), sqlx::Error>"));
    assert!(src.contains(".execute(pool)"));
    assert!(src.contains(".map(|_| ())"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn delete_users(pool: &PgPool, active: bool) -> Result<u64, sqlx::Error>"));
    assert!(src.contains(".map(|r| r.rows_affected())"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn get_user(pool: &PgPool, id: i64) -> Result<Option<User>, sqlx::Error>"));
    assert!(src.contains(".fetch_optional(pool)"));
}

#[test]
fn test_generate_many_query_infers_table_return_type() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::many(
        "ListUsers",
        "SELECT id, name, bio FROM user",
        vec![],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn list_users(pool: &PgPool) -> Result<Vec<User>, sqlx::Error>"));
    assert!(src.contains(".fetch_all(pool)"));
}

// ─── generate: inline row struct ────────────────────────────────────────

#[test]
fn test_generate_inline_row_struct_for_partial_result() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub struct GetUserNameRow {"));
    assert!(src.contains("Result<Option<GetUserNameRow>, sqlx::Error>"));
}

// ─── generate: list params ──────────────────────────────────────────────

#[test]
fn test_generate_pg_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("= ANY($1)"), "PG native should rewrite to ANY");
    assert!(!src.contains("IN ($1)"), "original IN clause should be gone");
}

#[test]
fn test_generate_pg_dynamic_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("placeholders"), "dynamic mode builds placeholders at runtime");
    assert!(src.contains("for v in ids"), "dynamic mode binds each element");
}

#[test]
fn test_generate_sqlite_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
    let files = sqlite().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("json_each"), "SQLite native uses json_each");
    assert!(src.contains("ids_json"), "should bind the json local variable");
    assert!(!src.contains("serde_json"), "must not require serde_json");
}

#[test]
fn test_generate_mysql_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
    let files = mysql().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("JSON_TABLE"), "MySQL native uses JSON_TABLE");
    assert!(src.contains("ids_json"), "should bind the json local variable");
    assert!(!src.contains("serde_json"), "must not require serde_json");
}

#[test]
fn test_generate_mysql_dynamic_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(ListParamStrategy::Dynamic) };
    let files = mysql().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("placeholders"), "dynamic strategy builds placeholders");
    assert!(!src.contains("JSON_TABLE"), "dynamic strategy does not use JSON_TABLE");
}

// ─── generate: nullable params ───────────────────────────────────────────

#[test]
fn test_generate_nullable_param_pg() {
    // Nullable param → `Option<T>` in function signature; sqlx handles binding.
    let schema = Schema { tables: vec![] };
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
    let schema = Schema { tables: vec![] };
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
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    // ?1 should be rewritten to ? for sqlx sqlite
    assert!(src.contains("\"DELETE FROM user WHERE id = ?\""));
}

// ─── rust_type mapping ───────────────────────────────────────────────────

#[test]
fn test_rust_type_primitives_non_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Boolean, false, t), "bool");
    assert_eq!(rust_type(&SqlType::SmallInt, false, t), "i16");
    assert_eq!(rust_type(&SqlType::Integer, false, t), "i32");
    assert_eq!(rust_type(&SqlType::BigInt, false, t), "i64");
    assert_eq!(rust_type(&SqlType::Real, false, t), "f32");
    assert_eq!(rust_type(&SqlType::Double, false, t), "f64");
}

#[test]
fn test_rust_type_primitives_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Boolean, true, t), "Option<bool>");
    assert_eq!(rust_type(&SqlType::BigInt, true, t), "Option<i64>");
    assert_eq!(rust_type(&SqlType::Double, true, t), "Option<f64>");
}

#[test]
fn test_rust_type_text_types() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Text, false, t), "String");
    assert_eq!(rust_type(&SqlType::Char(Some(10)), false, t), "String");
    assert_eq!(rust_type(&SqlType::VarChar(Some(255)), false, t), "String");
    assert_eq!(rust_type(&SqlType::Text, true, t), "Option<String>");
}

#[test]
fn test_rust_type_temporal() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Date, false, t), "time::Date");
    assert_eq!(rust_type(&SqlType::Time, false, t), "time::Time");
    assert_eq!(rust_type(&SqlType::Timestamp, false, t), "time::PrimitiveDateTime");
    assert_eq!(rust_type(&SqlType::TimestampTz, false, t), "time::OffsetDateTime");
}

#[test]
fn test_rust_type_uuid_and_json() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Uuid, false, t), "uuid::Uuid");
    assert_eq!(rust_type(&SqlType::Json, false, t), "serde_json::Value");
    assert_eq!(rust_type(&SqlType::Custom("geometry".to_string()), false, t), "serde_json::Value");
}

#[test]
fn test_rust_type_decimal_sqlite_vs_pg() {
    // SQLite stores DECIMAL as REAL; PG/MySQL use rust_decimal::Decimal
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Sqlite), "f64");
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Postgres), "rust_decimal::Decimal");
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Mysql), "rust_decimal::Decimal");
}

#[test]
fn test_rust_type_array_non_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), false, t), "Vec<i64>");
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::Text)), false, t), "Vec<String>");
}

#[test]
fn test_rust_type_array_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), true, t), "Option<Vec<i64>>");
}

// ─── generate: MySQL and SQLite targets ──────────────────────────────────

#[test]
fn test_generate_mysql_exec_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("MySqlPool"), "MySQL backend uses MySqlPool");
    // MySQL rewrites $1 → ? (JDBC style)
    assert!(src.contains("\"DELETE FROM user WHERE id = ?\""));
    assert!(src.contains("pub async fn delete_user"));
}

#[test]
fn test_generate_sqlite_one_query() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("SqlitePool"), "SQLite backend uses SqlitePool");
    assert!(src.contains("Result<Option<User>, sqlx::Error>"), "One returns Option");
    assert!(src.contains(".fetch_optional(pool)"));
}

#[test]
fn test_generate_mysql_one_query_returns_option() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("Result<Option<User>, sqlx::Error>"), "One returns Option");
    assert!(src.contains(".fetch_optional(pool)"));
}

// ─── generate: SQL embedding ─────────────────────────────────────────────

#[test]
fn test_generate_sql_is_inlined_not_constant() {
    // Rust backend inlines SQL directly into sqlx::query(). It does NOT emit
    // a named SQL constant (that is a JDBC backend pattern).
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    // SQL is inlined as a string literal in the sqlx call
    assert!(src.contains("\"DELETE FROM user WHERE id = $1\""), "SQL should be inlined");
    // No separate const for the SQL
    assert!(!src.contains("GET_USER_BY_ID"), "Rust does not emit SQL constants");
}

// ─── generate: repeated param ────────────────────────────────────────────

#[test]
fn test_generate_repeated_param_pg_binds_once_per_unique_param() {
    // Postgres uses $N reference-by-index, so sqlx only needs one .bind(genre)
    // even when $1 appears multiple times in the SQL.
    let schema = Schema { tables: vec![] };
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
    let schema = Schema { tables: vec![] };
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

// ─── generate: execrows ──────────────────────────────────────────────────

#[test]
fn test_generate_execrows_sqlite() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("Result<u64, sqlx::Error>"), "execrows returns u64");
    assert!(src.contains(".rows_affected()"), "execrows uses rows_affected");
}

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"users.rs"), "{names:?}");
    assert!(names.contains(&"posts.rs"), "{names:?}");
    assert!(names.contains(&"mod.rs"), "{names:?}");
    assert!(!names.contains(&"queries.rs"), "queries.rs must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "users.rs");
    let posts_src = get_file(&files, "posts.rs");
    assert!(users_src.contains("delete_user"), "users.rs must contain delete_user");
    assert!(!users_src.contains("delete_post"), "users.rs must not contain delete_post");
    assert!(posts_src.contains("delete_post"), "posts.rs must contain delete_post");
    assert!(!posts_src.contains("delete_user"), "posts.rs must not contain delete_user");
}

#[test]
fn test_generate_grouped_mod_lists_all_groups() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let mod_src = get_file(&files, "mod.rs");
    assert!(mod_src.contains("pub mod users"), "mod.rs must declare pub mod users");
    assert!(mod_src.contains("pub mod posts"), "mod.rs must declare pub mod posts");
}
