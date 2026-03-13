use super::*;

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
