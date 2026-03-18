use super::*;

// ─── generate: struct file ──────────────────────────────────────────────

#[test]
fn test_generate_table_struct() {
    let schema = Schema::with_tables(vec![user_table()]);
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
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "mod.rs");
    assert!(src.contains("pub mod _sqltgen;"));
    assert!(src.contains("pub mod user;"));
    assert!(src.contains("pub mod queries;"));
}

// ─── generate: pool type ────────────────────────────────────────────────

#[test]
fn test_generate_postgres_uses_pg_pool() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let helper = get_file(&files, "_sqltgen.rs");
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use super::_sqltgen::DbPool;"));
    assert!(helper.contains("pub type DbPool = sqlx::PgPool;"));
    assert!(src.contains("pool: &DbPool"));
}

#[test]
fn test_generate_sqlite_uses_sqlite_pool() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let helper = get_file(&files, "_sqltgen.rs");
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use super::_sqltgen::DbPool;"));
    assert!(helper.contains("pub type DbPool = sqlx::SqlitePool;"));
    assert!(src.contains("pool: &DbPool"));
}

// ─── generate: query commands ───────────────────────────────────────────

#[test]
fn test_generate_exec_query() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn delete_user(pool: &DbPool, id: i64) -> Result<(), sqlx::Error>"));
    assert!(src.contains(".execute(pool)"));
    assert!(src.contains(".map(|_| ())"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn delete_users(pool: &DbPool, active: bool) -> Result<u64, sqlx::Error>"));
    assert!(src.contains(".map(|r| r.rows_affected())"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn get_user(pool: &DbPool, id: i64) -> Result<Option<User>, sqlx::Error>"));
    assert!(src.contains(".fetch_optional(pool)"));
}

#[test]
fn test_generate_many_query_infers_table_return_type() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many(
        "ListUsers",
        "SELECT id, name, bio FROM user",
        vec![],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub async fn list_users(pool: &DbPool) -> Result<Vec<User>, sqlx::Error>"));
    assert!(src.contains(".fetch_all(pool)"));
}

// ─── generate: inline row struct ────────────────────────────────────────

#[test]
fn test_generate_inline_row_struct_for_partial_result() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub struct GetUserNameRow {"));
    assert!(src.contains("Result<Option<GetUserNameRow>, sqlx::Error>"));
}

// ─── generate: MySQL and SQLite targets ──────────────────────────────────

#[test]
fn test_generate_mysql_exec_query() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let helper = get_file(&files, "_sqltgen.rs");
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use super::_sqltgen::DbPool;"));
    assert!(helper.contains("pub type DbPool = sqlx::MySqlPool;"));
    // MySQL rewrites $1 → ? (JDBC style); SQL is in a raw string binding
    assert!(src.contains("DELETE FROM user WHERE id = ?"));
    assert!(src.contains("pub async fn delete_user"));
}

#[test]
fn test_generate_sqlite_one_query() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let helper = get_file(&files, "_sqltgen.rs");
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("use super::_sqltgen::DbPool;"));
    assert!(helper.contains("pub type DbPool = sqlx::SqlitePool;"), "SQLite helper aliases SqlitePool");
    assert!(src.contains("Result<Option<User>, sqlx::Error>"), "One returns Option");
    assert!(src.contains(".fetch_optional(pool)"));
}

#[test]
fn test_generate_mysql_one_query_returns_option() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("Result<Option<User>, sqlx::Error>"), "One returns Option");
    assert!(src.contains(".fetch_optional(pool)"));
}

#[test]
fn test_generate_querier_wrapper_is_emitted() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("pub struct Querier<'a> {"));
    assert!(src.contains("pub fn new(pool: &'a DbPool) -> Self"));
    assert!(src.contains("pub async fn delete_user(&self, id: i64) -> Result<(), sqlx::Error>"));
    assert!(src.contains("delete_user(self.pool, id).await"));
}

// ─── generate: SQL embedding ─────────────────────────────────────────────

#[test]
fn test_generate_sql_is_local_binding_not_constant() {
    // Rust backend emits SQL as a local `let sql = r##"..."##` binding. It does NOT
    // emit a named SQL constant (that is a JDBC backend pattern).
    let schema = Schema::default();
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    // SQL is in a raw-string local binding
    assert!(src.contains("let sql = r##\""), "SQL should be in a raw string binding");
    assert!(src.contains("DELETE FROM user WHERE id = $1"), "SQL text should be present");
    assert!(src.contains("sqlx::query(sql)"), "sqlx call should reference the local binding");
    // No separate const for the SQL
    assert!(!src.contains("GET_USER_BY_ID"), "Rust does not emit SQL constants");
}

#[test]
fn test_generate_sql_raw_string_keeps_double_quotes_unescaped() {
    let schema = Schema::default();
    let query = Query::one(
        "GetQuotedUser",
        "SELECT \"name\" FROM \"user\" WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("let sql = r##\""), "SQL should be emitted as a raw string");
    assert!(src.contains("SELECT \"name\" FROM \"user\" WHERE id = $1"), "double quotes should remain unescaped in SQL");
    assert!(!src.contains("SELECT \\\"name\\\" FROM \\\"user\\\" WHERE id = $1"), "raw SQL should not contain escaped double quotes");
}

// ─── generate: execrows ──────────────────────────────────────────────────

#[test]
fn test_generate_execrows_sqlite() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("Result<u64, sqlx::Error>"), "execrows returns u64");
    assert!(src.contains(".rows_affected()"), "execrows uses rows_affected");
}
