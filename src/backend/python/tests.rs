use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, Parameter, Query, ResultColumn, Schema, SqlType, Table};

fn pg() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Postgres }
}
fn sq() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Sqlite }
}
fn my() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Mysql }
}

// ─── generate: dataclass file ───────────────────────────────────────────

#[test]
fn test_generate_table_dataclass() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "user.py");
    assert!(src.contains("@dataclasses.dataclass"));
    assert!(src.contains("class User:"));
    assert!(src.contains("id: int"));
    assert!(src.contains("name: str"));
    assert!(src.contains("bio: str | None"));
}

#[test]
fn test_generate_init_file_exports_tables_and_queries() {
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
    let src = get_file(&files, "__init__.py");
    assert!(src.contains("from .user import User"));
    assert!(src.contains("from . import queries"));
}

// ─── generate: driver import ────────────────────────────────────────────

#[test]
fn test_generate_postgres_imports_psycopg() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("import psycopg"));
    assert!(!src.contains("import sqlite3"));
}

#[test]
fn test_generate_sqlite_imports_sqlite3() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("import sqlite3"));
    assert!(!src.contains("import psycopg"));
}

// ─── generate: SQL constant name ────────────────────────────────────────

#[test]
fn test_generate_sql_const_name_is_screaming_snake_case() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("SQL_GET_USER_BY_ID"));
}

// ─── generate: query commands (psycopg) ─────────────────────────────────

#[test]
fn test_generate_psycopg_exec_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def delete_user(conn: psycopg.Connection, id: int) -> None:"));
    assert!(src.contains("with conn.cursor() as cur:"));
    assert!(src.contains("cur.execute(SQL_DELETE_USER, (id,))"));
}

#[test]
fn test_generate_psycopg_one_query_infers_table_return_type() {
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
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def get_user(conn: psycopg.Connection, id: int) -> User | None:"));
    assert!(src.contains("row = cur.fetchone()"));
    assert!(src.contains("return User(*row)"));
}

#[test]
fn test_generate_psycopg_many_query_infers_table_return_type() {
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
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def list_users(conn: psycopg.Connection) -> list[User]:"));
    assert!(src.contains("return [User(*row) for row in cur.fetchall()]"));
}

#[test]
fn test_generate_psycopg_execrows_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def delete_users(conn: psycopg.Connection, active: bool) -> int:"));
    assert!(src.contains("return cur.rowcount"));
}

// ─── generate: query commands (sqlite3) ─────────────────────────────────

#[test]
fn test_generate_sqlite_exec_query_uses_conn_execute() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def delete_user(conn: sqlite3.Connection, id: int) -> None:"));
    // sqlite3 uses conn.execute() directly — no cursor context manager
    assert!(src.contains("conn.execute(SQL_DELETE_USER, (id,))"));
    assert!(!src.contains("with conn.cursor()"));
}

#[test]
fn test_generate_sqlite_one_query_infers_table_return_type() {
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
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def get_user(conn: sqlite3.Connection, id: int) -> User | None:"));
    assert!(src.contains("row = conn.execute(SQL_GET_USER, (id,)).fetchone()"));
    assert!(src.contains("return User(*row)"));
}

// ─── generate: inline row dataclass ─────────────────────────────────────

#[test]
fn test_generate_inline_row_dataclass_for_partial_result() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("class GetUserNameRow:"));
    assert!(src.contains("GetUserNameRow | None"));
}

// ─── generate: mysql target ─────────────────────────────────────────────

#[test]
fn test_generate_mysql_imports_connector() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("import mysql.connector"));
    assert!(!src.contains("import psycopg"));
}

#[test]
fn test_generate_mysql_uses_mysql_connection_type() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("conn: mysql.connector.MySQLConnection"));
}

#[test]
fn test_generate_mysql_uses_cursor_context_manager() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("with conn.cursor() as cur:"));
}

#[test]
fn test_generate_mysql_rewrites_placeholders_to_percent_s() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("\"DELETE FROM user WHERE id = %s\""));
}

#[test]
fn test_generate_mysql_one_query() {
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
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("def get_user(conn: mysql.connector.MySQLConnection, id: int) -> User | None:"));
    assert!(src.contains("row = cur.fetchone()"));
    assert!(src.contains("return User(*row)"));
}

// ─── generate: JSON type mapping ────────────────────────────────────────

#[test]
fn test_python_type_json_postgres_is_object() {
    assert_eq!(python_type(&SqlType::Json, false, &PythonTarget::Postgres), "object");
    assert_eq!(python_type(&SqlType::Jsonb, false, &PythonTarget::Postgres), "object");
    assert_eq!(python_type(&SqlType::Json, true, &PythonTarget::Postgres), "object | None");
}

#[test]
fn test_python_type_json_sqlite_is_str() {
    assert_eq!(python_type(&SqlType::Json, false, &PythonTarget::Sqlite), "str");
    assert_eq!(python_type(&SqlType::Json, true, &PythonTarget::Sqlite), "str | None");
}

#[test]
fn test_python_type_json_mysql_is_str() {
    assert_eq!(python_type(&SqlType::Json, false, &PythonTarget::Mysql), "str");
    assert_eq!(python_type(&SqlType::Jsonb, false, &PythonTarget::Mysql), "str");
}

#[test]
fn test_generate_postgres_json_column_no_any_import() {
    let schema = Schema {
        tables: vec![Table {
            name: "doc".to_string(),
            columns: vec![Column { name: "data".to_string(), sql_type: SqlType::Json, nullable: false, is_primary_key: false }],
        }],
    };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "doc.py");
    assert!(src.contains("data: object"));
    assert!(!src.contains("from typing import Any"));
}

#[test]
fn test_generate_sqlite_json_column_no_any_import() {
    let schema = Schema {
        tables: vec![Table {
            name: "doc".to_string(),
            columns: vec![Column { name: "data".to_string(), sql_type: SqlType::Json, nullable: false, is_primary_key: false }],
        }],
    };
    let files = sq().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "doc.py");
    assert!(src.contains("data: str"));
    assert!(!src.contains("from typing import Any"));
}

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
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("= ANY(%s)"), "PG native should rewrite IN to = ANY");
    // psycopg3 accepts a Python list directly — no json.dumps needed.
    assert!(!src.contains("json.dumps"), "PG native must not serialise to JSON");
    assert!(src.contains("(ids,)"), "list is passed directly as the bound value");
}

#[test]
fn test_generate_pg_native_list_param_with_scalar() {
    // Scalar before list param: args tuple order must mirror SQL order.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("active: bool"), "scalar param before list");
    assert!(src.contains("ids: list[int]"), "list param after scalar");
    // active comes before ids in SQL order → (active, ids) tuple.
    assert!(src.contains("(active, ids)"), "args must be in SQL parameter order");
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
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("json_each"), "SQLite native uses json_each");
    assert!(src.contains("json.dumps(ids)"), "SQLite native serialises list to JSON");
    assert!(src.contains("ids_json"), "JSON variable must be named ids_json");
    // SQLite uses conn.execute, not a cursor context manager.
    assert!(src.contains("conn.execute("), "SQLite uses conn.execute directly");
    assert!(!src.contains("= ANY"), "SQLite must not use pg ANY syntax");
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
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("JSON_TABLE"), "MySQL native uses JSON_TABLE");
    assert!(src.contains("json.dumps(ids)"), "MySQL native serialises list to JSON");
    assert!(src.contains("ids_json"), "JSON variable must be named ids_json");
    // MySQL connector uses a cursor context manager, not conn.execute directly.
    assert!(src.contains("with conn.cursor() as cur:"), "MySQL uses cursor context manager");
    assert!(!src.contains("json_each"), "MySQL must not use SQLite json_each");
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
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // PG dynamic uses %s placeholders.
    assert!(src.contains(r#""%s""#), "PG dynamic must use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    // Dynamic strategy must NOT emit a SQL constant for this query.
    assert!(!src.contains("GET_BY_IDS"), "dynamic strategy must not emit a SQL constant");
}

#[test]
fn test_generate_pg_dynamic_list_param_with_scalar() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // active is before IN — must come first in args: (active,) + tuple(ids).
    assert!(src.contains("(active,) + tuple(ids)"), "scalar before IN must precede list in args");
}

#[test]
fn test_generate_sqlite_dynamic_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = sq().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // SQLite dynamic uses ? placeholders (not %s).
    assert!(src.contains(r#""?""#), "SQLite dynamic must use ? placeholder");
    assert!(!src.contains(r#""%s""#), "SQLite dynamic must not use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    // SQLite dynamic uses conn.execute, not a cursor context manager.
    assert!(src.contains("conn.execute(sql,"), "SQLite dynamic uses conn.execute");
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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = my().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // MySQL dynamic uses %s placeholders (same as PG).
    assert!(src.contains(r#""%s""#), "MySQL dynamic must use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    assert!(src.contains("with conn.cursor() as cur:"), "MySQL uses cursor context manager");
}

#[test]
fn test_generate_list_param_text_type() {
    // Text list params use list[str] — verify correct Python type annotation.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("tags: list[str]"), "Text list param should use list[str]");
}

#[test]
fn test_generate_list_param_execrows_cmd() {
    // List params work with :execrows — DELETE / UPDATE with IN clause.
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteByIds", "DELETE FROM t WHERE id IN ($1)", vec![Parameter::list(1, "ids", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("= ANY(%s)"), "PG native should rewrite IN to = ANY");
    assert!(src.contains("return cur.rowcount"), "execrows must return rowcount");
}

// ─── Bug B: dynamic strategy binds scalars at wrong position when scalar follows IN

#[test]
fn test_bug_b_postgres_dynamic_scalar_after_in_binding_order() {
    // Bug B: when a scalar param appears *after* the IN clause in the SQL, the
    // Dynamic strategy incorrectly places it *before* the list elements in the
    // execute args. Correct order: tuple(list) + (scalar,).
    // This test fails until the root cause is fixed.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // Bug: active precedes list elements in the execute args tuple.
    assert!(!src.contains("(active,) + tuple(ids)"), "active must not precede list in args when it follows IN");
    // Fix: list elements come first, then active.
    assert!(src.contains("tuple(ids) + (active,)"), "list elements must precede the scalar-after in args tuple");
}

#[test]
fn test_bug_b_postgres_dynamic_scalar_before_in_no_regression() {
    // When the scalar param appears *before* the IN clause, the current order
    // is correct. Confirm the fix preserves this common pattern.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // active is before IN in the SQL — must come first in the args tuple.
    assert!(src.contains("(active,) + tuple(ids)"), "scalar before IN must precede list in args tuple");
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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = sq().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // Bug: active precedes list elements in the execute args.
    assert!(!src.contains("(active,) + tuple(ids)"), "active must not precede list in args when it follows IN");
    // Fix: list elements come first, then active.
    assert!(src.contains("tuple(ids) + (active,)"), "list elements must precede the scalar-after in execute args");
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
    assert!(names.contains(&"users.py"), "{names:?}");
    assert!(names.contains(&"posts.py"), "{names:?}");
    assert!(names.contains(&"__init__.py"), "{names:?}");
    assert!(!names.contains(&"queries.py"), "queries.py must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "users.py");
    let posts_src = get_file(&files, "posts.py");
    assert!(users_src.contains("delete_user"), "users.py must contain delete_user");
    assert!(!users_src.contains("delete_post"), "users.py must not contain delete_post");
    assert!(posts_src.contains("delete_post"), "posts.py must contain delete_post");
    assert!(!posts_src.contains("delete_user"), "posts.py must not contain delete_user");
}

#[test]
fn test_generate_grouped_init_imports_all_groups() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let init_src = get_file(&files, "__init__.py");
    assert!(init_src.contains("from . import users"), "__init__.py must import users");
    assert!(init_src.contains("from . import posts"), "__init__.py must import posts");
}
