use super::*;

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
