use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query, ResultColumn};

// ─── exec query ──────────────────────────────────────────────────────────

#[test]
fn test_exec_query_function_decl() {
    let schema = Schema::default();
    let query = Query::exec(
        "DeleteUser",
        "DELETE FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("void delete_user(pqxx::connection& db, const std::int64_t& id);"));
}

#[test]
fn test_exec_query_sql_constant() {
    let schema = Schema::default();
    let query = Query::exec(
        "DeleteUser",
        "DELETE FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("SQL_DELETE_USER"), "should emit SQL constant");
    assert!(src.contains("DELETE FROM user WHERE id = $1"), "SQL text should be present");
}

// ─── execrows query ──────────────────────────────────────────────────────

#[test]
fn test_execrows_query_returns_int64() {
    let schema = Schema::default();
    let query = Query::exec_rows(
        "DeleteUsers",
        "DELETE FROM user WHERE active = $1",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("std::int64_t delete_users(pqxx::connection& db, const bool& active);"));
}

// ─── one query ───────────────────────────────────────────────────────────

#[test]
fn test_one_query_returns_optional_table_type() {
    let schema = Schema::with_tables(vec![user_table()]);
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
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("std::optional<User> get_user(pqxx::connection& db, const std::int64_t& id);"));
}

#[test]
fn test_one_query_includes_table_header() {
    let schema = Schema::with_tables(vec![user_table()]);
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
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include \"user.hpp\""), "should include table header");
}

// ─── many query ──────────────────────────────────────────────────────────

#[test]
fn test_many_query_returns_vector_of_table_type() {
    let schema = Schema::with_tables(vec![user_table()]);
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
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("std::vector<User> list_users(pqxx::connection& db);"));
}

// ─── inline row struct ───────────────────────────────────────────────────

#[test]
fn test_inline_row_struct_for_partial_result() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("struct GetUserNameRow {"), "should emit inline row struct");
    assert!(src.contains("std::string name;"), "row struct should have the column");
    assert!(src.contains("std::optional<GetUserNameRow> get_user_name("), "function should use the row struct");
}

// ─── pragma once and header comment ──────────────────────────────────────

#[test]
fn test_queries_header_has_pragma_once() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.starts_with("#pragma once\n"));
    assert!(src.contains("// Generated by sqltgen. Do not edit."));
}

// ─── db client include ───────────────────────────────────────────────────

#[test]
fn test_postgres_includes_pqxx() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <pqxx/pqxx>"));
}

#[test]
fn test_sqlite_includes_sqlite3() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <sqlite3.h>"));
    assert!(src.contains("sqlite3* db"), "SQLite uses sqlite3* connection type");
}

#[test]
fn test_mysql_includes_mysql_h() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <mysql/mysql.h>"));
    assert!(src.contains("MYSQL* db"), "MySQL uses MYSQL* connection type");
}

// ─── SQL rewriting for MySQL ─────────────────────────────────────────────

#[test]
fn test_mysql_rewrites_dollar_params_to_question_mark() {
    let schema = Schema::default();
    let query = Query::exec(
        "DeleteUser",
        "DELETE FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("DELETE FROM user WHERE id = ?"), "MySQL should rewrite $1 to ?");
    assert!(!src.contains("$1"), "MySQL should not contain $1");
}

#[test]
fn test_postgres_keeps_dollar_params() {
    let schema = Schema::default();
    let query = Query::exec(
        "DeleteUser",
        "DELETE FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("DELETE FROM user WHERE id = $1"), "Postgres keeps $1");
}

// ─── namespace ───────────────────────────────────────────────────────────

#[test]
fn test_queries_header_namespace() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let config = OutputConfig { out: "out".to_string(), package: "mydb".to_string(), list_params: None, ..Default::default() };
    let files = pg().generate(&schema, &[query], &config).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("namespace mydb {"));
    assert!(src.contains("} // namespace mydb"));
}

#[test]
fn test_queries_header_no_namespace_when_empty() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(!src.contains("namespace"));
}

// ─── no queries means no query file ─────────────────────────────────────

#[test]
fn test_no_queries_produces_no_query_file() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    assert!(files.iter().all(|f| f.path.extension().is_some_and(|ext| ext == "hpp")
        && f.path.file_stem().is_some_and(|s| s != "queries")),
        "should not produce queries.hpp when there are no queries");
}

// ─── multiple params ─────────────────────────────────────────────────────

#[test]
fn test_multiple_params_in_signature() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateUser",
        "UPDATE user SET name = $1 WHERE id = $2",
        vec![
            Parameter::scalar(1, "name", SqlType::Text, false),
            Parameter::scalar(2, "id", SqlType::BigInt, false),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::string& name"), "should have name param");
    assert!(src.contains("const std::int64_t& id"), "should have id param");
}

// ─── nullable param ──────────────────────────────────────────────────────

#[test]
fn test_nullable_param_uses_optional() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = $1 WHERE id = $2",
        vec![
            Parameter::scalar(1, "bio", SqlType::Text, true),
            Parameter::scalar(2, "id", SqlType::BigInt, false),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::optional<std::string>& bio"), "nullable param should be optional");
}

// ─── SQL raw string literal ──────────────────────────────────────────────

#[test]
fn test_sql_uses_raw_string_literal() {
    let schema = Schema::default();
    let query = Query::exec(
        "DeleteUser",
        "DELETE FROM \"user\" WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("R\"sql("), "should use raw string literal");
    assert!(src.contains("DELETE FROM \"user\" WHERE id = $1"), "double quotes should be unescaped inside raw string");
}
