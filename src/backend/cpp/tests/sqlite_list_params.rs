use super::*;
use crate::backend::Codegen;
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn};

#[test]
fn test_sqlite_list_param_in_function_signature() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::vector<std::int64_t>& ids"));
}

#[test]
fn test_sqlite_list_param_native_sql_in_header_constant() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)
            .with_native_list("SELECT id FROM t WHERE id IN (SELECT value FROM json_each(?1))", NativeListBind::Json)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("json_each(?1)"));
    assert!(!src.contains("IN (?1)"));
}

#[test]
fn test_sqlite_list_param_binds_via_json_text() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::string ids_json = \"[\";"));
    assert!(src.contains("sqlite3_bind_text(stmt, 1, ids_json.c_str(), -1, SQLITE_TRANSIENT);"));
}

#[test]
fn test_sqlite_scalar_then_list_param_order_is_preserved() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = ?1 AND id IN (?2)",
        vec![
            Parameter::scalar(1, "active", SqlType::Boolean, false),
            Parameter::list(2, "ids", SqlType::BigInt, false)
                .with_native_list("SELECT id FROM t WHERE active = ?1 AND id IN (SELECT value FROM json_each(?2))", NativeListBind::Json),
        ],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let scalar_pos = src.find("sqlite3_bind_int(stmt, 1, active);").unwrap();
    let list_pos = src.find("sqlite3_bind_text(stmt, 2, ids_json.c_str(), -1, SQLITE_TRANSIENT);").unwrap();
    assert!(scalar_pos < list_pos, "scalar bind should precede list bind\n{src}");
}
