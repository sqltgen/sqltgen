use super::*;
use crate::backend::Codegen;
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn};

#[test]
fn test_postgres_list_param_in_function_signature() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::vector<std::int64_t>& ids"));
}

#[test]
fn test_postgres_list_param_native_sql_in_header_constant() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE id = ANY($1)", NativeListBind::Array)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("= ANY($1)"));
    assert!(!src.contains("IN ($1)"));
}

#[test]
fn test_postgres_list_param_binds_via_pqxx_params() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE id = ANY($1)", NativeListBind::Array)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("pqxx::params{ids}"));
}

#[test]
fn test_postgres_scalar_then_list_param_order_is_preserved() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![
            Parameter::scalar(1, "active", SqlType::Boolean, false),
            Parameter::list(2, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE active = $1 AND id = ANY($2)", NativeListBind::Array),
        ],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("pqxx::params{active, ids}"));
}
