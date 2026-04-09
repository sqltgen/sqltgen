use super::*;
use crate::backend::Codegen;
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn};

#[test]
fn test_mysql_list_param_in_function_signature() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::vector<std::int64_t>& ids"));
}

#[test]
fn test_mysql_list_param_native_sql_in_header_constant() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)
            .with_native_list("SELECT id FROM t WHERE id IN (SELECT value FROM JSON_TABLE(?, '$[*]' COLUMNS(value BIGINT PATH '$')) jt)", NativeListBind::Json)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("JSON_TABLE"));
    assert!(!src.contains("IN (?1)"));
    assert!(!src.contains("IN (?)"));
}

#[test]
fn test_mysql_list_param_serializes_to_json_blob() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::string p_ids_json = \"[\";"));
    assert!(src.contains("for (size_t i = 0; i < ids.size(); ++i) {"));
    assert!(src.contains("if (i > 0) p_ids_json += \",\";"));
    assert!(src.contains("p_ids_json += \"]\";"));
}

#[test]
fn test_mysql_list_param_numeric_uses_to_string() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("p_ids_json += std::to_string(ids[i]);"));
}

#[test]
fn test_mysql_list_param_text_uses_json_escape() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByNames",
        "SELECT id FROM t WHERE name IN (?1)",
        vec![Parameter::list(1, "names", SqlType::Text, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("p_names_json += json_escape(names[i]);"));
}

#[test]
fn test_mysql_list_param_bind_block_uses_string_type() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(p_ids_json.c_str());"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_STRING;"));
    assert!(src.contains("unsigned long p_ids_json_len = p_ids_json.size();"));
    assert!(src.contains("bind[0].length = &p_ids_json_len;"));
    assert!(src.contains("bind[0].buffer_length = p_ids_json_len;"));
}

#[test]
fn test_mysql_scalar_then_list_param_preserves_order() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = ?1 AND id IN (?2)",
        vec![
            Parameter::scalar(1, "active", SqlType::Boolean, false),
            Parameter::list(2, "ids", SqlType::BigInt, false),
        ],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let scalar_pos = src.find("bind[0].buffer = const_cast<bool*>(&active);").unwrap();
    let list_pos = src.find("bind[1].buffer = const_cast<char*>(p_ids_json.c_str());").unwrap();
    assert!(scalar_pos < list_pos, "scalar bind should precede list bind\n{src}");
}
