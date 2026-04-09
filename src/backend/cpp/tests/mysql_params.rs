use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query};

#[test]
fn test_mysql_multiple_params_in_signature() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateUser",
        "UPDATE user SET name = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "name", SqlType::Text, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::string& name"));
    assert!(src.contains("std::int64_t id"));
}

#[test]
fn test_mysql_nullable_param_uses_optional_in_signature() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::optional<std::string>& bio"));
    assert!(src.contains("std::int64_t id"));
    assert!(!src.contains("const std::optional<std::int64_t>& id"));
}

#[test]
fn test_mysql_repeated_param_bound_to_multiple_slots_with_single_declaration() {
    let schema = Schema::default();
    let query = Query::many(
        "ListByGenreOrAll",
        "SELECT id FROM t WHERE ?1 = 'all' OR genre = ?1",
        vec![Parameter::scalar(1, "genre", SqlType::Text, false)],
        vec![crate::ir::ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    // Same param backing two bind slots: buffer assignment appears per slot...
    assert!(src.contains("bind[0].buffer = const_cast<char*>(genre.c_str());"));
    assert!(src.contains("bind[1].buffer = const_cast<char*>(genre.c_str());"));
    // ...but helper locals are declared exactly once.
    let len_decls = src.matches("unsigned long p_genre_len = genre.size();").count();
    assert_eq!(len_decls, 1, "expected genre length local declared once, got {len_decls}\n{src}");
}
