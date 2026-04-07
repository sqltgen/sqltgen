use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query};

#[test]
fn test_sqlite_multiple_params_in_signature() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateUser",
        "UPDATE user SET name = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "name", SqlType::Text, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::string& name"));
    assert!(src.contains("std::int64_t id"));
}

#[test]
fn test_sqlite_nullable_param_uses_optional_in_signature() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("const std::optional<std::string>& bio"));
    assert!(src.contains("std::int64_t id"));
    assert!(!src.contains("const std::optional<std::int64_t>& id"));
}

#[test]
fn test_repeated_sqlite_param_is_bound_once_using_numbered_placeholder() {
    let schema = Schema::default();
    let query = Query::many(
        "ListByGenreOrAll",
        "SELECT id FROM t WHERE ?1 = 'all' OR genre = ?1",
        vec![Parameter::scalar(1, "genre", SqlType::Text, false)],
        vec![crate::ir::ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let count = src.matches("sqlite3_bind_text(stmt, 1, genre.c_str(), -1, SQLITE_TRANSIENT);").count();
    assert_eq!(count, 1, "expected one sqlite bind for repeated ?1 placeholder, got {count}\n{src}");
}
