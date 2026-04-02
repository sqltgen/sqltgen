use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query, ResultColumn};

#[test]
fn test_queries_header_includes_pqxx() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <pqxx/pqxx>"));
    assert!(src.contains("void delete_user(pqxx::connection& db, const std::int64_t& id);"));
}

#[test]
fn test_exec_query_uses_exec_with_params_and_no_rows() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("pqxx::work txn(db);"));
    assert!(src.contains("txn.exec(SQL_DELETE_USER, pqxx::params{id}).no_rows();"));
    assert!(src.contains("txn.commit();"));
    assert!(!src.contains("exec_params("));
}

#[test]
fn test_execrows_query_uses_affected_rows() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("auto affected = txn.exec(SQL_DELETE_USERS, pqxx::params{active}).affected_rows();"));
    assert!(src.contains("return static_cast<std::int64_t>(affected);"));
}

#[test]
fn test_one_query_uses_query01_not_manual_result_walking() {
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
    )
    .with_source_table(Some("user".to_string()));
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("txn.query01<std::int64_t, std::string, std::optional<std::string>>(SQL_GET_USER, pqxx::params{id})"));
    assert!(src.contains("if (!opt) return std::nullopt;"));
    assert!(!src.contains("r.empty()"));
    assert!(!src.contains("row[0].as<"));
}

#[test]
fn test_many_query_uses_query_not_manual_result_walking() {
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
    )
    .with_source_table(Some("user".to_string()));
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("for (auto [id_, name_, bio_] : txn.query<std::int64_t, std::string, std::optional<std::string>>(SQL_LIST_USERS)) {"));
    assert!(src.contains("rows.push_back(User{std::move(id_), std::move(name_), std::move(bio_)});"));
    assert!(!src.contains("for (const auto& row : r)"));
    assert!(!src.contains("[f0, f1, f2]"));
}
