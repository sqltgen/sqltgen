use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query, ResultColumn};

// ─── basic sqlite shape ──────────────────────────────────────────────────

#[test]
fn test_sqlite_function_uses_sqlite3_connection_type() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("void delete_user(sqlite3* db, const std::int64_t& id) {"));
}

#[test]
fn test_sqlite_header_includes_sqlite3() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <sqlite3.h>"));
}

#[test]
fn test_sqlite_querier_uses_sqlite3_pointer() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("sqlite3* db_;"));
    assert!(src.contains("explicit Querier(sqlite3* db) : db_(db) {}"));
}

#[test]
fn test_sqlite_body_prepares_statement() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_stmt* stmt;"));
    assert!(src.contains("sqlite3_prepare_v2(db, SQL_DELETE_USER.c_str(), -1, &stmt, nullptr)"));
}

// ─── shared finalize / rc checks ────────────────────────────────────────

#[test]
fn test_sqlite_exec_finalizes_stmt() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_finalize(stmt);"));
}

#[test]
fn test_sqlite_execrows_finalizes_stmt() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_finalize(stmt);"));
}

#[test]
fn test_sqlite_one_finalizes_stmt() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_finalize(stmt);"));
}

#[test]
fn test_sqlite_many_finalizes_stmt() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_finalize(stmt);"));
}

#[test]
fn test_sqlite_exec_checks_sqlite_done() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("if (rc != SQLITE_DONE) {"));
    assert!(src.contains("throw std::runtime_error(sqlite3_errmsg(db));"));
}

#[test]
fn test_sqlite_execrows_checks_sqlite_done() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("if (rc != SQLITE_DONE) {"));
}

#[test]
fn test_sqlite_one_uses_row_collecting_loop() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
    assert!(src.contains("int rc;"));
    assert!(src.contains("while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {"));
}

#[test]
fn test_sqlite_many_uses_row_collecting_loop() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {"));
}

// ─── Exec ───────────────────────────────────────────────────────────────

#[test]
fn test_sqlite_exec_body_with_params() {
    let schema = Schema::default();
    let query = Query::exec(
        "AddSaleItem",
        "INSERT INTO sale_item (sale_id, book_id, quantity, unit_price) VALUES (?1, ?2, ?3, ?4)",
        vec![
            Parameter::scalar(1, "sale_id", SqlType::Integer, false),
            Parameter::scalar(2, "book_id", SqlType::Integer, false),
            Parameter::scalar(3, "quantity", SqlType::Integer, false),
            Parameter::scalar(4, "unit_price", SqlType::Decimal, false),
        ],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_int(stmt, 1, sale_id);"));
    assert!(src.contains("sqlite3_bind_int(stmt, 2, book_id);"));
    assert!(src.contains("sqlite3_bind_int(stmt, 3, quantity);"));
    assert!(src.contains("sqlite3_bind_text(stmt, 4, unit_price.c_str(), -1, SQLITE_TRANSIENT);"));
    assert!(src.contains("int rc = sqlite3_step(stmt);"));
}

#[test]
fn test_sqlite_exec_body_no_params() {
    let schema = Schema::default();
    let query = Query::exec("DeleteAll", "DELETE FROM user", vec![]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(!src.contains("sqlite3_bind_"), "no params should emit no bind calls");
    assert!(src.contains("int rc = sqlite3_step(stmt);"));
}

#[test]
fn test_sqlite_exec_body_binds_multiple_params() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateUser",
        "UPDATE user SET name = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "name", SqlType::Text, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);"));
    assert!(src.contains("sqlite3_bind_int64(stmt, 2, id);"));
}

// ─── ExecRows ───────────────────────────────────────────────────────────

#[test]
fn test_sqlite_execrows_uses_changes64() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteBookById", "DELETE FROM book WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::Integer, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::int64_t affected = sqlite3_changes64(db);"));
}

#[test]
fn test_sqlite_execrows_returns_affected() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteBookById", "DELETE FROM book WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::Integer, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return affected;"));
}

#[test]
fn test_sqlite_execrows_reads_changes_before_return() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteBookById", "DELETE FROM book WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::Integer, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let changes_pos = src.find("sqlite3_changes64(db)").unwrap();
    let finalize_pos = src.find("sqlite3_finalize(stmt);").unwrap();
    assert!(changes_pos < finalize_pos, "changes64 should be read before finalize");
}

// ─── One ────────────────────────────────────────────────────────────────

#[test]
fn test_sqlite_one_collects_rows_into_vector() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
}

#[test]
fn test_sqlite_one_checks_more_than_one_row() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("if (rows.size() > 1) {"));
    assert!(src.contains("throw std::runtime_error(\"query returned more than one row\");"));
}

#[test]
fn test_sqlite_one_returns_nullopt_when_empty() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return rows.empty() ? std::nullopt : std::optional<User>(std::move(rows[0]));"));
}

#[test]
fn test_sqlite_one_returns_optional_moved_first_row() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::optional<User>(std::move(rows[0]))"));
}

// ─── Many ───────────────────────────────────────────────────────────────

#[test]
fn test_sqlite_many_creates_vector() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
}

#[test]
fn test_sqlite_many_loops_over_sqlite_step() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {"));
}

#[test]
fn test_sqlite_many_pushes_row_type() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("rows.push_back(User{"));
}

#[test]
fn test_sqlite_many_returns_rows() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return rows;"));
}

// ─── bind type selection ─────────────────────────────────────────────────

#[test]
fn test_sqlite_bind_integer_types() {
    let schema = Schema::default();
    let query = Query::exec(
        "Ints",
        "SELECT ?1, ?2, ?3",
        vec![
            Parameter::scalar(1, "b", SqlType::Boolean, false),
            Parameter::scalar(2, "s", SqlType::SmallInt, false),
            Parameter::scalar(3, "i", SqlType::Integer, false),
        ],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_int(stmt, 1, b);"));
    assert!(src.contains("sqlite3_bind_int(stmt, 2, s);"));
    assert!(src.contains("sqlite3_bind_int(stmt, 3, i);"));
}

#[test]
fn test_sqlite_bind_bigint() {
    let schema = Schema::default();
    let query = Query::exec("Big", "SELECT ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_int64(stmt, 1, id);"));
}

#[test]
fn test_sqlite_bind_double_types() {
    let schema = Schema::default();
    let query = Query::exec("Nums", "SELECT ?1, ?2", vec![Parameter::scalar(1, "r", SqlType::Real, false), Parameter::scalar(2, "d", SqlType::Double, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_double(stmt, 1, r);"));
    assert!(src.contains("sqlite3_bind_double(stmt, 2, d);"));
}

#[test]
fn test_sqlite_bind_text_like_types() {
    let schema = Schema::default();
    let query = Query::exec(
        "Texty",
        "SELECT ?1, ?2, ?3",
        vec![
            Parameter::scalar(1, "name", SqlType::Text, false),
            Parameter::scalar(2, "price", SqlType::Decimal, false),
            Parameter::scalar(3, "when_", SqlType::Date, false),
        ],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);"));
    assert!(src.contains("sqlite3_bind_text(stmt, 2, price.c_str(), -1, SQLITE_TRANSIENT);"));
    assert!(src.contains("sqlite3_bind_text(stmt, 3, when_.c_str(), -1, SQLITE_TRANSIENT);"));
}

#[test]
fn test_sqlite_bind_blob() {
    let schema = Schema::default();
    let query = Query::exec("Bloby", "SELECT ?1", vec![Parameter::scalar(1, "data", SqlType::Bytes, false)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_bind_blob(stmt, 1, data.data(), static_cast<int>(data.size()), SQLITE_TRANSIENT);"));
}

#[test]
fn test_sqlite_bind_nullable_text() {
    let schema = Schema::default();
    let query = Query::exec("MaybeName", "SELECT ?1", vec![Parameter::scalar(1, "name", SqlType::Text, true)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("name.has_value() ? sqlite3_bind_text(stmt, 1, name.value().c_str(), -1, SQLITE_TRANSIENT) : sqlite3_bind_null(stmt, 1);"));
}

#[test]
fn test_sqlite_bind_nullable_bigint() {
    let schema = Schema::default();
    let query = Query::exec("MaybeId", "SELECT ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, true)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("id.has_value() ? sqlite3_bind_int64(stmt, 1, id.value()) : sqlite3_bind_null(stmt, 1);"));
}

#[test]
fn test_sqlite_bind_list_param_serializes_json() {
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
fn test_sqlite_bind_numeric_list_param_uses_to_string() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN (?1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("ids_json += std::to_string(ids[i]);"));
}

#[test]
fn test_sqlite_bind_text_list_param_quotes_elements() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByNames",
        "SELECT id FROM t WHERE name IN (?1)",
        vec![Parameter::list(1, "names", SqlType::Text, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("names_json += json_escape(names[i]);"));
}

// ─── column reading ──────────────────────────────────────────────────────

#[test]
fn test_sqlite_read_bigint_column() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Ids", "SELECT id FROM user", vec![], vec![ResultColumn::not_nullable("id", SqlType::BigInt)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_column_int64(stmt, 0)"));
}

#[test]
fn test_sqlite_read_text_column() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Names", "SELECT name FROM user", vec![], vec![ResultColumn::not_nullable("name", SqlType::Text)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)))"));
}

#[test]
fn test_sqlite_read_blob_column() {
    let schema = Schema::default();
    let query = Query::many("GetBlob", "SELECT thumbnail FROM product", vec![], vec![ResultColumn::not_nullable("thumbnail", SqlType::Bytes)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, 0))"));
    assert!(src.contains("sqlite3_column_bytes(stmt, 0)"));
}

#[test]
fn test_sqlite_read_boolean_column() {
    let schema = Schema::default();
    let query = Query::many("Flags", "SELECT active FROM product", vec![], vec![ResultColumn::not_nullable("active", SqlType::Boolean)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("static_cast<bool>(sqlite3_column_int(stmt, 0))"));
}

#[test]
fn test_sqlite_nullable_text_column_uses_column_type() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Names", "SELECT bio FROM user", vec![], vec![ResultColumn::nullable("bio", SqlType::Text)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_column_type(stmt, 0) == SQLITE_NULL ? std::nullopt : std::optional<std::string>(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))))"));
}

#[test]
fn test_sqlite_nullable_integer_column_uses_column_type() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Years", "SELECT birth_year FROM user", vec![], vec![ResultColumn::nullable("birth_year", SqlType::Integer)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("sqlite3_column_type(stmt, 0) == SQLITE_NULL ? std::nullopt : std::optional<std::int32_t>(sqlite3_column_int(stmt, 0))"));
}

// ─── row type construction ───────────────────────────────────────────────

#[test]
fn test_sqlite_one_uses_table_row_type() {
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
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
}

#[test]
fn test_sqlite_many_uses_table_row_type() {
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
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("rows.push_back(User{"));
}

#[test]
fn test_sqlite_inline_row_struct_used_for_projection() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("GetUserName", "SELECT name FROM user", vec![], vec![ResultColumn::not_nullable("name", SqlType::Text)]);
    let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("rows.push_back(GetUserNameRow{"));
}
