use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query, ResultColumn};

// ─── libmariadb driver: null-flag type is `my_bool` ──────────────────────

#[test]
fn test_mariadb_nullable_param_uses_my_bool_flag() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, true)]);
    let files = mysql_mariadb().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("my_bool p_id_is_null = !id.has_value();"), "expected my_bool flag for libmariadb\n{src}");
}

#[test]
fn test_mariadb_result_column_uses_my_bool_flag() {
    let schema = Schema::default();
    let query = Query::one(
        "GetUser",
        "SELECT id FROM user WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql_mariadb().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("my_bool id_is_null = false;"), "expected my_bool flag for libmariadb result column\n{src}");
}

// ─── basic mysql shape ───────────────────────────────────────────────────

#[test]
fn test_mysql_function_uses_mysql_connection_type() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("void delete_user(MYSQL* db, std::int64_t id) {"));
}

#[test]
fn test_mysql_header_includes_mysql_h() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("#include <mysql.h>"));
}

#[test]
fn test_mysql_source_includes_cstring() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("#include <cstring>"));
}

#[test]
fn test_mysql_querier_uses_mysql_pointer() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.hpp");
    assert!(src.contains("MYSQL* db_;"));
    assert!(src.contains("explicit Querier(MYSQL* db) : db_(db) {}"));
}

#[test]
fn test_mysql_emits_mysql_stmt_helper_class() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("class MysqlStmt {"));
}

#[test]
fn test_mysql_omits_json_escape_helper_without_list_params() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(!src.contains("static std::string json_escape("));
}

#[test]
fn test_mysql_emits_json_escape_helper_for_text_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByNames",
        "SELECT id FROM t WHERE name IN (?1)",
        vec![Parameter::list(1, "names", SqlType::Text, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("static std::string json_escape("));
}

#[test]
fn test_mysql_body_constructs_stmt_wrapper() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("MysqlStmt stmt(db, SQL_DELETE_USER);"));
}

#[test]
fn test_mysql_generator_never_emits_mysql_stmt_close_in_body() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    // `mysql_stmt_close` may appear inside the MysqlStmt helper class (destructor
    // + prepare-failure path). It must not leak into a generated query body.
    let helper_end = src.find("my_ulonglong affected_rows()").unwrap();
    let after_helper = &src[helper_end..];
    let after_class = &after_helper[after_helper.find("};").unwrap() + 2..];
    assert!(!after_class.contains("mysql_stmt_close"));
}

// ─── bind array setup ────────────────────────────────────────────────────

#[test]
fn test_mysql_bind_array_sized_and_zeroed() {
    let schema = Schema::default();
    let query = Query::exec(
        "AddSaleItem",
        "INSERT INTO sale_item (sale_id, book_id, quantity) VALUES (?1, ?2, ?3)",
        vec![
            Parameter::scalar(1, "sale_id", SqlType::Integer, false),
            Parameter::scalar(2, "book_id", SqlType::Integer, false),
            Parameter::scalar(3, "quantity", SqlType::Integer, false),
        ],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("MYSQL_BIND bind[3];"));
    assert!(src.contains("memset(bind, 0, sizeof(bind));"));
}

#[test]
fn test_mysql_no_params_emits_no_bind_array() {
    let schema = Schema::default();
    let query = Query::exec("DeleteAll", "DELETE FROM user", vec![]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    // Locate the generated function body; the helper class above contains
    // `MYSQL_BIND*` signatures we don't want to match against.
    let body_start = src.find("void delete_all(MYSQL* db)").unwrap();
    let body = &src[body_start..];
    assert!(!body.contains("MYSQL_BIND bind["));
    assert!(!body.contains("stmt.bind_param("));
}

#[test]
fn test_mysql_bind_param_called_after_all_bind_blocks() {
    let schema = Schema::default();
    let query = Query::exec(
        "UpdateUser",
        "UPDATE user SET name = ?1 WHERE id = ?2",
        vec![Parameter::scalar(1, "name", SqlType::Text, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let bind_param_count = src.matches("stmt.bind_param(bind);").count();
    assert_eq!(bind_param_count, 1);
    let bind_param_pos = src.find("stmt.bind_param(bind);").unwrap();
    let last_block_pos = src.find("bind[1].buffer = const_cast<std::int64_t*>(&id);").unwrap();
    assert!(last_block_pos < bind_param_pos);
}

// ─── scalar param bind blocks — non-null ────────────────────────────────

#[test]
fn test_mysql_bind_bigint_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "DELETE FROM t WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<std::int64_t*>(&id);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_LONGLONG;"));
    assert!(!src.contains("bind[0].length ="));
}

#[test]
fn test_mysql_bind_integer_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "DELETE FROM t WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::Integer, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<std::int32_t*>(&id);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_LONG;"));
}

#[test]
fn test_mysql_bind_smallint_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "s", SqlType::SmallInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<std::int16_t*>(&s);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_SHORT;"));
}

#[test]
fn test_mysql_bind_boolean_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "b", SqlType::Boolean, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<bool*>(&b);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_TINY;"));
}

#[test]
fn test_mysql_bind_double_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "d", SqlType::Double, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<double*>(&d);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_DOUBLE;"));
}

#[test]
fn test_mysql_bind_real_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "r", SqlType::Real, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<float*>(&r);"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_FLOAT;"));
}

#[test]
fn test_mysql_bind_text_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "name", SqlType::Text, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(name.c_str());"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_STRING;"));
    assert!(src.contains("unsigned long p_name_len = name.size();"));
    assert!(src.contains("bind[0].length = &p_name_len;"));
    assert!(src.contains("bind[0].buffer_length = p_name_len;"));
}

#[test]
fn test_mysql_bind_decimal_param_uses_string_type() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "price", SqlType::Decimal, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(price.c_str());"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_STRING;"));
    assert!(src.contains("unsigned long p_price_len = price.size();"));
}

#[test]
fn test_mysql_bind_blob_param() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "data", SqlType::Bytes, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(reinterpret_cast<const char*>(data.data()));"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_BLOB;"));
    assert!(src.contains("unsigned long p_data_len = data.size();"));
}

// ─── scalar param bind blocks — nullable ────────────────────────────────

#[test]
fn test_mysql_bind_nullable_fixed_width_materializes_local() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, true)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::int64_t id_val = id.value_or(std::int64_t{});"));
    assert!(src.contains("bind[0].buffer = &id_val;"));
    assert!(src.contains("bind[0].buffer_type = MYSQL_TYPE_LONGLONG;"));
    assert!(src.contains("bool p_id_is_null = !id.has_value();"));
    assert!(src.contains("bind[0].is_null = &p_id_is_null;"));
}

#[test]
fn test_mysql_bind_nullable_text_uses_ternary_for_buffer() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "bio", SqlType::Text, true)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(bio.has_value() ? bio.value().c_str() : \"\");"));
    assert!(src.contains("unsigned long p_bio_len = bio.has_value() ? bio.value().size() : 0;"));
    assert!(src.contains("bool p_bio_is_null = !bio.has_value();"));
    assert!(src.contains("bind[0].is_null = &p_bio_is_null;"));
}

#[test]
fn test_mysql_bind_nullable_bytes_uses_nullptr_for_empty() {
    let schema = Schema::default();
    let query = Query::exec("Q", "SELECT ?1", vec![Parameter::scalar(1, "data", SqlType::Bytes, true)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("data.has_value() ? reinterpret_cast<const char*>(data.value().data()) : nullptr"));
    assert!(src.contains("bool p_data_is_null = !data.has_value();"));
}

// ─── repeated placeholders ──────────────────────────────────────────────

#[test]
fn test_mysql_repeated_param_declares_locals_once() {
    let schema = Schema::default();
    let query = Query::many(
        "ListByGenreOrAll",
        "SELECT id FROM t WHERE ?1 = 'all' OR genre = ?1",
        vec![Parameter::scalar(1, "genre", SqlType::Text, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let decl_count = src.matches("unsigned long p_genre_len = genre.size();").count();
    assert_eq!(decl_count, 1, "genre length local should be declared once, got {decl_count}\n{src}");
    assert!(src.contains("bind[0].buffer = const_cast<char*>(genre.c_str());"));
    assert!(src.contains("bind[1].buffer = const_cast<char*>(genre.c_str());"));
}

#[test]
fn test_mysql_repeated_nullable_param_declares_is_null_flag_once() {
    let schema = Schema::default();
    let query = Query::many(
        "Q",
        "SELECT id FROM t WHERE bio IS NULL OR bio != ?1 OR ?1 IS NULL",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let flag_count = src.matches("bool p_bio_is_null = !bio.has_value();").count();
    assert_eq!(flag_count, 1, "p_bio_is_null flag should be declared once, got {flag_count}\n{src}");
}

// ─── Exec / ExecRows / One / Many dispatch ──────────────────────────────

#[test]
fn test_mysql_exec_calls_execute() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("stmt.execute();"));
    let body_start = src.find("void delete_user(MYSQL* db").unwrap();
    assert!(!src[body_start..].contains("affected_rows"));
}

#[test]
fn test_mysql_execrows_returns_affected_rows() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUser", "DELETE FROM user WHERE id = ?1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return static_cast<std::int64_t>(stmt.affected_rows());"));
}

#[test]
fn test_mysql_one_fetches_single_row_and_returns_nullopt_when_empty() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("if (!stmt.fetch_row()) return std::nullopt;"));
    assert!(src.contains("return User{"));
    assert!(!src.contains("rows.size() > 1"));
}

#[test]
fn test_mysql_many_uses_fetch_row_loop() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
    assert!(src.contains("while (stmt.fetch_row()) {"));
    assert!(src.contains("return rows;"));
}

#[test]
fn test_mysql_one_does_not_loop_fetch() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(!src.contains("while (stmt.fetch_row())"));
}

// ─── result bind setup ──────────────────────────────────────────────────

#[test]
fn test_mysql_result_bind_sized_and_zeroed() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("MYSQL_BIND result_bind[3];"));
    assert!(src.contains("memset(result_bind, 0, sizeof(result_bind));"));
}

#[test]
fn test_mysql_result_bind_fixed_width_column_allocates_local() {
    let schema = Schema::default();
    let query = Query::many("Ids", "SELECT id FROM t", vec![], vec![ResultColumn::not_nullable("id", SqlType::BigInt)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::int64_t id_val{};"));
    assert!(src.contains("result_bind[0].buffer = &id_val;"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_LONGLONG;"));
    assert!(src.contains("bool id_is_null = false;"));
    assert!(src.contains("result_bind[0].is_null = &id_is_null;"));
}

#[test]
fn test_mysql_result_bind_integer_column() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT n FROM t", vec![], vec![ResultColumn::not_nullable("n", SqlType::Integer)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::int32_t n_val{};"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_LONG;"));
}

#[test]
fn test_mysql_result_bind_boolean_column() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT active FROM t", vec![], vec![ResultColumn::not_nullable("active", SqlType::Boolean)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bool active_val{};"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_TINY;"));
}

#[test]
fn test_mysql_result_bind_double_column() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT d FROM t", vec![], vec![ResultColumn::not_nullable("d", SqlType::Double)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("double d_val{};"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_DOUBLE;"));
}

#[test]
fn test_mysql_bind_result_called_before_fetch() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let bind_result_pos = src.find("stmt.bind_result(result_bind);").unwrap();
    let fetch_row_pos = src.find("while (stmt.fetch_row()").unwrap();
    assert!(bind_result_pos < fetch_row_pos);
}

// ─── result bind + fetch (two-phase for varlen) ─────────────────────────

#[test]
fn test_mysql_result_bind_text_column_two_phase_setup() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT name FROM t", vec![], vec![ResultColumn::not_nullable("name", SqlType::Text)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("result_bind[0].buffer = nullptr; // filled below"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_STRING;"));
    assert!(src.contains("unsigned long name_len = 0;"));
    assert!(src.contains("result_bind[0].length = &name_len;"));
    assert!(src.contains("result_bind[0].buffer_length = 0;"));
}

#[test]
fn test_mysql_result_bind_blob_column_two_phase_setup() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT thumbnail FROM t", vec![], vec![ResultColumn::not_nullable("thumbnail", SqlType::Bytes)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("result_bind[0].buffer = nullptr; // filled below"));
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_BLOB;"));
    assert!(src.contains("unsigned long thumbnail_len = 0;"));
    assert!(src.contains("result_bind[0].length = &thumbnail_len;"));
}

#[test]
fn test_mysql_one_fetch_column_allocates_string_after_fetch_row() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name FROM user WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::string name_val(name_len, '\\0');"));
    assert!(src.contains("result_bind[1].buffer = name_val.data();"));
    assert!(src.contains("result_bind[1].buffer_length = name_len;"));
    assert!(src.contains("stmt.fetch_column(&result_bind[1], 1);"));
    let fetch_row_pos = src.find("if (!stmt.fetch_row()) return std::nullopt;").unwrap();
    let alloc_pos = src.find("std::string name_val(name_len, '\\0');").unwrap();
    assert!(fetch_row_pos < alloc_pos);
}

#[test]
fn test_mysql_many_fetch_column_happens_inside_loop() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many(
        "ListNames",
        "SELECT id, name FROM user",
        vec![],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let loop_pos = src.find("while (stmt.fetch_row()) {").unwrap();
    let alloc_pos = src.find("std::string name_val(name_len, '\\0');").unwrap();
    let push_pos = src.find("rows.push_back(").unwrap();
    assert!(loop_pos < alloc_pos);
    assert!(alloc_pos < push_pos);
}

#[test]
fn test_mysql_fetch_column_for_blob_uses_uint8_vector() {
    let schema = Schema::default();
    let query = Query::one(
        "GetBlob",
        "SELECT thumbnail FROM product WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("thumbnail", SqlType::Bytes)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<std::uint8_t> thumbnail_val(thumbnail_len);"));
    assert!(src.contains("result_bind[0].buffer = thumbnail_val.data();"));
    assert!(!src.contains("std::string thumbnail_val"));
}

#[test]
fn test_mysql_decimal_column_uses_string_two_phase() {
    let schema = Schema::default();
    let query = Query::many("Q", "SELECT price FROM t", vec![], vec![ResultColumn::not_nullable("price", SqlType::Decimal)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("result_bind[0].buffer_type = MYSQL_TYPE_STRING;"));
    assert!(src.contains("std::string price_val(price_len, '\\0');"));
}

#[test]
fn test_mysql_fixed_width_column_has_no_fetch_column_call() {
    let schema = Schema::default();
    let query = Query::many(
        "Q",
        "SELECT id, n FROM t",
        vec![],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("n", SqlType::Integer)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(!src.contains("stmt.fetch_column("));
}

#[test]
fn test_mysql_many_loop_without_varlen_cols_has_no_per_row_allocations() {
    let schema = Schema::default();
    let query = Query::many(
        "Q",
        "SELECT id, n FROM t",
        vec![],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("n", SqlType::Integer)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    let loop_start = src.find("while (stmt.fetch_row()) {").unwrap();
    let loop_tail = &src[loop_start..];
    let loop_end = loop_tail.find("    }").unwrap();
    let loop_body = &loop_tail[..loop_end];
    assert!(!loop_body.contains("_val("));
}

// ─── row construction ───────────────────────────────────────────────────

#[test]
fn test_mysql_one_returns_row_type_with_moved_varlen_fields() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name FROM user WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return GetUserRow{"));
    assert!(src.contains("std::move(name_val)"));
    // id is fixed-width so no move.
    assert!(src.contains("        id_val,"));
}

#[test]
fn test_mysql_many_pushes_row_with_moved_varlen_fields() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many(
        "ListUsers",
        "SELECT id, name FROM user",
        vec![],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("rows.push_back(ListUsersRow{"));
    assert!(src.contains("std::move(name_val)"));
}

#[test]
fn test_mysql_nullable_fixed_width_column_uses_is_null_flag_without_move() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Years", "SELECT birth_year FROM user", vec![], vec![ResultColumn::nullable("birth_year", SqlType::Integer)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("birth_year_is_null ? std::nullopt : std::optional<std::int32_t>(birth_year_val)"));
    assert!(!src.contains("std::optional<std::int32_t>(std::move(birth_year_val))"));
}

#[test]
fn test_mysql_nullable_varlen_column_uses_is_null_flag_with_move() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("Bios", "SELECT bio FROM user", vec![], vec![ResultColumn::nullable("bio", SqlType::Text)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("bio_is_null ? std::nullopt : std::optional<std::string>(std::move(bio_val))"));
}

#[test]
fn test_mysql_one_uses_table_row_type() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("return User{"));
}

#[test]
fn test_mysql_many_uses_table_row_type() {
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
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("std::vector<User> rows;"));
    assert!(src.contains("rows.push_back(User{"));
}

#[test]
fn test_mysql_inline_row_struct_used_for_projection() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::many("GetUserName", "SELECT name FROM user", vec![], vec![ResultColumn::not_nullable("name", SqlType::Text)]);
    let files = mysql().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.cpp");
    assert!(src.contains("rows.push_back(GetUserNameRow{"));
}
