use super::*;

// ─── generate: table record ─────────────────────────────────────────────

#[test]
fn test_generate_table_record() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "User.java");
    assert!(src.contains("public record User("));
    assert!(src.contains("long id"));
    assert!(src.contains("String name"));
    assert!(src.contains("String bio"));
}

#[test]
fn test_generate_package_declaration() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
    let src = get_file(&files, "User.java");
    assert!(src.contains("package com.example.db;"));
}

#[test]
fn test_generate_no_queries_produces_no_queries_file() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    assert_eq!(files.len(), 1);
}

// ─── generate: query commands ───────────────────────────────────────────

#[test]
fn test_generate_exec_query() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static void deleteUser(Connection conn, long id)"));
    assert!(src.contains("ps.executeUpdate();"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema::default();
    let query =
        Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active".to_string(), SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static long deleteUsers("));
    assert!(src.contains("return ps.executeUpdate();"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static Optional<User> getUser("));
    assert!(src.contains("if (!rs.next()) return Optional.empty();"));
    assert!(src.contains("return Optional.of(new User("));
}

#[test]
fn test_generate_many_query_infers_table_return_type() {
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
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static List<User> listUsers(Connection conn)"));
    assert!(src.contains("while (rs.next()) rows.add(new User("));
    assert!(src.contains("return rows;"));
}

// ─── generate: SQL constant name ────────────────────────────────────────

#[test]
fn test_generate_sql_const_name_is_screaming_snake_case() {
    let schema = Schema::default();
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("SQL_GET_USER_BY_ID"));
}

// ─── generate: inline row record ────────────────────────────────────────

#[test]
fn test_generate_inline_row_record_for_partial_result() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("name", SqlType::Text)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public record GetUserNameRow("));
    assert!(src.contains("Optional<GetUserNameRow>"));
}

// ─── generate: nullable result column uses getObject ────────────────────

#[test]
fn test_generate_nullable_integer_result_uses_get_object() {
    // rs.getInt returns 0 for NULL; nullable Integer columns must use the
    // getNullableInt helper (wasNull-based, compatible with all JDBC drivers)
    let schema = Schema::default();
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::nullable("count", SqlType::Integer)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("getNullableInt(rs, 1)"));
    assert!(!src.contains("rs.getInt(1)"));
}

#[test]
fn test_generate_non_nullable_integer_result_uses_get_int() {
    let schema = Schema::default();
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("count", SqlType::Integer)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("rs.getInt(1)"));
}

// ─── generate: Querier ────────────────────────────────────────────────

#[test]
fn test_generate_queries_ds_file_is_emitted() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "Querier.java")));
}

#[test]
fn test_generate_queries_ds_constructor_and_datasource_import() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Querier.java");
    assert!(src.contains("import javax.sql.DataSource;"));
    assert!(src.contains("public final class Querier {"));
    assert!(src.contains("private final DataSource dataSource;"));
    assert!(src.contains("public Querier(DataSource dataSource)"));
}

#[test]
fn test_generate_queries_ds_exec_method_delegates_to_queries() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Querier.java");
    assert!(src.contains("public void deleteUser(long id) throws SQLException"));
    assert!(src.contains("try (Connection conn = dataSource.getConnection())"));
    assert!(src.contains("Queries.deleteUser(conn, id);"));
}

#[test]
fn test_generate_queries_ds_one_method_returns_optional() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("bio", SqlType::Text),
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Querier.java");
    assert!(src.contains("import java.util.Optional;"));
    assert!(src.contains("public Optional<User> getUser(long id) throws SQLException"));
    assert!(src.contains("return Queries.getUser(conn, id);"));
}

#[test]
fn test_generate_queries_ds_many_method_returns_list() {
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
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Querier.java");
    assert!(src.contains("import java.util.List;"));
    assert!(src.contains("public List<User> listUsers() throws SQLException"));
    assert!(src.contains("return Queries.listUsers(conn);"));
}
