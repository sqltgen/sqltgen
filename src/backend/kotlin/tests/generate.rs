use super::*;

// ─── generate: data class ──────────────────────────────────────────────

#[test]
fn test_generate_table_data_class() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "User.kt");
    assert!(src.contains("data class User("));
    assert!(src.contains("val id: Long"));
    assert!(src.contains("val name: String"));
    assert!(src.contains("val bio: String?")); // nullable → String?
}

#[test]
fn test_generate_package_declaration() {
    let schema = Schema::with_tables(vec![user_table()]);
    let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
    let src = get_file(&files, "User.kt");
    // Kotlin package has no semicolon
    assert!(src.contains("package com.example.db\n"));
    assert!(!src.contains("package com.example.db;"));
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
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("fun deleteUser(conn: Connection, id: Long): Unit"));
    assert!(src.contains("ps.executeUpdate()"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("fun deleteUsers(conn: Connection, active: Boolean): Long"));
    assert!(src.contains("return ps.executeUpdate().toLong()"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
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
    let src = get_file(&files, "Queries.kt");
    // Kotlin :one return type is nullable (T?) not Optional<T>
    assert!(src.contains("fun getUser(conn: Connection, id: Long): User?"));
    assert!(src.contains("if (!rs.next()) return null"));
    assert!(src.contains("return User("));
}

#[test]
fn test_generate_many_query_infers_table_return_type() {
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
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("fun listUsers(conn: Connection): List<User>"));
    assert!(src.contains("while (rs.next()) rows.add(User("));
    assert!(src.contains("return rows"));
}

// ─── generate: SQL constant name ────────────────────────────────────────

#[test]
fn test_generate_sql_const_name_is_screaming_snake_case() {
    let schema = Schema::default();
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("SQL_GET_USER_BY_ID"));
}

// ─── generate: inline row data class ────────────────────────────────────

#[test]
fn test_generate_inline_row_class_for_partial_result() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("data class GetUserNameRow("));
    assert!(src.contains("GetUserNameRow?"));
}

// ─── generate: nullable result column uses getObject ────────────────────

#[test]
fn test_generate_nullable_long_result_uses_get_object() {
    // rs.getLong returns 0L for NULL; nullable Long? columns must use the
    // getNullableLong helper (wasNull-based, compatible with all JDBC drivers)
    let schema = Schema::default();
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: true }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("getNullableLong(rs, 1)"));
    assert!(!src.contains("rs.getLong(1)"));
}

#[test]
fn test_generate_non_nullable_long_result_uses_get_long() {
    let schema = Schema::default();
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("rs.getLong(1)"));
}

// ─── generate: QueriesDs ────────────────────────────────────────────────

#[test]
fn test_generate_queries_ds_file_is_emitted() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "QueriesDs.kt")));
}

#[test]
fn test_generate_queries_ds_class_and_datasource_import() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("import javax.sql.DataSource"));
    assert!(src.contains("class QueriesDs(private val dataSource: DataSource)"));
}

#[test]
fn test_generate_queries_ds_exec_method_delegates_via_use() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("fun deleteUser(id: Long): Unit ="));
    assert!(src.contains("dataSource.connection.use { conn -> Queries.deleteUser(conn, id) }"));
}

#[test]
fn test_generate_queries_ds_one_method_returns_nullable() {
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
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("fun getUser(id: Long): User? ="));
    assert!(src.contains("dataSource.connection.use { conn -> Queries.getUser(conn, id) }"));
}

#[test]
fn test_generate_queries_ds_many_method_returns_list() {
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
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("fun listUsers(): List<User> ="));
    assert!(src.contains("dataSource.connection.use { conn -> Queries.listUsers(conn) }"));
}
