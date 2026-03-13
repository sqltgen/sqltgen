use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType};

fn cfg_pkg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None }
}

fn pg() -> KotlinCodegen {
    KotlinCodegen { target: JdbcTarget::Postgres }
}

// ─── kotlin_type ───────────────────────────────────────────────────────

#[test]
fn test_kotlin_type_boolean_non_nullable() {
    // Kotlin has no primitive/boxed split — Boolean is always Boolean
    assert_eq!(kotlin_type(&SqlType::Boolean, false), "Boolean");
}

#[test]
fn test_kotlin_type_boolean_nullable() {
    assert_eq!(kotlin_type(&SqlType::Boolean, true), "Boolean?");
}

#[test]
fn test_kotlin_type_integer_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Integer, false), "Int");
}

#[test]
fn test_kotlin_type_integer_nullable() {
    assert_eq!(kotlin_type(&SqlType::Integer, true), "Int?");
}

#[test]
fn test_kotlin_type_bigint_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::BigInt, false), "Long");
}

#[test]
fn test_kotlin_type_bigint_nullable() {
    assert_eq!(kotlin_type(&SqlType::BigInt, true), "Long?");
}

#[test]
fn test_kotlin_type_text_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Text, false), "String");
}

#[test]
fn test_kotlin_type_text_nullable() {
    assert_eq!(kotlin_type(&SqlType::Text, true), "String?");
}

#[test]
fn test_kotlin_type_decimal() {
    assert_eq!(kotlin_type(&SqlType::Decimal, false), "java.math.BigDecimal");
}

#[test]
fn test_kotlin_type_temporal() {
    assert_eq!(kotlin_type(&SqlType::Date, false), "java.time.LocalDate");
    assert_eq!(kotlin_type(&SqlType::Time, false), "java.time.LocalTime");
    assert_eq!(kotlin_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
    assert_eq!(kotlin_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
}

#[test]
fn test_kotlin_type_uuid() {
    assert_eq!(kotlin_type(&SqlType::Uuid, false), "java.util.UUID");
}

#[test]
fn test_kotlin_type_json() {
    assert_eq!(kotlin_type(&SqlType::Json, false), "String");
    assert_eq!(kotlin_type(&SqlType::Jsonb, false), "String");
}

#[test]
fn test_kotlin_type_array_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), false), "List<String>");
}

#[test]
fn test_kotlin_type_array_nullable() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), true), "List<String>?");
}

#[test]
fn test_kotlin_type_array_of_integers() {
    // Inner type is non-nullable (List element, not the List itself)
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "List<Int>");
}

#[test]
fn test_resultset_read_array_text() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), false, 3);
    assert_eq!(expr, "(rs.getArray(3).array as Array<String>).toList()");
}

#[test]
fn test_resultset_read_array_nullable() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), true, 5);
    assert_eq!(expr, "rs.getArray(5)?.let { (it.array as Array<String>).toList() }");
}

#[test]
fn test_kotlin_type_custom() {
    assert_eq!(kotlin_type(&SqlType::Custom("citext".to_string()), false), "Any");
}

// ─── generate: data class ──────────────────────────────────────────────

#[test]
fn test_generate_table_data_class() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "User.kt");
    assert!(src.contains("data class User("));
    assert!(src.contains("val id: Long"));
    assert!(src.contains("val name: String"));
    assert!(src.contains("val bio: String?")); // nullable → String?
}

#[test]
fn test_generate_package_declaration() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
    let src = get_file(&files, "User.kt");
    // Kotlin package has no semicolon
    assert!(src.contains("package com.example.db\n"));
    assert!(!src.contains("package com.example.db;"));
}

#[test]
fn test_generate_no_queries_produces_no_queries_file() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    assert_eq!(files.len(), 1);
}

// ─── generate: query commands ───────────────────────────────────────────

#[test]
fn test_generate_exec_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("fun deleteUser(conn: Connection, id: Long): Unit"));
    assert!(src.contains("ps.executeUpdate()"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("fun deleteUsers(conn: Connection, active: Boolean): Long"));
    assert!(src.contains("return ps.executeUpdate().toLong()"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
    let schema = Schema { tables: vec![user_table()] };
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
    let schema = Schema { tables: vec![user_table()] };
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
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("SQL_GET_USER_BY_ID"));
}

// ─── generate: inline row data class ────────────────────────────────────

#[test]
fn test_generate_inline_row_class_for_partial_result() {
    let schema = Schema { tables: vec![user_table()] };
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
    let schema = Schema { tables: vec![] };
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
    let schema = Schema { tables: vec![] };
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
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "QueriesDs.kt")));
}

#[test]
fn test_generate_queries_ds_class_and_datasource_import() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("import javax.sql.DataSource"));
    assert!(src.contains("class QueriesDs(private val dataSource: DataSource)"));
}

#[test]
fn test_generate_queries_ds_exec_method_delegates_via_use() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.kt");
    assert!(src.contains("fun deleteUser(id: Long): Unit ="));
    assert!(src.contains("dataSource.connection.use { conn -> Queries.deleteUser(conn, id) }"));
}

#[test]
fn test_generate_queries_ds_one_method_returns_nullable() {
    let schema = Schema { tables: vec![user_table()] };
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
    let schema = Schema { tables: vec![user_table()] };
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

// ─── generate: repeated parameter binding ───────────────────────────────

#[test]
fn test_generate_repeated_param_emits_bind_per_occurrence() {
    // $1 appears 4 times, $2 once — must emit 5 bind calls in SQL order
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "FindItems",
        "SELECT * FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2",
        vec![Parameter::scalar(1, "accountId", SqlType::BigInt, false), Parameter::scalar(2, "inputData", SqlType::Text, false)],
        vec![],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ps.setLong(1, accountId)"));
    assert!(src.contains("ps.setLong(2, accountId)"));
    assert!(src.contains("ps.setLong(3, accountId)"));
    assert!(src.contains("ps.setLong(4, accountId)"));
    assert!(src.contains("ps.setString(5, inputData)"));
}

// ─── generate: parameter binding ────────────────────────────────────────

#[test]
fn test_generate_nullable_param_uses_set_object() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ps.setObject(1, bio)")); // nullable → setObject
    assert!(src.contains("ps.setLong(2, id)")); // non-nullable → typed setter
}

// ─── generate: list params ──────────────────────────────────────────────

#[test]
fn test_generate_pg_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ids: List<Long>"), "should use List<Long> for list param");
    assert!(src.contains("= ANY(?)"), "PG native should use ANY");
    assert!(src.contains("createArrayOf(\"bigint\""), "should call createArrayOf");
    assert!(src.contains("ps.setArray(1, arr)"), "should setArray");
}

#[test]
fn test_generate_pg_dynamic_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ids: List<Long>"), "should use List<Long> for list param");
    assert!(src.contains("joinToString"), "dynamic builds IN at runtime");
    assert!(src.contains("forEachIndexed"), "dynamic must have a bind loop for list elements");
}

#[test]
fn test_generate_sqlite_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
    assert!(!src.contains("IN ($1)"), "IN clause must be replaced by json_each rewrite");
    assert!(!src.contains("JSON_TABLE"), "SQLite should not use MySQL JSON_TABLE");
    assert!(src.contains("ps.setString"), "should bind JSON string");
}

// ─── Array column reads and Array/JSON param binds ─────────────────────

#[test]
fn test_generate_array_result_column_uses_get_array() {
    // Bug: Array columns previously fell through to rs.getObject(idx),
    // which returns a raw JDBC Array object instead of a typed List.
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetTags",
        "SELECT tags FROM t WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn { name: "tags".to_string(), sql_type: SqlType::Array(Box::new(SqlType::Text)), nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("rs.getArray(1)"), "should read array column via getArray: {src}");
    assert!(!src.contains("rs.getObject(1)"), "should not fall through to getObject for array column");
    assert!(src.contains("rs.getArray(1).array as Array<String>"), "should cast array to Array<String>");
}

#[test]
fn test_generate_array_param_uses_set_array() {
    // Bug: Array params previously used ps.setObject(idx, val),
    // which doesn't work with PostgreSQL JDBC for array types.
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateTags",
        "UPDATE t SET tags = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "tags", SqlType::Array(Box::new(SqlType::Text)), false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("createArrayOf(\"text\", tags.toTypedArray())"), "should create JDBC array: {src}");
    assert!(src.contains("ps.setArray(1,"), "should bind array param via setArray: {src}");
}

#[test]
fn test_generate_jsonb_param_uses_types_other() {
    // Bug: JSONB params previously used ps.setObject(idx, val) without
    // the Types.OTHER hint, which PostgreSQL JDBC rejects.
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateMeta",
        "UPDATE t SET meta = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "metadata", SqlType::Jsonb, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("ps.setObject(1, metadata, java.sql.Types.OTHER)"), "JSONB must use Types.OTHER: {src}");
}

// ─── Bug A: JSON escaping for text list params in native strategy ────────────

#[test]
fn test_bug_a_sqlite_native_text_list_json_escaping() {
    // Bug A: The SQLite/MySQL native strategy uses joinToString(",") with no
    // transform for all element types. For Text params this produces bare
    // unquoted strings — invalid JSON. This test fails until fixed.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    // Bare joinToString(",") produces unquoted strings — invalid JSON for Text.
    assert!(!src.contains(r#"joinToString(",") + "]""#), "text list must not use bare joinToString (produces unquoted strings)");
    // The fix must use a transform lambda that wraps each element in \"...\"
    // and escapes special characters.
    assert!(src.contains(r#"joinToString(",") {"#), "text list must use joinToString with a transform lambda");
    assert!(src.contains(r#".replace("\\", "\\\\")"#), "backslashes in text values must be escaped");
}

#[test]
fn test_bug_a_numeric_list_no_quoting_needed() {
    // Numeric types produce valid JSON via toString() — no per-element quoting
    // is needed. Confirm the fix does not add a quoting lambda for numeric types.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
    assert!(!src.contains(r#"joinToString(",") {"#), "numeric list must not add a per-element quoting lambda");
}

// ─── Bug B: dynamic strategy binds scalars at wrong slot when scalar follows IN

#[test]
fn test_bug_b_dynamic_scalar_after_in_binding_order() {
    // Bug B: when a scalar param appears *after* the IN clause in the SQL, the
    // Dynamic strategy incorrectly binds it at slot 1 (before list elements).
    // Correct order: [list elements] + [scalar-after].
    // This test fails until the root cause is fixed.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = KotlinCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");
    // Bug: active is incorrectly bound at slot 1 before the list elements.
    assert!(!src.contains("ps.setBoolean(1, active)"), "active must not bind at slot 1 when it follows IN");
    // Fix: forEachIndexed (list loop) must appear before the scalar-after binding.
    let loop_pos = src.find("forEachIndexed").expect("list binding loop not found");
    let active_pos = src.find("setBoolean").expect("active binding not found");
    assert!(loop_pos < active_pos, "list binding loop must precede the scalar-after binding");
    // Fix: slot for active depends on the runtime list size.
    assert!(src.contains("ids.size"), "slot for active must be computed from ids.size at runtime");
}

#[test]
fn test_bug_b_dynamic_scalar_before_in_no_regression() {
    // When the scalar param appears *before* the IN clause, the current binding
    // order is correct. Confirm the fix preserves this common pattern.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
    let files = KotlinCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");
    // active is before IN in the SQL — must still bind at slot 1.
    assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
    // The scalar binding must precede the list forEachIndexed.
    let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
    let loop_pos = src.find("forEachIndexed").expect("list binding loop not found");
    assert!(active_pos < loop_pos, "before-scalar binding must precede the list binding loop");
}

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"UsersQueries.kt"), "expected UsersQueries.kt, got {names:?}");
    assert!(names.contains(&"UsersQueriesDs.kt"), "expected UsersQueriesDs.kt");
    assert!(names.contains(&"PostsQueries.kt"), "expected PostsQueries.kt");
    assert!(names.contains(&"PostsQueriesDs.kt"), "expected PostsQueriesDs.kt");
    assert!(!names.contains(&"Queries.kt"), "Queries.kt must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "UsersQueries.kt");
    let posts_src = get_file(&files, "PostsQueries.kt");
    assert!(users_src.contains("deleteUser"), "UsersQueries.kt must contain deleteUser");
    assert!(!users_src.contains("deletePost"), "UsersQueries.kt must not contain deletePost");
    assert!(posts_src.contains("deletePost"), "PostsQueries.kt must contain deletePost");
    assert!(!posts_src.contains("deleteUser"), "PostsQueries.kt must not contain deleteUser");
}

#[test]
fn test_generate_default_group_still_named_queries() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"Queries.kt"), "{names:?}");
    assert!(!names.iter().any(|n| n.contains("QueriesQueries")), "default group must not double the Queries suffix");
}
