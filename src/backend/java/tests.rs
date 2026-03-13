use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType};

fn cfg_pkg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None }
}

fn pg() -> JavaCodegen {
    JavaCodegen { target: JdbcTarget::Postgres }
}

// ─── java_type ─────────────────────────────────────────────────────────

#[test]
fn test_java_type_boolean_non_nullable() {
    assert_eq!(java_type(&SqlType::Boolean, false), "boolean");
}

#[test]
fn test_java_type_boolean_nullable() {
    assert_eq!(java_type(&SqlType::Boolean, true), "Boolean");
}

#[test]
fn test_java_type_integer_non_nullable() {
    assert_eq!(java_type(&SqlType::Integer, false), "int");
}

#[test]
fn test_java_type_integer_nullable() {
    assert_eq!(java_type(&SqlType::Integer, true), "Integer");
}

#[test]
fn test_java_type_bigint_non_nullable() {
    assert_eq!(java_type(&SqlType::BigInt, false), "long");
}

#[test]
fn test_java_type_bigint_nullable() {
    assert_eq!(java_type(&SqlType::BigInt, true), "Long");
}

#[test]
fn test_java_type_text_ignores_nullability() {
    // String is a reference type — same in both cases
    assert_eq!(java_type(&SqlType::Text, false), "String");
    assert_eq!(java_type(&SqlType::Text, true), "String");
}

#[test]
fn test_java_type_decimal() {
    assert_eq!(java_type(&SqlType::Decimal, false), "java.math.BigDecimal");
}

#[test]
fn test_java_type_temporal() {
    assert_eq!(java_type(&SqlType::Date, false), "java.time.LocalDate");
    assert_eq!(java_type(&SqlType::Time, false), "java.time.LocalTime");
    assert_eq!(java_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
    assert_eq!(java_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
}

#[test]
fn test_java_type_uuid() {
    assert_eq!(java_type(&SqlType::Uuid, false), "java.util.UUID");
}

#[test]
fn test_java_type_json() {
    assert_eq!(java_type(&SqlType::Json, false), "String");
    assert_eq!(java_type(&SqlType::Jsonb, false), "String");
}

#[test]
fn test_java_type_array_non_nullable() {
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), false), "java.util.List<String>");
}

#[test]
fn test_java_type_array_nullable() {
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), true), "@Nullable java.util.List<String>");
}

#[test]
fn test_java_type_array_of_integers_uses_boxed_type() {
    // Array elements must be boxed — List<int> is invalid Java
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "java.util.List<Integer>");
}

#[test]
fn test_resultset_read_array_text() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), false, 3);
    assert_eq!(expr, "java.util.Arrays.asList((String[]) rs.getArray(3).getArray())");
}

#[test]
fn test_resultset_read_array_integer_nullable() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Integer)), true, 5);
    assert!(expr.contains("rs.getArray(5) == null ? null :"));
    assert!(expr.contains("(Integer[]) rs.getArray(5).getArray()"));
}

#[test]
fn test_java_type_custom() {
    assert_eq!(java_type(&SqlType::Custom("citext".to_string()), false), "Object");
}

// ─── generate: table record ─────────────────────────────────────────────

#[test]
fn test_generate_table_record() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "User.java");
    assert!(src.contains("public record User("));
    assert!(src.contains("long id"));
    assert!(src.contains("String name"));
    assert!(src.contains("String bio"));
}

#[test]
fn test_generate_package_declaration() {
    let schema = Schema { tables: vec![user_table()] };
    let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
    let src = get_file(&files, "User.java");
    assert!(src.contains("package com.example.db;"));
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
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static void deleteUser(Connection conn, long id)"));
    assert!(src.contains("ps.executeUpdate();"));
}

#[test]
fn test_generate_execrows_query() {
    let schema = Schema { tables: vec![] };
    let query =
        Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active".to_string(), SqlType::Boolean, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static long deleteUsers("));
    assert!(src.contains("return ps.executeUpdate();"));
}

#[test]
fn test_generate_one_query_infers_table_return_type() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("public static List<User> listUsers(Connection conn)"));
    assert!(src.contains("while (rs.next()) rows.add(new User("));
    assert!(src.contains("return rows;"));
}

// ─── generate: SQL constant name ────────────────────────────────────────

#[test]
fn test_generate_sql_const_name_is_screaming_snake_case() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("GetUserById", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("SQL_GET_USER_BY_ID"));
}

// ─── generate: inline row record ────────────────────────────────────────

#[test]
fn test_generate_inline_row_record_for_partial_result() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUserName",
        "SELECT name FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
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
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: true }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("getNullableInt(rs, 1)"));
    assert!(!src.contains("rs.getInt(1)"));
}

#[test]
fn test_generate_non_nullable_integer_result_uses_get_int() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetCount",
        "SELECT count FROM stats WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("rs.getInt(1)"));
}

// ─── generate: QueriesDs ────────────────────────────────────────────────

#[test]
fn test_generate_queries_ds_file_is_emitted() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "QueriesDs.java")));
}

#[test]
fn test_generate_queries_ds_constructor_and_datasource_import() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.java");
    assert!(src.contains("import javax.sql.DataSource;"));
    assert!(src.contains("public final class QueriesDs {"));
    assert!(src.contains("private final DataSource dataSource;"));
    assert!(src.contains("public QueriesDs(DataSource dataSource)"));
}

#[test]
fn test_generate_queries_ds_exec_method_delegates_to_queries() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.java");
    assert!(src.contains("public void deleteUser(long id) throws SQLException"));
    assert!(src.contains("try (Connection conn = dataSource.getConnection())"));
    assert!(src.contains("Queries.deleteUser(conn, id);"));
}

#[test]
fn test_generate_queries_ds_one_method_returns_optional() {
    let schema = Schema { tables: vec![user_table()] };
    let query = Query::one(
        "GetUser",
        "SELECT id, name, bio FROM user WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
        ],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "QueriesDs.java");
    assert!(src.contains("import java.util.Optional;"));
    assert!(src.contains("public Optional<User> getUser(long id) throws SQLException"));
    assert!(src.contains("return Queries.getUser(conn, id);"));
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
    let src = get_file(&files, "QueriesDs.java");
    assert!(src.contains("import java.util.List;"));
    assert!(src.contains("public List<User> listUsers() throws SQLException"));
    assert!(src.contains("return Queries.listUsers(conn);"));
}

// ─── generate: repeated parameter binding ───────────────────────────────

#[test]
fn test_generate_repeated_param_emits_bind_per_occurrence() {
    // $1 appears 4 times, $2 once — must emit 5 bind calls in SQL order
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "FindItems",
        "SELECT * FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2",
        vec![Parameter::scalar(1, "accountId".to_string(), SqlType::BigInt, false), Parameter::scalar(2, "inputData".to_string(), SqlType::Text, false)],
        vec![],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    // Four bind calls for accountId (slots 1-4) and one for inputData (slot 5)
    assert!(src.contains("ps.setLong(1, accountId)"));
    assert!(src.contains("ps.setLong(2, accountId)"));
    assert!(src.contains("ps.setLong(3, accountId)"));
    assert!(src.contains("ps.setLong(4, accountId)"));
    assert!(src.contains("ps.setString(5, inputData)"));
    // Old (wrong) single binding must not appear
    assert!(!src.contains("ps.setString(2, inputData)") || src.contains("ps.setString(5, inputData)"));
}

// ─── generate: parameter binding ────────────────────────────────────────

#[test]
fn test_generate_nullable_param_uses_set_object() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec(
        "UpdateBio",
        "UPDATE user SET bio = $1 WHERE id = $2",
        vec![Parameter::scalar(1, "bio".to_string(), SqlType::Text, true), Parameter::scalar(2, "id".to_string(), SqlType::BigInt, false)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("List<Long> ids"), "should use List<Long> for list param");
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("List<Long> ids"), "should use List<Long> for list param");
    assert!(src.contains("IN (\" + marks + \")"), "dynamic builds IN at runtime");
    assert!(src.contains(r#""?""#), "dynamic placeholder marker must be ?");
    assert!(src.contains("for (int i = 0; i <"), "dynamic must have a bind loop for list elements");
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
    let files = JavaCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("rs.getArray(1)"), "should read array column via getArray: {src}");
    assert!(!src.contains("rs.getObject(1)"), "should not fall through to getObject for array column");
    assert!(src.contains("(String[]) rs.getArray(1).getArray()"), "should cast array to String[]");
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("createArrayOf(\"text\", tags.toArray())"), "should create JDBC array: {src}");
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
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("ps.setObject(1, metadata, java.sql.Types.OTHER)"), "JSONB must use Types.OTHER: {src}");
}

// ─── Bug A: JSON escaping for text list params in native strategy ────────────

#[test]
fn test_bug_a_sqlite_native_text_list_json_escaping() {
    // Bug A: The SQLite/MySQL native strategy uses Object::toString for all
    // element types. For Text params this produces bare unquoted strings —
    // invalid JSON. This test fails until the root cause is fixed.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = JavaCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    // Object::toString on a String yields a bare value with no JSON quoting.
    assert!(!src.contains("Object::toString"), "text list must not use Object::toString (produces bare strings)");
    // The fix must wrap each element in \"...\" and escape special characters.
    assert!(src.contains(r#""\"" + x"#), "each text element must be wrapped in JSON quotes");
    assert!(src.contains(r#".replace("\\", "\\\\")"#), "backslashes in text values must be escaped");
}

#[test]
fn test_bug_a_numeric_list_no_quoting_needed() {
    // Numeric types produce valid JSON via toString() — no per-element quoting
    // is needed. Confirm the fix does not introduce unnecessary quoting for
    // numeric list params.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = JavaCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
    assert!(!src.contains(r#""\"" + x"#), "numeric list must not add per-element quoting");
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
    let files = JavaCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");
    // Bug: active is incorrectly bound at slot 1 before the list elements.
    assert!(!src.contains("ps.setBoolean(1, active)"), "active must not bind at slot 1 when it follows IN");
    // Fix: the list binding loop must appear before the scalar-after binding.
    let loop_pos = src.find("for (int i = 0; i <").expect("list binding loop not found");
    let active_pos = src.find("setBoolean").expect("active binding not found");
    assert!(loop_pos < active_pos, "list binding loop must precede the scalar-after binding");
    // Fix: slot for active depends on the runtime list size.
    assert!(src.contains("ids.size()"), "slot for active must be computed from ids.size() at runtime");
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
    let files = JavaCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");
    // active is before IN in the SQL — must still bind at slot 1.
    assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
    // The scalar binding must precede the list loop.
    let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
    let loop_pos = src.find("for (int i = 0; i <").expect("list binding loop not found");
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
    assert!(names.contains(&"UsersQueries.java"), "expected UsersQueries.java, got {names:?}");
    assert!(names.contains(&"UsersQueriesDs.java"), "expected UsersQueriesDs.java");
    assert!(names.contains(&"PostsQueries.java"), "expected PostsQueries.java");
    assert!(names.contains(&"PostsQueriesDs.java"), "expected PostsQueriesDs.java");
    assert!(!names.contains(&"Queries.java"), "Queries.java must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "UsersQueries.java");
    let posts_src = get_file(&files, "PostsQueries.java");
    assert!(users_src.contains("deleteUser"), "UsersQueries.java must contain deleteUser");
    assert!(!users_src.contains("deletePost"), "UsersQueries.java must not contain deletePost");
    assert!(posts_src.contains("deletePost"), "PostsQueries.java must contain deletePost");
    assert!(!posts_src.contains("deleteUser"), "PostsQueries.java must not contain deleteUser");
}

#[test]
fn test_generate_default_group_still_named_queries() {
    // Single-file config: group is "" → output class is always Queries, not QueriesQueries.
    let schema = Schema { tables: vec![] };
    let query = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"Queries.java"), "{names:?}");
    assert!(!names.iter().any(|n| n.contains("QueriesQueries")), "default group must not double the Queries suffix");
}
