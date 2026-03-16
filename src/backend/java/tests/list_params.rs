use super::*;

// ─── generate: list params ──────────────────────────────────────────────

#[test]
fn test_generate_pg_native_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE id = ANY($1)", NativeListBind::Array)],
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
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("List<Long> ids"), "should use List<Long> for list param");
    assert!(src.contains("IN (\" + marks + \")"), "dynamic builds IN at runtime");
    assert!(src.contains(r#""?""#), "dynamic placeholder marker must be ?");
    assert!(src.contains("for (int i = 0; i <"), "dynamic must have a bind loop for list elements");
}

#[test]
fn test_generate_sqlite_native_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)
            .with_native_list("SELECT id FROM t WHERE id IN (SELECT value FROM json_each($1))", NativeListBind::Json)],
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
    let schema = Schema::default();
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
    let schema = Schema::default();
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
    let schema = Schema::default();
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
    let schema = Schema::default();
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)
            .with_native_list("SELECT id FROM t WHERE tag IN (SELECT value FROM json_each($1))", NativeListBind::Json)],
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
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)
            .with_native_list("SELECT id FROM t WHERE id IN (SELECT value FROM json_each($1))", NativeListBind::Json)],
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
    let schema = Schema::default();
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
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
    let schema = Schema::default();
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = JavaCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");
    // active is before IN in the SQL — must still bind at slot 1.
    assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
    // The scalar binding must precede the list loop.
    let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
    let loop_pos = src.find("for (int i = 0; i <").expect("list binding loop not found");
    assert!(active_pos < loop_pos, "before-scalar binding must precede the list binding loop");
}
