use super::*;

// ─── generate: list params ──────────────────────────────────────────────

#[test]
fn test_generate_pg_native_list_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list_sql("SELECT id FROM t WHERE id = ANY($1)")],
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
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
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
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list_sql("SELECT id FROM t WHERE id IN (SELECT value FROM json_each($1))")],
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
        vec![Parameter::list(1, "tags", SqlType::Text, false).with_native_list_sql("SELECT id FROM t WHERE tag IN (SELECT value FROM json_each($1))")],
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
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list_sql("SELECT id FROM t WHERE id IN (SELECT value FROM json_each($1))")],
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
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
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
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = KotlinCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");
    // active is before IN in the SQL — must still bind at slot 1.
    assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
    // The scalar binding must precede the list forEachIndexed.
    let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
    let loop_pos = src.find("forEachIndexed").expect("list binding loop not found");
    assert!(active_pos < loop_pos, "before-scalar binding must precede the list binding loop");
}

// ─── Bug C: = ANY(@ids::bigint[]) pattern falls back to unclean SQL ──────────

#[test]
fn test_bug_c_any_syntax_pg_native_should_rewrite_to_any_placeholder() {
    // Bug C: when the user writes `= ANY(@ids::bigint[])`, the named-param
    // preprocessor rewrites @ids → $1 but leaves ::bigint[] in the SQL, producing
    // `= ANY($1::bigint[])`. The frontend's replace_list_in_clause handles the
    // `= ANY($N...)` pattern (stripping the cast suffix) and stores the clean SQL
    // in native_list_sql.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id = ANY($1::bigint[])",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list_sql("SELECT id FROM t WHERE id = ANY($1)")],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    // Must generate the clean `= ANY(?)` form, not leave the ::bigint[] cast in place.
    assert!(src.contains("= ANY(?)"), "PgNative must produce = ANY(?), got: {src}");
    assert!(!src.contains("::bigint[]"), "redundant cast must not appear in generated SQL: {src}");
    // List-param boilerplate must still be emitted.
    assert!(src.contains("createArrayOf"), "must create a JDBC array: {src}");
    assert!(src.contains("setArray"), "must bind via setArray: {src}");
}

#[test]
fn test_bug_c_in_clause_annotation_works_correctly() {
    // Counterpart to test_bug_c_*: confirms that the standard `IN ($1)` pattern
    // with a list annotation already generates the correct `= ANY(?)` form.
    // This test must keep passing after the fix.
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list_sql("SELECT id FROM t WHERE id = ANY($1)")],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(src.contains("= ANY(?)"), "IN clause pattern must generate = ANY(?): {src}");
    assert!(!src.contains("IN ($1)"), "original IN ($1) must be replaced: {src}");
}
