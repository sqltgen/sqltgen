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
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("= ANY(%s)"), "PG native should rewrite IN to = ANY");
    // psycopg3 accepts a Python list directly — no json.dumps needed.
    assert!(!src.contains("json.dumps"), "PG native must not serialise to JSON");
    assert!(src.contains("(ids,)"), "list is passed directly as the bound value");
}

#[test]
fn test_generate_pg_native_list_param_with_scalar() {
    // Scalar before list param: args tuple order must mirror SQL order.
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![
            Parameter::scalar(1, "active", SqlType::Boolean, false),
            Parameter::list(2, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE active = $1 AND id = ANY($2)", NativeListBind::Array),
        ],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("active: bool"), "scalar param before list");
    assert!(src.contains("ids: list[int]"), "list param after scalar");
    // active comes before ids in SQL order → (active, ids) tuple.
    assert!(src.contains("(active, ids)"), "args must be in SQL parameter order");
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
    let files = sq().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("json_each"), "SQLite native uses json_each");
    assert!(src.contains("json.dumps(ids)"), "SQLite native serialises list to JSON");
    assert!(src.contains("ids_json"), "JSON variable must be named ids_json");
    // All engines use the _sqltgen execute helper
    assert!(src.contains("with execute(conn,"), "uses _sqltgen execute helper");
    assert!(!src.contains("= ANY"), "SQLite must not use pg ANY syntax");
}

#[test]
fn test_generate_mysql_native_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)
            .with_native_list("SELECT id FROM t WHERE id IN (SELECT value FROM JSON_TABLE($1,'$[*]' COLUMNS(value BIGINT PATH '$')) t)", NativeListBind::Json)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = my().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("JSON_TABLE"), "MySQL native uses JSON_TABLE");
    assert!(src.contains("json.dumps(ids)"), "MySQL native serialises list to JSON");
    assert!(src.contains("ids_json"), "JSON variable must be named ids_json");
    // All engines use the _sqltgen execute helper
    assert!(src.contains("with execute(conn,"), "uses _sqltgen execute helper");
    assert!(!src.contains("json_each"), "MySQL must not use SQLite json_each");
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
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // PG dynamic uses %s placeholders.
    assert!(src.contains(r#""%s""#), "PG dynamic must use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    // Dynamic strategy must NOT emit a SQL constant for this query.
    assert!(!src.contains("GET_BY_IDS"), "dynamic strategy must not emit a SQL constant");
}

#[test]
fn test_generate_pg_dynamic_list_param_with_scalar() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // active is before IN — must come first in args: (active,) + tuple(ids).
    assert!(src.contains("(active,) + tuple(ids)"), "scalar before IN must precede list in args");
}

#[test]
fn test_generate_sqlite_dynamic_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = sq().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // SQLite dynamic uses ? placeholders (not %s).
    assert!(src.contains(r#""?""#), "SQLite dynamic must use ? placeholder");
    assert!(!src.contains(r#""%s""#), "SQLite dynamic must not use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    // All engines use the _sqltgen execute helper
    assert!(src.contains("with execute(conn, sql,"), "SQLite dynamic uses _sqltgen execute helper");
}

#[test]
fn test_generate_mysql_dynamic_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = my().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    // MySQL dynamic uses %s placeholders (same as PG).
    assert!(src.contains(r#""%s""#), "MySQL dynamic must use %s placeholder");
    assert!(src.contains("placeholders"), "must build placeholders string at runtime");
    assert!(src.contains("tuple(ids)"), "list elements bound via tuple");
    assert!(src.contains("with execute(conn, sql,"), "MySQL dynamic uses _sqltgen execute helper");
}

#[test]
fn test_generate_list_param_text_type() {
    // Text list params use list[str] — verify correct Python type annotation.
    let schema = Schema::default();
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("tags: list[str]"), "Text list param should use list[str]");
}

#[test]
fn test_generate_list_param_execrows_cmd() {
    // List params work with :execrows — DELETE / UPDATE with IN clause.
    let schema = Schema::default();
    let query = Query::exec_rows(
        "DeleteByIds",
        "DELETE FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("DELETE FROM t WHERE id = ANY($1)", NativeListBind::Array)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.py");
    assert!(src.contains("ids: list[int]"), "list param should use list[int]");
    assert!(src.contains("= ANY(%s)"), "PG native should rewrite IN to = ANY");
    assert!(src.contains("return cur.rowcount"), "execrows must return rowcount");
}

// ─── Bug B: dynamic strategy binds scalars at wrong position when scalar follows IN

#[test]
fn test_bug_b_postgres_dynamic_scalar_after_in_binding_order() {
    // Bug B: when a scalar param appears *after* the IN clause in the SQL, the
    // Dynamic strategy incorrectly places it *before* the list elements in the
    // execute args. Correct order: tuple(list) + (scalar,).
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
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // Bug: active precedes list elements in the execute args tuple.
    assert!(!src.contains("(active,) + tuple(ids)"), "active must not precede list in args when it follows IN");
    // Fix: list elements come first, then active.
    assert!(src.contains("tuple(ids) + (active,)"), "list elements must precede the scalar-after in args tuple");
}

#[test]
fn test_bug_b_postgres_dynamic_scalar_before_in_no_regression() {
    // When the scalar param appears *before* the IN clause, the current order
    // is correct. Confirm the fix preserves this common pattern.
    let schema = Schema::default();
    let query = Query::many(
        "GetActiveByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let cfg =
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.py");
    // active is before IN in the SQL — must come first in the args tuple.
    assert!(src.contains("(active,) + tuple(ids)"), "scalar before IN must precede list in args tuple");
}
