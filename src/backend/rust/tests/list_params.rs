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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() };
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("= ANY($1)"), "PG native should rewrite to ANY");
    assert!(!src.contains("IN ($1)"), "original IN clause should be gone");
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
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("placeholders"), "dynamic mode builds placeholders at runtime");
    assert!(src.contains("for v in ids"), "dynamic mode binds each element");
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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() };
    let files = sqlite().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("json_each"), "SQLite native uses json_each");
    assert!(src.contains("ids_json"), "should bind the json local variable");
    assert!(!src.contains("serde_json"), "must not require serde_json");
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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() };
    let files = mysql().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("JSON_TABLE"), "MySQL native uses JSON_TABLE");
    assert!(src.contains("ids_json"), "should bind the json local variable");
    assert!(!src.contains("serde_json"), "must not require serde_json");
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
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(ListParamStrategy::Dynamic), ..Default::default() };
    let files = mysql().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("placeholders"), "dynamic strategy builds placeholders");
    assert!(!src.contains("JSON_TABLE"), "dynamic strategy does not use JSON_TABLE");
}
