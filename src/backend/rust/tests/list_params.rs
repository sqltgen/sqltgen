use super::*;

// ─── generate: list params ──────────────────────────────────────────────

#[test]
fn test_generate_pg_native_list_param() {
    let schema = Schema::default();
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("SELECT id FROM t WHERE id = ANY($1)", NativeListBind::Array)],
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
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
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
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
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
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
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
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
        vec![ResultColumn::not_nullable("id", SqlType::BigInt)],
    );
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(ListParamStrategy::Dynamic), ..Default::default() };
    let files = mysql().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");
    assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
    assert!(src.contains("placeholders"), "dynamic strategy builds placeholders");
    assert!(!src.contains("JSON_TABLE"), "dynamic strategy does not use JSON_TABLE");
}

// ─── regression: array param used as a real array (bug #128) ─────────────

/// A `type[]` param used as a real SQL array (`unnest`, `<> ALL`) must bind the
/// whole slice as one array — not be mis-rewritten as a dynamic `IN (...)` list.
#[test]
fn test_generate_pg_array_param_used_as_scalar_array() {
    use crate::ir::{Column, Table};
    let schema = Schema::with_tables(vec![Table::new(
        "item",
        vec![
            Column::new_primary_key("id", SqlType::BigInt),
            Column::new_not_nullable("parent_id", SqlType::BigInt),
            Column::new_not_nullable("amount", SqlType::Double),
        ],
    )]);
    let sql = r#"
-- name: SyncItems :one
-- @ids bigint[]
-- @amounts double precision[]
WITH input(child_id, amount) AS (
    SELECT * FROM unnest(@ids::bigint[], @amounts::double precision[])
),
ups AS (
    INSERT INTO item (id, parent_id, amount)
    SELECT child_id, @parent_id, amount FROM input
    ON CONFLICT (id) DO UPDATE SET amount = excluded.amount
    RETURNING 1
)
SELECT count(*)::bigint AS synced FROM ups;

-- name: DeleteItemsNotIn :exec
-- @keep_ids bigint[]
DELETE FROM item WHERE parent_id = @parent_id AND id <> ALL(@keep_ids::bigint[]);
"#;
    let queries = crate::frontend::postgres::query::parse_queries(sql, &schema, None).unwrap();
    let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() };
    let files = pg().generate(&schema, &queries, &cfg).unwrap();
    let src = get_file(&files, "queries.rs");

    // Defect 1: no stray dynamic `IN (...)` fragment spliced onto the SQL.
    assert!(!src.contains("{placeholders}"), "must not splice a dynamic IN list onto a direct-array query");
    assert!(!src.contains("upsIN"), "must not splice IN onto `FROM ups`");
    assert!(!src.contains("let placeholders"), "direct array binds natively — no runtime placeholder building");
    // Signatures use slices.
    assert!(src.contains("ids: &[i64]"), "ids should be a slice");
    assert!(src.contains("amounts: &[f64]"), "amounts should be a slice");
    assert!(src.contains("keep_ids: &[i64]"), "keep_ids should be a slice");
    // Defect 2: the array casts stay in the SQL and the slice is bound directly.
    assert!(src.contains("unnest($1::bigint[], $2::double precision[])"), "unnest SQL left structurally intact");
    assert!(src.contains("<> ALL($2::bigint[])"), "ALL SQL left structurally intact");
    // Defect 3: the :exec uses the exec path, never query_as::<_, serde_json::Value>.
    assert!(!src.contains("serde_json::Value"), ":exec must not use query_as with serde_json::Value");
}
