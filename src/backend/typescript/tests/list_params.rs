use super::*;

fn list_by_ids_query() -> Query {
    Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1)",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    )
}

fn dynamic_cfg() -> OutputConfig {
    OutputConfig { out: "src".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic), ..Default::default() }
}

fn src_has_sql_constant(content: &str, needle: &str) -> bool {
    // SQL constants are emitted as: CONST_NAME = "...sql..."
    // They appear before the function definitions.
    content.contains(needle)
}

// ─── list params ─────────────────────────────────────────────────────────

#[test]
fn test_pg_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant rewrites IN ($1) → = ANY($1); pg accepts a JS array directly.
    assert!(src_has_sql_constant(&content, "= ANY($1)"), "PG native should rewrite to = ANY");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    assert!(!content.contains("JSON.stringify"), "PG native must not call JSON.stringify");
    // Args array includes ids at its position.
    assert!(content.contains("[ids]"), "ids array passed directly to pg");
}

#[test]
fn test_pg_native_list_param_js() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::JavaScript, &config()).unwrap();
    assert!(src_has_sql_constant(&content, "= ANY($1)"), "PG native should rewrite to = ANY");
    // JS output uses JSDoc comments, not inline TypeScript type annotations.
    assert!(!content.contains("ids: number[]"), "JS output must not use inline TS type annotations");
}

#[test]
fn test_pg_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // Dynamic builds $N numbered placeholders at runtime.
    assert!(content.contains("'$' +"), "PG dynamic builds $N placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args array");
    // Dynamic must NOT emit a SQL constant.
    assert!(!content.contains("GET_BY_IDS"), "dynamic strategy must not emit a SQL constant");
}

#[test]
fn test_pg_dynamic_list_param_with_scalar_before() {
    // Scalar before IN: arg order must be [scalar, ...list].
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE active = $1 AND id IN ($2)",
        vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("active, ...ids"), "scalar before IN must come first in args");
}

#[test]
fn test_pg_dynamic_list_param_with_scalar_after() {
    // Scalar after IN: arg order must be [...list, scalar].
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("...ids, active"), "scalar after IN must follow list in args");
}

#[test]
fn test_sqlite_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant uses json_each for SQLite.
    assert!(src_has_sql_constant(&content, "json_each"), "SQLite native should use json_each");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // SQLite native uses JSON.stringify at runtime.
    assert!(content.contains("JSON.stringify(ids)"), "SQLite native must JSON.stringify the list");
    assert!(content.contains("idsJson"), "JSON variable should be named idsJson");
    assert!(!content.contains("= ANY"), "SQLite must not use PG ANY syntax");
}

#[test]
fn test_sqlite_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // SQLite dynamic uses anonymous ? placeholders.
    assert!(content.contains(r#"() => "?""#), "SQLite dynamic must use ? placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args");
    assert!(!content.contains("'$' +"), "SQLite dynamic must not use $N placeholders");
}

#[test]
fn test_sqlite_dynamic_list_param_with_scalar_after() {
    // TypeScript correctly places scalar-after after the list (no Bug B for TS).
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByIds",
        "SELECT id FROM t WHERE id IN ($1) AND active = $2",
        vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Sqlite, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("...ids, active"), "scalar after IN must follow list in args");
}

#[test]
fn test_mysql_native_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &config()).unwrap();
    // SQL constant uses JSON_TABLE for MySQL.
    assert!(src_has_sql_constant(&content, "JSON_TABLE"), "MySQL native should use JSON_TABLE");
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    assert!(content.contains("JSON.stringify(ids)"), "MySQL native must JSON.stringify the list");
    assert!(content.contains("idsJson"), "JSON variable should be named idsJson");
    assert!(!content.contains("json_each"), "MySQL must not use SQLite json_each");
}

#[test]
fn test_mysql_dynamic_list_param_ts() {
    let schema = Schema { tables: vec![] };
    let content = build_queries_file(&[list_by_ids_query()], &schema, &JsTarget::Mysql, &JsOutput::TypeScript, &dynamic_cfg()).unwrap();
    assert!(content.contains("ids: number[]"), "list param must use number[] type");
    // MySQL dynamic uses anonymous ? placeholders (same as SQLite, unlike PG).
    assert!(content.contains(r#"() => "?""#), "MySQL dynamic must use ? placeholders");
    assert!(content.contains("placeholders"), "must assemble placeholder string");
    assert!(content.contains("...ids"), "list elements spread into args");
}

#[test]
fn test_list_param_text_type_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::many(
        "GetByTags",
        "SELECT id FROM t WHERE tag IN ($1)",
        vec![Parameter::list(1, "tags", SqlType::Text, false)],
        vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
    );
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("tags: string[]"), "Text list param should use string[] type");
}

#[test]
fn test_list_param_execrows_pg_ts() {
    let schema = Schema { tables: vec![] };
    let query = Query::exec_rows("DeleteByIds", "DELETE FROM t WHERE id IN ($1)", vec![Parameter::list(1, "ids", SqlType::BigInt, false)]);
    let content = build_queries_file(&[query], &schema, &JsTarget::Postgres, &JsOutput::TypeScript, &config()).unwrap();
    assert!(content.contains("Promise<number>"), "execrows should return Promise<number>");
    assert!(content.contains("rowCount"), "execrows should use rowCount");
}
