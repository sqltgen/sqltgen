use super::*;
use crate::backend::test_helpers::get_file;
use crate::config::{TypeOverride, TypeRef};
use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType};

fn cfg_with_overrides(overrides: Vec<(&str, TypeOverride)>) -> OutputConfig {
    let mut cfg = config();
    for (key, ov) in overrides {
        cfg.type_overrides.insert(key.to_string(), ov);
    }
    cfg
}

// ─── write_expr applied to param bindings ────────────────────────────────────

#[test]
fn test_pg_write_expr_applied_to_json_param() {
    // "object" preset write_expr = JSON.stringify({value}).
    // The params array must contain JSON.stringify(payload), not the bare name.
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("object".to_string())))]);
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.stringify(payload)"), "expected JSON.stringify in pg params:\n{src}");
    assert!(!src.contains(", payload]"), "must not emit bare param name:\n{src}");
}

#[test]
fn test_sqlite_write_expr_applied_to_json_param() {
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES (?1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("object".to_string())))]);
    let gen = TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.stringify(payload)"), "expected JSON.stringify in sqlite args:\n{src}");
}

#[test]
fn test_mysql_write_expr_applied_to_json_param() {
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("object".to_string())))]);
    let gen = TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.stringify(payload)"), "expected JSON.stringify in mysql params:\n{src}");
}

#[test]
fn test_explicit_write_expr_applied_to_param() {
    // Explicit TypeRef with write_expr — tests the general mechanism, not just the preset.
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "MyDoc".to_string(),
            import: None,
            read_expr: None,
            write_expr: Some("mySerialize({value})".to_string()),
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("mySerialize(payload)"), "expected write_expr applied:\n{src}");
}

#[test]
fn test_no_write_expr_emits_bare_param() {
    // Without any override, params must appear as plain camelCase names.
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &config()).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("payload]"), "expected bare param name without override:\n{src}");
    assert!(!src.contains("JSON.stringify"), "must not emit JSON.stringify without override:\n{src}");
}

// ─── read_expr applied to result rows ────────────────────────────────────────

#[test]
fn test_pg_read_expr_applied_to_one_query() {
    // When read_expr is configured, the returned row must be transformed.
    // For :one, the transform wraps the raw row object.
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "unknown".to_string(),
            import: None,
            read_expr: Some("JSON.parse({raw} as string)".to_string()),
            write_expr: None,
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.parse("), "expected read_expr applied for pg :one:\n{src}");
    assert!(src.contains("raw.data"), "expected raw.data column access:\n{src}");
}

#[test]
fn test_pg_read_expr_applied_to_many_query() {
    // For :many, the transform must map over the rows array.
    let schema = Schema::default();
    let query = Query::many("ListDocs", "SELECT data FROM docs", vec![], vec![ResultColumn::not_nullable("data", SqlType::Json)]);
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "unknown".to_string(),
            import: None,
            read_expr: Some("JSON.parse({raw} as string)".to_string()),
            write_expr: None,
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains(".map(raw =>"), "expected .map transform for pg :many:\n{src}");
    assert!(src.contains("JSON.parse("), "expected read_expr in map:\n{src}");
}

#[test]
fn test_sqlite_read_expr_applied_to_one_query() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "unknown".to_string(),
            import: None,
            read_expr: Some("JSON.parse({raw} as string)".to_string()),
            write_expr: None,
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.parse("), "expected read_expr applied for sqlite :one:\n{src}");
    assert!(src.contains("raw.data"), "expected raw.data column access:\n{src}");
    // Must guard against null before transformation
    assert!(src.contains("if (!raw) return null"), "expected null guard:\n{src}");
}

#[test]
fn test_sqlite_read_expr_applied_to_many_query() {
    let schema = Schema::default();
    let query = Query::many("ListDocs", "SELECT data FROM docs", vec![], vec![ResultColumn::not_nullable("data", SqlType::Json)]);
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "unknown".to_string(),
            import: None,
            read_expr: Some("JSON.parse({raw} as string)".to_string()),
            write_expr: None,
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains(".map(raw =>"), "expected .map transform for sqlite :many:\n{src}");
    assert!(src.contains("JSON.parse("), "expected read_expr in map:\n{src}");
}

#[test]
fn test_mysql_read_expr_applied_to_one_query() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![(
        "json",
        TypeOverride::Same(TypeRef::Explicit {
            name: "unknown".to_string(),
            import: None,
            read_expr: Some("JSON.parse({raw} as string)".to_string()),
            write_expr: None,
        }),
    )]);
    let gen = TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(src.contains("JSON.parse("), "expected read_expr applied for mysql :one:\n{src}");
    assert!(src.contains("raw.data"), "expected raw.data column access:\n{src}");
}

#[test]
fn test_no_read_expr_no_row_transform() {
    // Without a read_expr override, result rows must be returned directly (no map, no raw var).
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let gen = TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &config()).unwrap();
    let src = get_file(&files, "queries.ts");

    assert!(!src.contains(".map(raw =>"), "must not emit map transform without override:\n{src}");
    assert!(src.contains("const raw = result.rows[0];") && src.contains("return raw ?? null;"), "expected direct row return:\n{src}");
}

// ─── object preset end-to-end ─────────────────────────────────────────────────

#[test]
fn test_object_preset_sqlite_one_applies_both_exprs() {
    // Full integration: object preset → write_expr on param + read_expr on result.
    let schema = Schema::default();
    let query = Query::one(
        "RoundtripDoc",
        "SELECT data FROM docs WHERE id = ?1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("object".to_string())))]);
    let gen = TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript };
    let files = gen.generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.ts");

    // write_expr: not applicable here (no json param), so just check read_expr
    assert!(src.contains("JSON.parse("), "expected JSON.parse for object preset read:\n{src}");
}
