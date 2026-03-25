//! E2E snapshot tests for sqltgen codegen.
//!
//! Each test feeds fixture SQL (schema + queries) through the full pipeline
//! (frontend → IR → backend) and compares the output against golden files.
//!
//! Set `UPDATE_GOLDEN=1` to regenerate golden files after intentional changes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sqltgen::backend::go::{GoCodegen, GoTarget};
use sqltgen::backend::java::JavaCodegen;
use sqltgen::backend::jdbc::JdbcTarget;
use sqltgen::backend::kotlin::KotlinCodegen;
use sqltgen::backend::python::{PythonCodegen, PythonTarget};
use sqltgen::backend::rust::{RustCodegen, RustTarget};
use sqltgen::backend::typescript::{JsOutput, JsTarget, TypeScriptCodegen};
use sqltgen::backend::{Codegen, GeneratedFile};
use sqltgen::config::{OutputConfig, TypeOverride, TypeRef};
use sqltgen::frontend::mysql::MysqlParser;
use sqltgen::frontend::postgres::PostgresParser;
use sqltgen::frontend::sqlite::SqliteParser;
use sqltgen::frontend::DialectParser;
use sqltgen::ir::{Query, Schema};

// ─── Error resilience helpers ──────────────────────────────────────────────

/// Parse queries against a schema and run codegen. Returns the generated files
/// without comparing to golden — used for error resilience tests that verify
/// the pipeline doesn't crash on edge-case input.
fn parse_and_generate(parser: &dyn DialectParser, ddl: &str, queries_sql: &str, codegen: &dyn Codegen) -> Vec<GeneratedFile> {
    let schema = parser.parse_schema(ddl).expect("schema parse should not fail");
    let queries = parser.parse_queries(queries_sql, &schema).expect("query parse should not fail");
    let config = output_config();
    codegen.generate(&schema, &queries, &config).expect("codegen should not fail")
}

/// Root of the e2e test tree, relative to crate root.
fn e2e_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/e2e")
}

/// Load fixture schema and queries for a dialect.
fn load_fixtures(dialect: &str, parser: &dyn DialectParser) -> (Schema, Vec<Query>) {
    let root = e2e_root().join("fixtures").join(dialect);
    let ddl = std::fs::read_to_string(root.join("schema.sql")).unwrap_or_else(|e| panic!("reading {dialect}/schema.sql: {e}"));
    let queries_sql = std::fs::read_to_string(root.join("queries.sql")).unwrap_or_else(|e| panic!("reading {dialect}/queries.sql: {e}"));

    let schema = parser.parse_schema(&ddl).unwrap_or_else(|e| panic!("parsing {dialect} schema: {e}"));
    let queries = parser.parse_queries(&queries_sql, &schema).unwrap_or_else(|e| panic!("parsing {dialect} queries: {e}"));
    (schema, queries)
}

/// Default output config for snapshot tests.
fn output_config() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: "db".to_string(), list_params: None, ..Default::default() }
}

/// Run a codegen backend and return the generated files sorted by path.
fn run_codegen(codegen: &dyn Codegen, schema: &Schema, queries: &[Query]) -> Vec<GeneratedFile> {
    let config = output_config();
    let mut files = codegen.generate(schema, queries, &config).expect("codegen failed");
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files
}

/// Compare generated files against golden files in the given directory.
/// If `UPDATE_GOLDEN=1`, write the generated files as the new golden files.
fn check_golden(golden_dir: &Path, files: &[GeneratedFile]) {
    let update = std::env::var("UPDATE_GOLDEN").is_ok_and(|v| v == "1");

    if update {
        // Clear old golden dir and write fresh
        if golden_dir.exists() {
            std::fs::remove_dir_all(golden_dir).expect("removing old golden dir");
        }
        std::fs::create_dir_all(golden_dir).expect("creating golden dir");

        // Write a manifest so we know which files to expect
        let manifest: Vec<String> = files.iter().map(|f| f.path.to_string_lossy().to_string()).collect();
        std::fs::write(golden_dir.join("MANIFEST"), manifest.join("\n") + "\n").expect("writing MANIFEST");

        for file in files {
            let dest = golden_dir.join(&file.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).expect("creating golden subdirs");
            }
            std::fs::write(&dest, &file.content).expect("writing golden file");
        }
        return;
    }

    // Read manifest to know expected files
    let manifest_path = golden_dir.join("MANIFEST");
    assert!(manifest_path.exists(), "Golden files not found at {}. Run with UPDATE_GOLDEN=1 to generate them.", golden_dir.display());

    let manifest = std::fs::read_to_string(&manifest_path).expect("reading MANIFEST");
    let expected_paths: Vec<&str> = manifest.lines().filter(|l| !l.is_empty()).collect();

    // Build a map of generated files for easy lookup
    let generated: BTreeMap<String, &str> = files.iter().map(|f| (f.path.to_string_lossy().to_string(), f.content.as_str())).collect();

    // Check that we have exactly the expected files
    let gen_paths: Vec<String> = generated.keys().cloned().collect();
    let exp_paths: Vec<String> = expected_paths.iter().map(|s| s.to_string()).collect();
    assert_eq!(gen_paths, exp_paths, "Generated file list differs from golden MANIFEST.\n  Generated: {gen_paths:?}\n  Expected:  {exp_paths:?}");

    // Compare each file byte-for-byte
    for path_str in &expected_paths {
        let golden_file = golden_dir.join(path_str);
        let expected = std::fs::read_to_string(&golden_file).unwrap_or_else(|e| panic!("reading golden file {}: {e}", golden_file.display()));
        let actual = generated[*path_str];

        if expected != actual {
            // Show a useful diff
            let mut diff = String::new();
            diff.push_str(&format!("--- golden/{path_str}\n+++ generated/{path_str}\n"));
            for line in unified_diff(&expected, actual) {
                diff.push_str(&line);
                diff.push('\n');
            }
            panic!("Golden file mismatch for {path_str}:\n{diff}\nRun with UPDATE_GOLDEN=1 to update.");
        }
    }
}

/// Simple unified-diff-like output for readable test failures.
fn unified_diff(expected: &str, actual: &str) -> Vec<String> {
    let exp_lines: Vec<&str> = expected.lines().collect();
    let act_lines: Vec<&str> = actual.lines().collect();
    let mut out = Vec::new();

    let max = exp_lines.len().max(act_lines.len());
    for i in 0..max {
        let e = exp_lines.get(i).copied().unwrap_or("");
        let a = act_lines.get(i).copied().unwrap_or("");
        if e != a {
            if i < exp_lines.len() {
                out.push(format!("-{:>4}| {e}", i + 1));
            }
            if i < act_lines.len() {
                out.push(format!("+{:>4}| {a}", i + 1));
            }
        }
    }
    out
}

/// Helper to run a full snapshot test for a backend × dialect combination.
fn snapshot_test(dialect: &str, parser: &dyn DialectParser, backend_name: &str, codegen: &dyn Codegen) {
    let (schema, queries) = load_fixtures(dialect, parser);
    let files = run_codegen(codegen, &schema, &queries);
    // Path: golden/<fixture>/<backend>/<dialect>  (dialect may include a sub-path like "bookstore/postgresql")
    let golden_dir = match dialect.split_once('/') {
        Some((fixture, db_dialect)) => e2e_root().join("golden").join(fixture).join(backend_name).join(db_dialect),
        None => e2e_root().join("golden").join(dialect).join(backend_name),
    };
    check_golden(&golden_dir, &files);
}

// ─── Rust backend ──────────────────────────────────────────────────────────

#[test]
fn snapshot_rust_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "rust", &RustCodegen { target: RustTarget::Postgres });
}

#[test]
fn snapshot_rust_sqlite() {
    snapshot_test("bookstore/sqlite", &SqliteParser, "rust", &RustCodegen { target: RustTarget::Sqlite });
}

#[test]
fn snapshot_rust_mysql() {
    snapshot_test("bookstore/mysql", &MysqlParser, "rust", &RustCodegen { target: RustTarget::Mysql });
}

// ─── Java backend ──────────────────────────────────────────────────────────

#[test]
fn snapshot_java_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "java", &JavaCodegen { target: JdbcTarget::Postgres });
}

// ─── Kotlin backend ────────────────────────────────────────────────────────

#[test]
fn snapshot_kotlin_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "kotlin", &KotlinCodegen { target: JdbcTarget::Postgres });
}

// ─── Python backend ────────────────────────────────────────────────────────

#[test]
fn snapshot_python_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "python", &PythonCodegen { target: PythonTarget::Psycopg });
}

#[test]
fn snapshot_python_sqlite() {
    snapshot_test("bookstore/sqlite", &SqliteParser, "python", &PythonCodegen { target: PythonTarget::Sqlite3 });
}

#[test]
fn snapshot_python_mysql() {
    snapshot_test("bookstore/mysql", &MysqlParser, "python", &PythonCodegen { target: PythonTarget::MysqlConnector });
}

// ─── TypeScript backend ────────────────────────────────────────────────────

#[test]
fn snapshot_typescript_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "typescript", &TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_typescript_sqlite() {
    snapshot_test("bookstore/sqlite", &SqliteParser, "typescript", &TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_typescript_mysql() {
    snapshot_test("bookstore/mysql", &MysqlParser, "typescript", &TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::TypeScript });
}

// ─── JavaScript backend ───────────────────────────────────────────────────

#[test]
fn snapshot_javascript_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "javascript", &TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_javascript_sqlite() {
    snapshot_test("bookstore/sqlite", &SqliteParser, "javascript", &TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_javascript_mysql() {
    snapshot_test("bookstore/mysql", &MysqlParser, "javascript", &TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::JavaScript });
}

// ─── Go backend ───────────────────────────────────────────────────────────

#[test]
fn snapshot_go_postgresql() {
    snapshot_test("bookstore/postgresql", &PostgresParser, "go", &GoCodegen { target: GoTarget::Postgres });
}

#[test]
fn snapshot_go_sqlite() {
    snapshot_test("bookstore/sqlite", &SqliteParser, "go", &GoCodegen { target: GoTarget::Sqlite });
}

#[test]
fn snapshot_go_mysql() {
    snapshot_test("bookstore/mysql", &MysqlParser, "go", &GoCodegen { target: GoTarget::Mysql });
}

// ─── Views fixture snapshot tests ─────────────────────────────────────────

#[test]
fn snapshot_views_rust_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "rust", &RustCodegen { target: RustTarget::Postgres });
}

#[test]
fn snapshot_views_rust_sqlite() {
    snapshot_test("views/sqlite", &SqliteParser, "rust", &RustCodegen { target: RustTarget::Sqlite });
}

#[test]
fn snapshot_views_rust_mysql() {
    snapshot_test("views/mysql", &MysqlParser, "rust", &RustCodegen { target: RustTarget::Mysql });
}

#[test]
fn snapshot_views_java_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "java", &JavaCodegen { target: JdbcTarget::Postgres });
}

#[test]
fn snapshot_views_kotlin_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "kotlin", &KotlinCodegen { target: JdbcTarget::Postgres });
}

#[test]
fn snapshot_views_python_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "python", &PythonCodegen { target: PythonTarget::Psycopg });
}

#[test]
fn snapshot_views_python_sqlite() {
    snapshot_test("views/sqlite", &SqliteParser, "python", &PythonCodegen { target: PythonTarget::Sqlite3 });
}

#[test]
fn snapshot_views_python_mysql() {
    snapshot_test("views/mysql", &MysqlParser, "python", &PythonCodegen { target: PythonTarget::MysqlConnector });
}

#[test]
fn snapshot_views_typescript_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "typescript", &TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_views_typescript_sqlite() {
    snapshot_test("views/sqlite", &SqliteParser, "typescript", &TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_views_typescript_mysql() {
    snapshot_test("views/mysql", &MysqlParser, "typescript", &TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_views_javascript_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "javascript", &TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_views_javascript_sqlite() {
    snapshot_test("views/sqlite", &SqliteParser, "javascript", &TypeScriptCodegen { target: JsTarget::BetterSqlite3, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_views_javascript_mysql() {
    snapshot_test("views/mysql", &MysqlParser, "javascript", &TypeScriptCodegen { target: JsTarget::Mysql2, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_views_go_postgresql() {
    snapshot_test("views/postgresql", &PostgresParser, "go", &GoCodegen { target: GoTarget::Postgres });
}

#[test]
fn snapshot_views_go_sqlite() {
    snapshot_test("views/sqlite", &SqliteParser, "go", &GoCodegen { target: GoTarget::Sqlite });
}

#[test]
fn snapshot_views_go_mysql() {
    snapshot_test("views/mysql", &MysqlParser, "go", &GoCodegen { target: GoTarget::Mysql });
}

// ─── Provenance snapshot tests ────────────────────────────────────────────
//
// Verify that source_table is correctly resolved through CTEs and derived
// tables, and that this flows through to the codegen output (the generated
// functions should reuse the existing Users model type rather than emitting
// an anonymous result struct).

#[test]
fn snapshot_rust_provenance() {
    snapshot_test("provenance", &PostgresParser, "rust", &RustCodegen { target: RustTarget::Postgres });
}

// ─── Type override snapshot tests ────────────────────────────────────────
//
// These use a dedicated fixture with JSON/JSONB columns and per-backend
// preset configurations (jackson, serde_json, object).

fn snapshot_test_with_config(fixture: &str, parser: &dyn DialectParser, backend_name: &str, codegen: &dyn Codegen, config: OutputConfig) {
    let (schema, queries) = load_fixtures(fixture, parser);
    let mut files = codegen.generate(&schema, &queries, &config).expect("codegen failed");
    files.sort_by(|a, b| a.path.cmp(&b.path));
    // Path: golden/<fixture>/<backend>  (no dialect sub-path for fixture-level tests)
    let golden_dir = e2e_root().join("golden").join(fixture).join(backend_name);
    check_golden(&golden_dir, &files);
}

/// OutputConfig with all common type overrides for Java/Kotlin (jackson preset + FQN temporal/uuid).
fn config_jackson() -> OutputConfig {
    let mut cfg = output_config();
    let fqn = |s: &str| TypeOverride::Same(TypeRef::String(s.to_string()));
    cfg.type_overrides.insert("json".to_string(), fqn("jackson"));
    cfg.type_overrides.insert("jsonb".to_string(), fqn("jackson"));
    cfg.type_overrides.insert("uuid".to_string(), fqn("java.util.UUID"));
    cfg.type_overrides.insert("date".to_string(), fqn("java.time.LocalDate"));
    cfg.type_overrides.insert("time".to_string(), fqn("java.time.LocalTime"));
    cfg.type_overrides.insert("timestamp".to_string(), fqn("java.time.LocalDateTime"));
    cfg.type_overrides.insert("timestamptz".to_string(), fqn("java.time.OffsetDateTime"));
    cfg
}

/// OutputConfig with JSON/JSONB → serde_json preset (Rust).
fn config_serde_json() -> OutputConfig {
    let mut cfg = output_config();
    let preset = |s: &str| TypeOverride::Same(TypeRef::String(s.to_string()));
    cfg.type_overrides.insert("json".to_string(), preset("serde_json"));
    cfg.type_overrides.insert("jsonb".to_string(), preset("serde_json"));
    cfg
}

/// OutputConfig with JSON/JSONB → object preset (TypeScript/JavaScript).
fn config_object() -> OutputConfig {
    let mut cfg = output_config();
    let preset = |s: &str| TypeOverride::Same(TypeRef::String(s.to_string()));
    cfg.type_overrides.insert("json".to_string(), preset("object"));
    cfg.type_overrides.insert("jsonb".to_string(), preset("object"));
    cfg
}

#[test]
fn snapshot_type_overrides_java() {
    snapshot_test_with_config("type_overrides", &PostgresParser, "java", &JavaCodegen { target: JdbcTarget::Postgres }, config_jackson());
}

#[test]
fn snapshot_type_overrides_kotlin() {
    snapshot_test_with_config("type_overrides", &PostgresParser, "kotlin", &KotlinCodegen { target: JdbcTarget::Postgres }, config_jackson());
}

#[test]
fn snapshot_type_overrides_rust() {
    snapshot_test_with_config("type_overrides", &PostgresParser, "rust", &RustCodegen { target: RustTarget::Postgres }, config_serde_json());
}

#[test]
fn snapshot_type_overrides_typescript() {
    snapshot_test_with_config(
        "type_overrides",
        &PostgresParser,
        "typescript",
        &TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript },
        config_object(),
    );
}

// ─── Error resilience tests ───────────────────────────────────────────────
//
// These verify the pipeline handles bad/edge-case input gracefully without
// panicking.  They don't compare golden files — just assert no crash.

const SIMPLE_PG_SCHEMA: &str = include_str!("fixtures/resilience/postgresql/schema.sql");

#[test]
fn resilience_unknown_table_query_skipped() {
    let queries_sql = "-- name: GetGhost :one\nSELECT id, name FROM ghost WHERE id = $1;\n";
    let files = parse_and_generate(&PostgresParser, SIMPLE_PG_SCHEMA, queries_sql, &RustCodegen { target: RustTarget::Postgres });
    // Query references unknown table "ghost" — should be skipped, but
    // codegen should still produce model files.
    assert!(!files.is_empty());
}

#[test]
fn resilience_empty_query_file() {
    let files = parse_and_generate(&PostgresParser, SIMPLE_PG_SCHEMA, "", &RustCodegen { target: RustTarget::Postgres });
    // No queries → only model files (or possibly nothing), but no crash.
    let _ = files;
}

#[test]
fn resilience_comment_only_query_file() {
    let queries_sql = "-- just a comment\n-- another comment\n";
    let files = parse_and_generate(&PostgresParser, SIMPLE_PG_SCHEMA, queries_sql, &RustCodegen { target: RustTarget::Postgres });
    let _ = files;
}

#[test]
fn resilience_malformed_annotation_skipped() {
    // Missing command (:one/:many/:exec/:execrows)
    let queries_sql = "-- name: BadQuery\nSELECT id FROM users;\n";
    let files = parse_and_generate(&PostgresParser, SIMPLE_PG_SCHEMA, queries_sql, &RustCodegen { target: RustTarget::Postgres });
    let _ = files;
}

#[test]
fn resilience_unknown_column_graceful() {
    let queries_sql = "-- name: GetUserMissing :one\nSELECT id, name, nonexistent FROM users WHERE id = $1;\n";
    let files = parse_and_generate(&PostgresParser, SIMPLE_PG_SCHEMA, queries_sql, &RustCodegen { target: RustTarget::Postgres });
    assert!(!files.is_empty());
}

#[test]
fn resilience_all_backends_empty_queries() {
    let backends: Vec<Box<dyn Codegen>> = vec![
        Box::new(RustCodegen { target: RustTarget::Postgres }),
        Box::new(PythonCodegen { target: PythonTarget::Psycopg }),
        Box::new(JavaCodegen { target: JdbcTarget::Postgres }),
        Box::new(KotlinCodegen { target: JdbcTarget::Postgres }),
        Box::new(GoCodegen { target: GoTarget::Postgres }),
        Box::new(TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::TypeScript }),
        Box::new(TypeScriptCodegen { target: JsTarget::Pg, output: JsOutput::JavaScript }),
    ];
    let schema = PostgresParser.parse_schema(SIMPLE_PG_SCHEMA).unwrap();
    let config = output_config();
    for backend in &backends {
        let _ = backend.generate(&schema, &[], &config).expect("codegen with no queries should not fail");
    }
}
