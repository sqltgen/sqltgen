//! E2E snapshot tests for sqltgen codegen.
//!
//! Each test feeds fixture SQL (schema + queries) through the full pipeline
//! (frontend → IR → backend) and compares the output against golden files.
//!
//! Set `UPDATE_GOLDEN=1` to regenerate golden files after intentional changes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sqltgen::backend::java::{JavaCodegen, JavaTarget};
use sqltgen::backend::kotlin::{KotlinCodegen, KotlinTarget};
use sqltgen::backend::python::{PythonCodegen, PythonTarget};
use sqltgen::backend::rust::{RustCodegen, RustTarget};
use sqltgen::backend::typescript::{JsOutput, JsTarget, TypeScriptCodegen};
use sqltgen::backend::{Codegen, GeneratedFile};
use sqltgen::config::OutputConfig;
use sqltgen::frontend::mysql::MysqlParser;
use sqltgen::frontend::postgres::PostgresParser;
use sqltgen::frontend::sqlite::SqliteParser;
use sqltgen::frontend::DialectParser;
use sqltgen::ir::{Query, Schema};

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
    OutputConfig { out: "out".to_string(), package: "db".to_string(), list_params: None }
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
    let golden_dir = e2e_root().join("golden").join(backend_name).join(dialect);
    check_golden(&golden_dir, &files);
}

// ─── Rust backend ──────────────────────────────────────────────────────────

#[test]
fn snapshot_rust_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "rust", &RustCodegen { target: RustTarget::Postgres });
}

#[test]
fn snapshot_rust_sqlite() {
    snapshot_test("sqlite", &SqliteParser, "rust", &RustCodegen { target: RustTarget::Sqlite });
}

#[test]
fn snapshot_rust_mysql() {
    snapshot_test("mysql", &MysqlParser, "rust", &RustCodegen { target: RustTarget::Mysql });
}

// ─── Java backend ──────────────────────────────────────────────────────────

#[test]
fn snapshot_java_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "java", &JavaCodegen { target: JavaTarget::Postgres });
}

// ─── Kotlin backend ────────────────────────────────────────────────────────

#[test]
fn snapshot_kotlin_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "kotlin", &KotlinCodegen { target: KotlinTarget::Postgres });
}

// ─── Python backend ────────────────────────────────────────────────────────

#[test]
fn snapshot_python_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "python", &PythonCodegen { target: PythonTarget::Postgres });
}

#[test]
fn snapshot_python_sqlite() {
    snapshot_test("sqlite", &SqliteParser, "python", &PythonCodegen { target: PythonTarget::Sqlite });
}

#[test]
fn snapshot_python_mysql() {
    snapshot_test("mysql", &MysqlParser, "python", &PythonCodegen { target: PythonTarget::Mysql });
}

// ─── TypeScript backend ────────────────────────────────────────────────────

#[test]
fn snapshot_typescript_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "typescript", &TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_typescript_sqlite() {
    snapshot_test("sqlite", &SqliteParser, "typescript", &TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::TypeScript });
}

#[test]
fn snapshot_typescript_mysql() {
    snapshot_test("mysql", &MysqlParser, "typescript", &TypeScriptCodegen { target: JsTarget::Mysql, output: JsOutput::TypeScript });
}

// ─── JavaScript backend ───────────────────────────────────────────────────

#[test]
fn snapshot_javascript_postgresql() {
    snapshot_test("postgresql", &PostgresParser, "javascript", &TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_javascript_sqlite() {
    snapshot_test("sqlite", &SqliteParser, "javascript", &TypeScriptCodegen { target: JsTarget::Sqlite, output: JsOutput::JavaScript });
}

#[test]
fn snapshot_javascript_mysql() {
    snapshot_test("mysql", &MysqlParser, "javascript", &TypeScriptCodegen { target: JsTarget::Mysql, output: JsOutput::JavaScript });
}
