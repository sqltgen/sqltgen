use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};

use sqltgen::backend::{self, Codegen};
use sqltgen::config::{Engine, Language, SqltgenConfig};
use sqltgen::frontend::{mysql::MysqlParser, postgres::PostgresParser, sqlite::SqliteParser, DialectParser};

#[derive(Parser)]
#[command(name = "sqltgen", about = "SQL-to-code generator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate code from a sqltgen config file
    Generate {
        /// Path to the sqltgen JSON config file
        #[arg(long, short, default_value = "sqltgen.json")]
        config: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Generate { config } => run_generate(&config),
    }
}

/// Truncates `content` at the first line that exactly matches `stop_marker`
/// (after trimming whitespace). Used to discard the "down" section of
/// migration files that contain both up and down DDL.
fn strip_after_marker(content: &str, stop_marker: &str) -> String {
    let marker = stop_marker.trim();
    let mut out = String::new();
    for line in content.lines() {
        if line.trim() == marker {
            break;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn read_schema_ddl(path: &Path, stop_marker: Option<&str>) -> anyhow::Result<String> {
    let extract = |raw: String| match stop_marker {
        Some(marker) => strip_after_marker(&raw, marker),
        None => raw,
    };

    if path.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path)
            .with_context(|| format!("reading schema directory: {}", path.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "sql"))
            .collect();
        entries.sort();
        let mut ddl = String::new();
        for entry in &entries {
            let raw = std::fs::read_to_string(entry).with_context(|| format!("reading schema file: {}", entry.display()))?;
            ddl.push_str(&extract(raw));
            ddl.push('\n');
        }
        Ok(ddl)
    } else {
        let raw = std::fs::read_to_string(path).with_context(|| format!("reading schema file: {}", path.display()))?;
        Ok(extract(raw))
    }
}

fn run_generate(config_path: &Path) -> anyhow::Result<()> {
    let cfg = SqltgenConfig::load(config_path)?;

    let base_dir = config_path.parent().unwrap_or(Path::new("."));

    // Select dialect parser
    let parser: Box<dyn DialectParser> = match cfg.engine {
        Engine::Postgresql => Box::new(PostgresParser),
        Engine::Sqlite => Box::new(SqliteParser),
        Engine::Mysql => Box::new(MysqlParser),
    };

    // Effective default schema: user override > engine default
    let default_schema = cfg.default_schema.as_deref().or(cfg.engine.default_schema());

    // Read and parse schema (supports single file or directory of .sql files)
    let schema_path = base_dir.join(&cfg.schema);
    let ddl = read_schema_ddl(&schema_path, cfg.schema_stop_marker.as_deref())?;
    let schema = parser.parse_schema(&ddl, default_schema)?;

    // Read and parse queries (supports multiple files / globs)
    let query_paths = cfg.expand_queries(base_dir)?;
    let mut queries = Vec::new();
    for (query_path, group) in query_paths {
        let queries_sql = std::fs::read_to_string(&query_path).with_context(|| format!("reading queries file: {}", query_path.display()))?;
        let mut parsed = parser.parse_queries(&queries_sql, &schema, default_schema)?;
        for q in &mut parsed {
            q.group = group.clone();
        }
        queries.append(&mut parsed);
    }

    // Resolve Custom(name) → Enum(name) in query result columns and parameters
    let enum_names = schema.enum_names();
    sqltgen::ir::resolve_enum_in_queries(&mut queries, &enum_names);

    // Run each configured codegen target
    for (lang, output_config) in &cfg.gen {
        let driver = output_config.driver.as_deref();
        let codegen: Box<dyn Codegen> = match lang {
            Language::Java => Box::new(backend::java::JavaCodegen { target: backend::jdbc::JdbcTarget::from_engine_and_driver(cfg.engine, driver)? }),
            Language::Kotlin => Box::new(backend::kotlin::KotlinCodegen { target: backend::jdbc::JdbcTarget::from_engine_and_driver(cfg.engine, driver)? }),
            Language::Rust => Box::new(backend::rust::RustCodegen { target: backend::rust::RustTarget::from_engine_and_driver(cfg.engine, driver)? }),
            Language::Go => Box::new(backend::go::GoCodegen { target: backend::go::GoTarget::from_engine_and_driver(cfg.engine, driver)? }),
            Language::Python => Box::new(backend::python::PythonCodegen { target: backend::python::PythonTarget::from_engine_and_driver(cfg.engine, driver)? }),
            Language::TypeScript => Box::new(backend::typescript::TypeScriptCodegen {
                target: backend::typescript::JsTarget::from_engine_and_driver(cfg.engine, driver)?,
                output: backend::typescript::JsOutput::TypeScript,
            }),
            Language::JavaScript => Box::new(backend::typescript::TypeScriptCodegen {
                target: backend::typescript::JsTarget::from_engine_and_driver(cfg.engine, driver)?,
                output: backend::typescript::JsOutput::JavaScript,
            }),
        };

        let files = codegen.generate(&schema, &queries, output_config)?;

        for file in files {
            let dest = base_dir.join(&file.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).with_context(|| format!("creating directory: {}", parent.display()))?;
            }
            std::fs::write(&dest, &file.content).with_context(|| format!("writing file: {}", dest.display()))?;
            println!("wrote {}", dest.display());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqltgen::config::SqltgenConfig;
    use sqltgen::ir::SqlType;
    use sqltgen::{frontend::postgres::PostgresParser, frontend::DialectParser};

    #[test]
    fn test_strip_after_marker_dbmate() {
        let input = "-- migrate:up\nCREATE TABLE t (id INT);\n-- migrate:down\nDROP TABLE t;\n";
        let result = strip_after_marker(input, "-- migrate:down");
        assert!(result.contains("CREATE TABLE t"));
        assert!(!result.contains("DROP TABLE"));
        assert!(!result.contains("migrate:down"));
    }

    #[test]
    fn test_strip_after_marker_no_marker() {
        let input = "CREATE TABLE t (id INT);\n";
        let result = strip_after_marker(input, "-- migrate:down");
        assert_eq!(result, "CREATE TABLE t (id INT);\n");
    }

    #[test]
    fn test_strip_after_marker_trims_whitespace() {
        let input = "CREATE TABLE t (id INT);\n  -- migrate:down  \nDROP TABLE t;\n";
        let result = strip_after_marker(input, "-- migrate:down");
        assert!(result.contains("CREATE TABLE t"));
        assert!(!result.contains("DROP TABLE"));
    }

    #[test]
    fn test_default_schema_from_config_is_applied_to_query_resolution() {
        let cfg = SqltgenConfig::from_json(
            r#"{
            "version": "1",
            "engine": "postgresql",
            "schema": "schema.sql",
            "queries": "queries.sql",
            "default_schema": "internal",
            "gen": {}
        }"#,
        )
        .unwrap();

        let parser = PostgresParser;
        let default_schema = cfg.default_schema.as_deref().or(cfg.engine.default_schema());
        let schema = parser
            .parse_schema(
                r#"
                CREATE TABLE public.users (id INTEGER PRIMARY KEY);
                CREATE TABLE internal.users (id BIGINT PRIMARY KEY);
                "#,
                default_schema,
            )
            .unwrap();

        let sql = "-- name: GetUser :one\nSELECT id FROM users WHERE id = $1;";

        let queries_from_runtime = parser.parse_queries(sql, &schema, default_schema).unwrap();
        assert_eq!(cfg.default_schema.as_deref(), Some("internal"));
        assert_eq!(queries_from_runtime[0].result_columns[0].sql_type, SqlType::BigInt);
    }
}
