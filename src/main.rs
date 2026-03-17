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

fn read_schema_ddl(path: &Path) -> anyhow::Result<String> {
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
            ddl.push_str(&std::fs::read_to_string(entry).with_context(|| format!("reading schema file: {}", entry.display()))?);
            ddl.push('\n');
        }
        Ok(ddl)
    } else {
        std::fs::read_to_string(path).with_context(|| format!("reading schema file: {}", path.display()))
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

    // Read and parse schema (supports single file or directory of .sql files)
    let schema_path = base_dir.join(&cfg.schema);
    let ddl = read_schema_ddl(&schema_path)?;
    let schema = parser.parse_schema(&ddl)?;

    // Read and parse queries (supports multiple files / globs)
    let query_paths = cfg.expand_queries(base_dir)?;
    let mut queries = Vec::new();
    for (query_path, group) in query_paths {
        let queries_sql = std::fs::read_to_string(&query_path).with_context(|| format!("reading queries file: {}", query_path.display()))?;
        let mut parsed = parser.parse_queries(&queries_sql, &schema)?;
        for q in &mut parsed {
            q.group = group.clone();
        }
        queries.append(&mut parsed);
    }

    // Run each configured codegen target
    for (lang, output_config) in &cfg.gen {
        let codegen: Box<dyn Codegen> = match lang {
            Language::Java => Box::new(backend::java::JavaCodegen { target: cfg.engine.into() }),
            Language::Kotlin => Box::new(backend::kotlin::KotlinCodegen { target: cfg.engine.into() }),
            Language::Rust => Box::new(backend::rust::RustCodegen { target: cfg.engine.into() }),
            Language::Go => Box::new(backend::go::GoCodegen { target: cfg.engine.into() }),
            Language::Python => Box::new(backend::python::PythonCodegen { target: cfg.engine.into() }),
            Language::TypeScript => {
                Box::new(backend::typescript::TypeScriptCodegen { target: cfg.engine.into(), output: backend::typescript::JsOutput::TypeScript })
            },
            Language::JavaScript => {
                Box::new(backend::typescript::TypeScriptCodegen { target: cfg.engine.into(), output: backend::typescript::JsOutput::JavaScript })
            },
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
