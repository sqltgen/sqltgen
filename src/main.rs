mod backend;
mod config;
mod frontend;
mod ir;

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};

use backend::Codegen;
use config::{Engine, SqltgenConfig};
use frontend::{postgres::PostgresParser, sqlite::SqliteParser, DialectParser};

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

fn run_generate(config_path: &Path) -> anyhow::Result<()> {
    let cfg = SqltgenConfig::load(config_path)?;

    let base_dir = config_path.parent().unwrap_or(Path::new("."));

    // Select dialect parser
    let parser: Box<dyn DialectParser> = match cfg.engine {
        Engine::Postgresql => Box::new(PostgresParser),
        Engine::Sqlite => Box::new(SqliteParser),
    };

    // Read and parse schema
    let schema_path = base_dir.join(&cfg.schema);
    let ddl = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("reading schema file: {}", schema_path.display()))?;
    let schema = parser.parse_schema(&ddl)?;

    // Read and parse queries
    let queries_path = base_dir.join(&cfg.queries);
    let queries_sql = std::fs::read_to_string(&queries_path)
        .with_context(|| format!("reading queries file: {}", queries_path.display()))?;
    let queries = parser.parse_queries(&queries_sql, &schema)?;

    // Run each configured codegen target
    for (lang, output_config) in &cfg.gen {
        let codegen: Box<dyn Codegen> = match lang.as_str() {
            "java"       => Box::new(backend::java::JavaCodegen),
            "kotlin"     => Box::new(backend::kotlin::KotlinCodegen),
            "rust"       => Box::new(backend::rust::RustCodegen { sqlite: matches!(cfg.engine, Engine::Sqlite) }),
            "go"         => Box::new(backend::go::GoCodegen),
            "python"     => Box::new(backend::python::PythonCodegen),
            "typescript" => Box::new(backend::typescript::TypeScriptCodegen),
            other => anyhow::bail!("unknown codegen target: {other}"),
        };

        let files = codegen.generate(&schema, &queries, output_config)?;

        for file in files {
            let dest = base_dir.join(&file.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating directory: {}", parent.display()))?;
            }
            std::fs::write(&dest, &file.content)
                .with_context(|| format!("writing file: {}", dest.display()))?;
            println!("wrote {}", dest.display());
        }
    }

    Ok(())
}
