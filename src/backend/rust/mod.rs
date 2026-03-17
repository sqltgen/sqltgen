use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;

/// Database engine target for Rust/sqlx output.
pub enum RustTarget {
    /// PostgreSQL via `sqlx::PgPool`.
    Postgres,
    /// SQLite via `sqlx::SqlitePool`.
    Sqlite,
    /// MySQL via `sqlx::MySqlPool`.
    Mysql,
}

impl From<Engine> for RustTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => RustTarget::Postgres,
            Engine::Sqlite => RustTarget::Sqlite,
            Engine::Mysql => RustTarget::Mysql,
        }
    }
}

/// Code generator for Rust/sqlx output.
pub struct RustCodegen {
    /// Selected sqlx engine target.
    pub target: RustTarget,
}

impl Codegen for RustCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_rust_contract(&self.target);

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(&contract, config));
        files.extend(core::generate_core_files(schema, queries, &contract, config)?);
        Ok(files)
    }
}

#[cfg(test)]
fn rust_type(sql_type: &SqlType, nullable: bool) -> String {
    core::rust_type(sql_type, nullable)
}

#[cfg(test)]
mod tests;
