use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_snake_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;
mod typemap;

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

impl RustTarget {
    /// Resolve the target from an engine and optional driver string.
    ///
    /// The only supported driver is `"sqlx"` (or absent). Any other string is an error.
    pub fn from_engine_and_driver(engine: Engine, driver: Option<&str>) -> anyhow::Result<Self> {
        match driver {
            None | Some("sqlx") => Ok(engine.into()),
            Some(d) => anyhow::bail!("driver {:?} is not supported for rust/{}; supported drivers: sqlx", d, engine.as_str(),),
        }
    }

    fn engine_str(&self) -> &'static str {
        match self {
            RustTarget::Postgres => "postgresql",
            RustTarget::Sqlite => "sqlite",
            RustTarget::Mysql => "mysql",
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
        let adapter = adapter::build_adapter(&self.target);
        let type_map = typemap::build_rust_type_map(config);
        let strategy = config.list_params.clone().unwrap_or_default();
        let ctx = core::GenerationContext { schema, queries, config, adapter: adapter.as_ref(), type_map: &type_map, strategy };

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(adapter.as_ref(), config));
        files.extend(core::generate_core_files(&ctx)?);

        if let Some(manifest) = build_manifest_file(
            "rust",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_snake_case,
            &|st, nullable| type_map.field_type(st, nullable),
            &|p| type_map.param_type(&p.sql_type, p.nullable),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn rust_type(sql_type: &SqlType, nullable: bool) -> String {
    typemap::build_rust_type_map(&crate::config::OutputConfig::default()).field_type(sql_type, nullable)
}

#[cfg(test)]
mod tests;
