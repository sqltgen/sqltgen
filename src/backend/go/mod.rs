use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_pascal_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;
mod typemap;

/// Database engine target for Go output.
pub enum GoTarget {
    /// PostgreSQL via pgx/v5 native interface (DBTX).
    Postgres,
    /// SQLite via modernc.org/sqlite.
    Sqlite,
    /// MySQL via go-sql-driver/mysql.
    Mysql,
}

impl From<Engine> for GoTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => GoTarget::Postgres,
            Engine::Sqlite => GoTarget::Sqlite,
            Engine::Mysql => GoTarget::Mysql,
        }
    }
}

impl GoTarget {
    /// Resolve the target from an engine and optional driver string.
    ///
    /// PostgreSQL uses pgx native; SQLite and MySQL use `database/sql`.
    /// No driver variants exist yet — omit the key.
    pub fn from_engine_and_driver(engine: Engine, driver: Option<&str>) -> anyhow::Result<Self> {
        if let Some(d) = driver {
            anyhow::bail!("driver {:?} is not supported for go/{}; driver selection is not yet available for go", d, engine.as_str(),);
        }
        Ok(engine.into())
    }

    fn engine_str(&self) -> &'static str {
        match self {
            GoTarget::Postgres => "postgresql",
            GoTarget::Sqlite => "sqlite",
            GoTarget::Mysql => "mysql",
        }
    }
}

/// Code generator for Go. PostgreSQL uses pgx native; SQLite and MySQL use `database/sql`.
pub struct GoCodegen {
    /// Database driver target.
    pub target: GoTarget,
}

impl Codegen for GoCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_go_contract(&self.target);
        let type_map = typemap::build_go_type_map(config, contract.json_mode);
        let pkg = core::package_name(config);

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(&contract, &pkg, config));
        files.extend(core::generate_core_files(schema, queries, &contract, config, &type_map)?);

        if let Some(manifest) = build_manifest_file(
            "go",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_pascal_case,
            &|st, nullable| type_map.field_type(st, nullable),
            &|p| type_map.param_type(&p.sql_type, p.nullable),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn go_type(sql_type: &SqlType, nullable: bool, target: &GoTarget) -> String {
    let json_mode = match target {
        GoTarget::Postgres => adapter::GoJsonMode::Bytes,
        GoTarget::Sqlite | GoTarget::Mysql => adapter::GoJsonMode::String,
    };
    typemap::build_go_type_map(&crate::config::OutputConfig::default(), json_mode).field_type(sql_type, nullable)
}

#[cfg(test)]
mod tests;
