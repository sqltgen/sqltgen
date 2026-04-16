use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

mod adapter;
mod core;

pub enum CppTarget {
    /// PostgreSQL via libpqxx.
    Libpqxx,
    /// SQLite via sqlite3.
    Sqlite3,
    /// MySQL via Oracle's libmysql (mysql-connector-c / libmysqlclient 8+).
    Libmysql,
    /// MySQL via MariaDB Connector/C (libmariadb).
    Libmariadb,
}

impl CppTarget {
    /// Resolve the target from an engine and optional driver string.
    ///
    /// `driver: None` selects the default for the engine. An explicit driver name
    /// must match a supported driver for that engine; anything else is a fatal error.
    pub fn from_engine_and_driver(engine: Engine, driver: Option<&str>) -> anyhow::Result<Self> {
        match (engine, driver) {
            (Engine::Postgresql, None | Some("libpqxx")) => Ok(CppTarget::Libpqxx),
            (Engine::Sqlite, None | Some("sqlite3")) => Ok(CppTarget::Sqlite3),
            (Engine::Mysql, None | Some("libmysql")) => Ok(CppTarget::Libmysql),
            (Engine::Mysql, Some("libmariadb")) => Ok(CppTarget::Libmariadb),
            (_, Some(d)) => {
                anyhow::bail!("driver {:?} is not supported for cpp/{}; supported drivers: {}", d, engine.as_str(), Self::supported_drivers(engine),)
            },
        }
    }

    fn supported_drivers(engine: Engine) -> &'static str {
        match engine {
            Engine::Postgresql => "libpqxx",
            Engine::Sqlite => "sqlite3",
            Engine::Mysql => "libmysql, libmariadb",
        }
    }
}

pub struct CppCodegen {
    pub target: CppTarget,
}

impl Codegen for CppCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let needs_json_escape = adapter::needs_json_escape(queries);
        let contract = adapter::resolve_contract(&self.target, needs_json_escape);
        let mut files = core::generate_table_files(schema, config, contract.cpp_type_fn, contract.model_db_include)?;
        files.extend(core::generate_query_files(schema, queries, &contract, config)?);
        Ok(files)
    }
}

#[cfg(test)]
mod tests;
