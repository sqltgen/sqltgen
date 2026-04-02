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
    /// MySQL via libmysqlclient.
    Libmysqlclient,
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
            (Engine::Mysql, None | Some("libmysqlclient")) => Ok(CppTarget::Libmysqlclient),
            (_, Some(d)) => {
                anyhow::bail!("driver {:?} is not supported for cpp/{}; supported drivers: {}", d, engine.as_str(), Self::supported_drivers(engine),)
            },
        }
    }

    fn supported_drivers(engine: Engine) -> &'static str {
        match engine {
            Engine::Postgresql => "libpqxx",
            Engine::Sqlite => "sqlite3",
            Engine::Mysql => "libmysqlclient",
        }
    }
}

pub struct CppCodegen {
    pub target: CppTarget,
}

impl Codegen for CppCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_contract(&self.target);
        let mut files = core::generate_table_files(schema, config)?;
        files.extend(core::generate_query_files(schema, queries, &contract, config)?);
        Ok(files)
    }
}

#[cfg(test)]
mod tests;
