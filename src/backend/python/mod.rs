use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_snake_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;

pub enum PythonTarget {
    /// PostgreSQL via psycopg (psycopg3).
    Psycopg,
    /// SQLite via sqlite3 (stdlib).
    Sqlite3,
    /// MySQL via mysql-connector-python.
    MysqlConnector,
}

impl PythonTarget {
    /// Resolve the target from an engine and optional driver string.
    ///
    /// `driver: None` selects the default for the engine. An explicit driver name
    /// must match a supported driver for that engine; anything else is a fatal error.
    pub fn from_engine_and_driver(engine: crate::config::Engine, driver: Option<&str>) -> anyhow::Result<Self> {
        use crate::config::Engine;
        match (engine, driver) {
            (Engine::Postgresql, None | Some("psycopg")) => Ok(PythonTarget::Psycopg),
            (Engine::Sqlite, None | Some("sqlite3")) => Ok(PythonTarget::Sqlite3),
            (Engine::Mysql, None | Some("mysql-connector-python")) => Ok(PythonTarget::MysqlConnector),
            (_, Some(d)) => anyhow::bail!(
                "driver {:?} is not supported for python/{}; supported drivers: {}",
                d,
                engine.as_str(),
                Self::supported_drivers(engine),
            ),
        }
    }

    fn supported_drivers(engine: crate::config::Engine) -> &'static str {
        use crate::config::Engine;
        match engine {
            Engine::Postgresql => "psycopg",
            Engine::Sqlite => "sqlite3",
            Engine::Mysql => "mysql-connector-python",
        }
    }

    fn engine_str(&self) -> &'static str {
        match self {
            PythonTarget::Psycopg => "postgresql",
            PythonTarget::Sqlite3 => "sqlite",
            PythonTarget::MysqlConnector => "mysql",
        }
    }
}

pub struct PythonCodegen {
    pub target: PythonTarget,
}

impl Codegen for PythonCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_python_contract(&self.target);

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(&contract, config));
        files.extend(core::generate_core_files(schema, queries, &contract, config)?);

        if let Some(manifest) = build_manifest_file(
            "python",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_snake_case,
            &|st, nullable| core::python_field_type(st, nullable, &contract, config),
            &|p| core::python_param_type_resolved(&p.sql_type, p.nullable, &contract, config),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn python_type(sql_type: &SqlType, nullable: bool, target: &PythonTarget) -> String {
    core::python_type_for_target(sql_type, nullable, target)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod driver_tests {
    use super::*;
    use crate::config::Engine;

    #[test]
    fn test_from_engine_and_driver_defaults() {
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Postgresql, None).unwrap(), PythonTarget::Psycopg));
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Sqlite, None).unwrap(), PythonTarget::Sqlite3));
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Mysql, None).unwrap(), PythonTarget::MysqlConnector));
    }

    #[test]
    fn test_from_engine_and_driver_explicit_default() {
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Postgresql, Some("psycopg")).unwrap(), PythonTarget::Psycopg));
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Sqlite, Some("sqlite3")).unwrap(), PythonTarget::Sqlite3));
        assert!(matches!(PythonTarget::from_engine_and_driver(Engine::Mysql, Some("mysql-connector-python")).unwrap(), PythonTarget::MysqlConnector));
    }

    #[test]
    fn test_from_engine_and_driver_unsupported() {
        assert!(PythonTarget::from_engine_and_driver(Engine::Postgresql, Some("asyncpg")).is_err());
        assert!(PythonTarget::from_engine_and_driver(Engine::Sqlite, Some("aiosqlite")).is_err());
        assert!(PythonTarget::from_engine_and_driver(Engine::Mysql, Some("pymysql")).is_err());
    }
}
