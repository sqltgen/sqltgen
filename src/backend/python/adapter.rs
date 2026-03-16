use std::path::PathBuf;

use crate::backend::GeneratedFile;
use crate::config::OutputConfig;

use super::PythonTarget;

#[derive(Clone, Copy)]
pub(super) enum PythonSqlNormMode {
    PercentS,
    AnonParams,
}

#[derive(Clone, Copy)]
pub(super) enum PythonJsonMode {
    Object,
    Text,
}

/// Compile-time resolved Python backend contract.
///
/// This is the layer-1 boundary where engine/driver differences are selected.
/// Core emitters in layer 2 consume this contract and do not branch on target.
pub(super) struct PythonRuntimeContract {
    pub(super) runtime_banner: &'static str,
    pub(super) db_import: &'static str,
    pub(super) conn_type_expr: &'static str,
}

pub(super) struct PythonSqlContract {
    pub(super) dynamic_placeholder_token: &'static str,
    pub(super) sql_norm_mode: PythonSqlNormMode,
    pub(super) json_mode: PythonJsonMode,
}

pub(super) struct PythonCoreContract {
    pub(super) helper_source: &'static str,
    pub(super) runtime: PythonRuntimeContract,
    pub(super) sql: PythonSqlContract,
}

/// Resolve the Python adapter contract for the selected target.
pub(super) fn resolve_python_contract(target: &PythonTarget) -> PythonCoreContract {
    match target {
        PythonTarget::Postgres => PythonCoreContract {
            helper_source: include_str!("_sqltgen_cursor.py"),
            runtime: PythonRuntimeContract {
                runtime_banner: "# Runtime: psycopg (psycopg3) — pip install psycopg",
                db_import: "import psycopg",
                conn_type_expr: "psycopg.Connection",
            },
            sql: PythonSqlContract { dynamic_placeholder_token: "%s", sql_norm_mode: PythonSqlNormMode::PercentS, json_mode: PythonJsonMode::Object },
        },
        PythonTarget::Sqlite => PythonCoreContract {
            helper_source: include_str!("_sqltgen_sqlite.py"),
            runtime: PythonRuntimeContract { runtime_banner: "# Runtime: sqlite3 (stdlib)", db_import: "import sqlite3", conn_type_expr: "sqlite3.Connection" },
            sql: PythonSqlContract { dynamic_placeholder_token: "?", sql_norm_mode: PythonSqlNormMode::AnonParams, json_mode: PythonJsonMode::Text },
        },
        PythonTarget::Mysql => PythonCoreContract {
            helper_source: include_str!("_sqltgen_cursor.py"),
            runtime: PythonRuntimeContract {
                runtime_banner: "# Runtime: mysql-connector-python — pip install mysql-connector-python",
                db_import: "import mysql.connector",
                conn_type_expr: "mysql.connector.MySQLConnection",
            },
            sql: PythonSqlContract { dynamic_placeholder_token: "%s", sql_norm_mode: PythonSqlNormMode::PercentS, json_mode: PythonJsonMode::Text },
        },
    }
}

/// Emit the engine-specific helper module selected by the contract.
pub(super) fn emit_helper_file(contract: &PythonCoreContract, config: &OutputConfig) -> GeneratedFile {
    GeneratedFile { path: PathBuf::from(&config.out).join("_sqltgen.py"), content: contract.helper_source.to_string() }
}
