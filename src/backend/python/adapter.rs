use std::path::PathBuf;

use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::SqlType;

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

/// A driver-specific conversion applied when reading a column from the cursor.
///
/// MySQL returns `datetime.timedelta` for TIME columns instead of `datetime.time`.
/// The converter normalises driver quirks so generated models use canonical Python types.
pub(super) struct FieldReadConverter {
    /// The SQL type this converter applies to.
    pub(super) sql_type: SqlType,
    /// Name of the helper function emitted in the model file.
    pub(super) fn_name: &'static str,
    /// Full body of the helper function (including `def` line).
    pub(super) fn_body: &'static str,
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
    /// Optional wrapper for JSON/JSONB bind params, e.g. `"Jsonb({value})"`.
    /// When set, the corresponding import is added to the queries file.
    pub(super) json_param_wrapper: Option<&'static str>,
    /// Extra import line required by the JSON param wrapper.
    pub(super) json_param_import: Option<&'static str>,
}

pub(super) struct PythonCoreContract {
    pub(super) helper_source: &'static str,
    pub(super) runtime: PythonRuntimeContract,
    pub(super) sql: PythonSqlContract,
    /// Per-type read converters for driver quirks (e.g. MySQL timedelta → time).
    pub(super) field_read_converters: &'static [FieldReadConverter],
}

/// Resolve the Python adapter contract for the selected target.
pub(super) fn resolve_python_contract(target: &PythonTarget) -> PythonCoreContract {
    match target {
        PythonTarget::Psycopg => PythonCoreContract {
            helper_source: include_str!("_sqltgen_cursor.py"),
            runtime: PythonRuntimeContract {
                runtime_banner: "# Runtime: psycopg (psycopg3) — pip install psycopg",
                db_import: "import psycopg",
                conn_type_expr: "psycopg.Connection",
            },
            sql: PythonSqlContract {
                dynamic_placeholder_token: "%s",
                sql_norm_mode: PythonSqlNormMode::PercentS,
                json_mode: PythonJsonMode::Object,
                json_param_wrapper: Some("Jsonb({value})"),
                json_param_import: Some("from psycopg.types.json import Jsonb"),
            },
            field_read_converters: &[],
        },
        PythonTarget::Sqlite3 => PythonCoreContract {
            helper_source: include_str!("_sqltgen_sqlite.py"),
            runtime: PythonRuntimeContract { runtime_banner: "# Runtime: sqlite3 (stdlib)", db_import: "import sqlite3", conn_type_expr: "sqlite3.Connection" },
            sql: PythonSqlContract {
                dynamic_placeholder_token: "?",
                sql_norm_mode: PythonSqlNormMode::AnonParams,
                json_mode: PythonJsonMode::Text,
                json_param_wrapper: None,
                json_param_import: None,
            },
            field_read_converters: &[],
        },
        PythonTarget::MysqlConnector => PythonCoreContract {
            helper_source: include_str!("_sqltgen_cursor.py"),
            runtime: PythonRuntimeContract {
                runtime_banner: "# Runtime: mysql-connector-python — pip install mysql-connector-python",
                db_import: "import mysql.connector",
                conn_type_expr: "mysql.connector.MySQLConnection",
            },
            sql: PythonSqlContract {
                dynamic_placeholder_token: "%s",
                sql_norm_mode: PythonSqlNormMode::PercentS,
                json_mode: PythonJsonMode::Object,
                json_param_wrapper: Some("json.dumps({value})"),
                json_param_import: Some("import json"),
            },
            field_read_converters: &[
                FieldReadConverter {
                    sql_type: SqlType::Time,
                    fn_name: "_to_time",
                    fn_body: "def _to_time(v):\n    \"\"\"Convert MySQL timedelta to datetime.time.\"\"\"\n    if isinstance(v, datetime.timedelta):\n        return (datetime.datetime.min + v).time()\n    return v\n",
                },
                FieldReadConverter {
                    sql_type: SqlType::Json,
                    fn_name: "_load_json",
                    fn_body: "def _load_json(v):\n    \"\"\"Deserialize a JSON string returned by MySQL.\"\"\"\n    if v is None:\n        return None\n    return json.loads(v)\n",
                },
            ],
        },
    }
}

/// Emit the engine-specific helper module selected by the contract.
///
/// `needs_enum_array_parser` controls whether the `_parse_enum_array` helper
/// is appended to the file. It is only required when at least one generated
/// query module reads a column of type `enum[]`.
pub(super) fn emit_helper_file(contract: &PythonCoreContract, config: &OutputConfig, needs_enum_array_parser: bool) -> GeneratedFile {
    let mut content = String::from(contract.helper_source);
    if needs_enum_array_parser {
        content.push_str(include_str!("_sqltgen_enum_array.py"));
    }
    GeneratedFile { path: PathBuf::from(&config.out).join("sqltgen.py"), content }
}
