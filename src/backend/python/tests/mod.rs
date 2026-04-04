use super::*;
use crate::backend::test_helpers::{cfg, get_file, get_file_by_path, user_summary_view, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, NativeListBind, Parameter, Query, ResultColumn, Schema, SqlType, Table};

pub fn pg() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Psycopg }
}
pub fn sq() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Sqlite3 }
}
pub fn my() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::MysqlConnector }
}

mod architecture;
mod generate;
mod grouping;
mod list_params;
mod params;
mod views;
