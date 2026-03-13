use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, Parameter, Query, ResultColumn, Schema, SqlType, Table};

pub fn pg() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Postgres }
}
pub fn sq() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Sqlite }
}
pub fn my() -> PythonCodegen {
    PythonCodegen { target: PythonTarget::Mysql }
}

mod generate;
mod grouping;
mod list_params;
mod params;
