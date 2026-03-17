use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn, Schema, SqlType};

pub fn cfg_pkg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None, ..Default::default() }
}

pub fn pg() -> JavaCodegen {
    JavaCodegen { target: JdbcTarget::Postgres }
}

mod architecture;
mod generate;
mod grouping;
mod list_params;
mod params;
mod type_overrides;
mod types;
