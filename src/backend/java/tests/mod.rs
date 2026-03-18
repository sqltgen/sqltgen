use super::*;
use crate::backend::test_helpers::{cfg, cfg_pkg, get_file, user_summary_view, user_table};
use crate::config::OutputConfig;
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn, Schema, SqlType};

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
mod views;
