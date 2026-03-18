use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_summary_view, user_table};
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{NativeListBind, Parameter, Query, ResultColumn, Schema, SqlType};

pub fn pg() -> RustCodegen {
    RustCodegen { target: RustTarget::Postgres }
}
pub fn sqlite() -> RustCodegen {
    RustCodegen { target: RustTarget::Sqlite }
}
pub fn mysql() -> RustCodegen {
    RustCodegen { target: RustTarget::Mysql }
}

mod architecture;
mod generate;
mod grouping;
mod list_params;
mod params;
mod type_overrides;
mod types;
mod views;
