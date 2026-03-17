use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType};

pub fn pg() -> GoCodegen {
    GoCodegen { target: GoTarget::Postgres }
}
pub fn sq() -> GoCodegen {
    GoCodegen { target: GoTarget::Sqlite }
}
pub fn my() -> GoCodegen {
    GoCodegen { target: GoTarget::Mysql }
}

mod generate;
mod params;
