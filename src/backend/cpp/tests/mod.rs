use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, Schema, SqlType, Table};

pub fn pg() -> CppCodegen {
    CppCodegen { target: CppTarget::Postgres }
}

pub fn sqlite() -> CppCodegen {
    CppCodegen { target: CppTarget::Sqlite }
}

pub fn mysql() -> CppCodegen {
    CppCodegen { target: CppTarget::Mysql }
}

mod generate;
mod queries;
mod types;
