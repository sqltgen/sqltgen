use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, Schema, SqlType, Table};

pub fn pg() -> CppCodegen {
    CppCodegen { target: CppTarget::Postgres }
}

mod generate;
mod types;
