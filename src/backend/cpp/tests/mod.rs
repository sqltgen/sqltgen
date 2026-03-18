use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_table};
use crate::config::OutputConfig;
use crate::ir::{Column, NativeListBind, Schema, SqlType, Table, TableKind};

pub fn pg() -> CppCodegen {
    CppCodegen { target: CppTarget::Postgres }
}

pub fn sqlite() -> CppCodegen {
    CppCodegen { target: CppTarget::Sqlite }
}

pub fn mysql() -> CppCodegen {
    CppCodegen { target: CppTarget::Mysql }
}

mod bodies;
mod generate;
mod queries;
mod types;
