use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_summary_view, user_table};
use crate::config::OutputConfig;
use crate::ir::{Schema, SqlType};

pub fn pg() -> CppCodegen {
    CppCodegen { target: CppTarget::Libpqxx }
}

mod architecture;
mod generate;
mod grouping;
mod list_params;
mod params;
mod types;
mod views;
