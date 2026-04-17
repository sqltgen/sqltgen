use super::*;
use crate::backend::test_helpers::{cfg, get_file, user_summary_view, user_table};
use crate::config::OutputConfig;
use crate::ir::{Schema, SqlType};

pub fn pg() -> CppCodegen {
    CppCodegen { target: CppTarget::Libpqxx }
}

pub fn sqlite() -> CppCodegen {
    CppCodegen { target: CppTarget::Sqlite3 }
}

pub fn mysql() -> CppCodegen {
    CppCodegen { target: CppTarget::Libmysql }
}

pub fn mysql_mariadb() -> CppCodegen {
    CppCodegen { target: CppTarget::Libmariadb }
}

mod architecture;
mod generate;
mod grouping;
mod mysql_bodies;
mod mysql_list_params;
mod mysql_params;
mod postgres_bodies;
mod postgres_list_params;
mod postgres_params;
mod sqlite_bodies;
mod sqlite_list_params;
mod sqlite_params;
mod types;
mod views;
