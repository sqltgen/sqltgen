use super::*;
use crate::backend::test_helpers::{get_file, get_file_by_path, user_summary_view};
use crate::config::OutputConfig;
use crate::ir::{Column, NativeListBind, Parameter, Query, ResultColumn, Schema, SqlType, Table};

pub fn schema_with_users() -> Schema {
    Schema {
        tables: vec![Table::new(
            "users",
            vec![
                Column::new_primary_key("id", SqlType::BigInt),
                Column::new_not_nullable("name", SqlType::Text),
                Column::new("email", SqlType::VarChar(Some(255))),
            ],
        )],
        ..Default::default()
    }
}

pub fn get_user_query() -> Query {
    Query::one(
        "GetUser",
        "SELECT id, name, email FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("email", SqlType::VarChar(Some(255))),
        ],
    )
}

pub fn list_users_query() -> Query {
    Query::many(
        "ListUsers",
        "SELECT id, name, email FROM users",
        vec![],
        vec![
            ResultColumn::not_nullable("id", SqlType::BigInt),
            ResultColumn::not_nullable("name", SqlType::Text),
            ResultColumn::nullable("email", SqlType::VarChar(Some(255))),
        ],
    )
}

pub fn delete_user_query() -> Query {
    Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)])
}

pub fn delete_users_query() -> Query {
    Query::exec_rows("DeleteUsers", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)])
}

pub fn config() -> OutputConfig {
    OutputConfig { out: "src".to_string(), package: String::new(), list_params: None, ..Default::default() }
}

mod generate;
mod grouping;
mod list_params;
mod params;
mod type_overrides;
mod types;
mod views;
