use super::*;
use crate::backend::test_helpers::get_file;
use crate::config::OutputConfig;
use crate::ir::{Column, Parameter, Query, ResultColumn, Schema, SqlType, Table};

pub fn schema_with_users() -> Schema {
    Schema {
        tables: vec![Table {
            name: "users".to_string(),
            columns: vec![
                Column { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".to_string(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true, is_primary_key: false },
            ],
        }],
    }
}

pub fn get_user_query() -> Query {
    Query::one(
        "GetUser",
        "SELECT id, name, email FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true },
        ],
    )
}

pub fn list_users_query() -> Query {
    Query::many(
        "ListUsers",
        "SELECT id, name, email FROM users",
        vec![],
        vec![
            ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ResultColumn { name: "email".to_string(), sql_type: SqlType::VarChar(Some(255)), nullable: true },
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
    OutputConfig { out: "src".to_string(), package: String::new(), list_params: None }
}

mod generate;
mod grouping;
mod list_params;
mod params;
mod types;
