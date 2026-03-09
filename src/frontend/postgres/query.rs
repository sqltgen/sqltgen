use sqlparser::dialect::PostgreSqlDialect;

use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{Query, Schema};

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, QueryCmd, SqlType, Table};

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "users".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "name".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                    Column { name: "email".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                    Column { name: "bio".into(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
                ],
            }],
        }
    }

    #[test]
    fn parses_select_with_dollar_placeholder() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
        let queries = parse_queries(sql, &make_schema()).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "GetUser");
        assert_eq!(queries[0].cmd, QueryCmd::One);
        assert_eq!(queries[0].result_columns.len(), 2);
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].params[0].name, "id");
        assert_eq!(queries[0].params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn parses_insert_with_multiple_params() {
        let sql = "-- name: CreateUser :exec\nINSERT INTO users (name, email) VALUES ($1, $2);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::Exec);
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "email");
    }

    #[test]
    fn parses_update() {
        let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = $1 WHERE id = $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "id");
    }

    #[test]
    fn parses_delete() {
        let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::Exec);
        assert_eq!(q.params.len(), 1);
    }

    #[test]
    fn parses_select_many() {
        let sql = "-- name: ListUsers :many\nSELECT id, name, email FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::Many);
        assert_eq!(q.result_columns.len(), 3);
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn strips_trailing_semicolons() {
        let sql = "-- name: ListUsers :many\nSELECT id FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert!(!q.sql.ends_with(';'));
    }

    #[test]
    fn parses_multiple_queries() {
        let sql = "\
            -- name: GetUser :one\nSELECT id FROM users WHERE id = $1;\n\n\
            -- name: ListUsers :many\nSELECT id, name FROM users;\n\n\
            -- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
        let queries = parse_queries(sql, &make_schema()).unwrap();
        assert_eq!(queries.len(), 3);
    }
}
