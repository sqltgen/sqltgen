use sqlparser::dialect::GenericDialect;

use crate::backend::common::mysql_json_table_col_type;
use crate::backend::sql_rewrite::replace_list_in_clause;
use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{NativeListBind, Parameter, Query, Schema, SqlType};

/// Parses an annotated MySQL query file into a list of [Query] models.
///
/// MySQL query files currently use `$N` positional placeholders (same format as PostgreSQL).
/// We parse with `GenericDialect` (rather than `MySqlDialect`) because MySqlDialect does not
/// recognise `$N` as a placeholder token. This means MySQL-specific syntax that `GenericDialect`
/// rejects will fall back to a bare query (no typed params / result columns).
///
/// Future work: switch to proper bare `?` and named param (`:name` / `@name`) support.
pub(crate) fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(
        &GenericDialect {},
        sql,
        schema,
        &ResolverConfig {
            sum_integer_type: SqlType::Decimal,
            // MySQL: SUM/AVG of integer columns → DECIMAL on the wire (not DOUBLE)
            sum_bigint_type: SqlType::Decimal,
            avg_integer_type: SqlType::Decimal,
            typemap: crate::frontend::mysql::typemap::map,
            native_list_sql: Some(mysql_native_list_sql),
        },
    )
}

/// Compute the MySQL native list SQL: replace `IN ($N)` with `IN (SELECT value FROM JSON_TABLE($N,...))`.
///
/// Returns the full rewritten query SQL, or `None` if the IN clause is not
/// found (the backend will fall back to dynamic expansion).
fn mysql_native_list_sql(p: &Parameter, sql: &str) -> Option<(String, NativeListBind)> {
    let col_type = mysql_json_table_col_type(&p.sql_type);
    let replacement = format!("IN (SELECT value FROM JSON_TABLE(${},'$[*]' COLUMNS(value {col_type} PATH '$')) t)", p.index);
    let rewritten = replace_list_in_clause(sql, p.index, &replacement)?;
    Some((rewritten, NativeListBind::Json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, Schema, SqlType, Table};

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "users".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "name".into(), sql_type: SqlType::VarChar(None), nullable: false, is_primary_key: false },
                    Column { name: "email".into(), sql_type: SqlType::VarChar(None), nullable: false, is_primary_key: false },
                    Column { name: "bio".into(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
                ],
            }],
        }
    }

    #[test]
    fn parses_select_one() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
        let queries = parse_queries(sql, &make_schema()).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "GetUser");
        assert_eq!(queries[0].cmd, crate::ir::QueryCmd::One);
        assert_eq!(queries[0].result_columns.len(), 2);
        assert_eq!(queries[0].params.len(), 1);
        assert_eq!(queries[0].params[0].name, "id");
        assert_eq!(queries[0].params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn parses_select_many() {
        let sql = "-- name: ListUsers :many\nSELECT id, name, email FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, crate::ir::QueryCmd::Many);
        assert_eq!(q.result_columns.len(), 3);
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn parses_insert() {
        let sql = "-- name: CreateUser :exec\nINSERT INTO users (name, email) VALUES ($1, $2);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, crate::ir::QueryCmd::Exec);
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[0].sql_type, SqlType::VarChar(None));
        assert_eq!(q.params[1].name, "email");
    }

    #[test]
    fn parses_update() {
        let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = $1, email = $2 WHERE id = $3;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "email");
        assert_eq!(q.params[2].name, "id");
    }

    #[test]
    fn parses_delete() {
        let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, crate::ir::QueryCmd::Exec);
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn strips_trailing_semicolons() {
        let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
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
        let names: Vec<_> = queries.iter().map(|q| q.name.as_str()).collect();
        assert_eq!(names, ["GetUser", "ListUsers", "DeleteUser"]);
    }
}
