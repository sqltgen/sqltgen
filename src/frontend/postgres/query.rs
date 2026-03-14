use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::sql_rewrite::replace_list_in_clause;
use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{Parameter, Query, Schema, SqlType};

pub(crate) fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(
        &PostgreSqlDialect {},
        sql,
        schema,
        &ResolverConfig {
            typemap: crate::frontend::postgres::typemap::map,
            // PG: SUM(bigint) → numeric, AVG(integer/bigint) → numeric
            sum_bigint_type: SqlType::Decimal,
            avg_integer_type: SqlType::Decimal,
            native_list_sql: Some(pg_native_list_sql),
            ..ResolverConfig::default()
        },
    )
}

/// Compute the PostgreSQL native list SQL: replace `IN ($N)` with `= ANY($N)`.
///
/// Returns the full rewritten query SQL, or `None` if the IN clause is not
/// found (the backend will fall back to dynamic expansion).
fn pg_native_list_sql(p: &Parameter, sql: &str) -> Option<String> {
    replace_list_in_clause(sql, p.index, &format!("= ANY(${})", p.index))
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

    // ─── Bug C: list-param patterns with named params ─────────────────────────

    #[test]
    fn test_bug_c_in_named_param_without_annotation_not_detected_as_list() {
        // Bug C (frontend aspect): `IN (@ids)` without a `-- @ids bigint[] not null`
        // annotation does not auto-detect the list nature of `ids`.  The parameter is
        // treated as a scalar, which generates wrong Kotlin code at runtime.
        //
        // The fix must either (a) auto-detect `is_list` from the `IN ($N)` context when
        // only a single placeholder appears in the list, or (b) infer `is_list` from the
        // `IN (@name)` named-param syntax itself.
        let sql = "-- name: GetByIds :many\nSELECT id, name FROM users WHERE id IN (@ids);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "ids");
        // Without annotation, is_list must be false today (confirming the gap).
        // The fix should make this true.
        assert!(q.params[0].is_list, "ids inside IN (@ids) must be detected as a list param");
    }

    #[test]
    fn test_bug_c_any_named_param_without_annotation_not_detected_as_list() {
        // Bug C (frontend aspect): `= ANY(@ids::bigint[])` without annotation.
        // The ::bigint[] cast on the named param signals that ids is an array, but the
        // preprocessor only sees `@ids` and ignores the trailing cast.  The result is a
        // scalar parameter with no list flag set.
        //
        // The fix must detect `is_list` from the `::type[]` cast on the named param or
        // from the surrounding ANY() context.
        let sql = "-- name: GetByIds :many\nSELECT id, name FROM users WHERE id = ANY(@ids::bigint[]);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "ids");
        // Must be recognised as a list param.
        assert!(q.params[0].is_list, "ids in = ANY(@ids::bigint[]) must be detected as a list param");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "type must be inferred as BigInt from the cast");
    }

    #[test]
    fn test_bug_c_in_named_param_with_annotation_is_list() {
        // With annotation the list flag must be set — this is the current workaround
        // and must keep working after any fix.
        let sql = "-- name: GetByIds :many\n-- @ids bigint[] not null\nSELECT id, name FROM users WHERE id IN (@ids);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "ids");
        assert!(q.params[0].is_list, "annotation must set is_list");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }
}
