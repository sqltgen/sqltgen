use sqlparser::dialect::PostgreSqlDialect;

use crate::backend::sql_rewrite::replace_list_in_clause;
use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{NativeListBind, Parameter, Query, Schema, SqlType};

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
            default_schema: Some("public".into()),
            ..ResolverConfig::default()
        },
    )
}

/// Compute the PostgreSQL native list SQL: replace `IN ($N)` with `= ANY($N)`.
///
/// Returns the rewritten SQL and [`NativeListBind::Array`] (pg passes the list
/// directly to the driver), or `None` if the IN clause is not found.
fn pg_native_list_sql(p: &Parameter, sql: &str) -> Option<(String, NativeListBind)> {
    let rewritten = replace_list_in_clause(sql, p.index, &format!("= ANY(${})", p.index))?;
    Some((rewritten, NativeListBind::Array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, QueryCmd, SqlType, Table};

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table::new(
                "users",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("name", SqlType::Text),
                    Column::new_not_nullable("email", SqlType::Text),
                    Column::new("bio", SqlType::Text),
                ],
            )],
            ..Default::default()
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

    #[test]
    fn test_is_null_param_inferred_as_nullable() {
        // Pattern: `(@id::bigint IS NULL OR tbl.id = @id::bigint)` — a named param
        // checked for NULL must be inferred as nullable so backends emit an
        // Option/nullable type rather than a non-null type.
        let sql = "-- name: FilterById :many\nSELECT id, name FROM users WHERE (@id::bigint IS NULL OR id = @id::bigint);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert!(q.params[0].nullable, "param inside IS NULL must be inferred as nullable");
    }

    #[test]
    fn test_is_null_plain_placeholder_inferred_as_nullable() {
        // Same pattern with a plain $1 placeholder (no named params).
        let sql = "-- name: FilterById :many\nSELECT id, name FROM users WHERE ($1 IS NULL OR id = $1);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert!(q.params[0].nullable, "plain $1 inside IS NULL must be inferred as nullable");
    }

    #[test]
    fn test_is_null_in_case_branch_inferred_as_nullable() {
        // A parameter tested with IS NULL inside a CASE condition must still be
        // inferred as nullable.
        let sql = "-- name: FilterCase :many\n\
            SELECT id, name FROM users WHERE CASE WHEN $1::bigint IS NULL THEN true ELSE id = $1 END;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p = &q.params[0];
        assert_eq!(p.sql_type, SqlType::BigInt);
        assert!(p.nullable, "param inside IS NULL in CASE branch must be nullable");
    }

    #[test]
    fn test_is_null_in_subquery_inferred_as_nullable() {
        // A parameter tested with IS NULL inside a subquery WHERE clause must be
        // inferred as nullable.
        let sql = "-- name: FilterSub :many\n\
            SELECT id, name FROM users WHERE EXISTS (SELECT 1 FROM users u2 WHERE $1::bigint IS NULL OR u2.id = $1);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p = &q.params[0];
        assert_eq!(p.sql_type, SqlType::BigInt);
        assert!(p.nullable, "param inside IS NULL in subquery must be nullable");
    }

    #[test]
    fn test_is_null_in_join_on_inferred_as_nullable() {
        // A parameter tested with IS NULL inside a JOIN ON clause must be
        // inferred as nullable.
        let sql = "-- name: FilterJoin :many\n\
            SELECT u.id, u.name FROM users u JOIN users u2 ON ($1::bigint IS NULL OR u.id = u2.id);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p = &q.params[0];
        assert_eq!(p.sql_type, SqlType::BigInt);
        assert!(p.nullable, "param inside IS NULL in JOIN ON must be nullable");
    }

    #[test]
    fn test_is_null_in_function_arg_inferred_as_nullable() {
        // A parameter tested with IS NULL inside a function argument must be
        // inferred as nullable (e.g. COALESCE wrapping an IS NULL check).
        let sql = "-- name: FilterFunc :many\n\
            SELECT id, name FROM users WHERE id = COALESCE((CASE WHEN $1::bigint IS NULL THEN NULL ELSE $1 END), 0);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p = &q.params[0];
        assert_eq!(p.sql_type, SqlType::BigInt);
        assert!(p.nullable, "param inside IS NULL nested in function arg must be nullable");
    }

    // ─── Null-aware comparison operators ─────────────────────────────────────

    #[test]
    fn test_is_distinct_from_param_inferred_as_nullable() {
        let sql = "-- name: Q :many\nSELECT id FROM users WHERE id IS DISTINCT FROM $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert!(q.params[0].nullable, "param in IS DISTINCT FROM must be nullable");
    }

    #[test]
    fn test_is_not_distinct_from_param_inferred_as_nullable() {
        let sql = "-- name: Q :many\nSELECT id FROM users WHERE id IS NOT DISTINCT FROM $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert!(q.params[0].nullable, "param in IS NOT DISTINCT FROM must be nullable");
    }

    #[test]
    fn test_is_distinct_from_param_on_left_inferred() {
        let sql = "-- name: Q :many\nSELECT id FROM users WHERE $1 IS DISTINCT FROM id;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert!(q.params[0].nullable, "left-side param in IS DISTINCT FROM must be nullable");
    }

    #[test]
    fn test_is_distinct_from_result_type_is_boolean() {
        // The expression `id IS DISTINCT FROM $1` must resolve to non-nullable Boolean
        // in the projection so backends emit the correct return type.
        let sql = "-- name: Q :one\nSELECT (id IS DISTINCT FROM $1::bigint) AS result FROM users WHERE true;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 1);
        assert_eq!(q.result_columns[0].sql_type, crate::ir::SqlType::Boolean);
        assert!(!q.result_columns[0].nullable);
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

    // ─── Bug: INSERT … SELECT param inference ────────────────────────────────

    fn make_insert_select_schema() -> Schema {
        Schema {
            tables: vec![
                Table::new(
                    "resource",
                    vec![
                        Column::new_primary_key("id", SqlType::BigInt),
                        Column::new_not_nullable("owner_id", SqlType::BigInt),
                        Column::new_not_nullable("name", SqlType::Text),
                    ],
                ),
                Table::new("owner", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("account_id", SqlType::BigInt)]),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn test_insert_select_list_params_typed_from_target_columns() {
        // Repro 1: params in the SELECT list of INSERT…SELECT must be typed from
        // the INSERT target columns (positional). Currently they are unresolved and
        // fall back to Text/Custom.
        let sql = "-- name: CreateResource :one\n\
            INSERT INTO resource (id, owner_id, name)\n\
            SELECT @resource_id, o.id, @name\n\
            FROM owner o\n\
            WHERE o.id = @owner_id AND o.account_id = @account_id\n\
            RETURNING id, owner_id, name;";
        let q = &parse_queries(sql, &make_insert_select_schema()).unwrap()[0];
        let resource_id = q.params.iter().find(|p| p.name == "resource_id").expect("resource_id param");
        assert_eq!(resource_id.sql_type, SqlType::BigInt, "resource_id must be BigInt (from resource.id)");
        let name = q.params.iter().find(|p| p.name == "name").expect("name param");
        assert_eq!(name.sql_type, SqlType::Text, "name must be Text (from resource.name)");
        let owner_id = q.params.iter().find(|p| p.name == "owner_id").expect("owner_id param");
        assert_eq!(owner_id.sql_type, SqlType::BigInt, "owner_id must be BigInt (from owner.id)");
        let account_id = q.params.iter().find(|p| p.name == "account_id").expect("account_id param");
        assert_eq!(account_id.sql_type, SqlType::BigInt, "account_id must be BigInt (from owner.account_id)");
    }

    // ─── Bug: INSERT … VALUES with cast-wrapped placeholders ─────────────────

    #[test]
    fn test_insert_values_cast_placeholder_typed_from_column() {
        // INSERT INTO … VALUES (@id::bigint, @name) — the named-param preprocessor
        // detects @id::bigint as an inline cast and sets BigInt via annotation.
        // This is the common/supported path.
        let sql = "-- name: CreateUser :exec\n\
            INSERT INTO users (id, name) VALUES (@id::bigint, @name);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let id_p = q.params.iter().find(|p| p.name == "id").expect("id param");
        assert_eq!(id_p.sql_type, SqlType::BigInt, "id must be BigInt (from inline cast)");
        let name_p = q.params.iter().find(|p| p.name == "name").expect("name param");
        assert_eq!(name_p.sql_type, SqlType::Text, "name must be Text (from users.name column)");
    }

    #[test]
    fn test_insert_values_bare_cast_placeholder_typed_from_column() {
        // INSERT INTO … VALUES ($1::bigint, $2) — a cast-wrapped placeholder in
        // VALUES. The positional column type (users.id = BigInt) must be used,
        // not the fallback default.
        let sql = "-- name: CreateUser :exec\n\
            INSERT INTO users (id, name) VALUES ($1::bigint, $2);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1::bigint must be BigInt (from users.id)");
        assert_eq!(q.params[1].sql_type, SqlType::Text, "$2 must be Text (from users.name)");
    }

    // ─── UPDATE SET param inference ──────────────────────────────────────────

    #[test]
    fn test_update_set_param_typed_from_column() {
        // Basic case: UPDATE users SET name = @name WHERE id = @id
        // @name must be typed as Text (from users.name), @id as BigInt (from users.id).
        let sql = "-- name: UpdateUser :exec\n\
            UPDATE users SET name = @name WHERE id = @id;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let name_p = q.params.iter().find(|p| p.name == "name").expect("name param");
        assert_eq!(name_p.sql_type, SqlType::Text, "name must be Text (from users.name)");
        let id_p = q.params.iter().find(|p| p.name == "id").expect("id param");
        assert_eq!(id_p.sql_type, SqlType::BigInt, "id must be BigInt (from users.id)");
    }

    #[test]
    fn test_update_set_cast_placeholder_typed_from_column() {
        // Cast-wrapped placeholder in SET: UPDATE users SET id = $1::bigint WHERE ...
        // The bare-Placeholder pattern in collect_update_params must not miss this.
        let sql = "-- name: UpdateUserId :exec\n\
            UPDATE users SET id = $1::bigint WHERE name = $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1::bigint in SET must be BigInt (from users.id)");
        assert_eq!(q.params[1].sql_type, SqlType::Text, "$2 in WHERE must be Text (from users.name)");
    }

    // ─── LIMIT / OFFSET param type ───────────────────────────────────────────

    #[test]
    fn test_limit_offset_params_typed_as_bigint() {
        // All three supported engines treat LIMIT/OFFSET as 64-bit integers
        // (PostgreSQL: bigint-range, SQLite: 64-bit in memory, MySQL: up to 2^64−1).
        let sql = "-- name: ListUsersPaged :many\nSELECT id, name FROM users LIMIT $1 OFFSET $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        let limit_p = q.params.iter().find(|p| p.name == "limit").expect("limit param");
        assert_eq!(limit_p.sql_type, SqlType::BigInt, "LIMIT param must be BigInt");
        let offset_p = q.params.iter().find(|p| p.name == "offset").expect("offset param");
        assert_eq!(offset_p.sql_type, SqlType::BigInt, "OFFSET param must be BigInt");
    }

    // ─── Unknown function result type ────────────────────────────────────────

    #[test]
    fn test_unknown_function_aliased_result_defaults_to_text() {
        // Bug: select fetch_resource_payload(@a, @b) as payload
        // `fetch_resource_payload` is not in the known-function catalog.
        // The result column must default to Text (nullable), not Custom("expr")
        // which backends render as Object / Any?.
        let sql = "-- name: FetchPayload :one\n\
            SELECT fetch_resource_payload($1, $2) AS payload;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 1);
        let col = &q.result_columns[0];
        assert_eq!(col.name, "payload");
        assert_eq!(col.sql_type, SqlType::Text, "unknown function result must default to Text, not Custom");
        assert!(col.nullable, "unknown function result must be nullable");
    }

    #[test]
    fn test_unknown_function_without_alias_is_omitted() {
        // An unresolvable expression WITHOUT an alias is silently dropped from
        // the result set (the caller has no way to name the column). This is
        // existing behaviour — the test guards against accidental regressions.
        let sql = "-- name: FetchPayload :one\n\
            SELECT fetch_resource_payload($1, $2);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 0, "unnamed unresolvable expr must be omitted");
    }

    // ─── RETURNING clause param inference ────────────────────────────────────

    #[test]
    fn test_returning_expr_param_typed_from_column() {
        // Params inside RETURNING expressions must be typed from the sibling column,
        // not just fall through to the Text default.
        // RETURNING id + $2 → $2 must be BigInt (from users.id), not Text.
        let sql = "-- name: CreateUser :one\n\
            INSERT INTO users (name) VALUES ($1) RETURNING id + $2 AS adjusted_id;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        let p2 = q.params.iter().find(|p| p.index == 2).expect("$2");
        assert_eq!(p2.sql_type, SqlType::BigInt, "$2 in RETURNING id + $2 must be BigInt (from users.id)");
    }

    #[test]
    fn test_returning_expr_param_in_cte_insert() {
        // Data-modifying CTE: WITH ins AS (INSERT … RETURNING id + $2)
        // $2 in the RETURNING must be typed from users.id (BigInt), not fall back to Text.
        let sql = "-- name: CreateUserAndReturn :one\n\
            WITH ins AS (\
              INSERT INTO users (name) VALUES ($1) RETURNING id + $2 AS adjusted_id\
            ) SELECT adjusted_id FROM ins;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p2 = q.params.iter().find(|p| p.index == 2).expect("$2");
        assert_eq!(p2.sql_type, SqlType::BigInt, "$2 in CTE INSERT RETURNING id + $2 must be BigInt");
    }

    #[test]
    fn test_returning_expr_param_in_cte_update() {
        // Data-modifying CTE: WITH upd AS (UPDATE … RETURNING id + $2)
        // $2 in the RETURNING must be typed from users.id (BigInt), not fall back to Text.
        let sql = "-- name: UpdateUserAndReturn :one\n\
            WITH upd AS (\
              UPDATE users SET name = $1 WHERE id = $2 RETURNING id + $3 AS adjusted_id\
            ) SELECT adjusted_id FROM upd;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let p3 = q.params.iter().find(|p| p.index == 3).expect("$3");
        assert_eq!(p3.sql_type, SqlType::BigInt, "$3 in CTE UPDATE RETURNING id + $3 must be BigInt");
    }

    // ─── Repro 4: nullable predicate context (regression guard) ──────────────

    #[test]
    fn test_nullable_param_in_predicate_context() {
        // Repro 4: a param used in `(@id IS NULL OR col = @id)` must be inferred
        // nullable. This is already covered by test_is_null_param_inferred_as_nullable
        // but this variant uses @name syntax to guard against regressions.
        let sql = "-- name: FindResource :many\n\
            SELECT id, name FROM users WHERE (@id::bigint IS NULL OR id = @id::bigint);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let id_p = q.params.iter().find(|p| p.name == "id").expect("id param");
        assert_eq!(id_p.sql_type, SqlType::BigInt);
        assert!(id_p.nullable, "param tested with IS NULL must be nullable");
    }

    fn make_udf_schema() -> Schema {
        use crate::ir::ScalarFunction;
        Schema {
            tables: make_schema().tables,
            functions: vec![ScalarFunction { name: "fetch_payload".into(), return_type: SqlType::Text, param_types: vec![SqlType::BigInt] }],
        }
    }

    #[test]
    fn test_udf_return_type_inferred_from_schema() {
        let sql = "-- name: GetPayload :one\nSELECT fetch_payload($1) AS payload FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_udf_schema()).unwrap()[0];
        let payload = q.result_columns.iter().find(|c| c.name == "payload").expect("payload column");
        assert_eq!(payload.sql_type, SqlType::Text);
        assert!(payload.nullable, "UDF results are always nullable");
    }

    #[test]
    fn test_udf_params_typed_from_signature() {
        let schema = {
            use crate::ir::ScalarFunction;
            Schema {
                tables: make_schema().tables,
                functions: vec![ScalarFunction {
                    name: "fetch_payload".into(),
                    return_type: SqlType::Text,
                    param_types: vec![SqlType::BigInt, SqlType::BigInt],
                }],
            }
        };
        let sql = "-- name: GetPayload :one\nSELECT fetch_payload($1, $2) AS payload FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        let p1 = q.params.iter().find(|p| p.index == 1).expect("$1");
        let p2 = q.params.iter().find(|p| p.index == 2).expect("$2");
        assert_eq!(p1.sql_type, SqlType::BigInt);
        assert_eq!(p2.sql_type, SqlType::BigInt);
    }

    #[test]
    fn test_unknown_function_still_defaults_to_text() {
        // Regression guard: functions not in schema fall back to Text (nullable)
        let sql = "-- name: DoStuff :one\nSELECT mystery_func($1) AS result FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let result = q.result_columns.iter().find(|c| c.name == "result").expect("result column");
        assert_eq!(result.sql_type, SqlType::Text);
        assert!(result.nullable);
    }

    #[test]
    fn test_tvf_in_from_resolves_column_types() {
        // A table-valued function registered as a view should resolve columns
        // when used in a FROM clause, just like a regular table.
        let mut schema = make_schema();
        schema.tables.push(Table::view("active_users", vec![Column::new_not_nullable("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text)]));
        let sql = "-- name: GetActive :many\nSELECT id, name FROM active_users();";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].name, "name");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn test_tvf_wildcard_resolves_all_columns() {
        let mut schema = make_schema();
        schema.tables.push(Table::view("get_stats", vec![Column::new_not_nullable("total", SqlType::Integer), Column::new("avg_score", SqlType::Decimal)]));
        let sql = "-- name: Stats :one\nSELECT * FROM get_stats();";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "total");
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
        assert_eq!(q.result_columns[1].name, "avg_score");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Decimal);
    }

    #[test]
    fn test_tvf_parameterised_from_collects_params() {
        // $1 passed as a TVF argument must appear as a query parameter.
        // Because the IR records only result columns (not TVF parameter types),
        // the placeholder falls back to nullable=false Text.
        let mut schema = make_schema();
        schema
            .tables
            .push(Table::view("get_active_users", vec![Column::new_not_nullable("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text)]));
        let sql = "-- name: GetActiveUsers :many\nSELECT id, name FROM get_active_users($1);";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "param1");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn test_tvf_parameterised_from_and_where() {
        // A TVF arg ($1) and a WHERE clause param ($2) must both be collected.
        // $1 falls back to Text; $2 is inferred from the `id: BigInt` column.
        let mut schema = make_schema();
        schema
            .tables
            .push(Table::view("get_active_users", vec![Column::new_not_nullable("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text)]));
        let sql = "-- name: GetActiveUserById :one\nSELECT id, name FROM get_active_users($1) WHERE id = $2;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "param1");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
        assert_eq!(q.params[1].name, "id");
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }
}
