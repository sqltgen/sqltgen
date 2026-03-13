use super::*;

#[test]
fn expr_integer_literal() {
    let schema = make_schema();
    let sql = "-- name: GetOne :one\nSELECT 1 AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns.len(), 1);
    assert_eq!(q.result_columns[0].name, "n");
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    assert!(!q.result_columns[0].nullable);
}

#[test]
fn expr_bigint_literal() {
    let schema = make_schema();
    let sql = "-- name: GetBig :one\nSELECT 9999999999 AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
}

#[test]
fn expr_float_literal() {
    let schema = make_schema();
    let sql = "-- name: GetPi :one\nSELECT 3.14 AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Double);
}

#[test]
fn expr_string_literal() {
    let schema = make_schema();
    let sql = "-- name: GetHello :one\nSELECT 'hello' AS s FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(!q.result_columns[0].nullable);
}

#[test]
fn expr_boolean_literal() {
    let schema = make_schema();
    let sql = "-- name: GetBool :one\nSELECT true AS b FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
}

#[test]
fn expr_null_literal() {
    let schema = make_schema();
    let sql = "-- name: GetNull :one\nSELECT NULL AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable);
}

#[test]
fn expr_arithmetic_literals() {
    let schema = make_schema();
    let sql = "-- name: Calc :one\nSELECT 1 + 2 AS sum FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn expr_arithmetic_promotes_to_wider() {
    let schema = make_schema();
    let sql = "-- name: Calc :one\nSELECT id + 1 AS result FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    // id is BigInt, 1 is Integer → BigInt (wider)
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
}

#[test]
fn expr_string_concat() {
    let schema = make_schema();
    let sql = "-- name: Concat :one\nSELECT name || ' ' || email AS full FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
}

#[test]
fn expr_comparison_returns_boolean() {
    let schema = make_schema();
    let sql = "-- name: Check :one\nSELECT id > 5 AS is_high FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
}

#[test]
fn expr_cast_to_text() {
    let schema = make_schema();
    let sql = "-- name: Str :one\nSELECT CAST(id AS TEXT) AS s FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(!q.result_columns[0].nullable, "id is not nullable, so CAST should preserve that");
}

#[test]
fn expr_cast_to_integer() {
    let schema = make_schema();
    let sql = "-- name: Num :one\nSELECT CAST(name AS INTEGER) AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn expr_case_when_then_column() {
    let schema = make_schema();
    let sql = "-- name: Label :one\nSELECT CASE WHEN id > 5 THEN name ELSE 'unknown' END AS label FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    // ELSE is present and not nullable → not nullable
    assert!(!q.result_columns[0].nullable);
}

#[test]
fn expr_case_without_else_is_nullable() {
    let schema = make_schema();
    let sql = "-- name: Label :one\nSELECT CASE WHEN id > 5 THEN name END AS label FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable, "CASE without ELSE is nullable");
}

#[test]
fn expr_coalesce_non_nullable_first() {
    let schema = make_schema();
    let sql = "-- name: CoalName :one\nSELECT COALESCE(name, 'fallback') AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    // name is not nullable → COALESCE is not nullable
    assert!(!q.result_columns[0].nullable);
}

#[test]
fn expr_coalesce_all_nullable() {
    let schema = make_schema();
    let sql = "-- name: CoalBio :one\nSELECT COALESCE(bio, NULL) AS b FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable, "all args nullable → result nullable");
}

#[test]
fn expr_upper_lower_return_text() {
    let schema = make_schema();
    let sql = "-- name: Upper :one\nSELECT UPPER(name) AS u, LOWER(email) AS l FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn expr_length_returns_integer() {
    let schema = make_schema();
    let sql = "-- name: Len :one\nSELECT LENGTH(name) AS len FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn expr_abs_preserves_type() {
    let schema = make_schema();
    let sql = "-- name: AbsId :one\nSELECT ABS(id) AS a FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
}

#[test]
fn expr_sqrt_returns_double() {
    let schema = make_schema();
    let sql = "-- name: Root :one\nSELECT SQRT(id) AS r FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Double);
}

#[test]
fn expr_now_returns_timestamp_tz() {
    let schema = make_schema();
    let sql = "-- name: GetNow :one\nSELECT NOW() AS ts FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::TimestampTz);
    assert!(!q.result_columns[0].nullable);
}

#[test]
fn expr_nullif_always_nullable() {
    let schema = make_schema();
    let sql = "-- name: NullIf :one\nSELECT NULLIF(name, 'admin') AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable, "NULLIF can always return NULL");
}

#[test]
fn expr_row_number_returns_bigint() {
    let schema = make_schema();
    let sql = "-- name: WithRowNum :many\nSELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[1].name, "rn");
    assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt);
}

#[test]
fn expr_nested_parenthesized() {
    let schema = make_schema();
    let sql = "-- name: Parens :one\nSELECT (id + 1) AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
}

#[test]
fn expr_unary_minus() {
    let schema = make_schema();
    let sql = "-- name: Neg :one\nSELECT -id AS n FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
}

#[test]
fn expr_not_returns_boolean() {
    let schema = make_schema();
    let sql = "-- name: NotCheck :one\nSELECT NOT (id > 5) AS flag FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
}

#[test]
fn expr_unnamed_literal_produces_result_column() {
    // Previously, `SELECT 1` (no alias) was silently skipped.
    // Now it resolves as Integer.
    let schema = make_schema();
    let sql = "-- name: Bare :one\nSELECT 1 FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns.len(), 1);
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
}

#[test]
fn expr_literal_does_not_override_param_type_from_column() {
    // `@p = -1 or col = @p` — param type must come from col (BigInt), not from -1 (Integer).
    let schema = make_schema();
    let sql = "-- name: Filter :many\nSELECT id FROM users WHERE $1 = -1 OR id = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "param type must come from column, not literal");
}

// ─── Param cast type inference ─────────────────────────────────────────────

#[test]
fn param_cast_infers_type() {
    let schema = make_schema();
    // $1::bigint in the WHERE clause — no column context, cast is the only signal.
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE $1::bigint > 0;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_cast_same_type_idempotent() {
    let schema = make_schema();
    // $1::text appears twice — should still produce a single Text param without conflict.
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE $1::text IS NOT NULL AND $1::text != '';";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn param_cast_conflicting_types_falls_back_to_custom() {
    let schema = make_schema();
    // $1 cast to both text and bigint — conflict → Custom("unknown").
    let sql = "-- name: ConflictQ :exec\nSELECT id FROM users WHERE $1::text IS NOT NULL AND $1::bigint > 0;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Custom("unknown".into()));
}

#[test]
fn param_cast_annotation_resolves_conflict() {
    let schema = make_schema();
    // @id is cast to both text and bigint, but the annotation declares it as bigint.
    let sql = "-- name: AnnotatedQ :exec\n-- @id bigint\nSELECT id FROM users WHERE @id::text IS NOT NULL AND @id::bigint > 0;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_cast_overrides_column_comparison_type() {
    let schema = make_schema();
    // id is BigInt, but the developer explicitly casts $1 to Text.
    // The cast is the authoritative signal — Text wins.
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id = $1::text;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn cast_on_column_side_infers_param_type() {
    let schema = make_schema();
    // CAST(id AS TEXT) = $1 — the column cast resolves to Text, so $1 gets Text.
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE CAST(id AS TEXT) = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn pg_cast_on_column_side_infers_param_type() {
    let schema = make_schema();
    // id::text = $1 — Postgres cast on the column side resolves to Text.
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id::text = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn param_column_comparison_then_conflicting_cast_warns_and_falls_back() {
    let schema = make_schema();
    // id = $1 gives BigInt; $1::text adds a conflicting Text cast → Custom("unknown").
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id = $1 AND $1::text IS NOT NULL;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Custom("unknown".into()));
}

#[test]
fn param_field_comparison_no_cast_infers_type() {
    let schema = make_schema();
    // Plain comparison — no cast anywhere. Type comes entirely from the column.
    let sql = "-- name: Q :one\nSELECT id FROM users WHERE name = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn param_field_comparison_two_columns_same_type() {
    let schema = make_schema();
    // $1 compared to two Text columns — both agree, so Text (first-wins, no conflict).
    let sql = "-- name: Q :exec\nSELECT id FROM users WHERE name = $1 OR email = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}
