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
fn expr_mixed_projection_integer_literal_and_column() {
    let schema = make_schema();
    let sql = "-- name: MixedIntAndColumn :one\nSELECT 1 AS flag, name FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "flag");
    assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn expr_mixed_projection_null_literal_and_column() {
    let schema = make_schema();
    let sql = "-- name: MixedNullAndColumn :one\nSELECT NULL AS placeholder, name FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "placeholder");
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable);
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
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
fn expr_case_nullable_branch_makes_result_nullable() {
    // bio is nullable → CASE result is nullable even though ELSE is a literal
    let schema = make_schema();
    let sql = "-- name: Label :one\nSELECT CASE WHEN id > 5 THEN bio ELSE 'none' END AS label FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(q.result_columns[0].nullable, "nullable THEN branch makes CASE nullable");
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
fn expr_coalesce_nullable_col_with_literal_fallback_is_non_nullable() {
    // bio is nullable, but 'fallback' is a non-null literal → result is non-nullable
    let schema = make_schema();
    let sql = "-- name: CoalBioFb :one\nSELECT COALESCE(bio, 'fallback') AS b FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    assert!(!q.result_columns[0].nullable, "COALESCE with non-null fallback is non-nullable");
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
    assert!(!q.result_columns[1].nullable, "ROW_NUMBER is never null");
}

#[test]
fn expr_rank_dense_rank_ntile_return_bigint_non_nullable() {
    let schema = make_schema();
    for (func, alias) in [("RANK()", "r"), ("DENSE_RANK()", "dr"), ("NTILE(4)", "nt")] {
        let sql = format!("-- name: Q :many\nSELECT id, {func} OVER (ORDER BY id) AS {alias} FROM users;");
        let q = &parse_queries(&sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt, "{func} should be BigInt");
        assert!(!q.result_columns[1].nullable, "{func} should be non-nullable");
    }
}

#[test]
fn expr_cume_dist_percent_rank_return_double_non_nullable() {
    let schema = make_schema();
    for (func, alias) in [("CUME_DIST()", "cd"), ("PERCENT_RANK()", "pr")] {
        let sql = format!("-- name: Q :many\nSELECT id, {func} OVER (ORDER BY id) AS {alias} FROM users;");
        let q = &parse_queries(&sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[1].sql_type, SqlType::Double, "{func} should be Double");
        assert!(!q.result_columns[1].nullable, "{func} should be non-nullable");
    }
}

#[test]
fn expr_lag_lead_return_column_type_nullable() {
    let schema = make_schema();
    for func in ["LAG", "LEAD"] {
        let sql = format!(
            "-- name: Q :many\nSELECT id, {func}(id) OVER (ORDER BY id) AS val FROM users;"
        );
        let q = &parse_queries(&sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt, "{func}(id) should inherit BigInt");
        assert!(q.result_columns[1].nullable, "{func} can go out of bounds → nullable");
    }
}

#[test]
fn expr_first_value_last_value_nth_value_return_column_type_nullable() {
    let schema = make_schema();
    for func in ["FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"] {
        let arg = if func == "NTH_VALUE" { "name, 2" } else { "name" };
        let sql = format!(
            "-- name: Q :many\nSELECT id, {func}({arg}) OVER (ORDER BY id) AS val FROM users;"
        );
        let q = &parse_queries(&sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text, "{func}(name) should inherit Text");
        assert!(q.result_columns[1].nullable, "{func} should be nullable");
    }
}

#[test]
fn expr_sum_over_window_returns_nullable_widened_type() {
    let schema = make_schema();
    let sql = "-- name: Q :many\nSELECT id, SUM(id) OVER (ORDER BY id) AS running FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    // SUM(BigInt) is widened per dialect config; default test config uses BigInt→Decimal
    assert!(q.result_columns[1].nullable, "SUM window should be nullable");
}

#[test]
fn expr_count_over_window_returns_non_nullable_bigint() {
    let schema = make_schema();
    let sql = "-- name: Q :many\nSELECT id, COUNT(*) OVER (PARTITION BY id) AS cnt FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt);
    assert!(!q.result_columns[1].nullable, "COUNT is never null, even as window function");
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

// ─── JSON expression type inference ────────────────────────────────────────

#[test]
fn expr_jsonb_agg_resolves_to_json() {
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT jsonb_agg(name) AS result FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
    assert!(q.result_columns[0].nullable);
}

#[test]
fn expr_json_agg_resolves_to_json() {
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT json_agg(name) AS result FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
}

#[test]
fn expr_jsonb_build_object_resolves_to_json() {
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT jsonb_build_object('id', id, 'name', name) AS obj FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
}

#[test]
fn expr_jsonb_build_array_resolves_to_json() {
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT jsonb_build_array(1, 2, 3) AS arr FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
}

#[test]
fn expr_nested_jsonb_agg_build_object_resolves_to_json() {
    // jsonb_agg(jsonb_build_object(...)) — nested JSON functions.
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT jsonb_agg(jsonb_build_object('id', id, 'name', name)) AS result FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
}

#[test]
fn expr_coalesce_jsonb_agg_build_array_resolves_to_json() {
    // coalesce(jsonb_agg(...), jsonb_build_array()) — type from first arg, nullable based on args.
    let schema = make_schema();
    let sql = "-- name: Q :one\nSELECT coalesce(jsonb_agg(name), jsonb_build_array()) AS result FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.result_columns[0].sql_type, SqlType::Json);
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
