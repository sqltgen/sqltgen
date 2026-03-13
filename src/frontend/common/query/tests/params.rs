use super::*;

fn make_json_schema() -> Schema {
    Schema {
        tables: vec![Table {
            name: "docs".into(),
            columns: vec![
                Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "data".into(), sql_type: SqlType::Jsonb, nullable: false, is_primary_key: false },
            ],
        }],
    }
}

// ─── Named param integration tests ─────────────────────────────────────────

#[test]
fn test_named_param_select_type_inferred() {
    let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = @user_id;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "user_id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[0].nullable, false);
    assert!(q.sql.contains("$1"));
    assert!(!q.sql.contains("@user_id"));
}

#[test]
fn test_named_param_repeated_becomes_one_param() {
    let sql = "-- name: Test :exec\nUPDATE users SET name = @name WHERE name = @name;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.sql.matches("$1").count(), 2);
}

#[test]
fn test_named_param_annotation_forces_nullable() {
    let sql = "-- name: Test :many\n-- @bio null\nSELECT id FROM users WHERE bio = @bio;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params[0].name, "bio");
    assert_eq!(q.params[0].nullable, true);
}

#[test]
fn test_named_param_annotation_forces_type_and_not_null() {
    let sql = "-- name: Test :many\n-- @bio text not null\nSELECT id FROM users WHERE bio = @bio;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text);
    assert_eq!(q.params[0].nullable, false);
}

#[test]
fn test_named_param_update() {
    let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = @name WHERE id = @user_id;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[0].sql_type, SqlType::Text);
    assert_eq!(q.params[1].name, "user_id");
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
}

// ─── Parameter resolution in non-WHERE clauses ─────────────────────────────

#[test]
fn param_in_join_on_clause_is_typed() {
    // $1 in JOIN ON should be typed from the column it's compared to
    let sql = "-- name: GetPostsByUser :many\n\
        SELECT p.id, p.title FROM posts p JOIN users u ON u.id = p.user_id AND u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_in_list_is_typed() {
    let sql = "-- name: GetUsers :many\n\
        SELECT id, name FROM users WHERE id IN ($1, $2, $3);";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    assert_eq!(q.params[2].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_between_is_typed() {
    let sql = "-- name: GetUsers :many\n\
        SELECT id, name FROM users WHERE id BETWEEN $1 AND $2;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_like_is_typed() {
    let sql = "-- name: SearchUsers :many\n\
        SELECT id, name FROM users WHERE name LIKE $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

#[test]
fn param_in_case_when_is_typed() {
    let sql = "-- name: GetUsers :many\n\
        SELECT id, CASE WHEN id = $1 THEN 'match' ELSE 'no' END AS label FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_case_when_then_is_collected() {
    // $1 in THEN branch — no column context, but should at least be collected
    let sql = "-- name: GetUsers :many\n\
        SELECT id, CASE WHEN id > 0 THEN $1 ELSE name END AS label FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
}

#[test]
fn param_in_coalesce_is_recursed() {
    // WHERE COALESCE(bio, $1) — the function body should be recursed into
    // so $1 is at least found (even without direct column type inference)
    let sql = "-- name: GetUsers :many\n\
        SELECT id FROM users WHERE COALESCE(bio, $1) = 'default';";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    // $1 is inside a function arg — no adjacent column context, but should still be found
}

#[test]
fn param_in_where_function_arg_is_found() {
    // WHERE id = ABS($1) — $1 is inside a function; should be recursed into
    // so the param is at least found by count_params even though typing
    // can't infer through the function boundary
    let sql = "-- name: GetUser :one\n\
        SELECT id, name FROM users WHERE id = ABS($1);";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    // Ideally this would be BigInt (from id), but the function wrapping
    // prevents direct column inference — falls back to Text
}

#[test]
fn param_in_having_clause_is_typed() {
    let schema = make_join_schema();
    let sql = "-- name: GetActiveUsers :many\n\
        SELECT u.id, u.name FROM users u JOIN posts p ON p.user_id = u.id \
        GROUP BY u.id, u.name \
        HAVING COUNT(*) > $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    // COUNT(*) > $1 — the param is compared to a count (BigInt)
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_limit_is_typed() {
    let sql = "-- name: ListUsers :many\n\
        SELECT id, name FROM users LIMIT $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    // LIMIT should produce BigInt (or Integer)
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_offset_is_typed() {
    let sql = "-- name: ListUsers :many\n\
        SELECT id, name FROM users LIMIT $1 OFFSET $2;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].sql_type, SqlType::BigInt);
}

// ─── Param deduplication ───────────────────────────────────────────────────

#[test]
fn param_dedup_between() {
    let schema = make_schema();
    let sql = "-- name: GetByIdRange :many\nSELECT * FROM users WHERE id BETWEEN $1 AND $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[1].name, "id_2");
}

#[test]
fn param_dedup_in_list() {
    let schema = make_schema();
    let sql = "-- name: GetByNames :many\nSELECT * FROM users WHERE name IN ($1, $2, $3);";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[1].name, "name_2");
    assert_eq!(q.params[2].name, "name_3");
}

#[test]
fn param_dedup_same_column_or() {
    let schema = make_schema();
    let sql = "-- name: GetByIdOr :many\nSELECT * FROM users WHERE id = $1 OR id = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[1].name, "id_2");
}

#[test]
fn param_dedup_different_columns_no_suffix() {
    let schema = make_schema();
    let sql = "-- name: GetByIdAndName :many\nSELECT * FROM users WHERE id = $1 AND name = $2;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[1].name, "name");
}

#[test]
fn repeated_param_same_index_different_columns() {
    // WHERE col_a = $1 OR col_b = $1 — same param index used with two columns.
    // The param should get one name (from first resolution) and no dedup suffix.
    let schema = make_schema();
    let sql = "-- name: SearchByIdOrName :many\nSELECT * FROM users WHERE id = $1 OR name = $1;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 1);
    // First resolution wins — id is encountered first
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

// ─── ORDER BY param inference ──────────────────────────────────────────────

#[test]
fn param_in_order_by_case_expr() {
    // ORDER BY CASE WHEN id = $1 THEN 0 ELSE 1 END — $1 should be BigInt, not Text
    let schema = make_schema();
    let sql = "-- name: ListUsersOrderByParam :many\nSELECT id, name FROM users ORDER BY CASE WHEN id = $1 THEN 0 ELSE 1 END, id;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn param_in_order_by_simple_comparison() {
    // ORDER BY (name = $1) DESC — $1 should be Text
    let schema = make_schema();
    let sql = "-- name: ListUsersNameFirst :many\nSELECT id, name FROM users ORDER BY name = $1 DESC;";
    let queries = parse_queries(sql, &schema).unwrap();
    let q = &queries[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[0].sql_type, SqlType::Text);
}

// ─── Type inference from various expression contexts ──────────────────────

#[test]
fn test_param_type_inferred_from_bitwise_op() {
    // id & $mask — $mask must be typed BigInt from the bitwise-and context.
    let schema = make_schema();
    let sql = "-- name: Mask :many\nSELECT id FROM users WHERE id & $1 > 0;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 in bitwise op must be typed from column");
}

#[test]
fn test_param_type_inferred_from_string_concat() {
    // name || $1 — $1 must be Text from the string-concat context.
    let schema = make_schema();
    let sql = "-- name: Search :many\nSELECT id FROM users WHERE name = name || $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text, "$1 in string concat must be Text");
}

#[test]
fn test_param_type_inferred_from_on_conflict_set() {
    // $2 (increment) is only referenced in the ON CONFLICT SET clause, not in VALUES.
    // build_insert used to map params by col_names position, which would either
    // give the wrong column type or fall through to Text.
    let schema = make_upsert_schema();
    let sql = "-- name: UpsertItem :one\n\
        INSERT INTO item (id) VALUES ($1)\n\
        ON CONFLICT (id) DO UPDATE SET count = excluded.count + $2\n\
        RETURNING id, count;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 (id) must be BigInt");
    assert_eq!(q.params[1].sql_type, SqlType::Integer, "$2 (increment) must be Integer from count column");
}

#[test]
fn test_param_type_inferred_from_exists_subquery() {
    // $1 is inside an EXISTS subquery WHERE clause — must still be typed from the column.
    let schema = make_join_schema();
    let sql = "-- name: GetActive :many\nSELECT id FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id AND posts.id > $1);";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "param inside EXISTS subquery must be typed from column");
}

#[test]
fn test_param_type_inferred_from_coalesce() {
    // $1 is the fallback in COALESCE(id, $1) — must be typed BigInt from the first arg's column.
    // (Text is the default for untyped params, so using a BigInt column proves inference works.)
    let schema = make_schema();
    let sql = "-- name: GetId :many\nSELECT COALESCE(id, $1) AS effective_id FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "COALESCE fallback param must be typed from the first arg");
}

#[test]
fn test_param_type_inferred_from_json_arrow() {
    // data -> $1 — key param must be Text (JSON field access by name).
    let schema = make_json_schema();
    let sql = "-- name: GetField :many\nSELECT data -> $1 FROM docs;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text, "JSON -> key param must be Text");
}

#[test]
fn test_param_type_inferred_from_json_long_arrow() {
    // data ->> $1 — key param must be Text (JSON field access as text).
    let schema = make_json_schema();
    let sql = "-- name: GetFieldText :many\nSELECT data ->> $1 FROM docs;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Text, "JSON ->> key param must be Text");
}

#[test]
fn test_param_type_inferred_from_json_path_arrow() {
    // data #> $1 — path param must be Text[] (JSON path access by key array).
    let schema = make_json_schema();
    let sql = "-- name: GetPath :many\nSELECT data #> $1 FROM docs;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Array(Box::new(SqlType::Text)), "JSON #> path param must be Text[]");
}

#[test]
fn test_param_type_inferred_from_json_contains() {
    // data @> $1 — $1 must be Jsonb (JSONB containment; symmetric types).
    let schema = make_json_schema();
    let sql = "-- name: Contains :many\nSELECT id FROM docs WHERE data @> $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params[0].sql_type, SqlType::Jsonb, "JSONB @> param must be Jsonb");
}
