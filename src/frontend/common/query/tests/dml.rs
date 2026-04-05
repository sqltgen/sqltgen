use super::*;
use sqlparser::dialect::GenericDialect;

#[test]
fn resolves_insert_params_from_column_list() {
    let sql = "-- name: CreateUser :exec\nINSERT INTO users (name, email, bio) VALUES ($1, $2, $3);";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[1].name, "email");
    assert_eq!(q.params[2].name, "bio");
}

#[test]
fn resolves_update_params_from_set_clause() {
    let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = $1, email = $2 WHERE id = $3;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[1].name, "email");
    assert_eq!(q.params[2].name, "id");
}

#[test]
fn resolves_delete_param_from_where() {
    let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.result_columns.len(), 0);
}

#[test]
fn comparison_operator_infers_param_type() {
    // col < $1 should produce the same type inference as col = $1
    let sql = "-- name: GetRecentUsers :many\n\
        SELECT id, name FROM users WHERE id < $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn insert_returning_star() {
    let sql = "-- name: CreateUser :one\n\
        INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[0].sql_type, SqlType::Text);
    assert_eq!(q.params[1].name, "email");
    assert_eq!(q.params[1].sql_type, SqlType::Text);
    assert_eq!(q.result_columns.len(), 4);
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email", "bio"]);
}

#[test]
fn insert_returning_columns() {
    let sql = "-- name: CreateUser :one\n\
        INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn update_returning_star() {
    let sql = "-- name: UpdateUser :one\n\
        UPDATE users SET name = $1 WHERE id = $2 RETURNING *;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "name");
    assert_eq!(q.params[1].name, "id");
    assert_eq!(q.result_columns.len(), 4);
}

#[test]
fn update_returning_columns() {
    let sql = "-- name: UpdateUser :one\n\
        UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING id, name, email;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 3);
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email"]);
}

#[test]
fn delete_returning_star() {
    let sql = "-- name: DeleteUser :one\n\
        DELETE FROM users WHERE id = $1 RETURNING *;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.result_columns.len(), 4);
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email", "bio"]);
}

#[test]
fn delete_returning_columns() {
    let sql = "-- name: DeleteUser :one\n\
        DELETE FROM users WHERE id = $1 RETURNING id, name;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[1].name, "name");
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

// ─── INSERT ... SELECT ────────────────────────────────────────────────────────

#[test]
fn insert_select_projection_params_typed_from_target_columns() {
    // $1 in the SELECT projection position 0 maps to posts.user_id (BigInt)
    // $2 in position 1 maps to posts.title (Text)
    let schema = make_join_schema();
    let sql = "-- name: ImportPosts :exec\nINSERT INTO posts (user_id, title) SELECT $1, $2 FROM users;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 2);
    assert_eq!(q.params[0].name, "user_id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.params[1].name, "title");
    assert_eq!(q.params[1].sql_type, SqlType::Text);
}

#[test]
fn insert_select_where_params_typed_from_source_table() {
    // $1 in the WHERE clause is typed from the source table (users.id = BigInt)
    let schema = make_join_schema();
    let sql = "-- name: CopyUserPosts :exec\nINSERT INTO posts (user_id, title) SELECT id, name FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn insert_select_returning_produces_result_columns() {
    // RETURNING on INSERT...SELECT (PostgreSQL) produces result columns from RETURNING list
    let schema = make_join_schema();
    let sql = "-- name: CopyUserPost :one\nINSERT INTO posts (user_id, title) SELECT id, name FROM users WHERE id = $1 RETURNING id, title;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[0].name, "id");
    assert_eq!(q.result_columns[1].name, "title");
}

// ─── ON CONFLICT / ON DUPLICATE KEY UPDATE ───────────────────────────────────

#[test]
fn on_conflict_do_nothing_collects_no_extra_params() {
    let schema = make_upsert_schema();
    let sql = "-- name: InsertItem :exec\nINSERT INTO item (id) VALUES ($1) ON CONFLICT (id) DO NOTHING;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn on_conflict_do_update_unqualified_ref_types_param() {
    // count = count + $3: unqualified `count` resolves from the target table
    let schema = make_upsert_schema();
    let sql = "-- name: UpsertItem :exec\nINSERT INTO item (id, count) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET count = count + $3;";
    let q = &parse_queries(sql, &schema).unwrap()[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 (id)");
    assert_eq!(q.params[1].sql_type, SqlType::Integer, "$2 (count)");
    assert_eq!(q.params[2].sql_type, SqlType::Integer, "$3 (count increment via unqualified ref)");
}

#[test]
fn on_duplicate_key_update_collects_params() {
    // MySQL ON DUPLICATE KEY UPDATE syntax (GenericDialect, $N placeholders)
    let schema = make_upsert_schema();
    let sql = "-- name: UpsertItem :exec\nINSERT INTO item (id, count) VALUES ($1, $2) ON DUPLICATE KEY UPDATE count = count + $3;";
    let qs = parse_queries_with_config(&GenericDialect {}, sql, &schema, &ResolverConfig::default()).unwrap();
    let q = &qs[0];
    assert_eq!(q.params.len(), 3);
    assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 (id)");
    assert_eq!(q.params[1].sql_type, SqlType::Integer, "$2 (count)");
    assert_eq!(q.params[2].sql_type, SqlType::Integer, "$3 (count increment)");
}
