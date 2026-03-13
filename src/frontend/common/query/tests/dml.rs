use super::*;

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
