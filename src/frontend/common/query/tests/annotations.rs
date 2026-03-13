use super::*;

#[test]
fn parses_one_annotation() {
    let sql = "-- name: GetUser :one\nSELECT id, name, email FROM users WHERE id = $1;";
    let queries = parse_queries(sql, &make_schema()).unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].name, "GetUser");
    assert_eq!(queries[0].cmd, QueryCmd::One);
}

#[test]
fn parses_many_annotation() {
    let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.cmd, QueryCmd::Many);
}

#[test]
fn parses_exec_annotation() {
    let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.cmd, QueryCmd::Exec);
}

#[test]
fn parses_execrows_annotation() {
    let sql = "-- name: UpdateName :execrows\nUPDATE users SET name = $1 WHERE id = $2;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.cmd, QueryCmd::ExecRows);
}

#[test]
fn parses_multiple_queries() {
    let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;\n\n-- name: ListUsers :many\nSELECT id, name FROM users;\n\n-- name: CreateUser :exec\nINSERT INTO users (name, email) VALUES ($1, $2);";
    let queries = parse_queries(sql, &make_schema()).unwrap();
    assert_eq!(queries.len(), 3);
    let names: Vec<_> = queries.iter().map(|q| q.name.as_str()).collect();
    assert_eq!(names, ["GetUser", "ListUsers", "CreateUser"]);
}

#[test]
fn strips_trailing_semicolons() {
    let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert!(!q.sql.ends_with(';'));
}
