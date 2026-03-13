use super::*;

#[test]
fn resolves_select_result_columns() {
    let sql = "-- name: GetUser :one\nSELECT id, name, email, bio FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 4);
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email", "bio"]);
}

#[test]
fn resolves_select_star() {
    let sql = "-- name: ListUsers :many\nSELECT * FROM users;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 4);
}

#[test]
fn preserves_nullability_in_select_result() {
    let sql = "-- name: GetUser :one\nSELECT id, bio FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert!(!q.result_columns.iter().find(|c| c.name == "id").unwrap().nullable);
    assert!(q.result_columns.iter().find(|c| c.name == "bio").unwrap().nullable);
}

#[test]
fn resolves_select_param_from_where() {
    let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].index, 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}
