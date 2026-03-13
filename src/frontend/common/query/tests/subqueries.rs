use super::*;

#[test]
fn subquery_in_where_does_not_add_inner_table_to_scope() {
    // posts is only referenced inside a subquery — it must not leak into the alias map
    let sql = "-- name: GetUsersWithPosts :many\n\
        SELECT u.id, u.name FROM users u WHERE u.id IN (SELECT user_id FROM posts);";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name"]);
    assert_eq!(q.params.len(), 0);
}

#[test]
fn correlated_scalar_subquery_resolves_to_inner_column_type() {
    // (SELECT p.title FROM posts p WHERE p.user_id = u.id) AS post_title
    // The inner column `p.title` is TEXT; the result should be Option<Text>.
    // Before the fix, the generator fell back to Custom("expr") → serde_json::Value.
    let sql = "-- name: GetUserPost :one\n\
        SELECT u.name, \
               (SELECT p.title FROM posts p WHERE p.user_id = u.id) AS post_title \
        FROM users u WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let post_col = q.result_columns.iter().find(|c| c.name == "post_title").unwrap();
    assert_eq!(post_col.sql_type, SqlType::Text, "scalar subquery should resolve to inner column type");
    assert!(post_col.nullable, "scalar subquery is always nullable");
}

#[test]
fn scalar_subquery_in_select_does_not_truncate_outer_columns() {
    // The inner FROM must not cut off the outer select list
    let sql = "-- name: GetUserPostCount :many\n\
        SELECT u.name, (SELECT COUNT(*) FROM posts p WHERE p.user_id = u.id) \
        FROM users u;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    // u.name must be resolved; the scalar subquery result is unresolvable but
    // it must not prevent name from being in the result set
    assert!(q.result_columns.iter().any(|c| c.name == "name"));
}

#[test]
fn subquery_param_in_where_resolves_from_outer_table() {
    // $1 appears in the outer WHERE, bound to the outer table
    let sql = "-- name: GetUser :one\n\
        SELECT u.id, u.name FROM users u \
        WHERE u.id = $1 AND u.id IN (SELECT user_id FROM posts);";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}
