use super::*;

#[test]
fn join_resolves_qualified_columns() {
    let sql = "-- name: GetUserPost :one\n\
        SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "title"]);
    assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    assert_eq!(q.result_columns[2].sql_type, SqlType::Text);
}

#[test]
fn join_resolves_unqualified_columns() {
    let sql = "-- name: ListUserPosts :many\n\
        SELECT name, title FROM users JOIN posts ON posts.user_id = users.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["name", "title"]);
    assert_eq!(q.params.len(), 0);
}

#[test]
fn join_resolves_params_with_qualifier() {
    let sql = "-- name: GetPostsByUser :many\n\
        SELECT p.id, p.title FROM users u JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.params.len(), 1);
    assert_eq!(q.params[0].name, "id");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn join_select_star_returns_all_columns() {
    let sql = "-- name: GetAll :many\n\
        SELECT * FROM users u JOIN posts p ON p.user_id = u.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    // users(2) + posts(3)
    assert_eq!(q.result_columns.len(), 5);
}

#[test]
fn join_left_join_alias() {
    let sql = "-- name: GetUserWithPost :one\n\
        SELECT u.id, p.title FROM users AS u LEFT JOIN posts AS p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert_eq!(q.result_columns[1].name, "title");
    assert_eq!(q.params[0].sql_type, SqlType::BigInt);
}

#[test]
fn left_join_makes_right_side_nullable() {
    // posts columns should become nullable because posts is on the right side of a LEFT JOIN
    let sql = "-- name: GetUserWithPost :one\n\
        SELECT u.id, u.name, p.id, p.title FROM users u LEFT JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 4);
    // Left side (users) keeps original nullability
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
    // Right side (posts) becomes nullable
    assert!(q.result_columns[2].nullable, "posts.id should become nullable in LEFT JOIN");
    assert!(q.result_columns[3].nullable, "posts.title should become nullable in LEFT JOIN");
}

#[test]
fn right_join_makes_left_side_nullable() {
    // users columns should become nullable because users is on the left side of a RIGHT JOIN
    let sql = "-- name: GetPostWithUser :one\n\
        SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON p.user_id = u.id WHERE p.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    // Left side (users) becomes nullable
    assert!(q.result_columns[0].nullable, "users.name should become nullable in RIGHT JOIN");
    // Right side (posts) keeps original nullability
    assert!(!q.result_columns[1].nullable, "posts.title should remain non-nullable");
}

#[test]
fn full_outer_join_makes_both_sides_nullable() {
    let sql = "-- name: AllUsersAndPosts :many\n\
        SELECT u.name, p.title FROM users u FULL OUTER JOIN posts p ON p.user_id = u.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(q.result_columns[0].nullable, "users.name should become nullable in FULL OUTER JOIN");
    assert!(q.result_columns[1].nullable, "posts.title should become nullable in FULL OUTER JOIN");
}

#[test]
fn inner_join_preserves_nullability() {
    // INNER JOIN should not change nullability — both sides must match
    let sql = "-- name: GetUserPost :one\n\
        SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable in INNER JOIN");
    assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable in INNER JOIN");
    assert!(!q.result_columns[2].nullable, "posts.title should remain non-nullable in INNER JOIN");
}

#[test]
fn left_join_wildcard_makes_right_side_nullable() {
    // SELECT * with LEFT JOIN — right-side columns become nullable
    let sql = "-- name: AllUserPosts :many\n\
        SELECT * FROM users u LEFT JOIN posts p ON p.user_id = u.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    // users(2) + posts(3) = 5
    assert_eq!(q.result_columns.len(), 5);
    // users columns (first 2) stay non-nullable
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
    // posts columns (last 3) become nullable
    assert!(q.result_columns[2].nullable, "posts.id should become nullable");
    assert!(q.result_columns[3].nullable, "posts.user_id should become nullable");
    assert!(q.result_columns[4].nullable, "posts.title should become nullable");
}

#[test]
fn left_join_unqualified_column_from_right_becomes_nullable() {
    // Unqualified column that resolves to the outer-joined table
    let sql = "-- name: GetUserTitle :one\n\
        SELECT u.name, title FROM users u LEFT JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.name should remain non-nullable");
    assert!(q.result_columns[1].nullable, "title (from posts via LEFT JOIN) should become nullable");
}

#[test]
fn left_join_derived_subquery_becomes_nullable() {
    // LEFT OUTER JOIN on a derived table (subquery) — the subquery's
    // columns should become nullable just like a regular table would.
    let sql = "-- name: GetUserPosts :many\n\
        SELECT t1.id, t1.name, t2.id, t2.title \
        FROM users t1 LEFT OUTER JOIN (SELECT id, title, user_id FROM posts) t2 ON t1.id = t2.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 4);
    // Left side (users) keeps original nullability
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
    // Right side (derived subquery from posts) becomes nullable
    assert!(q.result_columns[2].nullable, "derived posts.id should become nullable in LEFT OUTER JOIN");
    assert!(q.result_columns[3].nullable, "derived posts.title should become nullable in LEFT OUTER JOIN");
}

#[test]
fn derived_table_join_resolves_column() {
    // b.user_id comes from the derived table — should resolve to BigInt
    let sql = "-- name: GetPosts :many\n\
        SELECT a.id, b.user_id \
        FROM users a JOIN (SELECT user_id FROM posts) b ON a.id = b.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "user_id"]);
    assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt);
}

#[test]
fn derived_table_column_alias_renames() {
    // title AS post_title in derived SELECT → outer sees b.post_title : Text
    let sql = "-- name: GetPosts :many\n\
        SELECT a.name, b.post_title \
        FROM users a JOIN (SELECT title AS post_title FROM posts) b ON a.id = b.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["name", "post_title"]);
    assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
}

#[test]
fn derived_table_star_expands() {
    // b.* should expand to the columns declared in the derived SELECT
    let sql = "-- name: GetAll :many\n\
        SELECT a.name, b.* \
        FROM users a JOIN (SELECT id, title FROM posts) b ON a.id = b.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["name", "id", "title"]);
}

#[test]
fn derived_table_count_star_resolves_to_bigint() {
    // COUNT(*) AS cnt — resolves to BigInt
    let sql = "-- name: GetCounts :many\n\
        SELECT a.name, b.cnt \
        FROM users a \
        JOIN (SELECT user_id, COUNT(*) AS cnt FROM posts GROUP BY user_id) b \
        ON a.id = b.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let cnt = q.result_columns.iter().find(|c| c.name == "cnt");
    assert!(cnt.is_some());
    assert_eq!(cnt.unwrap().sql_type, SqlType::BigInt);
}

#[test]
fn left_join_subquery_with_row_number_becomes_nullable() {
    let sql = "-- name: GetUsersRanked :many\n\
        SELECT u.id, sub.rn \
        FROM users u LEFT JOIN (SELECT user_id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM posts) sub ON u.id = sub.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(q.result_columns[1].nullable, "ROW_NUMBER from LEFT JOIN subquery should become nullable");
}

#[test]
fn left_join_subquery_with_count_becomes_nullable() {
    let sql = "-- name: GetUserPostCounts :many\n\
        SELECT u.id, sub.cnt \
        FROM users u LEFT JOIN (SELECT user_id, COUNT(*) AS cnt FROM posts GROUP BY user_id) sub ON u.id = sub.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(q.result_columns[1].nullable, "COUNT from LEFT JOIN subquery should become nullable");
}

#[test]
fn left_join_subquery_with_case_becomes_nullable() {
    let sql = "-- name: GetUserFlags :many\n\
        SELECT u.id, sub.flag \
        FROM users u LEFT JOIN (SELECT user_id, CASE WHEN title = 'x' THEN 1 ELSE 0 END AS flag FROM posts) sub ON u.id = sub.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(q.result_columns[1].nullable, "CASE from LEFT JOIN subquery should become nullable");
}

#[test]
fn left_join_subquery_with_arithmetic_becomes_nullable() {
    let sql = "-- name: GetUserCalc :many\n\
        SELECT u.id, sub.calc \
        FROM users u LEFT JOIN (SELECT user_id, id + 1 AS calc FROM posts) sub ON u.id = sub.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(q.result_columns[1].nullable, "arithmetic expr from LEFT JOIN subquery should become nullable");
}

#[test]
fn left_join_subquery_with_coalesce_becomes_nullable() {
    let sql = "-- name: GetUserCoalesce :many\n\
        SELECT u.id, sub.val \
        FROM users u LEFT JOIN (SELECT user_id, COALESCE(title, 'none') AS val FROM posts) sub ON u.id = sub.user_id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    assert_eq!(q.result_columns.len(), 2);
    assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
    assert!(q.result_columns[1].nullable, "COALESCE from LEFT JOIN subquery should become nullable");
}

#[test]
fn qualified_star_expands_single_table() {
    // SELECT a.* should expand to all columns of `users`
    let sql = "-- name: ListUsers :many\nSELECT a.* FROM users a;";
    let q = &parse_queries(sql, &make_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "email", "bio"]);
}

#[test]
fn qualified_star_expands_each_table_in_join() {
    // SELECT a.*, b.* should expand both tables independently
    let sql = "-- name: GetAll :many\n\
        SELECT a.*, b.* FROM users a INNER JOIN posts b ON b.user_id = a.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    // users has 2 cols, posts has 3 cols → 5 total, in order
    assert_eq!(q.result_columns.len(), 5);
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "id", "user_id", "title"]);
}

#[test]
fn qualified_star_mixed_with_regular_column() {
    // SELECT a.*, b.title — a.* expands, b.title resolves normally
    let sql = "-- name: GetUserPosts :many\n\
        SELECT a.*, b.title FROM users a INNER JOIN posts b ON b.user_id = a.id;";
    let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
    let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "title"]);
}
