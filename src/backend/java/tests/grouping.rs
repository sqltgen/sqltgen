use super::*;

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema::default();
    let mut users_q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"UsersQueries.java"), "expected UsersQueries.java, got {names:?}");
    assert!(names.contains(&"UsersQuerier.java"), "expected UsersQuerier.java");
    assert!(names.contains(&"PostsQueries.java"), "expected PostsQueries.java");
    assert!(names.contains(&"PostsQuerier.java"), "expected PostsQuerier.java");
    assert!(!names.contains(&"Queries.java"), "Queries.java must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema::default();
    let mut users_q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "UsersQueries.java");
    let posts_src = get_file(&files, "PostsQueries.java");
    assert!(users_src.contains("deleteUser"), "UsersQueries.java must contain deleteUser");
    assert!(!users_src.contains("deletePost"), "UsersQueries.java must not contain deletePost");
    assert!(posts_src.contains("deletePost"), "PostsQueries.java must contain deletePost");
    assert!(!posts_src.contains("deleteUser"), "PostsQueries.java must not contain deleteUser");
}

#[test]
fn test_generate_default_group_still_named_queries() {
    // Single-file config: group is "" → output class is always Queries, not QueriesQueries.
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"Queries.java"), "{names:?}");
    assert!(!names.iter().any(|n| n.contains("QueriesQueries")), "default group must not double the Queries suffix");
}
