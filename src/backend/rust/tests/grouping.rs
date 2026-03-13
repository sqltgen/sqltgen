use super::*;

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"users.rs"), "{names:?}");
    assert!(names.contains(&"posts.rs"), "{names:?}");
    assert!(names.contains(&"mod.rs"), "{names:?}");
    assert!(!names.contains(&"queries.rs"), "queries.rs must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "users.rs");
    let posts_src = get_file(&files, "posts.rs");
    assert!(users_src.contains("delete_user"), "users.rs must contain delete_user");
    assert!(!users_src.contains("delete_post"), "users.rs must not contain delete_post");
    assert!(posts_src.contains("delete_post"), "posts.rs must contain delete_post");
    assert!(!posts_src.contains("delete_user"), "posts.rs must not contain delete_user");
}

#[test]
fn test_generate_grouped_mod_lists_all_groups() {
    let schema = Schema { tables: vec![] };
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let mod_src = get_file(&files, "mod.rs");
    assert!(mod_src.contains("pub mod users"), "mod.rs must declare pub mod users");
    assert!(mod_src.contains("pub mod posts"), "mod.rs must declare pub mod posts");
}
