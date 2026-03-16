use super::*;

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema::default();
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"users.py"), "{names:?}");
    assert!(names.contains(&"posts.py"), "{names:?}");
    assert!(names.contains(&"__init__.py"), "{names:?}");
    assert!(!names.contains(&"queries.py"), "queries.py must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema::default();
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let users_src = get_file(&files, "users.py");
    let posts_src = get_file(&files, "posts.py");
    assert!(users_src.contains("delete_user"), "users.py must contain delete_user");
    assert!(!users_src.contains("delete_post"), "users.py must not contain delete_post");
    assert!(posts_src.contains("delete_post"), "posts.py must contain delete_post");
    assert!(!posts_src.contains("delete_user"), "posts.py must not contain delete_user");
}

#[test]
fn test_generate_grouped_init_imports_all_groups() {
    let schema = Schema::default();
    let mut users_q = Query::exec("delete_user", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("delete_post", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg().generate(&schema, &[users_q, posts_q], &cfg()).unwrap();
    let init_src = get_file(&files, "__init__.py");
    assert!(init_src.contains("from . import users"), "__init__.py must import users");
    assert!(init_src.contains("from . import posts"), "__init__.py must import posts");
}
