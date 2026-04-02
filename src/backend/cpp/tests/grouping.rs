use super::*;
use crate::backend::Codegen;
use crate::ir::{Parameter, Query};

#[test]
fn test_grouping_produces_separate_file_pairs() {
    let schema = Schema::default();
    let mut q1 = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    q1.group = "users".to_string();
    let mut q2 = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    q2.group = "posts".to_string();
    let files = pg().generate(&schema, &[q1, q2], &cfg()).unwrap();
    let _ = get_file(&files, "users.hpp");
    let _ = get_file(&files, "users.cpp");
    let _ = get_file(&files, "posts.hpp");
    let _ = get_file(&files, "posts.cpp");
}

#[test]
fn test_grouping_routes_queries_to_correct_header() {
    let schema = Schema::default();
    let mut q1 = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    q1.group = "users".to_string();
    let mut q2 = Query::exec("DeletePost", "DELETE FROM posts WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    q2.group = "posts".to_string();
    let files = pg().generate(&schema, &[q1, q2], &cfg()).unwrap();
    let users_hpp = get_file(&files, "users.hpp");
    let posts_hpp = get_file(&files, "posts.hpp");
    assert!(users_hpp.contains("delete_user"));
    assert!(!users_hpp.contains("delete_post"));
    assert!(posts_hpp.contains("delete_post"));
    assert!(!posts_hpp.contains("delete_user"));
}

#[test]
fn test_grouping_source_includes_own_group_header() {
    let schema = Schema::default();
    let mut q = Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    q.group = "users".to_string();
    let files = pg().generate(&schema, &[q], &cfg()).unwrap();
    let src = get_file(&files, "users.cpp");
    assert!(src.contains("#include \"users.hpp\""));
}
