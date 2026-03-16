use super::*;

fn pg_ts() -> TypeScriptCodegen {
    TypeScriptCodegen { target: JsTarget::Postgres, output: JsOutput::TypeScript }
}

fn ts_cfg() -> crate::config::OutputConfig {
    crate::config::OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() }
}

// ─── generate: query grouping ────────────────────────────────────────────

#[test]
fn test_generate_grouped_produces_one_file_per_group() {
    let schema = Schema::default();
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let names: Vec<&str> = files.iter().filter_map(|f| f.path.file_name().and_then(|n| n.to_str())).collect();
    assert!(names.contains(&"users.ts"), "{names:?}");
    assert!(names.contains(&"posts.ts"), "{names:?}");
    assert!(names.contains(&"index.ts"), "{names:?}");
    assert!(!names.contains(&"queries.ts"), "queries.ts must not appear when all queries are in named groups");
}

#[test]
fn test_generate_grouped_routes_queries_to_correct_file() {
    let schema = Schema::default();
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let users_src = get_file(&files, "users.ts");
    let posts_src = get_file(&files, "posts.ts");
    assert!(users_src.contains("getUser"), "users.ts must contain getUser");
    assert!(!users_src.contains("getPost"), "users.ts must not contain getPost");
    assert!(posts_src.contains("getPost"), "posts.ts must contain getPost");
    assert!(!posts_src.contains("getUser"), "posts.ts must not contain getUser");
}

#[test]
fn test_generate_grouped_index_exports_all_groups() {
    let schema = Schema::default();
    let mut users_q = Query::exec("getUser", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    users_q.group = "users".to_string();
    let mut posts_q = Query::exec("getPost", "SELECT 1 WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    posts_q.group = "posts".to_string();
    let files = pg_ts().generate(&schema, &[users_q, posts_q], &ts_cfg()).unwrap();
    let index_src = get_file(&files, "index.ts");
    assert!(index_src.contains("from './users'"), "index.ts must re-export users");
    assert!(index_src.contains("from './posts'"), "index.ts must re-export posts");
}
