use super::*;

#[test]
fn test_generate_kotlin_queries_has_no_engine_conditionals() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);

    let files = pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");
    assert!(!src.contains("if engine"));
    assert!(!src.contains("if target"));
    assert!(!src.contains("match target"));
    assert!(!src.contains("match engine"));
    assert!(!src.contains("JdbcTarget"));
    assert!(!src.contains("Postgres"));
}

#[test]
fn test_generate_kotlin_querier_has_no_engine_conditionals() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)]);

    let files = pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap();
    let src = get_file(&files, "Querier.kt");
    assert!(!src.contains("JdbcTarget"));
    assert!(!src.contains("Postgres"));
}
