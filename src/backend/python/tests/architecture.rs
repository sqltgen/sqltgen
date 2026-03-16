use super::*;

#[test]
fn test_generate_queries_module_uses_only_helper_db_api() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);

    for files in [
        pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        sq().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        my().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
    ] {
        let src = get_file(&files, "queries.py");
        assert!(src.contains("from ._sqltgen import execute, exec_stmt"));
        assert!(!src.contains("with conn.cursor() as cur:"));
        assert!(!src.contains("conn.execute("));
    }
}

#[test]
fn test_generate_queries_module_has_no_engine_conditionals() {
    let schema = Schema::default();
    let query = Query::exec_rows("DeleteUsers", "DELETE FROM user WHERE active = $1", vec![Parameter::scalar(1, "active", SqlType::Boolean, false)]);

    for files in [
        pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        sq().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        my().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
    ] {
        let src = get_file(&files, "queries.py");
        assert!(!src.contains("if engine"));
        assert!(!src.contains("if target"));
        assert!(!src.contains("match target"));
        assert!(!src.contains("match engine"));
    }
}
